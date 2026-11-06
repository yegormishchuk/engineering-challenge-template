//! Connection tests against a local mock websocket server.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};
use crate::writer::{self, OutMsg, Output};

type ServerWs = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

const BOOK_A: &str = r#"{"event_type":"book","market":"m","asset_id":"A","timestamp":"1793822781000","hash":"h","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"20"}]}"#;
const CHANGE_A: &str = r#"{"event_type":"price_change","market":"m","timestamp":"1793822781001","price_changes":[{"asset_id":"A","price":"0.40","size":"15","side":"BUY","hash":"h2"}]}"#;
const CHANGE_B: &str = r#"{"event_type":"price_change","market":"m","timestamp":"1793822781002","price_changes":[{"asset_id":"B","price":"0.10","size":"1","side":"SELL","hash":"h3"}]}"#;

fn fast_cfg(url: String) -> ConnectionConfig {
    ConnectionConfig {
        url,
        ping_interval: Duration::from_millis(50),
        pong_timeout: Duration::from_secs(5),
        connect_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_millis(500),
        send_timeout: Duration::from_secs(2),
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(100),
        stable_after: Duration::from_secs(60),
        subscribe_batch: 500,
        subscribe_pace: Duration::from_millis(5),
        empty_book_after: Duration::from_secs(60),
        empty_book_on_change_after: Duration::from_secs(60),
        verify: None,
    }
}

/// A mock server: each accepted socket is handed to `session(n, ws, log)`,
/// where `n` counts sessions from 0 and `log` collects every text frame the
/// client sent (across sessions).
async fn mock_server<F, Fut>(session: F) -> (String, Arc<Mutex<Vec<String>>>)
where
    F: Fn(usize, ServerWs, Arc<Mutex<Vec<String>>>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let log = Arc::new(Mutex::new(Vec::new()));
    let server_log = Arc::clone(&log);
    tokio::spawn(async move {
        let mut n = 0;
        while let Ok((stream, _)) = listener.accept().await {
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            tokio::spawn(session(n, ws, Arc::clone(&server_log)));
            n += 1;
        }
    });
    (url, log)
}

/// Reads one client text frame, answering PINGs on the way.
async fn next_text(ws: &mut ServerWs, log: &Mutex<Vec<String>>, answer_pings: bool) -> Option<String> {
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Text(t) = msg {
            let t = t.to_string();
            if t == "PING" {
                if answer_pings {
                    ws.send(Message::text("PONG")).await.ok()?;
                }
                continue;
            }
            log.lock().unwrap().push(t.clone());
            return Some(t);
        }
    }
    None
}

/// Keeps a session open, answering PINGs and logging everything else.
async fn idle(mut ws: ServerWs, log: Arc<Mutex<Vec<String>>>) {
    while next_text(&mut ws, &log, true).await.is_some() {}
}

/// Keeps reading for `dur`, answering PINGs, then returns the socket.
async fn answer_for(ws: &mut ServerWs, log: &Mutex<Vec<String>>, dur: Duration) {
    let _ = tokio::time::timeout(dur, async {
        while next_text(ws, log, true).await.is_some() {}
    })
    .await;
}

/// Reads for `dur` without answering PINGs yet, like a server that is still
/// sending earlier data. Returns how many PINGs are owed; answer them with
/// [`pong_owed`].
async fn ignore_pings_for(ws: &mut ServerWs, log: &Mutex<Vec<String>>, dur: Duration) -> usize {
    let mut owed = 0;
    let _ = tokio::time::timeout(dur, async {
        while let Some(Ok(msg)) = ws.next().await {
            if let Message::Text(t) = msg {
                if t.as_str() == "PING" {
                    owed += 1;
                } else {
                    log.lock().unwrap().push(t.to_string());
                }
            }
        }
    })
    .await;
    owed
}

/// Reads until one PING arrives and answers it.
async fn answer_one_ping(ws: &mut ServerWs, log: &Mutex<Vec<String>>) {
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Text(t) = msg {
            if t.as_str() == "PING" {
                ws.send(Message::text("PONG")).await.unwrap();
                return;
            }
            log.lock().unwrap().push(t.to_string());
        }
    }
}

/// Answers PINGs that were held back, in order.
async fn pong_owed(ws: &mut ServerWs, owed: usize) {
    for _ in 0..owed {
        ws.send(Message::text("PONG")).await.unwrap();
    }
}

/// A change for B with the given exchange timestamp.
fn change_b_at(ms: i64) -> String {
    CHANGE_B.replace("1793822781002", &ms.to_string())
}

async fn send(ws: &mut ServerWs, text: &str) {
    ws.send(Message::text(text)).await.unwrap();
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

struct Harness {
    cmds: mpsc::UnboundedSender<Command>,
    ticks: watch::Sender<chrono::DateTime<Utc>>,
    shutdown: CancellationToken,
    stats: Arc<ConnStats>,
    task: tokio::task::JoinHandle<Result<(), WriterGone>>,
    writer: writer::WriterHandle,
    path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

fn start(cfg: ConnectionConfig, symbols: &[&str]) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.sbe");
    let (out, writer) = writer::spawn(&Output::File(path.clone()), 1024).unwrap();
    let stats = Arc::new(ConnStats::default());
    let (cmds, cmd_rx) = mpsc::unbounded_channel();
    let (ticks, tick_rx) = watch::channel(Utc::now());
    let shutdown = CancellationToken::new();
    let conn = Connection::new(0, cfg, out, Arc::clone(&stats), None);
    cmds.send(Command::Add(symbols.iter().map(|s| s.to_string()).collect()))
        .unwrap();
    let task = tokio::spawn(conn.run(cmd_rx, tick_rx, shutdown.clone()));
    Harness {
        cmds,
        ticks,
        shutdown,
        stats,
        task,
        writer,
        path,
        _dir: dir,
    }
}

impl Harness {
    fn get(&self, f: impl Fn(&ConnStats) -> &AtomicU64) -> u64 {
        f(&self.stats).load(Ordering::Relaxed)
    }

    async fn finish(self) -> Vec<OutMsg> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .expect("connection stops on shutdown")
            .unwrap()
            .unwrap();
        drop(self.cmds);
        self.writer.join().unwrap();
        read_all(&self.path)
    }
}

fn read_all(path: &Path) -> Vec<OutMsg> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut out = Vec::new();
    while let Some(msg) = reader.read_msg().unwrap() {
        out.push(match msg {
            AnyMessage::Snapshot(d) => OutMsg::Snapshot(SnapshotDomain::try_from_decoder(d).unwrap()),
            AnyMessage::PriceLevelUpdate(d) => {
                OutMsg::Update(PriceLevelUpdateDomain::try_from_decoder(d).unwrap())
            }
            _ => panic!("unknown message"),
        });
    }
    out
}

fn kinds(msgs: &[OutMsg]) -> Vec<String> {
    msgs.iter()
        .map(|m| match m {
            OutMsg::Snapshot(s) => format!("S:{}", s.symbol),
            OutMsg::Update(u) => format!("U:{}:{}", u.symbol, u.quantity),
        })
        .collect()
}

#[tokio::test]
async fn subscribes_records_book_and_updates_and_skips_changes_without_baseline() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await; // initial subscribe
        send(&mut ws, BOOK_A).await;
        send(&mut ws, CHANGE_A).await;
        send(&mut ws, CHANGE_B).await; // B never got a book
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["A", "B"]);
    eventually("update applied", || h.get(|s| &s.levels) == 1).await;
    eventually("B skipped", || h.get(|s| &s.skipped_no_baseline) == 1).await;

    // A minute tick snapshots only symbols with a book.
    h.ticks.send(Utc::now()).unwrap();
    eventually("minute snapshot", || h.get(|s| &s.minute_snapshots) == 1).await;

    let first: serde_json::Value = serde_json::from_str(&log.lock().unwrap()[0]).unwrap();
    assert_eq!(first["type"], "market");
    assert_eq!(first["assets_ids"], serde_json::json!(["A", "B"]));

    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:A", "U:A:15000000", "S:A"]);
    let OutMsg::Update(u) = &msgs[1] else { panic!() };
    assert_eq!(u.exchange_timestamp.timestamp_millis(), 1793822781001);
    assert!(u.backtest_timestamp > u.exchange_timestamp);
    let OutMsg::Snapshot(minute) = &msgs[2] else { panic!() };
    assert_eq!(minute.bids[0].quantity, 15_000_000);
    assert_eq!(minute.exchange_timestamp.timestamp_millis(), 1793822781001);
}

#[tokio::test]
async fn reconnects_and_resubscribes_after_the_server_drops() {
    let (url, log) = mock_server(|n, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await;
        if n == 0 {
            return; // drop the socket
        }
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("second session", || h.get(|s| &s.connects) == 2).await;
    eventually("second book", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.disconnects), 1);
    {
        let log = log.lock().unwrap();
        assert_eq!(log.len(), 2, "one initial subscribe per session: {log:?}");
        assert!(log.iter().all(|m| m.contains("\"type\":\"market\"")));
    }
    // After a reconnect the book is a fresh baseline, not a checkpoint.
    assert_eq!(h.get(|s| &s.mismatched_books), 0);
    assert_eq!(kinds(&h.finish().await), ["S:A", "S:A"]);
}

#[tokio::test]
async fn missing_pong_counts_as_a_dead_connection() {
    let (url, _log) = mock_server(|n, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        // First session never answers PING; later ones do.
        while next_text(&mut ws, &log, n > 0).await.is_some() {}
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.pong_timeout = Duration::from_millis(200);
    let h = start(cfg, &["A"]);
    eventually("reconnect after pong timeout", || h.get(|s| &s.connects) >= 2).await;
    assert!(h.get(|s| &s.disconnects) >= 1);
    h.finish().await;
}

#[tokio::test]
async fn checkpoint_mismatch_is_counted_and_exchange_book_wins() {
    let mismatched = BOOK_A.replace("\"10\"", "\"11\"");
    let (url, _log) = mock_server(move |_, mut ws, log| {
        let mismatched = mismatched.clone();
        async move {
            next_text(&mut ws, &log, true).await;
            send(&mut ws, BOOK_A).await;
            send(&mut ws, BOOK_A).await; // matches
            send(&mut ws, &mismatched).await; // bid 10 -> 11 without a change
            idle(ws, log).await;
        }
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("three books", || h.get(|s| &s.books) == 3).await;
    assert_eq!(h.get(|s| &s.mismatched_books), 1);
    assert_eq!(h.get(|s| &s.mismatched_levels), 1);
    let msgs = h.finish().await;
    let OutMsg::Snapshot(last) = msgs.last().unwrap() else { panic!() };
    assert_eq!(last.bids[0].quantity, 11_000_000);
}

#[tokio::test]
async fn undecodable_frame_triggers_resync_of_all_symbols() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await;
        send(&mut ws, r#"{"event_type":"price_change","timestamp":"x","price_changes":[]}"#).await;
        // After the resync request, send a fresh book.
        next_text(&mut ws, &log, true).await; // unsubscribe
        next_text(&mut ws, &log, true).await; // subscribe
        send(&mut ws, BOOK_A).await;
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("fresh book after resync", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.decode_errors), 1);
    assert_eq!(h.get(|s| &s.resyncs), 1);
    {
        let log = log.lock().unwrap();
        assert!(log[1].contains("\"operation\":\"unsubscribe\""), "{log:?}");
        assert!(log[2].contains("\"operation\":\"subscribe\""), "{log:?}");
    }
    // The second book is a new baseline (book was dropped), not a checkpoint.
    assert_eq!(h.get(|s| &s.mismatched_books), 0);
    h.finish().await;
}

#[tokio::test]
async fn add_and_remove_on_a_live_socket() {
    let (url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("initial subscribe", || log.lock().unwrap().len() == 1).await;
    h.cmds.send(Command::Add(vec!["A".into(), "B".into()])).unwrap();
    eventually("subscribe op", || log.lock().unwrap().len() == 2).await;
    h.cmds.send(Command::Remove(vec!["A".into(), "Z".into()])).unwrap();
    eventually("unsubscribe op", || log.lock().unwrap().len() == 3).await;
    {
        let log = log.lock().unwrap();
        let add: serde_json::Value = serde_json::from_str(&log[1]).unwrap();
        assert_eq!(add["assets_ids"], serde_json::json!(["B"]), "only new symbols are sent");
        let remove: serde_json::Value = serde_json::from_str(&log[2]).unwrap();
        assert_eq!(remove["operation"], "unsubscribe");
        assert_eq!(remove["assets_ids"], serde_json::json!(["A"]), "only known symbols are sent");
    }
    h.finish().await;
}

#[tokio::test]
async fn idle_connection_without_symbols_does_not_connect() {
    let (url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let h = start(fast_cfg(url), &[]);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(h.get(|s| &s.connects), 0);
    h.cmds.send(Command::Add(vec!["A".into()])).unwrap();
    eventually("connects once it has work", || h.get(|s| &s.connects) == 1).await;
    eventually("subscribed", || log.lock().unwrap().len() == 1).await;
    h.finish().await;
}

#[tokio::test]
async fn subscriptions_are_batched() {
    let (url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let mut cfg = fast_cfg(url);
    cfg.subscribe_batch = 2;
    let h = start(cfg, &["A", "B", "C", "D", "E"]);
    eventually("three batches", || log.lock().unwrap().len() == 3).await;
    {
        let log = log.lock().unwrap();
        assert!(log[0].contains("\"type\":\"market\""));
        assert!(log[1].contains("\"operation\":\"subscribe\""));
        assert!(log[2].contains("[\"E\"]"));
    }
    h.finish().await;
}

#[tokio::test]
async fn subscriptions_are_paced_not_sent_in_one_burst() {
    let (url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let mut cfg = fast_cfg(url);
    cfg.subscribe_batch = 2;
    cfg.subscribe_pace = Duration::from_millis(150);
    let started = std::time::Instant::now();
    let h = start(cfg, &["A", "B", "C", "D", "E"]);
    eventually("first batch", || log.lock().unwrap().len() == 1).await;
    assert!(started.elapsed() < Duration::from_millis(150), "first batch goes out at once");
    eventually("all batches", || log.lock().unwrap().len() == 3).await;
    assert!(
        started.elapsed() >= Duration::from_millis(300),
        "three batches need two pauses: {:?}",
        started.elapsed()
    );
    h.finish().await;
}

#[tokio::test]
async fn removing_a_pending_symbol_cancels_its_subscription() {
    let (url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let mut cfg = fast_cfg(url);
    cfg.subscribe_batch = 1;
    cfg.subscribe_pace = Duration::from_millis(200);
    let h = start(cfg, &["A", "B", "C"]);
    eventually("first batch", || log.lock().unwrap().len() == 1).await;
    h.cmds.send(Command::Remove(vec!["C".into()])).unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    {
        let log = log.lock().unwrap();
        let subscribed: Vec<String> = log
            .iter()
            .filter(|m| !m.contains("unsubscribe"))
            .flat_map(|m| {
                let v: serde_json::Value = serde_json::from_str(m).unwrap();
                v["assets_ids"].as_array().unwrap().iter().map(|s| s.as_str().unwrap().to_owned()).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(subscribed, ["A", "B"], "{log:?}");
    }
    h.finish().await;
}

#[tokio::test]
async fn token_without_a_book_gets_an_empty_baseline_and_later_changes_apply() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await; // B never gets a book: it has no orders
        answer_for(&mut ws, &log, Duration::from_millis(800)).await;
        // B's first order arrives as a change.
        send(&mut ws, CHANGE_B).await;
        idle(ws, log).await;
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.empty_book_after = Duration::from_millis(300);
    let h = start(cfg, &["A", "B"]);
    eventually("empty baseline for B", || h.get(|s| &s.assumed_empty) == 1).await;
    eventually("B's change applied", || h.get(|s| &s.levels) == 1).await;
    assert_eq!(h.get(|s| &s.skipped_no_baseline), 0);
    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:A", "S:B", "U:B:1000000"]);
    let OutMsg::Snapshot(empty) = &msgs[1] else { panic!() };
    assert!(empty.bids.is_empty() && empty.asks.is_empty());
}

#[tokio::test]
async fn a_late_book_after_an_empty_guess_is_a_checkpoint() {
    let book_b = BOOK_A.replace("\"A\"", "\"B\"");
    let (url, _log) = mock_server(move |_, mut ws, log| {
        let book_b = book_b.clone();
        async move {
            next_text(&mut ws, &log, true).await;
            answer_for(&mut ws, &log, Duration::from_millis(800)).await;
            send(&mut ws, &book_b).await; // after the empty guess
            idle(ws, log).await;
        }
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.empty_book_after = Duration::from_millis(300);
    let h = start(cfg, &["B"]);
    eventually("late book", || h.get(|s| &s.books) == 1).await;
    assert_eq!(h.get(|s| &s.assumed_empty), 1);
    assert_eq!(h.get(|s| &s.mismatched_books), 1, "the wrong guess is caught");
    let msgs = h.finish().await;
    let OutMsg::Snapshot(last) = msgs.last().unwrap() else { panic!() };
    assert_eq!(last.bids.len(), 1, "the exchange book replaces the guess");
}

#[tokio::test]
async fn an_overdue_change_creates_the_empty_baseline_at_once() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        let now = Utc::now().timestamp_millis();
        // Too early by exchange time: B's book may still be on its way.
        send(&mut ws, &change_b_at(now)).await;
        // Exchange time well after the subscription: B's book is empty.
        send(&mut ws, &change_b_at(now + 1_000)).await;
        idle(ws, log).await;
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.empty_book_on_change_after = Duration::from_millis(200);
    let h = start(cfg, &["B"]);
    eventually("second change applied", || h.get(|s| &s.levels) == 1).await;
    assert_eq!(h.get(|s| &s.skipped_no_baseline), 1);
    assert_eq!(h.get(|s| &s.assumed_empty), 1);
    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:B", "U:B:1000000"]);
    let (OutMsg::Snapshot(base), OutMsg::Update(u)) = (&msgs[0], &msgs[1]) else { panic!() };
    assert!(
        base.received_at <= u.backtest_timestamp,
        "the baseline must not come after the change that needs it"
    );
}

#[tokio::test]
async fn reconnect_resubscribes_in_insertion_order() {
    let (url, log) = mock_server(|n, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        if n == 0 {
            return;
        }
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["z-yes", "z-no", "a-yes", "a-no"]);
    eventually("second session subscribed", || log.lock().unwrap().len() == 2).await;
    {
        let log = log.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&log[1]).unwrap();
        assert_eq!(v["assets_ids"], serde_json::json!(["z-yes", "z-no", "a-yes", "a-no"]));
    }
    h.finish().await;
}

/// A mock of the batch book endpoint: answers every POST with a book that has
/// orders for each requested token listed in `with_orders`, and records the
/// request bodies.
async fn mock_books_http(with_orders: &'static [&'static str]) -> (String, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/books", listener.local_addr().unwrap());
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&bodies);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let body = loop {
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let len: usize = text[..end]
                            .lines()
                            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
                            .unwrap_or(0);
                        if buf.len() >= end + 4 + len {
                            break text[end + 4..end + 4 + len].to_string();
                        }
                    }
                };
                let requested: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
                log.lock().unwrap().push(body);
                let books: Vec<serde_json::Value> = requested
                    .iter()
                    .filter_map(|r| r["token_id"].as_str())
                    .filter(|t| with_orders.contains(t))
                    .map(|t| serde_json::json!({"asset_id": t, "bids": [{"price": "0.1", "size": "1"}], "asks": []}))
                    .collect();
                let out = serde_json::Value::Array(books).to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{out}",
                    out.len()
                );
                stream.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });
    (url, bodies)
}

/// Like `mock_books_http`, with an explicit JSON book per token. With a
/// `gate`, the books are returned only once it is set (the orders exist from
/// then on).
async fn mock_books_http_with(
    books: &'static [(&'static str, &'static str)],
    gate: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/books", listener.local_addr().unwrap());
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&bodies);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = Arc::clone(&log);
            let gate = gate.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let body = loop {
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let len: usize = text[..end]
                            .lines()
                            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
                            .unwrap_or(0);
                        if buf.len() >= end + 4 + len {
                            break text[end + 4..end + 4 + len].to_string();
                        }
                    }
                };
                let requested: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
                log.lock().unwrap().push(body);
                let open = gate.as_ref().is_none_or(|g| g.load(std::sync::atomic::Ordering::SeqCst));
                let parts: Vec<&str> = requested
                    .iter()
                    .filter(|_| open)
                    .filter_map(|r| r["token_id"].as_str())
                    .filter_map(|t| books.iter().find(|(id, _)| *id == t).map(|(_, b)| *b))
                    .collect();
                let out = format!("[{}]", parts.join(","));
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{out}",
                    out.len()
                );
                stream.write_all(resp.as_bytes()).await.unwrap();
            });
        }
    });
    (url, bodies)
}

fn verify_cfg(url: String) -> Option<VerifyConfig> {
    Some(VerifyConfig {
        http: reqwest::Client::new(),
        limiter: RateLimiter::per_second(1000.0),
        url,
        batch: 200,
        every: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        confirm_after: Duration::from_millis(150),
        give_up_after: 3,
    })
}

#[tokio::test]
async fn rest_confirms_an_assumed_empty_book() {
    let (ws_url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let (http_url, requests) = mock_books_http(&[]).await;
    let mut cfg = fast_cfg(ws_url);
    cfg.empty_book_after = Duration::from_millis(200);
    cfg.verify = verify_cfg(http_url);
    let h = start(cfg, &["B"]);
    eventually("confirmed empty", || h.get(|s| &s.empty_confirmed) == 1).await;
    assert_eq!(h.get(|s| &s.empty_wrong), 0);
    assert_eq!(requests.lock().unwrap().as_slice(), [r#"[{"token_id":"B"}]"#]);
    assert_eq!(log.lock().unwrap().len(), 1, "no resubscribe");
    h.finish().await;
}

#[tokio::test]
async fn rest_showing_orders_for_an_assumed_empty_book_triggers_a_resync() {
    let (ws_url, log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let (http_url, _requests) = mock_books_http(&["B"]).await;
    let mut cfg = fast_cfg(ws_url);
    cfg.empty_book_after = Duration::from_millis(200);
    cfg.verify = verify_cfg(http_url);
    let h = start(cfg, &["A", "B"]);
    eventually("wrong guess found", || h.get(|s| &s.empty_wrong) >= 1).await;
    eventually("unsubscribe + subscribe for B", || log.lock().unwrap().len() >= 3).await;
    {
        let log = log.lock().unwrap();
        let texts: Vec<&String> = log.iter().filter(|m| m.as_str() != "PING").collect();
        assert!(texts[1].contains("unsubscribe") && texts[1].contains("\"B\"") && !texts[1].contains("\"A\""), "{log:?}");
        assert!(texts[2].contains("\"operation\":\"subscribe\"") && texts[2].contains("\"B\""), "{log:?}");
    }
    assert_eq!(h.get(|s| &s.empty_confirmed), 1, "A was confirmed");
    h.finish().await;
}

#[tokio::test]
async fn a_bad_book_message_resyncs_only_its_symbols() {
    let bad = CHANGE_A.replace("\"BUY\"", "\"HOLD\"");
    let (url, log) = mock_server(move |_, mut ws, log| {
        let bad = bad.clone();
        async move {
            next_text(&mut ws, &log, true).await;
            send(&mut ws, BOOK_A).await;
            send(&mut ws, &BOOK_A.replace("\"A\"", "\"B\"")).await;
            send(&mut ws, &bad).await;
            idle(ws, log).await;
        }
    })
    .await;
    let h = start(fast_cfg(url), &["A", "B"]);
    eventually("resync", || h.get(|s| &s.resyncs) == 1).await;
    eventually("resubscribe", || log.lock().unwrap().len() == 3).await;
    assert_eq!(h.get(|s| &s.decode_errors), 1);
    {
        let log = log.lock().unwrap();
        let unsub: serde_json::Value = serde_json::from_str(&log[1]).unwrap();
        assert_eq!(unsub["assets_ids"], serde_json::json!(["A"]), "B is untouched");
    }
    h.finish().await;
}

#[tokio::test]
async fn malformed_discovery_message_does_not_resync() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await;
        send(&mut ws, r#"{"event_type":"market_resolved","winning_asset_id":"1"}"#).await;
        send(&mut ws, "SOME SERVER NOTICE").await;
        send(&mut ws, CHANGE_A).await;
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("change applied", || h.get(|s| &s.levels) == 1).await;
    assert_eq!(h.get(|s| &s.resyncs), 0);
    assert_eq!(h.get(|s| &s.decode_errors), 0);
    assert_eq!(h.get(|s| &s.ignored), 2);
    h.finish().await;
}

#[tokio::test]
async fn first_book_after_a_reconnect_is_compared_with_the_old_book() {
    let changed = BOOK_A.replace("\"10\"", "\"99\"");
    let (url, _log) = mock_server(move |n, mut ws, log| {
        let changed = changed.clone();
        async move {
            next_text(&mut ws, &log, true).await;
            if n == 0 {
                send(&mut ws, BOOK_A).await;
                return;
            }
            send(&mut ws, &changed).await;
            idle(ws, log).await;
        }
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("second book", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.gap_mismatched_books), 1);
    assert_eq!(h.get(|s| &s.mismatched_books), 0, "a change during the gap is not a missed message");
    h.finish().await;
}

#[tokio::test]
async fn a_lagging_reader_does_not_guess_empty_while_books_may_be_unread() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        // The server is behind: PINGs go unanswered for a while, well past
        // the grace period; then the book arrives, then the owed PONGs.
        let owed = ignore_pings_for(&mut ws, &log, Duration::from_millis(900)).await;
        send(&mut ws, &BOOK_A.replace("\"A\"", "\"B\"")).await;
        pong_owed(&mut ws, owed).await;
        idle(ws, log).await;
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.empty_book_after = Duration::from_millis(100);
    let h = start(cfg, &["B"]);
    eventually("book", || h.get(|s| &s.books) == 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.get(|s| &s.assumed_empty), 0, "no guess before the PONG barrier");
    assert_eq!(h.get(|s| &s.mismatched_books), 0);
    h.finish().await;
}

#[tokio::test]
async fn a_book_in_flight_during_a_resync_is_dropped() {
    let bad = CHANGE_A.replace("\"BUY\"", "\"HOLD\"");
    let (url, log) = mock_server(move |_, mut ws, log| {
        let bad = bad.clone();
        async move {
            next_text(&mut ws, &log, true).await; // subscribe A
            send(&mut ws, BOOK_A).await;
            send(&mut ws, &bad).await; // the client resyncs A
            next_text(&mut ws, &log, true).await; // unsubscribe A
            send(&mut ws, BOOK_A).await; // sent before the unsubscribe took effect
            next_text(&mut ws, &log, true).await; // subscribe A again
            send(&mut ws, &BOOK_A.replace("\"10\"", "\"12\"")).await;
            idle(ws, log).await;
        }
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.subscribe_pace = Duration::from_millis(300);
    let h = start(cfg, &["A"]);
    eventually("fresh book after resync", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.dropped_during_resync), 1);
    assert_eq!(h.get(|s| &s.mismatched_books), 0, "the stale book was not a baseline");
    assert!(log.lock().unwrap()[1].contains("unsubscribe"));
    let msgs = h.finish().await;
    let OutMsg::Snapshot(last) = msgs.last().unwrap() else { panic!() };
    assert_eq!(last.bids[0].quantity, 12_000_000);
}

#[tokio::test]
async fn repeated_wrong_empty_guesses_give_up() {
    let (ws_url, _log) = mock_server(|_, ws, log| idle(ws, log)).await;
    let (http_url, _requests) = mock_books_http(&["B"]).await;
    let mut cfg = fast_cfg(ws_url);
    cfg.empty_book_after = Duration::from_millis(50);
    cfg.verify = verify_cfg(http_url).map(|mut v| {
        v.give_up_after = 2;
        v
    });
    let h = start(cfg, &["B"]);
    eventually("gave up", || h.get(|s| &s.empty_gave_up) == 1).await;
    let resyncs = h.get(|s| &s.resyncs);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.get(|s| &s.resyncs), resyncs, "no more resyncs after giving up");
    assert_eq!(h.get(|s| &s.empty_wrong), 2);
    h.finish().await;
}

#[tokio::test]
async fn an_order_placed_after_a_correct_empty_guess_is_not_a_wrong_guess() {
    // B's book really is empty; then an order arrives and REST sees it too.
    let (ws_url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        answer_for(&mut ws, &log, Duration::from_millis(300)).await;
        // The order REST will report: SELL 0.10 x 1.
        send(&mut ws, &change_b_at(Utc::now().timestamp_millis())).await;
        idle(ws, log).await;
    })
    .await;
    let (http_url, _requests) = mock_books_http_with(&[("B", r#"{"asset_id":"B","bids":[],"asks":[{"price":"0.10","size":"1"}]}"#)], None).await;
    let mut cfg = fast_cfg(ws_url);
    cfg.empty_book_after = Duration::from_millis(50);
    // REST sees the order ~300 ms before the stream does; the confirmation
    // window must be longer than that lead, as it is in production (5 s).
    cfg.verify = verify_cfg(http_url).map(|mut v| {
        v.confirm_after = Duration::from_secs(1);
        v
    });
    let h = start(cfg, &["B"]);
    eventually("change applied", || h.get(|s| &s.levels) == 1).await;
    eventually("checked after the change", || h.get(|s| &s.empty_confirmed) >= 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.get(|s| &s.empty_wrong), 0);
    assert_eq!(h.get(|s| &s.resyncs), 0);
    h.finish().await;
}

#[tokio::test]
async fn a_stale_book_read_after_the_resubscribe_is_still_dropped() {
    let bad = CHANGE_A.replace("\"BUY\"", "\"HOLD\"");
    let (url, _log) = mock_server(move |_, mut ws, log| {
        let bad = bad.clone();
        async move {
            next_text(&mut ws, &log, true).await; // subscribe A
            send(&mut ws, BOOK_A).await;
            send(&mut ws, &bad).await; // the client resyncs A
            // The client unsubscribes, sends its barrier PING and soon the new
            // subscribe; this server is behind and answers nothing yet.
            let owed = ignore_pings_for(&mut ws, &log, Duration::from_millis(300)).await;
            // Still from before the unsubscribe, read after the resubscribe.
            send(&mut ws, BOOK_A).await;
            send(&mut ws, CHANGE_A).await;
            // Now every PING sent so far is answered, in order, and the
            // fresh book follows.
            pong_owed(&mut ws, owed).await;
            send(&mut ws, &BOOK_A.replace("\"10\"", "\"12\"")).await;
            idle(ws, log).await;
        }
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("fresh book", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.dropped_during_resync), 2, "stale book and change dropped");
    assert_eq!(h.get(|s| &s.mismatched_books), 0);
    assert_eq!(h.get(|s| &s.levels), 0);
    h.finish().await;
}

#[tokio::test]
async fn a_rest_result_waits_until_the_stream_catches_up() {
    // A correct empty guess, then an order is placed. REST shows the order
    // at once; to this lagging reader the stream delivers it (and every
    // PONG) only much later. Judged before the stream caught up, the check
    // would call the guess wrong once `confirm_after` passed.
    let (ws_url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await; // the subscribe
        answer_one_ping(&mut ws, &log).await; // its barrier, so the guess can happen
        // Behind from here: hold every PING for many `confirm_after` periods.
        let owed = ignore_pings_for(&mut ws, &log, Duration::from_millis(800)).await;
        send(&mut ws, &change_b_at(Utc::now().timestamp_millis())).await;
        pong_owed(&mut ws, owed).await;
        idle(ws, log).await;
    })
    .await;
    let (http_url, requests) = mock_books_http_with(
        &[("B", r#"{"asset_id":"B","bids":[],"asks":[{"price":"0.10","size":"1"}]}"#)],
        None,
    )
    .await;
    let mut cfg = fast_cfg(ws_url);
    cfg.empty_book_after = Duration::from_millis(50);
    cfg.verify = verify_cfg(http_url).map(|mut v| {
        v.confirm_after = Duration::from_millis(100);
        v
    });
    let h = start(cfg, &["B"]);
    eventually("change applied", || h.get(|s| &s.levels) == 1).await;
    eventually("judged after catching up", || h.get(|s| &s.empty_confirmed) >= 1).await;
    assert_eq!(
        requests.lock().unwrap().len(),
        1,
        "the first result waited for the stream; no re-check while it waited"
    );
    assert_eq!(h.get(|s| &s.empty_wrong), 0);
    assert_eq!(h.get(|s| &s.resyncs), 0);
    h.finish().await;
}

#[tokio::test]
async fn a_symbol_added_back_right_after_removal_ignores_old_data() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await; // subscribe A
        send(&mut ws, BOOK_A).await;
        // Unsubscribe, barrier PING and re-subscribe arrive while this
        // server is behind.
        let owed = ignore_pings_for(&mut ws, &log, Duration::from_millis(400)).await;
        send(&mut ws, BOOK_A).await; // from the old subscription
        pong_owed(&mut ws, owed).await;
        send(&mut ws, &BOOK_A.replace("\"10\"", "\"12\"")).await;
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("first book", || h.get(|s| &s.books) == 1).await;
    h.cmds.send(Command::Remove(vec!["A".into()])).unwrap();
    h.cmds.send(Command::Add(vec!["A".into()])).unwrap();
    eventually("fresh book", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.dropped_during_resync), 1);
    assert_eq!(h.get(|s| &s.mismatched_books), 0);
    assert!(log.lock().unwrap().iter().any(|m| m.contains("unsubscribe")));
    h.finish().await;
}

#[tokio::test]
async fn rate_limiter_spaces_requests() {
    let limiter = RateLimiter::per_second(20.0);
    let started = std::time::Instant::now();
    for _ in 0..5 {
        limiter.acquire().await;
    }
    // Five slots at 50 ms intervals: the last starts 200 ms after the first.
    assert!(started.elapsed() >= Duration::from_millis(190), "{:?}", started.elapsed());
}
