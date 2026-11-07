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
use crate::ws::source::ShardSource;
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};
use crate::writer::{self, OutMsg, Output};

type ServerWs = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

const BOOK_A: &str = r#"{"event_type":"book","market":"m","asset_id":"A","timestamp":"1793822781000","hash":"h","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"20"}]}"#;
const CHANGE_A: &str = r#"{"event_type":"price_change","market":"m","timestamp":"1793822781001","price_changes":[{"asset_id":"A","price":"0.40","size":"15","side":"BUY","hash":"h2"}]}"#;
const CHANGE_B: &str = r#"{"event_type":"price_change","market":"m","timestamp":"1793822781002","price_changes":[{"asset_id":"B","price":"0.10","size":"1","side":"SELL","hash":"h3"}]}"#;

fn fast_cfg(url: String) -> ConnectionConfig {
    ConnectionConfig {
        exchange: Arc::new(Polymarket::with_url(url)),
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
        shard_check_every: Duration::from_millis(20),
        // Off unless a test turns it on (see `silence_cfg`).
        silence: crate::books::SilenceConfig {
            min_active: u32::MAX,
            ..Default::default()
        },
    }
}

/// `fast_cfg` with silence detection on, in 50 ms buckets: active if 8 of
/// 10 buckets had a change; silent after 4 empty buckets (~200 ms).
fn silence_cfg(url: String) -> ConnectionConfig {
    ConnectionConfig {
        silence: crate::books::SilenceConfig {
            check_every: Duration::from_millis(20),
            bucket: Duration::from_millis(50),
            quiet: 4,
            history: 10,
            min_active: 8,
            max_per_check: 100,
        },
        ..fast_cfg(url)
    }
}

/// `BOOK_A`, then a change for A every 25 ms for 700 ms (every bucket of
/// `silence_cfg` has one), then nothing more for A.
async fn busy_then_quiet(ws: &mut ServerWs) {
    send(ws, BOOK_A).await;
    for i in 0..28 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        send(ws, &change_a(&(16 + i).to_string(), 1793822781010 + i)).await;
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
    /// `None` with a [`writer::test_channel`]: the test reads the output.
    writer: Option<writer::WriterHandle>,
    path: std::path::PathBuf,
    _dir: Option<tempfile::TempDir>,
}

fn start(cfg: ConnectionConfig, symbols: &[&str]) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.sbe");
    let (out, writer) = writer::spawn(&Output::File(path.clone()), 1024).unwrap();
    Harness {
        writer: Some(writer),
        path,
        _dir: Some(dir),
        ..start_with_out(cfg, symbols, out)
    }
}

fn start_with_out(cfg: ConnectionConfig, symbols: &[&str], out: OutSender) -> Harness {
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
        writer: None,
        path: std::path::PathBuf::new(),
        _dir: None,
    }
}

impl Harness {
    fn get(&self, f: impl Fn(&ConnStats) -> &AtomicU64) -> u64 {
        f(&self.stats).load(Ordering::Relaxed)
    }

    /// Stops the connection. Returns the output file's messages; empty with
    /// a test channel.
    async fn finish(self) -> Vec<OutMsg> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .expect("connection stops on shutdown")
            .unwrap()
            .unwrap();
        drop(self.cmds);
        match self.writer {
            Some(writer) => {
                writer.join().unwrap();
                read_all(&self.path)
            }
            None => Vec::new(),
        }
    }
}

/// Drains a test channel from now on. The task ends once the connection
/// (the only sender) is gone.
fn drain(mut rx: mpsc::Receiver<OutMsg>) -> (Arc<Mutex<Vec<OutMsg>>>, tokio::task::JoinHandle<()>) {
    let got = Arc::new(Mutex::new(Vec::new()));
    let task = tokio::spawn({
        let got = Arc::clone(&got);
        async move {
            while let Some(m) = rx.recv().await {
                got.lock().unwrap().push(m);
            }
        }
    });
    (got, task)
}

/// The receive time a message carries.
fn received_at(m: &OutMsg) -> chrono::DateTime<Utc> {
    match m {
        OutMsg::Snapshot(s) => s.received_at,
        OutMsg::Update(u) => u.backtest_timestamp,
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

/// Points `cfg`'s exchange at the mock `POST /books` at `books_url` and
/// returns fast verification settings.
fn verify_cfg(cfg: &mut ConnectionConfig, books_url: String) -> Option<VerifyConfig> {
    cfg.exchange = Arc::new(Polymarket::with_url(cfg.exchange.ws_url()).with_books_url(books_url));
    Some(VerifyConfig {
        http: reqwest::Client::new(),
        limiter: RateLimiter::per_second(1000.0),
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
    cfg.verify = verify_cfg(&mut cfg, http_url);
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
    cfg.verify = verify_cfg(&mut cfg, http_url);
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
    cfg.verify = verify_cfg(&mut cfg, http_url).map(|mut v| {
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
    cfg.verify = verify_cfg(&mut cfg, http_url).map(|mut v| {
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
    cfg.verify = verify_cfg(&mut cfg, http_url).map(|mut v| {
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

// ---- Redundant connections (Extension 1) ----

/// Two (or more) connections of one shard, one mock server each, sharing a
/// writer and a `ShardSource`.
struct ShardHarness {
    cmds: Vec<mpsc::UnboundedSender<Command>>,
    ticks: watch::Sender<chrono::DateTime<Utc>>,
    shutdown: CancellationToken,
    stats: Vec<Arc<ConnStats>>,
    shard: Arc<ShardSource>,
    tasks: Vec<tokio::task::JoinHandle<Result<(), WriterGone>>>,
    writer: writer::WriterHandle,
    path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

fn start_shard(cfgs: Vec<ConnectionConfig>, symbols: &[&str]) -> ShardHarness {
    start_shard_with(cfgs, symbols, crate::ws::source::DetectConfig::default())
}

fn start_shard_with(
    cfgs: Vec<ConnectionConfig>,
    symbols: &[&str],
    detect: crate::ws::source::DetectConfig,
) -> ShardHarness {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.sbe");
    let (out, writer) = writer::spawn(&Output::File(path.clone()), 1024).unwrap();
    let (ticks, tick_rx) = watch::channel(Utc::now());
    let shutdown = CancellationToken::new();
    let shard = Arc::new(ShardSource::new(0, cfgs.len()).with_detect(detect));
    let (mut cmds, mut stats, mut tasks) = (Vec::new(), Vec::new(), Vec::new());
    for (index, cfg) in cfgs.into_iter().enumerate() {
        let s = Arc::new(ConnStats::default());
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(Command::Add(symbols.iter().map(|s| s.to_string()).collect()))
            .unwrap();
        let conn = Connection::new(index, cfg, out.clone(), Arc::clone(&s), None).with_member(Member {
            index,
            shard: Arc::clone(&shard),
        });
        tasks.push(tokio::spawn(conn.run(rx, tick_rx.clone(), shutdown.clone())));
        cmds.push(tx);
        stats.push(s);
    }
    ShardHarness {
        cmds,
        ticks,
        shutdown,
        stats,
        shard,
        tasks,
        writer,
        path,
        _dir: dir,
    }
}

impl ShardHarness {
    fn get(&self, member: usize, f: impl Fn(&ConnStats) -> &AtomicU64) -> u64 {
        f(&self.stats[member]).load(Ordering::Relaxed)
    }

    fn switches(&self) -> u64 {
        self.shard.switches.load(Ordering::Relaxed)
    }

    async fn finish(self) -> Vec<OutMsg> {
        self.shutdown.cancel();
        for task in self.tasks {
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .expect("connection stops on shutdown")
                .unwrap()
                .unwrap();
        }
        drop(self.cmds);
        self.writer.join().unwrap();
        read_all(&self.path)
    }
}

/// Replays `msgs` like `validate` does.
fn replay(msgs: &[OutMsg]) -> crate::validate::Report {
    let mut v = crate::validate::Validator::new();
    for m in msgs {
        match m {
            OutMsg::Snapshot(s) => v.snapshot(s),
            OutMsg::Update(u) => v.update(u),
        }
    }
    v.finish()
}

/// A change for A setting the 0.40 bid to `size`, at exchange time `ms`.
fn change_a(size: &str, ms: i64) -> String {
    CHANGE_A
        .replace("\"15\"", &format!("\"{size}\""))
        .replace("1793822781001", &ms.to_string())
}

#[tokio::test]
async fn redundant_members_write_every_change_once() {
    let mut urls = Vec::new();
    for _ in 0..2 {
        let (url, _log) = mock_server(|_, mut ws, log| async move {
            next_text(&mut ws, &log, true).await;
            send(&mut ws, BOOK_A).await;
            send(&mut ws, CHANGE_A).await;
            idle(ws, log).await;
        })
        .await;
        urls.push(url);
    }
    let h = start_shard(urls.into_iter().map(fast_cfg).collect(), &["A"]);
    eventually("both applied the change", || h.get(0, |s| &s.levels) == 1 && h.get(1, |s| &s.levels) == 1)
        .await;
    h.ticks.send(Utc::now()).unwrap();
    eventually("a minute snapshot", || {
        h.get(0, |s| &s.minute_snapshots) + h.get(1, |s| &s.minute_snapshots) == 1
    })
    .await;
    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:A", "U:A:15000000", "S:A"], "no duplicates from the peer");
}

#[tokio::test]
async fn a_dropped_source_hands_over_with_a_snapshot_and_no_gap() {
    let drop_source = Arc::new(tokio::sync::Notify::new());
    let peer_start = Arc::new(tokio::sync::Notify::new());
    let peer_go = Arc::new(tokio::sync::Notify::new());
    let (url0, _) = mock_server({
        let drop_source = Arc::clone(&drop_source);
        move |n, mut ws, log| {
            let drop_source = Arc::clone(&drop_source);
            async move {
                next_text(&mut ws, &log, true).await;
                if n > 0 {
                    // After the drop: subscribed again, but nothing comes.
                    return idle(ws, log).await;
                }
                send(&mut ws, BOOK_A).await;
                send(&mut ws, CHANGE_A).await;
                let _ = tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = drop_source.notified() => {}
                        _ = answer_for(&mut ws, &log, Duration::from_secs(5)) => {}
                    }
                })
                .await;
                // Dropping the socket ends the session abruptly.
            }
        }
    })
    .await;
    let (url1, _) = mock_server({
        let (peer_start, peer_go) = (Arc::clone(&peer_start), Arc::clone(&peer_go));
        move |_, mut ws, log| {
            let (peer_start, peer_go) = (Arc::clone(&peer_start), Arc::clone(&peer_go));
            async move {
                next_text(&mut ws, &log, true).await;
                // Only once member 0 is up, so it stays the source.
                tokio::select! {
                    _ = peer_start.notified() => {}
                    _ = answer_for(&mut ws, &log, Duration::from_secs(5)) => {}
                }
                send(&mut ws, BOOK_A).await;
                send(&mut ws, CHANGE_A).await;
                tokio::select! {
                    _ = peer_go.notified() => {}
                    _ = answer_for(&mut ws, &log, Duration::from_secs(5)) => {}
                }
                send(&mut ws, &change_a("20", 1793822782000)).await;
                idle(ws, log).await;
            }
        }
    })
    .await;
    let h = start_shard(vec![fast_cfg(url0), fast_cfg(url1)], &["A"]);
    eventually("member 0 connected", || h.get(0, |s| &s.connects) == 1).await;
    peer_start.notify_one();
    eventually("both have the change", || h.get(0, |s| &s.levels) == 1 && h.get(1, |s| &s.levels) == 1)
        .await;
    assert_eq!((h.shard.lock().await.source, h.switches()), (0, 0));
    drop_source.notify_one();
    eventually("peer took over", || h.switches() == 1).await;
    assert_eq!(h.shard.lock().await.source, 1);
    peer_go.notify_one();
    eventually("peer's change", || h.get(1, |s| &s.levels) == 2).await;
    eventually("written", || h.get(1, |s| &s.switch_snapshots) >= 1).await;
    let msgs = h.finish().await;

    // The peer's snapshot precedes its first update, and every change is
    // written once.
    let k = kinds(&msgs);
    let peer_update = k.iter().position(|m| m == "U:A:20000000").expect("peer's update written");
    assert_eq!(k[peer_update - 1], "S:A", "switch snapshot before the peer's update: {k:?}");
    assert_eq!(k.iter().filter(|m| *m == "U:A:15000000").count(), 1, "{k:?}");
    let r = replay(&msgs);
    assert!(r.file_is_consistent(), "{r:?}");
    assert_eq!((r.updates_without_baseline, r.mismatched), (0, 0), "{r:?}");
}

/// A change for A setting the 0.40 bid to `size`, with its own hash.
fn change_a_hashed(size: &str, ms: i64, hash: &str) -> String {
    change_a(size, ms).replace("\"hash\":\"h2\"", &format!("\"hash\":\"{hash}\""))
}

/// The bid quantity at 0.40 in the last snapshot of A.
fn last_a_bid(msgs: &[OutMsg]) -> i64 {
    msgs.iter()
        .rev()
        .find_map(|m| match m {
            OutMsg::Snapshot(s) if s.symbol == "A" => Some(s.bids[0].quantity),
            _ => None,
        })
        .unwrap()
}

/// Two members whose servers send BOOK_A and then `script` (server index,
/// websocket); member 1's server waits until member 0 is connected, so
/// member 0 is the source.
async fn two_member_shard<F, Fut>(detect: crate::ws::source::DetectConfig, script: F) -> (ShardHarness, Arc<tokio::sync::Notify>)
where
    F: Fn(usize, ServerWs, Arc<Mutex<Vec<String>>>) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let peer_start = Arc::new(tokio::sync::Notify::new());
    let mut urls = Vec::new();
    for server in 0..2 {
        let (script, peer_start) = (script.clone(), Arc::clone(&peer_start));
        let (url, _) = mock_server(move |n, mut ws, log| {
            let (script, peer_start) = (script.clone(), Arc::clone(&peer_start));
            async move {
                next_text(&mut ws, &log, true).await;
                if n > 0 {
                    return idle(ws, log).await;
                }
                if server == 1 {
                    tokio::select! {
                        _ = peer_start.notified() => {}
                        _ = answer_for(&mut ws, &log, Duration::from_secs(5)) => {}
                    }
                }
                send(&mut ws, BOOK_A).await;
                script(server, ws, log).await;
            }
        })
        .await;
        urls.push(url);
    }
    let h = start_shard_with(urls.into_iter().map(fast_cfg).collect(), &["A"], detect);
    eventually("member 0 connected", || h.get(0, |s| &s.connects) == 1).await;
    peer_start.notify_one();
    eventually("both have the book", || h.get(0, |s| &s.books) == 1 && h.get(1, |s| &s.books) == 1).await;
    (h, peer_start)
}

#[tokio::test]
async fn a_change_the_source_missed_switches_and_resyncs_it() {
    let detect = crate::ws::source::DetectConfig {
        loss_after: Duration::from_millis(300),
        hold: Duration::ZERO,
        ..Default::default()
    };
    let (h, _) = two_member_shard(detect, |server, mut ws, log| async move {
        send(&mut ws, &change_a_hashed("15", 1793822781001, "x1")).await;
        if server == 1 {
            // Only the peer gets this one.
            send(&mut ws, &change_a_hashed("30", 1793822781500, "x2")).await;
        }
        idle(ws, log).await;
    })
    .await;
    eventually("lost change detected and handed over", || h.switches() == 1).await;
    assert_eq!(h.shard.lock().await.source, 1);
    eventually("old source resyncs A", || h.get(0, |s| &s.resyncs) == 1).await;
    assert_eq!(h.shard.lost_on_source.load(Ordering::Relaxed), 1);
    // The new source writes its books (A at 30) at its next output.
    h.ticks.send(Utc::now()).unwrap();
    eventually("switch snapshot", || h.get(1, |s| &s.switch_snapshots) == 1).await;
    let msgs = h.finish().await;
    assert_eq!(last_a_bid(&msgs), 30_000_000, "{:?}", kinds(&msgs));
    let r = replay(&msgs);
    assert!(r.file_is_consistent(), "{r:?}");
    assert_eq!(r.updates_without_baseline, 0);
}

#[tokio::test]
async fn a_lagging_source_hands_over_to_a_faster_peer() {
    let detect = crate::ws::source::DetectConfig {
        min_lag_samples: 5,
        hold: Duration::ZERO,
        ..Default::default()
    };
    let (h, _) = two_member_shard(detect, |server, mut ws, log| async move {
        // The source's server (0) delivers every change 500 ms after the peer's.
        if server == 0 {
            answer_for(&mut ws, &log, Duration::from_millis(500)).await;
        }
        for i in 0..8 {
            send(&mut ws, &change_a_hashed(&(20 + i).to_string(), 1793822782000 + i, &format!("l{i}"))).await;
            answer_for(&mut ws, &log, Duration::from_millis(30)).await;
        }
        idle(ws, log).await;
    })
    .await;
    eventually("lag detected", || h.switches() == 1).await;
    assert_eq!(h.shard.lock().await.source, 1);
    let (p50, _, _) = h.shard.lag_stats(std::time::Instant::now()).unwrap_or((0.0, 0.0, 0));
    assert!(p50 < 200.0, "samples after the switch are against the new source: {p50}");
    assert_eq!(h.shard.lost_on_source.load(Ordering::Relaxed), 0, "late is not lost");
    let msgs = h.finish().await;
    let r = replay(&msgs);
    assert!(r.file_is_consistent(), "{r:?}");
    assert_eq!(r.updates_without_baseline, 0);
}

#[tokio::test]
async fn a_slow_reader_is_not_taken_for_a_lost_message() {
    let detect = crate::ws::source::DetectConfig {
        loss_after: Duration::from_millis(300),
        hold: Duration::ZERO,
        ..Default::default()
    };
    let (h, _) = two_member_shard(detect, |server, mut ws, log| async move {
        if server == 1 {
            send(&mut ws, &change_a_hashed("30", 1793822781500, "s1")).await;
            return idle(ws, log).await;
        }
        // Member 0 reads the change 1 s late, like a backlogged reader: after
        // a moment of normal PONGs (so it has a barrier), its PINGs are
        // answered only after the change.
        answer_for(&mut ws, &log, Duration::from_millis(150)).await;
        let owed = ignore_pings_for(&mut ws, &log, Duration::from_secs(1)).await;
        send(&mut ws, &change_a_hashed("30", 1793822781500, "s1")).await;
        pong_owed(&mut ws, owed).await;
        idle(ws, log).await;
    })
    .await;
    eventually("member 0 got the change", || h.get(0, |s| &s.levels) == 1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.shard.lost_on_source.load(Ordering::Relaxed), 0);
    assert_eq!((h.switches(), h.get(0, |s| &s.resyncs)), (0, 0));
    h.finish().await;
}

// ---- Subscription monitoring (Extension 2) ----

#[tokio::test]
async fn a_silent_symbol_is_resubscribed() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        busy_then_quiet(&mut ws).await;
        // A goes quiet; the resubscription gets no book either.
        idle(ws, log).await;
    })
    .await;
    let h = start(silence_cfg(url), &["A"]);
    eventually("A resubscribed", || h.get(|s| &s.silent_resyncs) == 1).await;
    assert_eq!(h.get(|s| &s.levels), 28);
    eventually("unsubscribe sent", || {
        log.lock().unwrap().iter().any(|m| m.contains("\"operation\":\"unsubscribe\"") && m.contains("\"A\""))
    })
    .await;
    // A now waits for a book: not live, so not flagged again.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(h.get(|s| &s.silent_resyncs), 1);
    let msgs = h.finish().await;
    let r = replay(&msgs);
    assert!(r.file_is_consistent(), "{r:?}");
    assert_eq!(r.updates_without_baseline, 0);
}

#[tokio::test]
async fn an_empty_guess_after_a_silent_resync_restarts_the_silence_clock() {
    // A busy market whose orders all went: after the resync no book comes,
    // so it gets an empty baseline. That must not look silent at once, or
    // it is resynced every check, forever.
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        busy_then_quiet(&mut ws).await;
        idle(ws, log).await;
    })
    .await;
    let h = start(
        ConnectionConfig {
            empty_book_after: Duration::from_millis(100),
            ..silence_cfg(url)
        },
        &["A"],
    );
    eventually("empty guess after the resync", || h.get(|s| &s.assumed_empty) == 1).await;
    // The guess counts as activity, and the empty buckets before it keep
    // the symbol below "active": it is never flagged again.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(h.get(|s| &s.silent_resyncs), 1);
    h.finish().await;
}

#[tokio::test]
async fn a_quiet_market_is_not_resubscribed() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await;
        for i in 0..3 {
            answer_for(&mut ws, &log, Duration::from_millis(1200)).await;
            send(&mut ws, &change_a(&(16 + i).to_string(), 1793822781010 + i)).await;
        }
        idle(ws, log).await;
    })
    .await;
    let h = start(silence_cfg(url), &["A"]);
    // The changes come 1.2 s apart (longer than `eventually` waits).
    for _ in 0..80 {
        if h.get(|s| &s.levels) == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(h.get(|s| &s.levels), 3);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!((h.get(|s| &s.silent_resyncs), h.get(|s| &s.resyncs)), (0, 0));
    h.finish().await;
}

#[tokio::test]
async fn a_recycle_reconnects_at_once() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await;
        idle(ws, log).await;
    })
    .await;
    let h = start(fast_cfg(url), &["A"]);
    eventually("first book", || h.get(|s| &s.books) == 1).await;
    h.cmds.send(Command::Recycle).unwrap();
    eventually("recycled", || h.get(|s| &s.recycles) == 1 && h.get(|s| &s.connects) == 2).await;
    eventually("book again", || h.get(|s| &s.books) == 2).await;
    assert_eq!(h.get(|s| &s.disconnects), 0, "a recycle is not a lost connection");
    let msgs = h.finish().await;
    let r = replay(&msgs);
    assert!(r.file_is_consistent(), "{r:?}");
}

#[tokio::test]
async fn recycling_the_source_hands_over_first() {
    let (h, _) = two_member_shard(Default::default(), |server, mut ws, log| async move {
        if server == 1 {
            // After the handover: written by the new source.
            answer_for(&mut ws, &log, Duration::from_millis(500)).await;
            send(&mut ws, &change_a("20", 1793822782000)).await;
        }
        idle(ws, log).await;
    })
    .await;
    assert_eq!(h.shard.lock().await.source, 0);
    h.cmds[0].send(Command::Recycle).unwrap();
    eventually("member 0 recycled", || h.get(0, |s| &s.recycles) == 1).await;
    assert_eq!((h.switches(), h.shard.lock().await.source), (1, 1));
    eventually("peer's change", || h.get(1, |s| &s.levels) == 1).await;
    let msgs = h.finish().await;
    assert!(kinds(&msgs).contains(&"U:A:20000000".to_string()), "{:?}", kinds(&msgs));
    let r = replay(&msgs);
    assert!(r.file_is_consistent(), "{r:?}");
    assert_eq!((r.updates_without_baseline, r.mismatched), (0, 0), "{r:?}");
}

#[tokio::test]
async fn recycling_the_source_without_a_ready_peer_is_skipped() {
    let member0_up = Arc::new(tokio::sync::Notify::new());
    let (url0, _) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        send(&mut ws, BOOK_A).await;
        idle(ws, log).await;
    })
    .await;
    let (url1, _) = mock_server({
        let member0_up = Arc::clone(&member0_up);
        move |_, ws, log| {
            let member0_up = Arc::clone(&member0_up);
            async move {
                // Silent until member 0 is the source; then never a book.
                member0_up.notified().await;
                idle(ws, log).await;
            }
        }
    })
    .await;
    let h = start_shard(vec![fast_cfg(url0), fast_cfg(url1)], &["A"]);
    eventually("member 0 has its book", || h.get(0, |s| &s.books) == 1).await;
    member0_up.notify_one();
    eventually("member 1 connected", || h.get(1, |s| &s.connects) == 1).await;
    assert_eq!(h.shard.lock().await.source, 0);
    h.cmds[0].send(Command::Recycle).unwrap();
    eventually("skipped", || h.get(0, |s| &s.recycles_skipped) == 1).await;
    assert_eq!((h.get(0, |s| &s.recycles), h.get(0, |s| &s.connects), h.switches()), (0, 1, 0));
    h.finish().await;
}

// ---- A slow writer must not delay receive times (Part A) ----

/// `BOOK_A`, then `n` changes for A (sizes 16, 17, ...) with `gap` before
/// each. Returns the wall time taken just before each send.
async fn book_then_changes(ws: &mut ServerWs, n: i64, gap: Duration) -> Vec<chrono::DateTime<Utc>> {
    let mut sent = vec![Utc::now()];
    send(ws, BOOK_A).await;
    for i in 0..n {
        if !gap.is_zero() {
            tokio::time::sleep(gap).await;
        }
        sent.push(Utc::now());
        send(ws, &change_a(&(16 + i).to_string(), 1793822781010 + i)).await;
    }
    sent
}

#[tokio::test]
async fn receive_times_do_not_wait_for_a_full_writer() {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let (url, _log) = mock_server({
        let sent = Arc::clone(&sent);
        move |_, mut ws, log| {
            let sent = Arc::clone(&sent);
            async move {
                next_text(&mut ws, &log, true).await;
                let times = book_then_changes(&mut ws, 5, Duration::from_millis(100)).await;
                *sent.lock().unwrap() = times;
                idle(ws, log).await;
            }
        }
    })
    .await;
    let (out, rx) = writer::test_channel(1);
    let h = start_with_out(fast_cfg(url), &["A"], out);
    tokio::time::sleep(Duration::from_millis(700)).await;
    let (got, drained) = drain(rx);
    eventually("6 messages", || got.lock().unwrap().len() == 6).await;
    assert!(h.get(|s| &s.max_queued_frames) >= 3, "{}", h.get(|s| &s.max_queued_frames));
    h.finish().await;
    drained.await.unwrap();

    let msgs = got.lock().unwrap().clone();
    let sent = sent.lock().unwrap().clone();
    assert_eq!((msgs.len(), sent.len()), (6, 6));
    for (m, at) in msgs.iter().zip(&sent).skip(1) {
        let OutMsg::Update(u) = m else { panic!("{m:?}") };
        let late = (u.backtest_timestamp - *at).num_milliseconds();
        assert!((0..50).contains(&late), "stamped {late} ms after the send");
    }
}

#[tokio::test]
async fn a_stalled_writer_does_not_trip_the_pong_timeout() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        book_then_changes(&mut ws, 20, Duration::ZERO).await;
        idle(ws, log).await;
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.pong_timeout = Duration::from_millis(300);
    let (out, rx) = writer::test_channel(1);
    let h = start_with_out(cfg, &["A"], out);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let (got, drained) = drain(rx);
    eventually("all written", || got.lock().unwrap().len() >= 21).await;
    // A few ping rounds after the stall.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(h.get(|s| &s.disconnects), 0);
    h.finish().await;
    drained.await.unwrap();
}

/// Answers the i-th PING `delay(i)` after it arrives, in order, like a
/// server whose PONGs wait behind a download backlog, or never if
/// `delay(i)` is `None` (lost). Logs every other text the client sends.
async fn answer_pings_late(
    ws: ServerWs,
    log: Arc<Mutex<Vec<String>>>,
    delay: impl Fn(usize) -> Option<Duration> + Send + 'static,
) {
    let (mut sink, mut stream) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<tokio::time::Instant>();
    let pongs = tokio::spawn(async move {
        while let Some(at) = rx.recv().await {
            tokio::time::sleep_until(at).await;
            if sink.send(Message::text("PONG")).await.is_err() {
                return;
            }
        }
    });
    let mut i = 0;
    while let Some(Ok(msg)) = stream.next().await {
        if let Message::Text(t) = msg {
            if t.as_str() == "PING" {
                if let Some(d) = delay(i) {
                    let _ = tx.send(tokio::time::Instant::now() + d);
                }
                i += 1;
            } else {
                log.lock().unwrap().push(t.to_string());
            }
        }
    }
    pongs.abort();
}

#[tokio::test]
async fn a_lost_pong_does_not_drop_a_healthy_connection() {
    // Every third PONG is lost; the rest come at once. Counted FIFO, the
    // oldest "unanswered" PING gets older and older.
    let (url, _log) = mock_server(|_, ws, log| answer_pings_late(ws, log, |i| (i % 3 != 2).then_some(Duration::ZERO))).await;
    let mut cfg = fast_cfg(url);
    cfg.pong_timeout = Duration::from_millis(300);
    let h = start(cfg, &["A"]);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(h.get(|s| &s.disconnects), 0);
    h.finish().await;
}

#[tokio::test]
async fn a_pong_late_behind_a_backlog_does_not_drop_a_healthy_connection() {
    // Live, under a saturated download, PONGs came in order but later and
    // later (up to 47 s after their PING), while earlier ones kept arriving.
    // Here each comes 20 ms later than the one before (one about every
    // 70 ms), ~600 ms late, twice the timeout, by the end.
    let (url, _log) = mock_server(|_, ws, log| {
        answer_pings_late(ws, log, |i| Some(Duration::from_millis(20 * i as u64)))
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.pong_timeout = Duration::from_millis(300);
    let h = start(cfg, &["A"]);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(h.get(|s| &s.disconnects), 0);
    h.finish().await;
}

#[tokio::test]
async fn a_silent_server_is_dropped_within_the_pong_timeout() {
    let silent_since = Arc::new(Mutex::new(None));
    let (url, _log) = mock_server({
        let silent_since = Arc::clone(&silent_since);
        move |n, mut ws, log| {
            let silent_since = Arc::clone(&silent_since);
            async move {
                if n == 0 {
                    answer_for(&mut ws, &log, Duration::from_millis(300)).await;
                    *silent_since.lock().unwrap() = Some(std::time::Instant::now());
                    // Still connected and reading, but never answers again.
                    while next_text(&mut ws, &log, false).await.is_some() {}
                } else {
                    idle(ws, log).await;
                }
            }
        }
    })
    .await;
    let mut cfg = fast_cfg(url);
    cfg.pong_timeout = Duration::from_millis(300);
    let (ping, timeout) = (cfg.ping_interval, cfg.pong_timeout);
    let h = start(cfg, &["A"]);
    eventually("disconnect after the server goes silent", || h.get(|s| &s.disconnects) >= 1).await;
    let waited = silent_since.lock().unwrap().expect("went silent").elapsed();
    // The first unanswered PING goes out within one interval of the last
    // PONG (one may be in flight as the server stops), and is judged on the
    // next tick after `pong_timeout`.
    assert!(waited + ping >= timeout, "dropped after {waited:?}");
    assert!(waited <= timeout + 2 * ping + Duration::from_millis(100), "dropped after {waited:?}");
    h.finish().await;
}

#[tokio::test]
async fn a_minute_snapshot_under_backlog_keeps_receive_order() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        book_then_changes(&mut ws, 40, Duration::from_millis(5)).await;
        idle(ws, log).await;
    })
    .await;
    // No PING during the test: the ping arm also catches up, and would
    // hide a tick arm that does not.
    let mut cfg = fast_cfg(url);
    cfg.ping_interval = Duration::from_secs(10);
    let (out, rx) = writer::test_channel(1);
    let h = start_with_out(cfg, &["A"], out);
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.ticks.send(Utc::now()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (got, drained) = drain(rx);
    eventually("book, 40 updates, minute snapshot", || got.lock().unwrap().len() == 42).await;
    assert_eq!(h.get(|s| &s.minute_snapshots), 1);
    h.finish().await;
    drained.await.unwrap();

    let msgs = got.lock().unwrap().clone();
    let r = replay(&msgs);
    assert_eq!((r.time_reversals, r.updates_without_baseline, r.mismatched), (0, 0, 0), "{r:?}");
    let minute = msgs
        .iter()
        .enumerate()
        .filter(|(_, m)| matches!(m, OutMsg::Snapshot(_)))
        .map(|(i, _)| i)
        .nth(1)
        .expect("a minute snapshot");
    let t = received_at(&msgs[minute]);
    assert!(msgs[..minute].iter().all(|m| received_at(m) < t), "{:?}", kinds(&msgs));
    assert!(msgs[minute + 1..].iter().all(|m| received_at(m) >= t), "{:?}", kinds(&msgs));
}

#[tokio::test]
async fn frames_read_before_the_socket_drops_are_all_written() {
    let (url, _log) = mock_server(|n, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        if n == 0 {
            book_then_changes(&mut ws, 20, Duration::ZERO).await;
            // Let the client read them. The client's PING is still unread
            // here, so the drop is a TCP reset, which discards whatever the
            // client has not read from its socket yet.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return; // drops the TCP stream without a Close
        }
        idle(ws, log).await;
    })
    .await;
    let (out, rx) = writer::test_channel(1);
    let h = start_with_out(fast_cfg(url), &["A"], out);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (got, drained) = drain(rx);
    eventually("all 21 messages", || got.lock().unwrap().len() >= 21).await;
    h.finish().await;
    drained.await.unwrap();
    let mut want = vec!["S:A".to_string()];
    want.extend((16..36).map(|q| format!("U:A:{q}000000")));
    assert_eq!(kinds(&got.lock().unwrap()), want);
}

#[tokio::test]
async fn shutdown_writes_frames_already_read() {
    let (url, _log) = mock_server(|_, mut ws, log| async move {
        next_text(&mut ws, &log, true).await;
        book_then_changes(&mut ws, 10, Duration::ZERO).await;
        idle(ws, log).await;
    })
    .await;
    let (out, rx) = writer::test_channel(1);
    let h = start_with_out(fast_cfg(url), &["A"], out);
    // All read while the writer is full. (`max_queued_frames` is sampled
    // only when the session takes a frame, and it is stuck on the second.)
    eventually("all frames read", || h.get(|s| &s.frames) >= 11).await;
    h.shutdown.cancel();
    let (got, drained) = drain(rx);
    h.finish().await;
    drained.await.unwrap();
    assert_eq!(got.lock().unwrap().len(), 11, "{:?}", kinds(&got.lock().unwrap()));
}

// A second exchange (Extension 3), test-only, with a deliberately different
// protocol: heartbeat `hb` → `hb-ack`; `{"op":"sub"|"unsub","syms":[..]}`;
// frames are JSON arrays of `snap` (full book) and `delta` (signed change)
// messages, each with a per-symbol sequence number.

use crate::Side;
use crate::exchange::{Capabilities, DecodeError, Decoded, Failure, LevelKind};
use crate::fixed::parse_fixed;

#[derive(Debug)]
struct MockExchange {
    url: String,
}

impl Exchange for MockExchange {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn ws_url(&self) -> &str {
        &self.url
    }

    fn ping_text(&self) -> &str {
        "hb"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            missing_book_means_empty: false,
        }
    }

    fn codec(&self) -> Box<dyn Codec> {
        Box::new(MockCodec::default())
    }
}

#[derive(Default)]
struct MockCodec {
    /// Last sequence number per symbol: set by a `snap`, cleared by a gap
    /// (later deltas fail until the next `snap`).
    last: HashMap<String, u64>,
}

fn mock_op(op: &str, symbols: &[String]) -> String {
    serde_json::json!({ "op": op, "syms": symbols }).to_string()
}

impl Codec for MockCodec {
    fn encode_initial_subscribe(&mut self, symbols: &[String]) -> String {
        mock_op("sub", symbols)
    }

    fn encode_subscribe(&mut self, symbols: &[String]) -> String {
        mock_op("sub", symbols)
    }

    fn encode_unsubscribe(&mut self, symbols: &[String]) -> String {
        mock_op("unsub", symbols)
    }

    fn decode(&mut self, text: &str) -> Result<Decoded, DecodeError> {
        let mut out = Decoded::default();
        if text == "hb-ack" {
            out.events.push(Event::Pong);
            return Ok(out);
        }
        let items: Vec<serde_json::Value> = serde_json::from_str(text).map_err(DecodeError::Json)?;
        let fixed = |v: &serde_json::Value| parse_fixed(v.as_str().unwrap()).unwrap();
        for m in items {
            let symbol = m["s"].as_str().unwrap().to_owned();
            let seq = m["seq"].as_u64().unwrap();
            let exchange_ts = DateTime::from_timestamp_millis(m["ts"].as_i64().unwrap()).unwrap();
            match m["k"].as_str().unwrap() {
                "snap" => {
                    let side = |k: &str| -> Vec<(i64, i64)> {
                        m[k].as_array().unwrap().iter().map(|l| (fixed(&l[0]), fixed(&l[1]))).collect()
                    };
                    self.last.insert(symbol.clone(), seq);
                    out.events.push(Event::Book {
                        bids: side("bids"),
                        asks: side("asks"),
                        symbol,
                        exchange_ts,
                        hash: None,
                    });
                }
                "delta" => match self.last.get(&symbol).copied() {
                    Some(last) if seq == last + 1 => {
                        self.last.insert(symbol.clone(), seq);
                        out.events.push(Event::Level {
                            side: if m["side"] == "bid" { Side::Buy } else { Side::Sell },
                            price: fixed(&m["px"]),
                            quantity: fixed(&m["dq"]),
                            kind: LevelKind::Delta,
                            symbol,
                            exchange_ts,
                            hash: None,
                        });
                    }
                    last => {
                        self.last.remove(&symbol);
                        out.failures.push(Failure {
                            kind: "delta",
                            symbols: vec![symbol],
                            error: DecodeError::Sequence {
                                expected: last.map_or(0, |l| l + 1),
                                got: seq,
                            },
                        });
                    }
                },
                other => out.events.push(Event::Ignored { kind: other.to_owned() }),
            }
        }
        Ok(out)
    }
}

fn mock_cfg(url: String) -> ConnectionConfig {
    ConnectionConfig {
        exchange: Arc::new(MockExchange { url }),
        ..fast_cfg(String::new())
    }
}

fn mock_snap(seq: u64, bid: &str) -> String {
    format!(r#"[{{"k":"snap","s":"A","seq":{seq},"ts":1793822781000,"bids":[["0.40","{bid}"]],"asks":[]}}]"#)
}

fn mock_delta(seq: u64, dq: &str) -> String {
    format!(r#"[{{"k":"delta","s":"A","seq":{seq},"ts":1793822781001,"side":"bid","px":"0.40","dq":"{dq}"}}]"#)
}

/// Reads one client text frame of the mock protocol, answering `hb` on the
/// way. Logs every text, heartbeats included.
async fn mock_next(ws: &mut ServerWs, log: &Mutex<Vec<String>>) -> Option<String> {
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Text(t) = msg {
            let t = t.to_string();
            log.lock().unwrap().push(t.clone());
            if t == "hb" {
                ws.send(Message::text("hb-ack")).await.ok()?;
                continue;
            }
            return Some(t);
        }
    }
    None
}

async fn mock_idle(mut ws: ServerWs, log: Arc<Mutex<Vec<String>>>) {
    while mock_next(&mut ws, &log).await.is_some() {}
}

/// The client's texts other than heartbeats, as JSON.
fn mock_ops(log: &Mutex<Vec<String>>) -> Vec<serde_json::Value> {
    let log = log.lock().unwrap();
    log.iter()
        .filter(|t| t.as_str() != "hb")
        .map(|t| serde_json::from_str(t).unwrap_or_else(|_| panic!("not mock protocol: {t:?}")))
        .collect()
}

#[tokio::test]
async fn a_second_exchange_records_books_and_deltas() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        mock_next(&mut ws, &log).await; // subscribe
        send(&mut ws, &mock_snap(1, "10")).await;
        send(&mut ws, &mock_delta(2, "5")).await;
        send(&mut ws, &mock_delta(3, "-3")).await;
        mock_idle(ws, log).await;
    })
    .await;
    let h = start(mock_cfg(url), &["A"]);
    eventually("both deltas applied", || h.get(|s| &s.levels) == 2).await;
    assert_eq!(mock_ops(&log), [serde_json::json!({"op":"sub","syms":["A"]})]);
    assert!(log.lock().unwrap().iter().any(|t| t == "hb"), "the exchange's heartbeat is used");
    assert_eq!(h.get(|s| &s.ignored), 0, "every heartbeat answer decoded as a PONG");
    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:A", "U:A:15000000", "U:A:12000000"], "absolute quantities");
}

#[tokio::test]
async fn a_sequence_gap_resyncs_the_symbol() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        mock_next(&mut ws, &log).await; // subscribe
        send(&mut ws, &mock_snap(1, "10")).await;
        send(&mut ws, &mock_delta(2, "-4")).await;
        send(&mut ws, &mock_delta(4, "1")).await; // 3 is missing
        mock_next(&mut ws, &log).await; // unsubscribe
        mock_next(&mut ws, &log).await; // subscribe again
        send(&mut ws, &mock_snap(10, "7")).await;
        send(&mut ws, &mock_delta(11, "2")).await;
        mock_idle(ws, log).await;
    })
    .await;
    let h = start(mock_cfg(url), &["A"]);
    eventually("delta after the new snap applied", || h.get(|s| &s.levels) == 2).await;
    assert_eq!(h.get(|s| &s.resyncs), 1);
    assert_eq!(h.get(|s| &s.decode_errors), 1);
    assert_eq!(
        mock_ops(&log),
        [
            serde_json::json!({"op":"sub","syms":["A"]}),
            serde_json::json!({"op":"unsub","syms":["A"]}),
            serde_json::json!({"op":"sub","syms":["A"]}),
        ]
    );
    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:A", "U:A:6000000", "S:A", "U:A:9000000"], "seq 4 not written");
}

#[tokio::test]
async fn a_negative_delta_resyncs_the_symbol() {
    let (url, log) = mock_server(|_, mut ws, log| async move {
        mock_next(&mut ws, &log).await; // subscribe
        send(&mut ws, &mock_snap(1, "10")).await;
        send(&mut ws, &mock_delta(2, "-15")).await; // 10 - 15 < 0: a change was missed
        mock_next(&mut ws, &log).await; // unsubscribe
        mock_next(&mut ws, &log).await; // subscribe again
        send(&mut ws, &mock_snap(5, "3")).await;
        send(&mut ws, &mock_delta(6, "1")).await;
        mock_idle(ws, log).await;
    })
    .await;
    let h = start(mock_cfg(url), &["A"]);
    eventually("delta after the new snap applied", || h.get(|s| &s.levels) == 1).await;
    assert_eq!(h.get(|s| &s.resyncs), 1);
    assert_eq!(
        mock_ops(&log),
        [
            serde_json::json!({"op":"sub","syms":["A"]}),
            serde_json::json!({"op":"unsub","syms":["A"]}),
            serde_json::json!({"op":"sub","syms":["A"]}),
        ]
    );
    let msgs = h.finish().await;
    assert_eq!(kinds(&msgs), ["S:A", "S:A", "U:A:4000000"], "the negative delta is not written");
}

#[tokio::test]
async fn mock_exchange_makes_no_empty_guesses() {
    let (url, log) = mock_server(|_, ws, log| mock_idle(ws, log)).await;
    let mut cfg = mock_cfg(url);
    cfg.empty_book_after = Duration::from_millis(100);
    let h = start(cfg, &["A"]);
    // The subscription's barrier and later heartbeats are answered: with
    // Polymarket, A would now be assumed empty.
    eventually("heartbeats answered", || {
        log.lock().unwrap().iter().filter(|t| t.as_str() == "hb").count() >= 4
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.get(|s| &s.assumed_empty), 0);
    assert!(h.finish().await.is_empty(), "no assumed-empty snapshot");
}
