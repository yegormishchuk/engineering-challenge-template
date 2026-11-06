//! One websocket connection to the Polymarket market channel.
//!
//! A connection owns the books of its symbols and is the only writer of
//! their messages, so books and output stay consistent without locks. It
//! keeps the socket alive with `PING`, reconnects with backoff, and recovers
//! a symbol whenever its book can no longer be trusted:
//!
//! | Trigger | Recovery |
//! | --- | --- |
//! | socket closed, error, or no `PONG` within `pong_timeout` | reconnect; mark all books stale; resubscribe everything |
//! | frame that looks like JSON but does not parse | mark all books stale; unsubscribe + subscribe everything |
//! | `book` / `price_change` that does not decode | the same, for the symbols it names (all if none) |
//! | invalid level change or book for a symbol | the same, for that symbol |
//! | REST shows orders for a book assumed empty | the same, for that symbol |
//!
//! While a symbol has no baseline, its level changes are skipped (and
//! counted): a reader would have nothing to apply them to. The next `book`
//! the exchange sends becomes the new baseline and is written as a snapshot.
//! A stale book (from before a gap) is not a baseline, but it is compared
//! with that next book and the result is counted as `gap_mismatched_books`.
//!
//! Polymarket sends no `book` for a token without orders, so a symbol still
//! without one `empty_book_after` its subscription gets an empty baseline,
//! and the guess is checked against the batch REST endpoint.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use indexmap::IndexSet;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::books::{Books, BooksError, SnapshotApplied};
use crate::exchange::{Event, polymarket};
use crate::writer::{OutSender, WriterGone};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Checks of assumed-empty books against REST. `None` in
/// [`ConnectionConfig::verify`] disables them (tests, or no network).
#[derive(Debug, Clone)]
pub struct VerifyConfig {
    pub http: reqwest::Client,
    pub url: String,
    /// Symbols per request.
    pub batch: usize,
    /// At most one request in flight per connection, at most this often.
    pub every: Duration,
}

impl VerifyConfig {
    pub fn polymarket(http: reqwest::Client) -> Self {
        Self {
            http,
            url: polymarket::BOOKS_URL.to_owned(),
            batch: 200,
            every: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub url: String,
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
    pub connect_timeout: Duration,
    /// How long a websocket Close may take at shutdown before giving up.
    pub close_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// A session shorter than this does not reset the backoff, so a server
    /// that accepts and immediately drops us is not hammered.
    pub stable_after: Duration,
    /// Symbols per subscribe/unsubscribe message.
    pub subscribe_batch: usize,
    /// Pause between subscribe messages. Each subscription makes the server
    /// push a full book per symbol at once; sending them all together
    /// overflows the server's send buffer for the socket, and it closes the
    /// connection with "slow consumer: send buffer full" (seen in M4).
    pub subscribe_pace: Duration,
    /// A symbol with no `book` this long after its subscription was sent is
    /// taken to have an empty book. Polymarket sends no `book` for tokens
    /// without orders (REST answers 404 for them); their first order arrives
    /// as a `price_change`, which needs a baseline. Books arrive within 0.74 s
    /// of subscribing (p99, 2000 tokens, M5), so 5 s leaves a wide margin; if
    /// a book does come later, it is a checkpoint and corrects the guess.
    pub empty_book_after: Duration,
    /// A level change for a symbol still waiting for its `book` this long
    /// after subscribing means the book is empty: books arrive within 0.74 s
    /// (p99). The change is then applied to an empty baseline at once,
    /// instead of being skipped until `empty_book_after`.
    pub empty_book_on_change_after: Duration,
    pub verify: Option<VerifyConfig>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: polymarket::WS_URL.to_owned(),
            ping_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(2),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            stable_after: Duration::from_secs(30),
            subscribe_batch: 100,
            subscribe_pace: Duration::from_millis(250),
            empty_book_after: Duration::from_secs(5),
            empty_book_on_change_after: Duration::from_millis(1500),
            verify: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Add(Vec<String>),
    Remove(Vec<String>),
}

/// Per-connection counters, readable while the connection runs.
#[derive(Debug, Default)]
pub struct ConnStats {
    pub connects: AtomicU64,
    pub disconnects: AtomicU64,
    pub frames: AtomicU64,
    pub books: AtomicU64,
    pub levels: AtomicU64,
    /// Exchange books that disagreed with the live local book: a change the
    /// stream missed.
    pub mismatched_books: AtomicU64,
    pub mismatched_levels: AtomicU64,
    /// First books after a gap that differ from the book before the gap:
    /// the market changed while we could not see it.
    pub gap_mismatched_books: AtomicU64,
    /// Level changes skipped because the symbol had no baseline book.
    pub skipped_no_baseline: AtomicU64,
    pub decode_errors: AtomicU64,
    pub resyncs: AtomicU64,
    pub ignored: AtomicU64,
    pub minute_snapshots: AtomicU64,
    /// Symbols given an empty baseline because no `book` arrived in time.
    pub assumed_empty: AtomicU64,
    /// Assumed-empty books confirmed by REST, and ones REST showed orders for.
    pub empty_confirmed: AtomicU64,
    pub empty_wrong: AtomicU64,
    pub verify_errors: AtomicU64,
    /// Bytes of text frames received.
    pub bytes: AtomicU64,
    /// Time spent handling frames (decode, books, queueing output), in µs.
    pub busy_us: AtomicU64,
    /// Longest single frame handling time, in µs.
    pub max_frame_us: AtomicU64,
}

fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn add(counter: &AtomicU64, n: usize) {
    counter.fetch_add(n as u64, Ordering::Relaxed);
}

/// Result of one REST check of assumed-empty books.
struct Verified {
    checked: Vec<String>,
    result: Result<Vec<String>, String>,
}

pub struct Connection {
    id: usize,
    cfg: ConnectionConfig,
    /// In the order they were added, so a market's tokens (added together)
    /// stay in the same subscription batch after a reconnect.
    symbols: IndexSet<String>,
    books: Books,
    out: OutSender,
    stats: Arc<ConnStats>,
    /// Where `new_market` / `market_resolved` go. Every connection forwards
    /// them; the tracker ignores repeats.
    discovery: Option<mpsc::UnboundedSender<Event>>,
    ticks_open: bool,
    /// Symbols waiting to be subscribed on the current socket, sent one batch
    /// per `subscribe_pace`.
    pending: VecDeque<String>,
    /// Whether the current socket has sent its initial subscription.
    initial_sent: bool,
    /// Symbols subscribed on the current socket that have no `book` yet, with
    /// the time their subscription was sent.
    awaiting_book: HashMap<String, Instant>,
    /// Assumed-empty symbols waiting for a REST check.
    to_verify: Vec<String>,
}

enum SessionEnd {
    Shutdown,
    Lost(String),
}

/// Why a frame handler stopped the session.
enum Stop {
    Lost(String),
    WriterGone,
}

impl From<WriterGone> for Stop {
    fn from(_: WriterGone) -> Self {
        Self::WriterGone
    }
}

impl Connection {
    pub fn new(
        id: usize,
        cfg: ConnectionConfig,
        out: OutSender,
        stats: Arc<ConnStats>,
        discovery: Option<mpsc::UnboundedSender<Event>>,
    ) -> Self {
        Self {
            id,
            cfg,
            symbols: IndexSet::new(),
            books: Books::new(),
            out,
            stats,
            discovery,
            ticks_open: true,
            pending: VecDeque::new(),
            initial_sent: false,
            awaiting_book: HashMap::new(),
            to_verify: Vec::new(),
        }
    }

    /// Runs until `shutdown` is cancelled or the command channel closes.
    /// Returns `Err` only if the writer is gone, which is fatal for the whole
    /// collector.
    pub async fn run(
        mut self,
        mut cmds: mpsc::UnboundedReceiver<Command>,
        mut ticks: watch::Receiver<DateTime<Utc>>,
        shutdown: CancellationToken,
    ) -> Result<(), WriterGone> {
        let mut backoff = self.cfg.backoff_min;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            if self.symbols.is_empty() {
                // Nothing to subscribe to; wait for work without a socket.
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    cmd = cmds.recv() => match cmd {
                        Some(cmd) => { self.apply_command_offline(cmd); continue; }
                        None => return Ok(()),
                    },
                }
            }

            let connect = tokio::time::timeout(
                self.cfg.connect_timeout,
                tokio_tungstenite::connect_async(self.cfg.url.as_str()),
            );
            let connected = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                r = connect => r,
            };
            match connected {
                Ok(Ok((ws, _))) => {
                    inc(&self.stats.connects);
                    info!(conn = self.id, symbols = self.symbols.len(), "connected");
                    let started = Instant::now();
                    let end = self.session(ws, &mut cmds, &mut ticks, &shutdown).await;
                    // The next session starts from fresh exchange books; the
                    // old ones stay only for comparison.
                    self.books.mark_all_stale();
                    self.to_verify.clear();
                    match end {
                        Ok(SessionEnd::Shutdown) => return Ok(()),
                        Ok(SessionEnd::Lost(reason)) => {
                            inc(&self.stats.disconnects);
                            warn!(conn = self.id, %reason, "connection lost");
                            if started.elapsed() >= self.cfg.stable_after {
                                backoff = self.cfg.backoff_min;
                            }
                        }
                        Err(WriterGone) => return Err(WriterGone),
                    }
                }
                Ok(Err(e)) => warn!(conn = self.id, error = %e, "connect failed"),
                Err(_) => warn!(conn = self.id, "connect timed out"),
            }

            debug!(conn = self.id, ?backoff, "reconnecting after backoff");
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(self.cfg.backoff_max);
        }
    }

    fn apply_command_offline(&mut self, cmd: Command) {
        match cmd {
            Command::Add(symbols) => self.symbols.extend(symbols),
            Command::Remove(symbols) => {
                for s in &symbols {
                    self.symbols.shift_remove(s);
                    self.books.remove(s);
                }
            }
        }
    }

    async fn close(&self, ws: &mut Ws) {
        if tokio::time::timeout(self.cfg.close_timeout, ws.close(None)).await.is_err() {
            debug!(conn = self.id, "websocket close timed out");
        }
    }

    async fn session(
        &mut self,
        mut ws: Ws,
        cmds: &mut mpsc::UnboundedReceiver<Command>,
        ticks: &mut watch::Receiver<DateTime<Utc>>,
        shutdown: &CancellationToken,
    ) -> Result<SessionEnd, WriterGone> {
        self.pending = self.symbols.iter().cloned().collect();
        self.initial_sent = false;
        self.awaiting_book.clear();
        let mut empty_check = tokio::time::interval(Duration::from_secs(1));
        empty_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick is immediate, so the first batch goes out at once.
        let mut pace = tokio::time::interval(self.cfg.subscribe_pace);
        pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping = tokio::time::interval(self.cfg.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await; // the first tick is immediate
        let mut last_pong = Instant::now();
        let verify_every = self.cfg.verify.as_ref().map_or(Duration::from_secs(3600), |v| v.every);
        let mut verify_tick = tokio::time::interval(verify_every);
        verify_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Results of REST checks started in this session.
        let (verified_tx, mut verified_rx) = mpsc::unbounded_channel::<Verified>();
        let mut verify_in_flight = false;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.close(&mut ws).await;
                    return Ok(SessionEnd::Shutdown);
                }
                frame = ws.next() => {
                    // Stamp first, before any parsing.
                    let received_at = Utc::now();
                    let text = match frame {
                        None => return Ok(SessionEnd::Lost("stream ended".into())),
                        Some(Err(e)) => return Ok(SessionEnd::Lost(e.to_string())),
                        Some(Ok(Message::Text(text))) => text,
                        Some(Ok(Message::Close(frame))) => {
                            return Ok(SessionEnd::Lost(format!("closed by server: {frame:?}")));
                        }
                        Some(Ok(_)) => continue, // protocol pings are answered by tungstenite
                    };
                    inc(&self.stats.frames);
                    add(&self.stats.bytes, text.len());
                    let busy = std::time::Instant::now();
                    let result = self.on_text(&mut ws, &text, received_at, &mut last_pong).await;
                    let us = busy.elapsed().as_micros() as u64;
                    self.stats.busy_us.fetch_add(us, Ordering::Relaxed);
                    self.stats.max_frame_us.fetch_max(us, Ordering::Relaxed);
                    match result {
                        Ok(()) => {}
                        Err(Stop::Lost(reason)) => return Ok(SessionEnd::Lost(reason)),
                        Err(Stop::WriterGone) => return Err(WriterGone),
                    }
                }
                _ = pace.tick(), if !self.pending.is_empty() => {
                    if let Err(reason) = self.send_next_batch(&mut ws).await {
                        return Ok(SessionEnd::Lost(reason));
                    }
                }
                _ = empty_check.tick(), if !self.awaiting_book.is_empty() => {
                    self.assume_empty_books().await?;
                }
                _ = verify_tick.tick(), if !verify_in_flight && !self.to_verify.is_empty() && self.cfg.verify.is_some() => {
                    verify_in_flight = true;
                    self.start_verify(verified_tx.clone());
                }
                Some(verified) = verified_rx.recv() => {
                    verify_in_flight = false;
                    match self.on_verified(&mut ws, verified).await {
                        Ok(()) => {}
                        Err(Stop::Lost(reason)) => return Ok(SessionEnd::Lost(reason)),
                        Err(Stop::WriterGone) => return Err(WriterGone),
                    }
                }
                _ = ping.tick() => {
                    if last_pong.elapsed() > self.cfg.pong_timeout {
                        return Ok(SessionEnd::Lost(format!(
                            "no PONG for {:?}", last_pong.elapsed()
                        )));
                    }
                    if let Err(e) = ws.send(Message::text(polymarket::PING)).await {
                        return Ok(SessionEnd::Lost(e.to_string()));
                    }
                }
                cmd = cmds.recv() => {
                    let result = match cmd {
                        None => {
                            self.close(&mut ws).await;
                            return Ok(SessionEnd::Shutdown);
                        }
                        Some(Command::Add(symbols)) => {
                            for s in symbols {
                                if self.symbols.insert(s.clone()) {
                                    self.pending.push_back(s);
                                }
                            }
                            Ok(())
                        }
                        Some(Command::Remove(symbols)) => {
                            let gone: Vec<String> = symbols
                                .into_iter()
                                .filter(|s| {
                                    self.books.remove(s);
                                    self.awaiting_book.remove(s);
                                    self.symbols.shift_remove(s)
                                })
                                .collect();
                            self.pending.retain(|p| !gone.contains(p));
                            self.to_verify.retain(|p| !gone.contains(p));
                            self.send_unsubscribe(&mut ws, &gone).await
                        }
                    };
                    if let Err(reason) = result {
                        return Ok(SessionEnd::Lost(reason));
                    }
                }
                changed = ticks.changed(), if self.ticks_open => {
                    if changed.is_err() {
                        self.ticks_open = false;
                        continue;
                    }
                    let emitted_at = Utc::now();
                    for msg in self.books.minute_snapshots(emitted_at) {
                        self.out.send(msg).await?;
                        inc(&self.stats.minute_snapshots);
                    }
                }
            }
        }
    }

    async fn on_text(
        &mut self,
        ws: &mut Ws,
        text: &str,
        received_at: DateTime<Utc>,
        last_pong: &mut Instant,
    ) -> Result<(), Stop> {
        let decoded = match polymarket::decode(text) {
            Ok(decoded) => decoded,
            Err(error) => {
                // Looked like JSON but did not parse: it may have held book
                // data for any symbol.
                inc(&self.stats.decode_errors);
                let sample: String = text.chars().take(200).collect();
                warn!(conn = self.id, %error, frame = %sample, "unparsable frame; resyncing all symbols");
                let all: Vec<String> = self.symbols.iter().cloned().collect();
                return self.resync(ws, &all).await;
            }
        };
        for event in decoded.events {
            self.on_event(ws, event, received_at, last_pong).await?;
        }
        for failure in decoded.failures {
            inc(&self.stats.decode_errors);
            let ours: Vec<String> = if failure.symbols.is_empty() {
                self.symbols.iter().cloned().collect()
            } else {
                failure
                    .symbols
                    .into_iter()
                    .filter(|s| self.symbols.contains(s))
                    .collect()
            };
            warn!(
                conn = self.id,
                kind = failure.kind,
                error = %failure.error,
                symbols = ours.len(),
                "undecodable book message; resyncing its symbols"
            );
            self.resync(ws, &ours).await?;
        }
        Ok(())
    }

    async fn on_event(
        &mut self,
        ws: &mut Ws,
        event: Event,
        received_at: DateTime<Utc>,
        last_pong: &mut Instant,
    ) -> Result<(), Stop> {
        match event {
            Event::Book {
                symbol,
                bids,
                asks,
                exchange_ts,
                ..
            } => {
                if !self.symbols.contains(&symbol) {
                    return Ok(()); // late message for a removed symbol
                }
                self.awaiting_book.remove(&symbol);
                match self.books.apply_snapshot(&symbol, bids, asks, exchange_ts, received_at) {
                    Ok(applied) => {
                        inc(&self.stats.books);
                        self.note_mismatches(&symbol, &applied);
                        self.out.send(applied.msg).await?;
                    }
                    Err(error) => {
                        warn!(conn = self.id, %error, "invalid exchange book; resyncing");
                        self.resync(ws, std::slice::from_ref(&symbol)).await?;
                    }
                }
            }
            Event::Level {
                symbol,
                side,
                price,
                quantity,
                exchange_ts,
                ..
            } => {
                if !self.symbols.contains(&symbol) {
                    return Ok(());
                }
                if !self.books.has_baseline(&symbol) {
                    let overdue = self
                        .awaiting_book
                        .get(&symbol)
                        .is_some_and(|sent| sent.elapsed() >= self.cfg.empty_book_on_change_after);
                    if !overdue {
                        inc(&self.stats.skipped_no_baseline);
                        return Ok(());
                    }
                    self.assume_empty(&symbol).await?;
                }
                match self.books.apply_level(&symbol, side, price, quantity, exchange_ts, received_at) {
                    Ok(msg) => {
                        inc(&self.stats.levels);
                        self.out.send(msg).await?;
                    }
                    Err(BooksError::NoBaseline { .. }) => inc(&self.stats.skipped_no_baseline),
                    Err(error) => {
                        warn!(conn = self.id, %error, "invalid level change; resyncing");
                        self.resync(ws, std::slice::from_ref(&symbol)).await?;
                    }
                }
            }
            Event::Pong => *last_pong = Instant::now(),
            event @ (Event::MarketAdded { .. } | Event::MarketResolved { .. }) => {
                if let Some(tx) = &self.discovery {
                    let _ = tx.send(event);
                }
            }
            Event::TickSizeChanged { symbol, old, new } => {
                info!(conn = self.id, %symbol, %old, %new, "tick size changed");
            }
            Event::Ignored { kind } => {
                inc(&self.stats.ignored);
                if kind.starts_with("malformed") || kind == "non-JSON text" {
                    debug!(conn = self.id, %kind, "ignored message");
                }
            }
        }
        Ok(())
    }

    fn note_mismatches(&self, symbol: &str, applied: &SnapshotApplied) {
        let Some(mismatches) = &applied.mismatches else {
            return;
        };
        if mismatches.is_empty() {
            return;
        }
        if applied.after_gap {
            inc(&self.stats.gap_mismatched_books);
            debug!(conn = self.id, %symbol, levels = mismatches.len(), "book changed during a gap");
        } else {
            inc(&self.stats.mismatched_books);
            add(&self.stats.mismatched_levels, mismatches.len());
            warn!(
                conn = self.id,
                %symbol,
                levels = mismatches.len(),
                first = ?mismatches.first(),
                "exchange book differs from local book"
            );
        }
    }

    /// Stops trusting the books of `symbols` and asks the exchange for fresh
    /// ones: unsubscribe now, subscribe again through the paced queue.
    async fn resync(&mut self, ws: &mut Ws, symbols: &[String]) -> Result<(), Stop> {
        if symbols.is_empty() {
            return Ok(());
        }
        inc(&self.stats.resyncs);
        for s in symbols {
            self.books.mark_stale(s);
            self.awaiting_book.remove(s);
        }
        self.to_verify.retain(|p| !symbols.contains(p));
        self.send_unsubscribe(ws, symbols).await.map_err(Stop::Lost)?;
        for s in symbols {
            if !self.pending.contains(s) {
                self.pending.push_back(s.clone());
            }
        }
        Ok(())
    }

    /// Sends the next batch of pending subscriptions. The first message on a
    /// socket uses the initial format.
    async fn send_next_batch(&mut self, ws: &mut Ws) -> Result<(), String> {
        let n = self.pending.len().min(self.cfg.subscribe_batch);
        let batch: Vec<String> = self.pending.drain(..n).collect();
        if batch.is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        for s in &batch {
            self.awaiting_book.insert(s.clone(), now);
        }
        let msg = if self.initial_sent {
            polymarket::encode_subscribe(&batch)
        } else {
            polymarket::encode_initial_subscribe(&batch)
        };
        self.initial_sent = true;
        ws.send(Message::text(msg)).await.map_err(|e| e.to_string())
    }

    /// Gives every symbol whose `book` is overdue an empty baseline and
    /// writes it as a snapshot.
    async fn assume_empty_books(&mut self) -> Result<(), WriterGone> {
        let grace = self.cfg.empty_book_after;
        let mut overdue: Vec<String> = self
            .awaiting_book
            .iter()
            .filter(|(_, sent)| sent.elapsed() >= grace)
            .map(|(s, _)| s.clone())
            .collect();
        overdue.sort_unstable();
        for symbol in overdue {
            self.assume_empty(&symbol).await?;
        }
        Ok(())
    }

    /// Gives one symbol an empty baseline, unless it already has one. Its
    /// exchange timestamp is the local time, since the exchange sent nothing.
    async fn assume_empty(&mut self, symbol: &str) -> Result<(), WriterGone> {
        self.awaiting_book.remove(symbol);
        if self.books.has_baseline(symbol) {
            return Ok(());
        }
        let now = Utc::now();
        let applied = self
            .books
            .apply_snapshot(symbol, [], [], now, now)
            .expect("an empty book is always valid");
        inc(&self.stats.assumed_empty);
        self.note_mismatches(symbol, &applied);
        if self.cfg.verify.is_some() {
            self.to_verify.push(symbol.to_owned());
        }
        self.out.send(applied.msg).await
    }

    /// Starts one REST check of up to `batch` assumed-empty symbols. Runs in
    /// its own task, so the socket keeps being read.
    fn start_verify(&mut self, done: mpsc::UnboundedSender<Verified>) {
        let Some(verify) = self.cfg.verify.clone() else {
            return;
        };
        let n = self.to_verify.len().min(verify.batch);
        let checked: Vec<String> = self.to_verify.drain(..n).collect();
        tokio::spawn(async move {
            let request = verify
                .http
                .post(&verify.url)
                .header("Content-Type", "application/json")
                .body(polymarket::encode_books_request(&checked));
            let result = async {
                let resp = request.send().await.map_err(|e| e.to_string())?;
                let status = resp.status();
                let body = resp.text().await.map_err(|e| e.to_string())?;
                if !status.is_success() {
                    return Err(format!("HTTP {status}"));
                }
                polymarket::symbols_with_orders(&body).map_err(|e| e.to_string())
            }
            .await;
            let _ = done.send(Verified { checked, result });
        });
    }

    async fn on_verified(&mut self, ws: &mut Ws, verified: Verified) -> Result<(), Stop> {
        let with_orders = match verified.result {
            Ok(with_orders) => with_orders,
            Err(error) => {
                inc(&self.stats.verify_errors);
                debug!(conn = self.id, %error, "empty-book check failed; will retry");
                // Retry later, keeping only symbols still ours.
                let retry = verified
                    .checked
                    .into_iter()
                    .filter(|s| self.symbols.contains(s));
                self.to_verify.extend(retry);
                return Ok(());
            }
        };
        let mut wrong = Vec::new();
        for s in &verified.checked {
            let still_empty = self
                .books
                .get(s)
                .is_some_and(|b| self.books.has_baseline(s) && b.bids().is_empty() && b.asks().is_empty());
            if with_orders.contains(s) && still_empty && self.symbols.contains(s) {
                wrong.push(s.clone());
            } else if !with_orders.contains(s) {
                inc(&self.stats.empty_confirmed);
            }
        }
        if !wrong.is_empty() {
            add(&self.stats.empty_wrong, wrong.len());
            warn!(
                conn = self.id,
                symbols = wrong.len(),
                first = %wrong[0],
                "REST shows orders for books assumed empty; resyncing them"
            );
            self.resync(ws, &wrong).await?;
        }
        Ok(())
    }

    /// Unsubscribes at once (no book burst follows). Nothing to do before the
    /// socket's first subscription.
    async fn send_unsubscribe(&self, ws: &mut Ws, symbols: &[String]) -> Result<(), String> {
        if !self.initial_sent {
            return Ok(());
        }
        for chunk in symbols.chunks(self.cfg.subscribe_batch) {
            ws.send(Message::text(polymarket::encode_unsubscribe(chunk)))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
