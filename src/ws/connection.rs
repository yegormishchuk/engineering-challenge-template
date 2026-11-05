//! One websocket connection to the Polymarket market channel.
//!
//! A connection owns the books of its symbols and is the only writer of
//! their messages, so books and output stay consistent without locks. It
//! keeps the socket alive with `PING`, reconnects with backoff, and recovers
//! a symbol whenever its book can no longer be trusted:
//!
//! | Trigger | Recovery |
//! | --- | --- |
//! | socket closed, error, or no `PONG` within `pong_timeout` | reconnect; drop all books; resubscribe everything |
//! | frame that does not decode | drop all books; unsubscribe + subscribe everything |
//! | invalid level change for a symbol | drop that book; unsubscribe + subscribe the symbol |
//!
//! While a symbol has no book, its level changes are skipped (and counted):
//! a reader would have no baseline to apply them to. The next `book` the
//! exchange sends becomes the new baseline and is written as a snapshot.
//! Polymarket sends no `book` for a token without orders, so a symbol still
//! without one `empty_book_after` its subscription gets an empty baseline.

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

use crate::books::{Books, BooksError};
use crate::exchange::{Event, polymarket};
use crate::writer::{OutSender, WriterGone};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub url: String,
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
    pub connect_timeout: Duration,
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
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: polymarket::WS_URL.to_owned(),
            ping_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            stable_after: Duration::from_secs(30),
            subscribe_batch: 100,
            subscribe_pace: Duration::from_millis(250),
            empty_book_after: Duration::from_secs(5),
            empty_book_on_change_after: Duration::from_millis(1500),
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
    /// Exchange books that disagreed with the local book (a missed change).
    pub mismatched_books: AtomicU64,
    pub mismatched_levels: AtomicU64,
    /// Level changes skipped because the symbol had no baseline book.
    pub skipped_no_baseline: AtomicU64,
    pub decode_errors: AtomicU64,
    pub resyncs: AtomicU64,
    pub ignored: AtomicU64,
    pub minute_snapshots: AtomicU64,
    /// Symbols given an empty baseline because no `book` arrived in time.
    pub assumed_empty: AtomicU64,
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

pub struct Connection {
    id: usize,
    cfg: ConnectionConfig,
    /// In the order they were added, so a market's tokens (added together)
    /// stay in the same subscription batch after a reconnect.
    symbols: IndexSet<String>,
    books: Books,
    out: OutSender,
    stats: Arc<ConnStats>,
    /// Where `new_market` / `market_resolved` go; only one connection gets a
    /// sender, since these events are broadcast to every socket.
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
                    // A new session starts from fresh exchange books.
                    self.books = Books::new();
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

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    let _ = ws.close(None).await;
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
                    self.stats.bytes.fetch_add(text.len() as u64, Ordering::Relaxed);
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
                    self.assume_empty_books()?;
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
                            let _ = ws.close(None).await;
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
                        self.out.send(msg)?;
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
        let events = match polymarket::decode(text) {
            Ok(events) => events,
            Err(error) => {
                inc(&self.stats.decode_errors);
                let sample: String = text.chars().take(200).collect();
                warn!(conn = self.id, %error, frame = %sample, "undecodable frame; resyncing all symbols");
                let all: Vec<String> = self.symbols.iter().cloned().collect();
                return self.resync(ws, &all).await;
            }
        };
        for event in events {
            match event {
                Event::Book {
                    symbol,
                    bids,
                    asks,
                    exchange_ts,
                    ..
                } => {
                    if !self.symbols.contains(&symbol) {
                        continue; // late message for a removed symbol
                    }
                    self.awaiting_book.remove(&symbol);
                    match self.books.apply_snapshot(&symbol, bids, asks, exchange_ts, received_at) {
                        Ok(applied) => {
                            inc(&self.stats.books);
                            if let Some(mismatches) = &applied.mismatches
                                && !mismatches.is_empty()
                            {
                                inc(&self.stats.mismatched_books);
                                self.stats
                                    .mismatched_levels
                                    .fetch_add(mismatches.len() as u64, Ordering::Relaxed);
                                warn!(
                                    conn = self.id,
                                    %symbol,
                                    levels = mismatches.len(),
                                    first = ?mismatches.first(),
                                    "exchange book differs from local book"
                                );
                            }
                            self.out.send(applied.msg)?;
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
                        continue;
                    }
                    match self.books.apply_level(&symbol, side, price, quantity, exchange_ts, received_at) {
                        Ok(msg) => {
                            inc(&self.stats.levels);
                            self.out.send(msg)?;
                        }
                        Err(BooksError::NoBaseline { .. }) => {
                            let overdue = self
                                .awaiting_book
                                .get(&symbol)
                                .is_some_and(|sent| sent.elapsed() >= self.cfg.empty_book_on_change_after);
                            if !overdue {
                                inc(&self.stats.skipped_no_baseline);
                                continue;
                            }
                            self.assume_empty(&symbol)?;
                            let msg = self
                                .books
                                .apply_level(&symbol, side, price, quantity, exchange_ts, received_at);
                            match msg {
                                Ok(msg) => {
                                    inc(&self.stats.levels);
                                    self.out.send(msg)?;
                                }
                                Err(error) => {
                                    warn!(conn = self.id, %error, "invalid level change; resyncing");
                                    self.resync(ws, std::slice::from_ref(&symbol)).await?;
                                }
                            }
                        }
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
                Event::Ignored { .. } => inc(&self.stats.ignored),
            }
        }
        Ok(())
    }

    /// Drops the books of `symbols` and asks the exchange for fresh ones:
    /// unsubscribe now, subscribe again through the paced queue.
    async fn resync(&mut self, ws: &mut Ws, symbols: &[String]) -> Result<(), Stop> {
        inc(&self.stats.resyncs);
        for s in symbols {
            self.books.remove(s);
            self.awaiting_book.remove(s);
        }
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
    /// writes it as a snapshot. Its exchange timestamp is the local time,
    /// since the exchange sent nothing.
    fn assume_empty_books(&mut self) -> Result<(), WriterGone> {
        let grace = self.cfg.empty_book_after;
        let mut overdue: Vec<String> = self
            .awaiting_book
            .iter()
            .filter(|(_, sent)| sent.elapsed() >= grace)
            .map(|(s, _)| s.clone())
            .collect();
        overdue.sort_unstable();
        for symbol in overdue {
            self.assume_empty(&symbol)?;
        }
        Ok(())
    }

    /// Gives one symbol an empty baseline, unless it already has a book.
    fn assume_empty(&mut self, symbol: &str) -> Result<(), WriterGone> {
        self.awaiting_book.remove(symbol);
        if self.books.contains(symbol) {
            return Ok(());
        }
        let now = Utc::now();
        let applied = self
            .books
            .apply_snapshot(symbol, [], [], now, now)
            .expect("an empty book is always valid");
        inc(&self.stats.assumed_empty);
        self.out.send(applied.msg)
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
