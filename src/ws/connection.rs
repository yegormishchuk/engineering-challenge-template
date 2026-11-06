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
//! without one gets an empty baseline, and the guess is checked against the
//! batch REST endpoint. The guess must not depend on how fast we read: under
//! load a book can sit unread in the socket long after it was sent. So it is
//! made only when the stream itself proves the book is not coming:
//! - a `PONG` arrived for a `PING` sent after the subscription (the server
//!   answers in order, so a book would have come first; checked live on
//!   2026-11-06: of 1944 books for 3000 tokens, none arrived after the PONG
//!   of the PING sent right after their subscription), and at least
//!   `empty_book_after` has passed; or
//! - a change arrives for the symbol whose *exchange* timestamp is at least
//!   `empty_book_on_change_after` later than the subscription.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use indexmap::IndexSet;
use tokio::net::TcpStream;
use tokio::sync::{OwnedMutexGuard, mpsc, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::books::{Books, BooksError, SilenceConfig, SnapshotApplied};
use crate::exchange::{Event, polymarket};
use crate::writer::{OutMsg, OutSender, WriterGone};
use crate::ws::source::{ShardSource, State as SourceState};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Spaces requests evenly, shared by every connection: at full scale ~100
/// connections each start REST checks at about the same time.
#[derive(Debug)]
pub struct RateLimiter {
    interval: Duration,
    next: std::sync::Mutex<Instant>,
}

impl RateLimiter {
    pub fn per_second(n: f64) -> Arc<Self> {
        Arc::new(Self {
            interval: Duration::from_secs_f64(1.0 / n.max(0.001)),
            next: std::sync::Mutex::new(Instant::now()),
        })
    }

    /// Waits for this caller's slot.
    pub async fn acquire(&self) {
        let at = {
            let mut next = self.next.lock().expect("limiter lock");
            let at = (*next).max(Instant::now());
            *next = at + self.interval;
            at
        };
        tokio::time::sleep_until(at).await;
    }
}

/// Checks of assumed-empty books against REST. `None` in
/// [`ConnectionConfig::verify`] disables them (tests, or no network).
#[derive(Debug, Clone)]
pub struct VerifyConfig {
    pub http: reqwest::Client,
    /// Shared by all connections; see [`RateLimiter`].
    pub limiter: Arc<RateLimiter>,
    pub url: String,
    /// Symbols per request.
    pub batch: usize,
    /// At most one request in flight per connection, at most this often.
    /// After a failed request the wait doubles, up to `max_backoff`.
    pub every: Duration,
    pub max_backoff: Duration,
    /// A guess is wrong only if REST levels stay missing from the local book
    /// for at least this long: REST and the stream are not read at the same
    /// instant, and a new order can reach one a little before the other.
    pub confirm_after: Duration,
    /// After this many REST results showing orders for a book the websocket
    /// never sends, stop checking the symbol (it would otherwise resync
    /// forever).
    pub give_up_after: u32,
}

impl VerifyConfig {
    pub fn polymarket(http: reqwest::Client, requests_per_second: f64) -> Self {
        Self {
            http,
            limiter: RateLimiter::per_second(requests_per_second),
            url: polymarket::BOOKS_URL.to_owned(),
            batch: 200,
            every: Duration::from_secs(2),
            max_backoff: Duration::from_secs(60),
            confirm_after: Duration::from_secs(5),
            give_up_after: 3,
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
    /// How long any other websocket send may take; a peer that stops reading
    /// would otherwise block the session, including shutdown.
    pub send_timeout: Duration,
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
    /// Minimum wait after a subscription before its symbol may be taken to
    /// have an empty book, on top of the `PING`/`PONG` barrier. Polymarket
    /// sends no `book` for tokens without orders (REST answers 404 for
    /// them); their first order arrives as a `price_change`, which needs a
    /// baseline. Books arrive within 0.74 s of subscribing (p99, 2000 tokens).
    pub empty_book_after: Duration,
    /// A change whose exchange timestamp is this much later than the
    /// subscription of a symbol still waiting for its `book` means the book
    /// is empty; the change is applied to an empty baseline at once. Exchange
    /// time, not read time, so a lagging reader cannot trigger it early.
    pub empty_book_on_change_after: Duration,
    pub verify: Option<VerifyConfig>,
    /// How often a redundant member checks for lost messages and lag.
    pub shard_check_every: Duration,
    /// When a symbol counts as gone quiet and is resubscribed (Extension 2).
    pub silence: SilenceConfig,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: polymarket::WS_URL.to_owned(),
            ping_interval: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(2),
            send_timeout: Duration::from_secs(10),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            stable_after: Duration::from_secs(30),
            subscribe_batch: 100,
            subscribe_pace: Duration::from_millis(250),
            empty_book_after: Duration::from_secs(5),
            empty_book_on_change_after: Duration::from_millis(1500),
            verify: None,
            shard_check_every: Duration::from_millis(500),
            silence: SilenceConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Reconnect now, resubscribing everything (Extension 2): clears any
    /// state built up on the exchange's side of the socket.
    Recycle,
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
    /// Symbols no longer checked after `give_up_after` wrong results.
    pub empty_gave_up: AtomicU64,
    pub verify_errors: AtomicU64,
    /// Book messages dropped because their symbol was being resynced.
    pub dropped_during_resync: AtomicU64,
    /// Bytes of text frames received.
    pub bytes: AtomicU64,
    /// Time spent handling frames (decode, books, queueing output), in µs.
    pub busy_us: AtomicU64,
    /// Longest single frame handling time, in µs.
    pub max_frame_us: AtomicU64,
    /// Snapshots written because this connection became its shard's source.
    pub switch_snapshots: AtomicU64,
    /// Symbols without a live book when this connection became the source:
    /// nothing is written for them until their next book.
    pub switch_without_baseline: AtomicU64,
    /// Time spent waiting for the shard's output lock, total and longest, µs.
    pub lock_wait_us: AtomicU64,
    pub max_lock_wait_us: AtomicU64,
    /// Active symbols resubscribed because they went quiet.
    pub silent_resyncs: AtomicU64,
    /// Gauges, as of the last check: symbols counted as active, and live
    /// baselines (books that changes apply to).
    pub active_symbols: AtomicU64,
    pub live_baselines: AtomicU64,
    /// Reconnects ordered by `Command::Recycle`, and ones skipped (the
    /// source had no ready peer to hand over to, or no session was open).
    pub recycles: AtomicU64,
    pub recycles_skipped: AtomicU64,
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
    result: Result<HashMap<String, polymarket::RestBook>, String>,
}

/// `now`, or `last` if the clock went back.
fn monotonic(now: DateTime<Utc>, last: DateTime<Utc>) -> DateTime<Utc> {
    now.max(last)
}

/// Levels as `(side is bid, price, quantity)`, for comparing books.
type LevelSet = HashSet<(bool, i64, i64)>;

fn level_set(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> LevelSet {
    bids.iter()
        .map(|&(p, q)| (true, p, q))
        .chain(asks.iter().map(|&(p, q)| (false, p, q)))
        .collect()
}

/// This connection's place in a shard of redundant connections.
#[derive(Debug, Clone)]
pub struct Member {
    pub index: usize,
    pub shard: Arc<ShardSource>,
}

pub struct Connection {
    id: usize,
    /// `None`: the only connection for its symbols, always writes.
    member: Option<Member>,
    /// The shard's switch epoch this connection last wrote under.
    seen_epoch: u64,
    /// Receive instant of the frame being handled, for hash tracking.
    frame_at: std::time::Instant,
    /// The latest output timestamp taken; see [`Self::stamp`].
    last_stamp: DateTime<Utc>,
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
    /// Symbols subscribed on the current socket that have no `book` yet: when
    /// the subscription was sent (monotonic, and wall clock for comparing
    /// with exchange timestamps) and the number of its barrier `PING`.
    awaiting_book: HashMap<String, (Instant, DateTime<Utc>, u64)>,
    /// `PING`s are numbered, so a barrier is identified exactly (two sends
    /// can share an `Instant`). The next number to use; never reset.
    next_ping: u64,
    /// Numbers of `PING`s not yet answered, oldest first.
    pings: VecDeque<(u64, std::time::Instant)>,
    /// Number of the newest `PING` answered by a `PONG`.
    acked_through: Option<u64>,
    /// When the newest answered `PING` was sent.
    acked_sent_at: Option<std::time::Instant>,
    /// Symbols unsubscribed for a resync, with the number of the `PING` that
    /// followed the unsubscribe. Until that `PING` is answered, data for them
    /// may still be from before the unsubscribe, so it is dropped; the new
    /// subscription's book comes after the `PONG`.
    resyncing: HashMap<String, u64>,
    /// Symbols whose current baseline is an empty guess, not an exchange book.
    assumed: HashSet<String>,
    /// Assumed-empty symbols waiting for a REST check.
    to_verify: Vec<String>,
    /// REST results waiting for the stream to be read past the moment they
    /// arrived: each with the send time of the `PING` sent then. Judged only
    /// once that `PING` is answered, so a lagging reader cannot make a
    /// correct guess look wrong.
    verify_waiting: VecDeque<(u64, Verified)>,
    /// Symbols unsubscribed on this socket, with the barrier `PING` sent
    /// after the unsubscribe. If one is added back before that `PING` is
    /// answered, data from the old subscription is dropped like in a resync.
    recently_removed: HashMap<String, u64>,
    /// REST levels missing from the local book, per symbol, since when. A
    /// guess is wrong only if some of the same levels are still missing
    /// `confirm_after` later.
    suspect: HashMap<String, (Instant, LevelSet)>,
    /// Wrong empty guesses per symbol, and symbols no longer checked.
    wrong_guesses: HashMap<String, u32>,
    gave_up: HashSet<String>,
    verify_not_before: Instant,
    verify_backoff: Duration,
}

enum SessionEnd {
    Shutdown,
    Lost(String),
    /// Closed on purpose by `Command::Recycle`; reconnect at once.
    Recycle,
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
            member: None,
            seen_epoch: 0,
            frame_at: std::time::Instant::now(),
            last_stamp: DateTime::<Utc>::MIN_UTC,
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
            next_ping: 0,
            pings: VecDeque::new(),
            acked_through: None,
            acked_sent_at: None,
            resyncing: HashMap::new(),
            assumed: HashSet::new(),
            to_verify: Vec::new(),
            suspect: HashMap::new(),
            verify_waiting: VecDeque::new(),
            recently_removed: HashMap::new(),
            wrong_guesses: HashMap::new(),
            gave_up: HashSet::new(),
            verify_not_before: Instant::now(),
            verify_backoff: Duration::ZERO,
        }
    }

    /// The wall-clock time for output, never earlier than the previous one
    /// taken on this connection. The host clock can step back (WSL2 resyncs
    /// with Windows: a 386 ms step was seen live), and a symbol's receive
    /// times must not go backwards.
    fn stamp(&mut self) -> DateTime<Utc> {
        self.last_stamp = monotonic(Utc::now(), self.last_stamp);
        self.last_stamp
    }

    /// Makes this connection one of a shard's redundant members.
    pub fn with_member(mut self, member: Member) -> Self {
        self.member = Some(member);
        self
    }

    /// Takes the shard's output lock and decides whether this connection
    /// writes. `None`: not the source, drop the output. `Some(guard)`: write
    /// while holding the guard (no guard without a shard). A connection that
    /// has just become the source first writes a snapshot of every symbol
    /// with a live book.
    ///
    /// `at` stamps the switch snapshot (before the floor, see [`Self::write`]).
    async fn begin_output(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<Option<Option<OwnedMutexGuard<SourceState>>>, WriterGone> {
        let Some(member) = &self.member else {
            return Ok(Some(None));
        };
        let (shard, me) = (Arc::clone(&member.shard), member.index);
        let waiting = std::time::Instant::now();
        let mut st = shard.lock_owned().await;
        let us = waiting.elapsed().as_micros() as u64;
        self.stats.lock_wait_us.fetch_add(us, Ordering::Relaxed);
        self.stats.max_lock_wait_us.fetch_max(us, Ordering::Relaxed);
        if !shard.take_over_if_source_down(&mut st, me) {
            return Ok(None);
        }
        if st.epoch != self.seen_epoch {
            self.seen_epoch = st.epoch;
            let mut written = 0;
            for msg in self.books.minute_snapshots(at) {
                self.write(Some(&mut st), msg).await?;
                written += 1;
            }
            add(&self.stats.switch_snapshots, written);
            add(&self.stats.switch_without_baseline, self.symbols.len().saturating_sub(written));
        }
        Ok(Some(Some(st)))
    }

    /// Sends one message. As a shard member (holding the lock), raises its
    /// receive time to the floor set at the last switch and records it.
    async fn write(&self, st: Option<&mut SourceState>, mut msg: OutMsg) -> Result<(), WriterGone> {
        if let Some(st) = st {
            let t = match &mut msg {
                OutMsg::Snapshot(s) => &mut s.received_at,
                OutMsg::Update(u) => &mut u.backtest_timestamp,
            };
            if let Some(floor) = st.floor {
                *t = (*t).max(floor);
            }
            st.last_written = Some(st.last_written.map_or(*t, |l| l.max(*t)));
        }
        self.out.send(msg).await
    }

    /// Writes one message if this connection is its shard's source.
    async fn emit(&mut self, msg: OutMsg) -> Result<(), WriterGone> {
        let at = match &msg {
            OutMsg::Snapshot(s) => s.received_at,
            OutMsg::Update(u) => u.backtest_timestamp,
        };
        let Some(mut guard) = self.begin_output(at).await? else {
            return Ok(());
        };
        self.write(guard.as_deref_mut(), msg).await
    }

    /// Records an applied hash with the shard (redundant members only).
    fn observe(&self, symbol: &str, hash: Option<&str>, lag_sample: bool) {
        if let (Some(m), Some(hash)) = (&self.member, hash) {
            m.shard.observe(m.index, symbol, hash, self.frame_at, lag_sample);
        }
    }

    /// Resyncs symbols a peer received changes for that this connection
    /// missed, and lets the shard hand over if this is a lagging or lossy
    /// source.
    async fn check_shard(&mut self, ws: &mut Ws) -> Result<(), Stop> {
        let Some(m) = self.member.clone() else {
            return Ok(());
        };
        // Keep a recent barrier: loss is judged against the last answered
        // PING (see `ShardSource::check_lost`).
        if self.pings.is_empty() {
            self.send_ping(ws).await.map_err(Stop::Lost)?;
        }
        let Some(cutoff) = self.acked_sent_at else {
            return Ok(());
        };
        let lost = m.shard.check_lost(m.index, cutoff, |s| self.books.live_since(s));
        m.shard
            .review(m.index, std::time::Instant::now(), !lost.is_empty())
            .await;
        if !lost.is_empty() {
            warn!(
                conn = self.id,
                symbols = lost.len(),
                first = %lost[0],
                "a peer received changes this connection did not; resyncing them"
            );
            self.resync(ws, &lost).await?;
        }
        Ok(())
    }

    /// Whether this connection may close for a recycle now; counts a skip.
    /// With redundancy the source hands over first, to a peer with books
    /// for at least 99% of the symbols.
    async fn release_for_recycle(&self) -> bool {
        let Some(m) = &self.member else {
            return true;
        };
        let ready = (self.symbols.len() * 99).div_ceil(100);
        if m.shard.release_for_recycle(m.index, ready).await {
            return true;
        }
        inc(&self.stats.recycles_skipped);
        info!(conn = self.id, "recycle skipped: no ready peer to take over");
        false
    }

    fn report_baselines(&self) {
        let live = self.books.live_len();
        self.stats.live_baselines.store(live as u64, Ordering::Relaxed);
        if let Some(m) = &self.member {
            m.shard.set_baselines(m.index, live);
        }
    }

    /// Resubscribes active symbols that went quiet (Extension 2): the
    /// exchange can stop sending a subscription without telling us.
    async fn check_silence(&mut self, ws: &mut Ws) -> Result<(), Stop> {
        let (mut silent, active) = self.books.silent(std::time::Instant::now(), &self.cfg.silence);
        self.stats.active_symbols.store(active as u64, Ordering::Relaxed);
        silent.truncate(self.cfg.silence.max_per_check);
        if silent.is_empty() {
            return Ok(());
        }
        add(&self.stats.silent_resyncs, silent.len());
        warn!(
            conn = self.id,
            symbols = silent.len(),
            first = %silent[0],
            "symbols went quiet; resubscribing"
        );
        self.resync(ws, &silent).await
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
                    if let Some(m) = &self.member {
                        m.shard.connected(m.index).await;
                    }
                    inc(&self.stats.connects);
                    // The local port identifies the socket (`ss`, packet captures).
                    let local_port = ws.get_ref().get_ref().local_addr().ok().map(|a| a.port());
                    info!(conn = self.id, symbols = self.symbols.len(), ?local_port, "connected");
                    let started = Instant::now();
                    let end = self.session(ws, &mut cmds, &mut ticks, &shutdown).await;
                    // The next session starts from fresh exchange books; the
                    // old ones stay only for comparison.
                    self.books.mark_all_stale();
                    self.to_verify.clear();
                    self.report_baselines();
                    match end {
                        Ok(SessionEnd::Shutdown) => return Ok(()),
                        Ok(SessionEnd::Recycle) => {
                            if let Some(m) = &self.member {
                                m.shard.lost(m.index).await;
                            }
                            inc(&self.stats.recycles);
                            info!(conn = self.id, "recycled; reconnecting");
                            continue;
                        }
                        Ok(SessionEnd::Lost(reason)) => {
                            // Hand over before logging, so the peer can
                            // write while this one reconnects.
                            if let Some(m) = &self.member {
                                m.shard.lost(m.index).await;
                            }
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
                    self.forget(s);
                }
            }
            // No session to recycle; counted so the manager does not wait.
            Command::Recycle => inc(&self.stats.recycles_skipped),
        }
    }

    /// Drops everything known about a symbol that is no longer ours.
    fn forget(&mut self, s: &str) -> bool {
        self.books.remove(s);
        self.awaiting_book.remove(s);
        self.resyncing.remove(s);
        self.assumed.remove(s);
        self.suspect.remove(s);
        self.wrong_guesses.remove(s);
        self.gave_up.remove(s);
        self.symbols.shift_remove(s)
    }

    async fn ws_send(&self, ws: &mut Ws, text: String) -> Result<(), String> {
        match tokio::time::timeout(self.cfg.send_timeout, ws.send(Message::text(text))).await {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => Err(format!("websocket send timed out after {:?}", self.cfg.send_timeout)),
        }
    }

    /// Sends a `PING` and returns its number.
    async fn send_ping(&mut self, ws: &mut Ws) -> Result<u64, String> {
        self.ws_send(ws, polymarket::PING.to_owned()).await?;
        let n = self.next_ping;
        self.next_ping += 1;
        self.pings.push_back((n, std::time::Instant::now()));
        Ok(n)
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
        self.pings.clear();
        self.acked_through = None;
        self.acked_sent_at = None;
        // Everything is resubscribed from scratch; old guesses are stale.
        self.resyncing.clear();
        self.assumed.clear();
        self.suspect.clear();
        self.verify_waiting.clear();
        self.recently_removed.clear();
        let empty_every = self
            .cfg
            .empty_book_after
            .clamp(Duration::from_millis(10), Duration::from_secs(1));
        let mut empty_check = tokio::time::interval(empty_every);
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
        let mut shard_check = tokio::time::interval(self.cfg.shard_check_every);
        shard_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut silence_check = tokio::time::interval(self.cfg.silence.check_every);
        silence_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.close(&mut ws).await;
                    return Ok(SessionEnd::Shutdown);
                }
                frame = ws.next() => {
                    // Stamp first, before any parsing.
                    let received_at = self.stamp();
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
                    self.frame_at = busy;
                    // Settle who writes before applying the frame, so a
                    // switch snapshot shows the books as they were before it.
                    if self.member.is_some() {
                        drop(self.begin_output(received_at).await?);
                    }
                    let result = self.on_text(&mut ws, &text, received_at, &mut last_pong).await;
                    self.report_baselines();
                    let us = busy.elapsed().as_micros() as u64;
                    self.stats.busy_us.fetch_add(us, Ordering::Relaxed);
                    self.stats.max_frame_us.fetch_max(us, Ordering::Relaxed);
                    match result {
                        Ok(()) => {}
                        Err(Stop::Lost(reason)) => return Ok(SessionEnd::Lost(reason)),
                        Err(Stop::WriterGone) => return Err(WriterGone),
                    }
                }
                _ = shard_check.tick(), if self.member.is_some() => {
                    match self.check_shard(&mut ws).await {
                        Ok(()) => {}
                        Err(Stop::Lost(reason)) => return Ok(SessionEnd::Lost(reason)),
                        Err(Stop::WriterGone) => return Err(WriterGone),
                    }
                }
                _ = silence_check.tick() => {
                    match self.check_silence(&mut ws).await {
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
                _ = verify_tick.tick(), if !verify_in_flight
                    && !self.to_verify.is_empty()
                    && self.cfg.verify.is_some()
                    && Instant::now() >= self.verify_not_before => {
                    verify_in_flight = true;
                    self.start_verify(verified_tx.clone());
                }
                Some(verified) = verified_rx.recv() => {
                    verify_in_flight = false;
                    let barrier = match self.send_ping(&mut ws).await {
                        Ok(n) => n,
                        Err(reason) => return Ok(SessionEnd::Lost(reason)),
                    };
                    self.verify_waiting.push_back((barrier, verified));
                }
                _ = ping.tick() => {
                    if last_pong.elapsed() > self.cfg.pong_timeout {
                        return Ok(SessionEnd::Lost(format!(
                            "no PONG for {:?}", last_pong.elapsed()
                        )));
                    }
                    if let Err(reason) = self.send_ping(&mut ws).await {
                        return Ok(SessionEnd::Lost(reason));
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
                                    if let Some(&barrier) = self.recently_removed.get(&s) {
                                        self.resyncing.insert(s.clone(), barrier);
                                    }
                                    self.pending.push_back(s);
                                }
                            }
                            Ok(())
                        }
                        Some(Command::Recycle) => {
                            if self.release_for_recycle().await {
                                self.close(&mut ws).await;
                                return Ok(SessionEnd::Recycle);
                            }
                            Ok(())
                        }
                        Some(Command::Remove(symbols)) => {
                            let gone: Vec<String> = symbols
                                .into_iter()
                                .filter(|s| self.forget(s))
                                .collect();
                            let gone_set: HashSet<&String> = gone.iter().collect();
                            self.pending.retain(|p| !gone_set.contains(p));
                            self.to_verify.retain(|p| !gone_set.contains(p));
                            self.unsubscribe_removed(&mut ws, &gone).await
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
                    let emitted_at = self.stamp();
                    if let Some(mut guard) = self.begin_output(emitted_at).await? {
                        for msg in self.books.minute_snapshots(emitted_at) {
                            self.write(guard.as_deref_mut(), msg).await?;
                            inc(&self.stats.minute_snapshots);
                        }
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
        // Resync the symbols of failed messages first, so no other message of
        // the same frame is applied to a book already known to be incomplete.
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
        for event in decoded.events {
            self.on_event(ws, event, received_at, last_pong).await?;
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
                hash,
            } => {
                if !self.symbols.contains(&symbol) {
                    return Ok(()); // late message for a removed symbol
                }
                if self.resyncing.contains_key(&symbol) {
                    inc(&self.stats.dropped_during_resync);
                    return Ok(());
                }
                // A book answering our own subscription (vs. one sent
                // mid-stream, which every member receives).
                let subscribed = self.awaiting_book.remove(&symbol).is_some();
                // A real exchange book settles any empty guess.
                self.assumed.remove(&symbol);
                self.suspect.remove(&symbol);
                self.wrong_guesses.remove(&symbol);
                self.gave_up.remove(&symbol);
                match self.books.apply_snapshot(&symbol, bids, asks, exchange_ts, received_at) {
                    Ok(applied) => {
                        inc(&self.stats.books);
                        self.books.note_activity(&symbol, self.frame_at, &self.cfg.silence);
                        if subscribed {
                            if let (Some(m), Some(hash)) = (&self.member, hash.as_deref()) {
                                m.shard.observe_existing(m.index, hash, self.frame_at);
                            }
                        } else {
                            self.observe(&symbol, hash.as_deref(), false);
                        }
                        self.note_mismatches(&symbol, &applied);
                        self.emit(applied.msg).await?;
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
                hash,
            } => {
                if !self.symbols.contains(&symbol) {
                    return Ok(());
                }
                if self.resyncing.contains_key(&symbol) {
                    inc(&self.stats.dropped_during_resync);
                    return Ok(());
                }
                if !self.books.has_baseline(&symbol) {
                    let margin = chrono::TimeDelta::from_std(self.cfg.empty_book_on_change_after)
                        .unwrap_or(chrono::TimeDelta::MAX);
                    let overdue = self
                        .awaiting_book
                        .get(&symbol)
                        .is_some_and(|(_, sent_at, _)| exchange_ts >= *sent_at + margin);
                    if !overdue {
                        inc(&self.stats.skipped_no_baseline);
                        return Ok(());
                    }
                    // Stamped with this frame's receive time, so the baseline
                    // never comes after the change that depends on it.
                    self.assume_empty(&symbol, received_at).await?;
                }
                match self.books.apply_level(&symbol, side, price, quantity, exchange_ts, received_at) {
                    Ok(msg) => {
                        inc(&self.stats.levels);
                        self.books.note_activity(&symbol, self.frame_at, &self.cfg.silence);
                        self.observe(&symbol, hash.as_deref(), true);
                        self.emit(msg).await?;
                    }
                    Err(BooksError::NoBaseline { .. }) => inc(&self.stats.skipped_no_baseline),
                    Err(error) => {
                        warn!(conn = self.id, %error, "invalid level change; resyncing");
                        self.resync(ws, std::slice::from_ref(&symbol)).await?;
                    }
                }
            }
            Event::Pong => {
                *last_pong = Instant::now();
                if let Some((acked, sent_at)) = self.pings.pop_front() {
                    self.acked_through = Some(acked);
                    self.acked_sent_at = Some(sent_at);
                    // Resyncs and removals whose old data has now all been read.
                    self.resyncing.retain(|_, barrier| *barrier > acked);
                    self.recently_removed.retain(|_, barrier| *barrier > acked);
                    // REST results the stream has now caught up with.
                    while self.verify_waiting.front().is_some_and(|(barrier, _)| *barrier <= acked) {
                        let (_, verified) = self.verify_waiting.pop_front().expect("checked");
                        Box::pin(self.on_verified(ws, verified)).await?;
                    }
                    // The barrier just moved: books still missing now are
                    // not coming.
                    self.assume_empty_books().await?;
                }
            }
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
        let set: HashSet<&String> = symbols.iter().collect();
        for s in symbols {
            self.books.mark_stale(s);
            self.awaiting_book.remove(s);
            self.assumed.remove(s);
            self.suspect.remove(s);
        }
        self.to_verify.retain(|p| !set.contains(p));
        if self.initial_sent {
            self.send_unsubscribe(ws, symbols).await.map_err(Stop::Lost)?;
            // Barrier: once answered, nothing from before the unsubscribe
            // is left unread.
            let barrier = self.send_ping(ws).await.map_err(Stop::Lost)?;
            for s in symbols {
                self.resyncing.insert(s.clone(), barrier);
            }
        }
        let queued: HashSet<String> = self.pending.iter().cloned().collect();
        for s in symbols {
            if !queued.contains(s) {
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
        let (now, now_utc) = (Instant::now(), Utc::now());
        // The PING sent right after this batch is its barrier.
        let barrier = self.next_ping;
        for s in &batch {
            self.awaiting_book.insert(s.clone(), (now, now_utc, barrier));
        }
        let msg = if self.initial_sent {
            polymarket::encode_subscribe(&batch)
        } else {
            polymarket::encode_initial_subscribe(&batch)
        };
        self.initial_sent = true;
        self.ws_send(ws, msg).await?;
        // The barrier: once this PING is answered, every book for the batch
        // has been delivered (see the module docs).
        let sent = self.send_ping(ws).await?;
        debug_assert_eq!(sent, barrier);
        Ok(())
    }

    /// Gives every symbol whose `book` is overdue an empty baseline and
    /// writes it as a snapshot.
    async fn assume_empty_books(&mut self) -> Result<(), WriterGone> {
        let grace = self.cfg.empty_book_after;
        let Some(acked) = self.acked_through else {
            return Ok(());
        };
        let mut overdue: Vec<String> = self
            .awaiting_book
            .iter()
            .filter(|(_, (sent, _, barrier))| sent.elapsed() >= grace && acked >= *barrier)
            .map(|(s, _)| s.clone())
            .collect();
        overdue.sort_unstable();
        for symbol in overdue {
            let at = self.stamp();
            self.assume_empty(&symbol, at).await?;
        }
        Ok(())
    }

    /// Gives one symbol an empty baseline, unless it already has one, stamped
    /// `at` (both timestamps: the exchange sent nothing).
    async fn assume_empty(&mut self, symbol: &str, at: DateTime<Utc>) -> Result<(), WriterGone> {
        self.awaiting_book.remove(symbol);
        if self.books.has_baseline(symbol) {
            return Ok(());
        }
        let applied = self
            .books
            .apply_snapshot(symbol, [], [], at, at)
            .expect("an empty book is always valid");
        inc(&self.stats.assumed_empty);
        // Like an exchange book: restarts the symbol's silence clock (a
        // market resynced for silence that has no orders gets this instead
        // of a book, and would otherwise look silent again at once).
        self.books.note_activity(symbol, std::time::Instant::now(), &self.cfg.silence);
        self.assumed.insert(symbol.to_owned());
        self.note_mismatches(symbol, &applied);
        if self.cfg.verify.is_some() && !self.gave_up.contains(symbol) {
            self.to_verify.push(symbol.to_owned());
        }
        self.emit(applied.msg).await
    }

    /// Starts one REST check of up to `batch` assumed-empty symbols. Runs in
    /// its own task, so the socket keeps being read.
    fn start_verify(&mut self, done: mpsc::UnboundedSender<Verified>) {
        let Some(verify) = self.cfg.verify.clone() else {
            return;
        };
        let n = self.to_verify.len().min(verify.batch);
        let checked: Vec<String> = self.to_verify.drain(..n).collect();
        let body = polymarket::encode_books_request(&checked);
        // The work runs in an inner task so that the outer one always
        // reports back, even if the work panics; otherwise `verify_in_flight`
        // would stay set and checks would stop for the session.
        tokio::spawn(async move {
            let work = tokio::spawn(async move {
                verify.limiter.acquire().await;
                let resp = verify
                    .http
                    .post(&verify.url)
                    .header("Content-Type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                let status = resp.status();
                let body = resp.text().await.map_err(|e| e.to_string())?;
                if !status.is_success() {
                    return Err(format!("HTTP {status}"));
                }
                polymarket::parse_books_response(&body).map_err(|e| e.to_string())
            });
            let result = match work.await {
                Ok(result) => result,
                Err(e) => Err(format!("check task failed: {e}")),
            };
            let _ = done.send(Verified { checked, result });
        });
    }

    async fn on_verified(&mut self, ws: &mut Ws, verified: Verified) -> Result<(), Stop> {
        let Some(verify) = self.cfg.verify.clone() else {
            return Ok(());
        };
        let rest_books = match verified.result {
            Ok(books) => {
                self.verify_backoff = Duration::ZERO;
                books
            }
            Err(error) => {
                inc(&self.stats.verify_errors);
                self.verify_backoff = (self.verify_backoff * 2).clamp(verify.every, verify.max_backoff);
                self.verify_not_before = Instant::now() + self.verify_backoff;
                debug!(conn = self.id, %error, backoff = ?self.verify_backoff, "empty-book check failed; will retry");
                // Retry later, keeping only symbols still assumed empty.
                let retry = verified.checked.into_iter().filter(|s| self.assumed.contains(s));
                self.to_verify.extend(retry);
                return Ok(());
            }
        };
        let mut wrong = Vec::new();
        let mut recheck = Vec::new();
        for s in verified.checked {
            // Only symbols whose baseline is still the guess: a real exchange
            // book since then has settled it either way.
            if !self.assumed.contains(&s) {
                continue;
            }
            // REST levels the local book (the guess plus any changes applied
            // to it) does not have. Orders placed after the guess reach both,
            // so they do not count; orders that rested before it reach only
            // REST.
            let missing: LevelSet = match (rest_books.get(&s), self.books.get(&s)) {
                (None, _) => LevelSet::new(),
                (Some(rest), local) => {
                    let local = local.map_or_else(LevelSet::new, |b| {
                        level_set(
                            &b.bids().iter().map(|(&p, &q)| (p, q)).collect::<Vec<_>>(),
                            &b.asks().iter().map(|(&p, &q)| (p, q)).collect::<Vec<_>>(),
                        )
                    });
                    level_set(&rest.bids, &rest.asks).difference(&local).copied().collect()
                }
            };
            if missing.is_empty() {
                self.suspect.remove(&s);
                inc(&self.stats.empty_confirmed);
                continue;
            }
            // Wrong only if some of the same levels stay missing for
            // `confirm_after`: a level that just changed can be in one source
            // and not yet in the other.
            let (since, still) = match self.suspect.remove(&s) {
                Some((since, before)) => {
                    let still: LevelSet = before.intersection(&missing).copied().collect();
                    if still.is_empty() { (Instant::now(), missing) } else { (since, still) }
                }
                None => (Instant::now(), missing),
            };
            if since.elapsed() < verify.confirm_after {
                self.suspect.insert(s.clone(), (since, still));
                recheck.push(s);
                continue;
            }
            inc(&self.stats.empty_wrong);
            let n = self.wrong_guesses.entry(s.clone()).or_default();
            *n += 1;
            if *n >= verify.give_up_after {
                inc(&self.stats.empty_gave_up);
                warn!(
                    conn = self.id,
                    symbol = %s,
                    "REST keeps showing orders but the websocket sends no book; no longer checking"
                );
                self.gave_up.insert(s);
            } else {
                wrong.push(s);
            }
        }
        self.to_verify.extend(recheck);
        if !wrong.is_empty() {
            warn!(
                conn = self.id,
                symbols = wrong.len(),
                first = %wrong[0],
                "REST shows resting orders missing from books assumed empty; resyncing them"
            );
            self.resync(ws, &wrong).await?;
        }
        Ok(())
    }

    /// Unsubscribes removed symbols and sends a barrier `PING`, remembered
    /// in case a symbol is added back before old data is drained.
    async fn unsubscribe_removed(&mut self, ws: &mut Ws, gone: &[String]) -> Result<(), String> {
        if gone.is_empty() || !self.initial_sent {
            return Ok(());
        }
        self.send_unsubscribe(ws, gone).await?;
        let barrier = self.send_ping(ws).await?;
        for s in gone {
            self.recently_removed.insert(s.clone(), barrier);
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
            self.ws_send(ws, polymarket::encode_unsubscribe(chunk)).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
