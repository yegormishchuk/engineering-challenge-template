//! Redundant connections (Extension 1): which connection of a shard writes.
//!
//! A shard is R connections subscribed to the same symbols, each with its own
//! books. Exactly one, the **source**, writes output; the others keep their
//! books current and drop their messages. Every message a member wants to
//! write goes through [`ShardSource::lock`], held while it checks whether it
//! is the source and sends, so a switch always falls between two messages,
//! never inside one.
//!
//! After a switch the new source first writes a snapshot of every symbol it
//! has a live book for (the members' books may differ), then its own
//! messages. It notices the switch through `epoch` at its next frame.
//!
//! **Detection.** Every book and change carries a `hash` of the resulting
//! book state, and all members receive the same hashes. Members record each
//! hash they apply ([`ShardSource::observe`]). A hash a peer received but a
//! member with a live book for that symbol has not, `loss_after` later, is a
//! message that member lost ([`ShardSource::check_lost`]): it resyncs the
//! symbol, and a source hands over. The receive-time difference for the same
//! change measures how far the source lags its peers; a median over
//! `lag_window` above `lag_threshold` also hands over
//! ([`ShardSource::review`]). Apart from a lost connection, there is at most
//! one switch per `hold`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};
use tracing::info;

#[derive(Debug, Clone)]
pub struct DetectConfig {
    /// A hash a peer received this long ago and a member has not is lost.
    pub loss_after: Duration,
    /// Hashes are forgotten after this long.
    pub keep: Duration,
    pub lag_window: Duration,
    pub lag_threshold: Duration,
    /// Fewer samples in the window than this: no lag decision.
    pub min_lag_samples: usize,
    /// At most this many samples are kept.
    pub max_lag_samples: usize,
    /// Minimum time between switches, except when the source is down.
    pub hold: Duration,
}

/// Measured live (2026-11-06, two sockets on the same 6000 tokens, after
/// the subscribe bursts): every hash reached both sockets, but the same
/// change arrived up to 4.0 s apart (p50 141 ms, p90 2.9 s, p99 3.8 s); in
/// collector runs one socket stayed ~4 s behind and changes came up to 16 s
/// late. A `PONG` can overtake queued changes, so only time tells late from
/// lost: a loss is declared after 30 s, and lag must be sustained.
impl Default for DetectConfig {
    fn default() -> Self {
        Self {
            loss_after: Duration::from_secs(30),
            keep: Duration::from_secs(35),
            lag_window: Duration::from_secs(30),
            lag_threshold: Duration::from_millis(250),
            min_lag_samples: 100,
            max_lag_samples: 2000,
            hold: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
struct Seen {
    at: Instant,
    symbol: String,
    /// Bit per member that received it, and per member already checked.
    seen: u32,
    reported: u32,
    first: usize,
}

#[derive(Debug, Default)]
struct Tracking {
    seen: HashMap<String, Seen>,
    /// (when, how many ms later the source received a change than the first
    /// member that did; negative if the source was first).
    lag: VecDeque<(Instant, f64)>,
    last_switch: Option<Instant>,
}

#[derive(Debug)]
pub struct State {
    pub source: usize,
    /// Bumped on every switch.
    pub epoch: u64,
    /// Whether each member has a session open now.
    pub connected: Vec<bool>,
    /// Latest receive time written for the shard.
    pub last_written: Option<DateTime<Utc>>,
    /// Receive times written after the last switch are at least this: the
    /// new source's clock reading of a change can be earlier than the old
    /// source's, and a symbol's receive times must not go backwards.
    pub floor: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct ShardSource {
    pub shard: usize,
    state: Arc<Mutex<State>>,
    /// Live baselines per member, updated by each member as it goes; read
    /// without the lock to pick the best candidate.
    baselines: Vec<AtomicUsize>,
    /// Source switches in this shard.
    pub switches: AtomicU64,
    /// Messages the source (or another member) lost, by hash.
    pub lost_on_source: AtomicU64,
    pub lost_on_peers: AtomicU64,
    /// Hashes reported lost that the member received later after all, and
    /// the longest such delay (ms): a too-short `loss_after`.
    pub late_after_loss: AtomicU64,
    pub late_after_loss_max_ms: AtomicU64,
    /// The source, readable without the async lock (it is only a hint for
    /// [`Self::observe`]; decisions use the locked state).
    source_hint: AtomicUsize,
    tracking: std::sync::Mutex<Tracking>,
    detect: DetectConfig,
}

impl ShardSource {
    pub fn new(shard: usize, members: usize) -> Self {
        assert!(members > 0);
        Self {
            shard,
            state: Arc::new(Mutex::new(State {
                source: 0,
                epoch: 0,
                connected: vec![false; members],
                last_written: None,
                floor: None,
            })),
            baselines: (0..members).map(|_| AtomicUsize::new(0)).collect(),
            switches: AtomicU64::new(0),
            lost_on_source: AtomicU64::new(0),
            lost_on_peers: AtomicU64::new(0),
            late_after_loss: AtomicU64::new(0),
            late_after_loss_max_ms: AtomicU64::new(0),
            source_hint: AtomicUsize::new(0),
            tracking: std::sync::Mutex::new(Tracking::default()),
            detect: DetectConfig::default(),
        }
    }

    pub fn with_detect(mut self, detect: DetectConfig) -> Self {
        self.detect = detect;
        self
    }

    fn tracking(&self) -> std::sync::MutexGuard<'_, Tracking> {
        self.tracking.lock().expect("tracking lock")
    }

    /// Records that `member` applied the change or book with `hash` for
    /// `symbol`, received at `at`. `lag_sample`: whether it may count toward
    /// lag (not for a book, which a member that just resubscribed receives
    /// long after its peers saw the same state).
    pub fn observe(&self, member: usize, symbol: &str, hash: &str, at: Instant, lag_sample: bool) {
        if self.members() == 1 {
            return;
        }
        let source = self.source_hint.load(Ordering::Relaxed);
        let bit = 1u32 << member;
        let mut t = self.tracking();
        let sample = match t.seen.get_mut(hash) {
            None => {
                t.seen.insert(
                    hash.to_owned(),
                    Seen {
                        at,
                        symbol: symbol.to_owned(),
                        seen: bit,
                        reported: 0,
                        first: member,
                    },
                );
                None
            }
            Some(s) if s.seen & bit != 0 => None,
            Some(s) => {
                s.seen |= bit;
                if s.reported & bit != 0 {
                    let ms = at.saturating_duration_since(s.at).as_millis() as u64;
                    self.late_after_loss.fetch_add(1, Ordering::Relaxed);
                    self.late_after_loss_max_ms.fetch_max(ms, Ordering::Relaxed);
                }
                let late_ms = at.saturating_duration_since(s.at).as_secs_f64() * 1000.0;
                let sample = if member == source && s.first != source {
                    Some(late_ms)
                } else if member != source && s.first == source {
                    Some(-late_ms)
                } else {
                    None
                };
                // Kept until it expires even when every member has it: one
                // message can repeat a hash for a token, and a record
                // recreated by a late repeat would look lost to the others.
                sample.filter(|_| lag_sample)
            }
        };
        if let Some(ms) = sample {
            if t.lag.len() >= self.detect.max_lag_samples {
                t.lag.pop_front();
            }
            t.lag.push_back((at, ms));
        }
    }

    /// Like [`Self::observe`] for a book that answers `member`'s own
    /// subscription: marks the hash if a peer recorded it, but opens no
    /// record. The book's hash is the symbol's current state, which a peer
    /// subscribed long ago saw only if the symbol changed lately; a record
    /// opened here would make every quiet symbol look lost on the peers
    /// after each reconnect or recycle.
    pub fn observe_existing(&self, member: usize, hash: &str, at: Instant) {
        if self.members() == 1 {
            return;
        }
        let bit = 1u32 << member;
        let mut t = self.tracking();
        if let Some(s) = t.seen.get_mut(hash)
            && s.seen & bit == 0
        {
            s.seen |= bit;
            if s.reported & bit != 0 {
                let ms = at.saturating_duration_since(s.at).as_millis() as u64;
                self.late_after_loss.fetch_add(1, Ordering::Relaxed);
                self.late_after_loss_max_ms.fetch_max(ms, Ordering::Relaxed);
            }
        }
    }

    /// Symbols `member` lost a message for: a peer received a hash at least
    /// `loss_after` before `cutoff` that `member` has not, and `member`'s
    /// book for the symbol was live before then (`live_since`). Each hash is
    /// reported once per member. Also forgets old hashes.
    ///
    /// `cutoff` must be the send time of `member`'s latest answered `PING`,
    /// not the current time: once that `PONG` is read, so is everything its
    /// server sent before the `PING`, so a member that is merely slow to read
    /// (its own backlog) is never taken for one that lost data. Measured live
    /// between two sockets: the same change arrives up to ~0.5 s apart (p99),
    /// while a busy reader can fall seconds behind.
    pub fn check_lost(&self, member: usize, cutoff: Instant, live_since: impl Fn(&str) -> Option<Instant>) -> Vec<String> {
        let now = cutoff;
        let bit = 1u32 << member;
        let mut lost = Vec::new();
        let mut t = self.tracking();
        for s in t.seen.values_mut() {
            if s.seen & bit == 0 && s.reported & bit == 0 && now.saturating_duration_since(s.at) >= self.detect.loss_after {
                s.reported |= bit;
                if live_since(&s.symbol).is_some_and(|l| l < s.at) {
                    lost.push(s.symbol.clone());
                }
            }
        }
        let keep = self.detect.keep;
        t.seen.retain(|_, s| now.saturating_duration_since(s.at) < keep);
        drop(t);
        lost.sort_unstable();
        lost.dedup();
        let counter = if member == self.source_hint.load(Ordering::Relaxed) {
            &self.lost_on_source
        } else {
            &self.lost_on_peers
        };
        counter.fetch_add(lost.len() as u64, Ordering::Relaxed);
        lost
    }

    /// Median and 99th percentile of the source's lag (ms) in the window,
    /// and the number of samples.
    pub fn lag_stats(&self, now: Instant) -> Option<(f64, f64, usize)> {
        let mut t = self.tracking();
        Self::prune_lag(&mut t, now, self.detect.lag_window);
        if t.lag.is_empty() {
            return None;
        }
        let mut v: Vec<f64> = t.lag.iter().map(|&(_, ms)| ms).collect();
        v.sort_by(f64::total_cmp);
        let pct = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize];
        Some((pct(0.5), pct(0.99), v.len()))
    }

    fn prune_lag(t: &mut Tracking, now: Instant, window: Duration) {
        while t.lag.front().is_some_and(|&(at, _)| now.saturating_duration_since(at) > window) {
            t.lag.pop_front();
        }
    }

    /// Called periodically by `member`: if it is the source and it just lost
    /// messages (`lost`) or lags its peers, hands over to the best connected
    /// peer, at most once per `hold`.
    pub async fn review(&self, member: usize, now: Instant, lost: bool) {
        if self.members() == 1 || self.source_hint.load(Ordering::Relaxed) != member {
            return;
        }
        let reason = {
            let mut t = self.tracking();
            if t.last_switch.is_some_and(|at| now.saturating_duration_since(at) < self.detect.hold) {
                return;
            }
            if lost {
                "source lost a message its peer received"
            } else {
                Self::prune_lag(&mut t, now, self.detect.lag_window);
                if t.lag.len() < self.detect.min_lag_samples {
                    return;
                }
                let mut v: Vec<f64> = t.lag.iter().map(|&(_, ms)| ms).collect();
                v.sort_by(f64::total_cmp);
                let median = v[v.len() / 2];
                if median <= self.detect.lag_threshold.as_secs_f64() * 1000.0 {
                    return;
                }
                "source lags its peer"
            }
        };
        let mut st = self.lock().await;
        if st.source != member {
            return;
        }
        if let Some(best) = self.best_connected(&st, member) {
            self.switch(&mut st, best, reason);
        }
    }

    pub fn members(&self) -> usize {
        self.baselines.len()
    }

    pub async fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().await
    }

    /// The lock as an owned guard, held across a member's `&mut self` calls.
    pub async fn lock_owned(&self) -> OwnedMutexGuard<State> {
        Arc::clone(&self.state).lock_owned().await
    }

    /// Current source, for logs and stats.
    pub fn try_source(&self) -> Option<usize> {
        self.state.try_lock().ok().map(|s| s.source)
    }

    pub fn set_baselines(&self, member: usize, n: usize) {
        self.baselines[member].store(n, Ordering::Relaxed);
    }

    pub async fn connected(&self, member: usize) {
        self.lock().await.connected[member] = true;
    }

    /// A member's session ended. If it was the source, the connected member
    /// with the most live baselines takes over at once.
    pub async fn lost(&self, member: usize) {
        let mut st = self.lock().await;
        st.connected[member] = false;
        if st.source == member
            && let Some(best) = self.best_connected(&st, member)
        {
            self.switch(&mut st, best, "source lost its connection");
        }
    }

    /// Whether `member` may disconnect for a recycle (Extension 2). A
    /// non-source may if the source is up. The source first hands over to
    /// the connected member with the most live baselines, if that member
    /// has at least `ready_at_least`; otherwise the recycle is skipped.
    /// Not limited by `hold`: a recycle is planned, not a reaction.
    pub async fn release_for_recycle(&self, member: usize, ready_at_least: usize) -> bool {
        let mut st = self.lock().await;
        if st.source != member {
            return st.connected[st.source];
        }
        match self.best_connected(&st, member) {
            Some(best) if self.baselines[best].load(Ordering::Relaxed) >= ready_at_least => {
                self.switch(&mut st, best, "recycle");
                true
            }
            _ => false,
        }
    }

    /// Called by a non-source member about to write: takes over if the
    /// source is down and this member is up. Returns whether it is the
    /// source now.
    pub fn take_over_if_source_down(&self, st: &mut State, member: usize) -> bool {
        if st.source == member {
            return true;
        }
        if st.connected[st.source] || !st.connected[member] {
            return false;
        }
        self.switch(st, member, "source is down");
        true
    }

    fn best_connected(&self, st: &State, except: usize) -> Option<usize> {
        (0..st.connected.len())
            .filter(|&m| m != except && st.connected[m])
            .max_by_key(|&m| (self.baselines[m].load(Ordering::Relaxed), std::cmp::Reverse(m)))
    }

    pub fn switch(&self, st: &mut State, to: usize, reason: &str) {
        let from = st.source;
        st.source = to;
        st.epoch += 1;
        st.floor = st.last_written;
        self.source_hint.store(to, Ordering::Relaxed);
        {
            let mut t = self.tracking();
            t.last_switch = Some(Instant::now());
            // Lag was measured against the old source.
            t.lag.clear();
        }
        self.switches.fetch_add(1, Ordering::Relaxed);
        info!(shard = self.shard, from, to, reason, "source switched");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lost_source_hands_over_to_the_best_connected_member() {
        let s = ShardSource::new(0, 3);
        for m in 0..3 {
            s.connected(m).await;
        }
        s.set_baselines(1, 10);
        s.set_baselines(2, 50);
        s.lost(0).await;
        let st = s.lock().await;
        assert_eq!((st.source, st.epoch), (2, 1));
    }

    #[tokio::test]
    async fn nobody_to_hand_over_to_keeps_the_source() {
        let s = ShardSource::new(0, 2);
        s.connected(0).await;
        s.lost(0).await;
        let mut st = s.lock().await;
        assert_eq!((st.source, st.epoch), (0, 0));
        // A member that comes up later takes over at its next message.
        assert!(!s.take_over_if_source_down(&mut st, 1), "member 1 is not connected yet");
        st.connected[1] = true;
        assert!(s.take_over_if_source_down(&mut st, 1));
        assert_eq!((st.source, st.epoch), (1, 1));
    }

    fn fast() -> DetectConfig {
        DetectConfig {
            loss_after: Duration::from_secs(2),
            keep: Duration::from_secs(5),
            min_lag_samples: 3,
            hold: Duration::ZERO,
            ..DetectConfig::default()
        }
    }

    #[tokio::test]
    async fn a_hash_only_the_peer_received_is_lost_once_the_book_was_live() {
        let s = ShardSource::new(0, 2).with_detect(fast());
        let t0 = Instant::now();
        s.observe(1, "A", "h1", t0, true);
        s.observe(1, "B", "h2", t0, true);
        s.observe(0, "B", "h2", t0, true); // both have h2: forgotten
        let live_before = |_: &str| Some(t0 - Duration::from_secs(1));
        assert!(s.check_lost(0, t0 + Duration::from_secs(1), live_before).is_empty(), "too early");
        assert_eq!(s.check_lost(0, t0 + Duration::from_secs(2), live_before), ["A"]);
        assert!(s.check_lost(0, t0 + Duration::from_secs(3), live_before).is_empty(), "reported once");
        assert_eq!(s.lost_on_source.load(Ordering::Relaxed), 1);
        // A book that became live after the hash was sent never saw it.
        s.observe(1, "C", "h3", t0, true);
        let live_after = |_: &str| Some(t0 + Duration::from_millis(1));
        assert!(s.check_lost(0, t0 + Duration::from_secs(2), live_after).is_empty());
        // Old hashes are forgotten.
        s.check_lost(0, t0 + Duration::from_secs(60), live_before);
        assert!(s.tracking().seen.is_empty());
    }

    #[tokio::test]
    async fn a_subscription_book_opens_no_loss_record() {
        // After a reconnect (or recycle) a member's subscription brings a book
        // for every symbol; for a symbol quiet longer than `keep`, its hash is
        // no longer on record, and the peer will never receive it again.
        let s = ShardSource::new(0, 2).with_detect(fast());
        let t0 = Instant::now();
        let live = |_: &str| Some(t0 - Duration::from_secs(1));
        s.observe_existing(1, "q", t0);
        assert!(s.check_lost(0, t0 + Duration::from_secs(3), live).is_empty());
        // It still marks a record the peer opened.
        s.observe(0, "A", "h", t0, true);
        s.observe_existing(1, "h", t0);
        assert!(s.check_lost(1, t0 + Duration::from_secs(3), live).is_empty());
    }

    #[tokio::test]
    async fn a_hash_repeated_within_a_message_is_not_a_loss() {
        // One price_change can carry several entries for a token with the
        // same (resulting-book) hash.
        let s = ShardSource::new(0, 2).with_detect(fast());
        let t0 = Instant::now();
        s.observe(0, "A", "h", t0, true);
        s.observe(0, "A", "h", t0, true);
        s.observe(1, "A", "h", t0, true);
        s.observe(1, "A", "h", t0, true);
        let live = |_: &str| Some(t0 - Duration::from_secs(1));
        let later = t0 + Duration::from_secs(3);
        assert!(s.check_lost(0, later, live).is_empty());
        assert!(s.check_lost(1, later, live).is_empty());
    }

    #[tokio::test]
    async fn a_lagging_source_hands_over_and_a_fast_one_does_not() {
        let s = ShardSource::new(0, 2).with_detect(fast());
        s.connected(0).await;
        s.connected(1).await;
        let t0 = Instant::now();
        // Source (0) receives each change 50 ms after the peer: below 200 ms.
        for i in 0..5 {
            let h = format!("a{i}");
            s.observe(1, "A", &h, t0, true);
            s.observe(0, "A", &h, t0 + Duration::from_millis(50), true);
        }
        s.review(0, t0 + Duration::from_secs(1), false).await;
        assert_eq!(s.lock().await.source, 0);
        let (p50, _, n) = s.lag_stats(t0 + Duration::from_secs(1)).unwrap();
        assert_eq!((p50.round(), n), (50.0, 5));
        // Now 500 ms behind, for most samples.
        for i in 0..10 {
            let h = format!("b{i}");
            s.observe(1, "A", &h, t0, true);
            s.observe(0, "A", &h, t0 + Duration::from_millis(500), true);
        }
        s.review(0, t0 + Duration::from_secs(1), false).await;
        assert_eq!(s.lock().await.source, 1);
        assert!(s.lag_stats(t0).is_none(), "samples were against the old source");
    }

    #[tokio::test]
    async fn hold_limits_switches_but_not_a_lost_connection() {
        let s = ShardSource::new(0, 2).with_detect(DetectConfig {
            hold: Duration::from_secs(10),
            ..fast()
        });
        s.connected(0).await;
        s.connected(1).await;
        s.review(0, Instant::now(), true).await;
        assert_eq!(s.lock().await.source, 1);
        s.review(1, Instant::now(), true).await;
        assert_eq!(s.lock().await.source, 1, "held");
        s.lost(1).await;
        assert_eq!(s.lock().await.source, 0, "a lost connection is not held");
    }

    #[tokio::test]
    async fn a_switch_sets_the_time_floor_to_the_last_written_time() {
        let s = ShardSource::new(0, 2);
        let mut st = s.lock().await;
        let t: DateTime<Utc> = "2026-11-06T10:00:00Z".parse().unwrap();
        st.last_written = Some(t);
        assert_eq!(st.floor, None);
        s.switch(&mut st, 1, "test");
        assert_eq!(st.floor, Some(t));
    }

    #[tokio::test]
    async fn a_connected_source_is_not_taken_over() {
        let s = ShardSource::new(0, 2);
        s.connected(0).await;
        s.connected(1).await;
        let mut st = s.lock().await;
        assert!(!s.take_over_if_source_down(&mut st, 1));
        assert_eq!(st.epoch, 0);
    }
}
