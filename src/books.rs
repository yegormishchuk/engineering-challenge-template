//! The books owned by one connection, and the rules for turning exchange
//! events into output messages with the spec's timestamps (Section 5):
//!
//! | Message | `received_at` / `backtest_timestamp` | `exchange_timestamp` |
//! | --- | --- | --- |
//! | exchange snapshot | local receive time | snapshot time from the exchange |
//! | level change | local receive time | change time from the exchange |
//! | minute snapshot | emit time | last exchange time seen for the symbol |
//!
//! After a gap (reconnect or resync) a symbol's book is kept but marked
//! **stale**: it is no baseline (changes are refused, minute snapshots skip
//! it), but the next exchange book is still compared with it. That
//! comparison is reported as `after_gap`, so offline validation, which
//! replays across the gap, can be reconciled with the collector's counters.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::Side;
use crate::book::{BookError, LevelMismatch, OrderBook, level_update};
use crate::exchange::LevelKind;
use crate::writer::OutMsg;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BooksError {
    /// A level change arrived for a symbol with no snapshot yet. It must not
    /// be written: a reader would have no baseline to apply it to.
    NoBaseline { symbol: String },
    Book { symbol: String, error: BookError },
}

impl fmt::Display for BooksError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoBaseline { symbol } => write!(f, "{symbol}: level change before any snapshot"),
            Self::Book { symbol, error } => write!(f, "{symbol}: {error}"),
        }
    }
}

impl std::error::Error for BooksError {}

/// Result of applying an exchange snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotApplied {
    pub msg: OutMsg,
    /// `None` for the first snapshot of a symbol; otherwise the levels where
    /// the local book differed from the exchange (empty = nothing missed).
    pub mismatches: Option<Vec<LevelMismatch>>,
    /// The book compared against was stale (from before a gap), so a
    /// mismatch is expected change during the gap, not a missed message.
    pub after_gap: bool,
}

#[derive(Debug)]
struct Entry {
    book: OrderBook,
    /// When the book last became a baseline (first book, or first book
    /// after a gap). Changes sent before then were never seen by this book.
    live_since: Instant,
    activity: Activity,
}

/// Which recent time buckets (minutes by default) had a change for a
/// symbol (Extension 2). Bit `i` is bucket `at - i`.
#[derive(Debug, Clone, Copy)]
struct Activity {
    bits: u64,
    /// Bucket index of bit 0, counted from [`Books`]' epoch.
    at: u64,
}

impl Activity {
    /// The bits as of bucket `now` (older buckets shift up; past 64 they
    /// are gone).
    fn as_of(self, now: u64) -> u64 {
        let shift = now.saturating_sub(self.at);
        if shift >= 64 { 0 } else { self.bits << shift }
    }
}

/// When a symbol counts as gone quiet. Polymarket markets change in
/// bursts (many changes within milliseconds, then minutes of nothing), so
/// gaps between changes say little; whole buckets do. A symbol is *active*
/// if at least `min_active` of the `history` buckets before the last
/// `quiet` ones had a change, and *silent* if it is active and the last
/// `quiet` buckets (the current one included) had none.
#[derive(Debug, Clone)]
pub struct SilenceConfig {
    /// How often a connection looks for silent symbols.
    pub check_every: Duration,
    pub bucket: Duration,
    pub quiet: u32,
    pub history: u32,
    pub min_active: u32,
    /// Most symbols resubscribed per check.
    pub max_per_check: usize,
}

impl Default for SilenceConfig {
    fn default() -> Self {
        Self {
            check_every: Duration::from_secs(5),
            bucket: Duration::from_secs(60),
            quiet: 10,
            history: 50,
            min_active: 45,
            max_per_check: 100,
        }
    }
}

impl SilenceConfig {
    fn bucket_of(&self, epoch: Instant, at: Instant) -> u64 {
        (at.saturating_duration_since(epoch).as_nanos() / self.bucket.as_nanos().max(1)) as u64
    }
}

#[derive(Debug)]
pub struct Books {
    books: HashMap<String, Entry>,
    stale: HashSet<String>,
    /// Activity buckets are counted from here.
    epoch: Instant,
}

impl Default for Books {
    fn default() -> Self {
        Self {
            books: HashMap::new(),
            stale: HashSet::new(),
            epoch: Instant::now(),
        }
    }
}

impl Books {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.books.len()
    }

    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    pub fn get(&self, symbol: &str) -> Option<&OrderBook> {
        self.books.get(symbol).map(|e| &e.book)
    }

    /// When the symbol's book became a baseline; `None` if it is not one.
    pub fn live_since(&self, symbol: &str) -> Option<Instant> {
        if self.stale.contains(symbol) {
            return None;
        }
        self.books.get(symbol).map(|e| e.live_since)
    }

    /// Books that are baselines (not stale).
    pub fn live_len(&self) -> usize {
        self.books.len() - self.stale.len()
    }

    /// Whether the symbol has a book that changes can be applied to.
    pub fn has_baseline(&self, symbol: &str) -> bool {
        self.books.contains_key(symbol) && !self.stale.contains(symbol)
    }

    /// Keeps the book for comparison but stops treating it as a baseline.
    pub fn mark_stale(&mut self, symbol: &str) {
        if self.books.contains_key(symbol) {
            self.stale.insert(symbol.to_owned());
        }
    }

    pub fn mark_all_stale(&mut self) {
        self.stale = self.books.keys().cloned().collect();
    }

    /// Drops a symbol, e.g. after unsubscribing. Returns whether it existed.
    pub fn remove(&mut self, symbol: &str) -> bool {
        self.stale.remove(symbol);
        self.books.remove(symbol).is_some()
    }

    /// Applies a full exchange book and returns the `Snapshot` to write. On
    /// error nothing changes.
    pub fn apply_snapshot(
        &mut self,
        symbol: &str,
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
        exchange_ts: DateTime<Utc>,
        received_at: DateTime<Utc>,
    ) -> Result<SnapshotApplied, BooksError> {
        let book_err = |error| BooksError::Book {
            symbol: symbol.to_owned(),
            error,
        };
        let mismatches = match self.books.get_mut(symbol) {
            Some(entry) => Some(
                entry
                    .book
                    .check_and_replace(bids, asks, exchange_ts)
                    .map_err(book_err)?,
            ),
            None => {
                let mut book = OrderBook::new();
                book.replace(bids, asks, exchange_ts).map_err(book_err)?;
                let now = Instant::now();
                let entry = Entry {
                    book,
                    live_since: now,
                    activity: Activity { bits: 0, at: 0 },
                };
                self.books.insert(symbol.to_owned(), entry);
                None
            }
        };
        let after_gap = self.stale.remove(symbol);
        let entry = self.books.get_mut(symbol).expect("inserted above");
        if after_gap {
            entry.live_since = Instant::now();
        }
        let book = &entry.book;
        Ok(SnapshotApplied {
            msg: OutMsg::Snapshot(book.snapshot(symbol, received_at, exchange_ts)),
            mismatches,
            after_gap,
        })
    }

    /// Applies one level change and returns the `PriceLevelUpdate` to write,
    /// always with the resulting absolute quantity (`0` removes the level).
    /// A [`LevelKind::Delta`] adds to the current quantity (a missing level
    /// is 0); a result below 0 or past `i64` is an error. On error nothing changes.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_level(
        &mut self,
        symbol: &str,
        side: Side,
        price: i64,
        quantity: i64,
        kind: LevelKind,
        exchange_ts: DateTime<Utc>,
        received_at: DateTime<Utc>,
    ) -> Result<OutMsg, BooksError> {
        let no_baseline = || BooksError::NoBaseline {
            symbol: symbol.to_owned(),
        };
        if self.stale.contains(symbol) {
            return Err(no_baseline());
        }
        let entry = self.books.get_mut(symbol).ok_or_else(no_baseline)?;
        let quantity = match kind {
            LevelKind::Absolute => quantity,
            LevelKind::Delta => {
                let levels = match side {
                    Side::Buy => entry.book.bids(),
                    Side::Sell => entry.book.asks(),
                };
                levels
                    .get(&price)
                    .copied()
                    .unwrap_or(0)
                    .checked_add(quantity)
                    .ok_or(BooksError::Book {
                        symbol: symbol.to_owned(),
                        error: BookError::QuantityOverflow { price },
                    })?
            }
        };
        entry
            .book
            .set_level(side, price, quantity, exchange_ts)
            .map_err(|error| BooksError::Book {
                symbol: symbol.to_owned(),
                error,
            })?;
        Ok(OutMsg::Update(level_update(
            symbol,
            side,
            price,
            quantity,
            received_at,
            exchange_ts,
        )))
    }

    /// Records a change (or a fresh book) for `symbol` at `at`.
    pub fn note_activity(&mut self, symbol: &str, at: Instant, cfg: &SilenceConfig) {
        let b = cfg.bucket_of(self.epoch, at);
        let Some(entry) = self.books.get_mut(symbol) else {
            return;
        };
        let a = &mut entry.activity;
        if b >= a.at {
            a.bits = a.as_of(b) | 1;
            a.at = b;
        } else if a.at - b < 64 {
            a.bits |= 1 << (a.at - b);
        }
    }

    /// Live symbols that went quiet by `cfg` at `now` (sorted), and how many
    /// live symbols are active.
    pub fn silent(&self, now: Instant, cfg: &SilenceConfig) -> (Vec<String>, usize) {
        assert!(cfg.quiet + cfg.history <= 64, "activity keeps 64 buckets");
        let b = cfg.bucket_of(self.epoch, now);
        let quiet_mask = (1u64 << cfg.quiet) - 1;
        let history_mask = ((1u64 << cfg.history) - 1) << cfg.quiet;
        let mut silent = Vec::new();
        let mut active = 0;
        for (symbol, entry) in &self.books {
            if self.stale.contains(symbol) {
                continue;
            }
            let bits = entry.activity.as_of(b);
            if (bits & history_mask).count_ones() < cfg.min_active {
                continue;
            }
            active += 1;
            if bits & quiet_mask == 0 {
                silent.push(symbol.clone());
            }
        }
        silent.sort_unstable();
        (silent, active)
    }

    /// One `Snapshot` per symbol with a baseline, for the minute timer, sorted
    /// by symbol so the output order is deterministic. Built one at a time as
    /// the caller sends them: all connections snapshot at the same instant,
    /// and holding every snapshot at once cost ~430 bytes per symbol.
    pub fn minute_snapshots(&self, emitted_at: DateTime<Utc>) -> impl Iterator<Item = OutMsg> + '_ {
        let mut symbols: Vec<&String> = self
            .books
            .keys()
            .filter(|s| !self.stale.contains(*s))
            .collect();
        symbols.sort_unstable();
        symbols.into_iter().map(move |symbol| {
            let book = &self.books[symbol].book;
            let exchange_ts = book.last_exchange_ts().unwrap_or(emitted_at);
            OutMsg::Snapshot(book.snapshot(symbol, emitted_at, exchange_ts))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    #[test]
    fn first_snapshot_has_no_check_and_uses_both_timestamps() {
        let mut books = Books::new();
        let applied = books
            .apply_snapshot("T", [(100, 1)], [(200, 2)], ts(10), ts(11))
            .unwrap();
        assert_eq!(applied.mismatches, None);
        let OutMsg::Snapshot(s) = applied.msg else { panic!() };
        assert_eq!((s.received_at, s.exchange_timestamp), (ts(11), ts(10)));
        assert_eq!(s.symbol, "T");
        assert_eq!(s.bids.len(), 1);
        assert_eq!(s.asks.len(), 1);
    }

    #[test]
    fn later_snapshot_reports_mismatches() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 1)], [], ts(1), ts(1)).unwrap();
        books.apply_level("T", Side::Buy, 100, 5, LevelKind::Absolute, ts(2), ts(2)).unwrap();
        let clean = books.apply_snapshot("T", [(100, 5)], [], ts(3), ts(3)).unwrap();
        assert_eq!(clean.mismatches, Some(vec![]));
        let dirty = books.apply_snapshot("T", [(100, 6)], [], ts(4), ts(4)).unwrap();
        assert_eq!(dirty.mismatches.unwrap().len(), 1);
        assert_eq!(books.get("T").unwrap().bids().get(&100), Some(&6));
    }

    #[test]
    fn level_change_uses_receive_time_as_backtest_timestamp() {
        let mut books = Books::new();
        books.apply_snapshot("T", [], [], ts(1), ts(1)).unwrap();
        let OutMsg::Update(u) = books.apply_level("T", Side::Sell, 300, 7, LevelKind::Absolute, ts(20), ts(21)).unwrap()
        else {
            panic!()
        };
        assert_eq!((u.backtest_timestamp, u.exchange_timestamp), (ts(21), ts(20)));
        assert_eq!((u.side, u.price_level, u.quantity), (Side::Sell, 300, 7));
    }

    #[test]
    fn level_change_without_baseline_is_rejected() {
        let mut books = Books::new();
        assert_eq!(
            books.apply_level("T", Side::Buy, 1, 1, LevelKind::Absolute, ts(1), ts(1)),
            Err(BooksError::NoBaseline { symbol: "T".into() })
        );
        assert!(books.is_empty());
    }

    #[test]
    fn invalid_change_leaves_book_unchanged() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 1)], [], ts(1), ts(1)).unwrap();
        assert!(books.apply_level("T", Side::Buy, 100, -1, LevelKind::Absolute, ts(2), ts(2)).is_err());
        assert!(books.apply_snapshot("T", [(100, -1)], [], ts(3), ts(3)).is_err());
        let book = books.get("T").unwrap();
        assert_eq!(book.bids().get(&100), Some(&1));
        assert_eq!(book.last_exchange_ts(), Some(ts(1)));
    }

    fn quantity(msg: OutMsg) -> i64 {
        let OutMsg::Update(u) = msg else { panic!("{msg:?}") };
        u.quantity
    }

    #[test]
    fn delta_levels_add_to_the_current_quantity() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 10)], [], ts(1), ts(1)).unwrap();
        let up = books.apply_level("T", Side::Buy, 100, 5, LevelKind::Delta, ts(2), ts(2)).unwrap();
        assert_eq!(quantity(up), 15, "written as the absolute quantity");
        assert_eq!(books.get("T").unwrap().bids().get(&100), Some(&15));
        let gone = books.apply_level("T", Side::Buy, 100, -15, LevelKind::Delta, ts(3), ts(3)).unwrap();
        assert_eq!(quantity(gone), 0);
        assert!(books.get("T").unwrap().bids().is_empty(), "0 removes the level");
        // A missing level counts as 0.
        let new = books.apply_level("T", Side::Sell, 200, 3, LevelKind::Delta, ts(4), ts(4)).unwrap();
        assert_eq!(quantity(new), 3);
    }

    #[test]
    fn a_delta_below_zero_is_rejected_and_leaves_the_book() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 10)], [], ts(1), ts(1)).unwrap();
        assert!(matches!(
            books.apply_level("T", Side::Buy, 100, -11, LevelKind::Delta, ts(2), ts(2)),
            Err(BooksError::Book { error: BookError::NegativeQuantity { .. }, .. })
        ));
        let book = books.get("T").unwrap();
        assert_eq!(book.bids().get(&100), Some(&10));
        assert_eq!(book.last_exchange_ts(), Some(ts(1)));
    }

    #[test]
    fn a_delta_that_overflows_is_rejected() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 10)], [], ts(1), ts(1)).unwrap();
        assert!(matches!(
            books.apply_level("T", Side::Buy, 100, i64::MAX, LevelKind::Delta, ts(2), ts(2)),
            Err(BooksError::Book { error: BookError::QuantityOverflow { price: 100 }, .. })
        ));
        let book = books.get("T").unwrap();
        assert_eq!(book.bids().get(&100), Some(&10));
        assert_eq!(book.last_exchange_ts(), Some(ts(1)));
    }

    #[test]
    fn minute_snapshots_cover_every_symbol_with_last_exchange_time() {
        let mut books = Books::new();
        books.apply_snapshot("B", [(1, 1)], [], ts(5), ts(6)).unwrap();
        books.apply_snapshot("A", [], [(2, 2)], ts(7), ts(8)).unwrap();
        books.apply_level("A", Side::Sell, 2, 3, LevelKind::Absolute, ts(9), ts(10)).unwrap();

        let snaps: Vec<OutMsg> = books.minute_snapshots(ts(60_000)).collect();
        let summary: Vec<_> = snaps
            .iter()
            .map(|m| match m {
                OutMsg::Snapshot(s) => (s.symbol.as_str(), s.received_at, s.exchange_timestamp),
                OutMsg::Update(_) => panic!(),
            })
            .collect();
        assert_eq!(summary, [("A", ts(60_000), ts(9)), ("B", ts(60_000), ts(5))]);
    }

    #[test]
    fn stale_book_is_no_baseline_but_is_compared_with_the_next_book() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 1)], [], ts(1), ts(1)).unwrap();
        books.apply_snapshot("U", [(100, 1)], [], ts(1), ts(1)).unwrap();
        books.mark_all_stale();
        assert!(!books.has_baseline("T"));
        assert!(matches!(
            books.apply_level("T", Side::Buy, 100, 2, LevelKind::Absolute, ts(2), ts(2)),
            Err(BooksError::NoBaseline { .. })
        ));
        assert!(books.minute_snapshots(ts(3)).next().is_none(), "stale books are not snapshotted");

        let same = books.apply_snapshot("T", [(100, 1)], [], ts(4), ts(4)).unwrap();
        assert_eq!((same.mismatches, same.after_gap), (Some(vec![]), true));
        let changed = books.apply_snapshot("U", [(100, 5)], [], ts(4), ts(4)).unwrap();
        assert!(changed.after_gap);
        assert_eq!(changed.mismatches.unwrap().len(), 1);
        assert!(books.has_baseline("T") && books.has_baseline("U"));
        let live = books.apply_snapshot("T", [(100, 1)], [], ts(5), ts(5)).unwrap();
        assert!(!live.after_gap, "only the first book after the gap");
        assert_eq!(books.minute_snapshots(ts(6)).count(), 2);
    }

    #[test]
    fn live_since_restarts_after_a_gap() {
        let mut books = Books::new();
        assert_eq!(books.live_since("T"), None);
        books.apply_snapshot("T", [], [], ts(1), ts(1)).unwrap();
        let first = books.live_since("T").unwrap();
        books.apply_snapshot("T", [], [], ts(2), ts(2)).unwrap();
        assert_eq!(books.live_since("T"), Some(first), "a live book stays live");
        books.mark_stale("T");
        assert_eq!(books.live_since("T"), None);
        std::thread::sleep(std::time::Duration::from_millis(2));
        books.apply_snapshot("T", [], [], ts(3), ts(3)).unwrap();
        assert!(books.live_since("T").unwrap() > first);
    }

    #[test]
    fn mark_stale_on_an_unknown_symbol_does_nothing() {
        let mut books = Books::new();
        books.mark_stale("nope");
        let first = books.apply_snapshot("nope", [], [], ts(1), ts(1)).unwrap();
        assert_eq!((first.mismatches, first.after_gap), (None, false));
    }

    // ---- Activity (Extension 2) ----

    /// Buckets of 1 s; silent after 10 empty buckets if at least 45 of the
    /// 50 before them had a change.
    fn secs() -> SilenceConfig {
        SilenceConfig {
            bucket: Duration::from_secs(1),
            ..SilenceConfig::default()
        }
    }

    /// A snapshot of `A`, then one event in each of `n` buckets of `cfg`
    /// starting at bucket 0. Returns the start and the last event's instant.
    fn every_bucket(books: &mut Books, n: u32, cfg: &SilenceConfig) -> (Instant, Instant) {
        books.apply_snapshot("A", [], [], ts(1), ts(1)).unwrap();
        let t0 = books.epoch;
        for i in 0..n {
            books.note_activity("A", t0 + cfg.bucket * i, cfg);
        }
        (t0, t0 + cfg.bucket * (n - 1))
    }

    #[test]
    fn a_symbol_busy_every_minute_is_silent_after_ten_empty_minutes() {
        let mut books = Books::new();
        let cfg = secs();
        let (_, last) = every_bucket(&mut books, 50, &cfg);
        // Buckets after the last event: 9 empty ones are not enough.
        assert_eq!(books.silent(last + cfg.bucket * 9, &cfg), (vec![], 1));
        assert_eq!(books.silent(last + cfg.bucket * 10, &cfg), (vec!["A".to_string()], 1));
    }

    #[test]
    fn a_bursty_symbol_is_not_active() {
        // Bursts every few minutes, many changes each: not "every minute".
        let mut books = Books::new();
        let cfg = secs();
        books.apply_snapshot("A", [], [], ts(1), ts(1)).unwrap();
        let t0 = books.epoch;
        for minute in (0..60).step_by(3) {
            for k in 0..50 {
                books.note_activity("A", t0 + cfg.bucket * minute + Duration::from_millis(k), &cfg);
            }
        }
        for later in 0..64 {
            assert_eq!(books.silent(t0 + cfg.bucket * (60 + later), &cfg), (vec![], 0));
        }
    }

    #[test]
    fn a_new_symbol_is_not_active_yet() {
        let mut books = Books::new();
        let cfg = secs();
        let (_, last) = every_bucket(&mut books, 30, &cfg);
        assert_eq!(books.silent(last + cfg.bucket * 20, &cfg), (vec![], 0));
    }

    #[test]
    fn stale_symbols_are_not_silent() {
        let mut books = Books::new();
        let cfg = secs();
        let (_, last) = every_bucket(&mut books, 50, &cfg);
        books.mark_stale("A");
        assert_eq!(books.silent(last + cfg.bucket * 10, &cfg), (vec![], 0));
    }

    #[test]
    fn a_market_that_stopped_trading_is_resynced_once() {
        let mut books = Books::new();
        let cfg = secs();
        let (_, last) = every_bucket(&mut books, 50, &cfg);
        let mut flagged = 0;
        for later in 1..=64 {
            let now = last + cfg.bucket * later;
            if !books.silent(now, &cfg).0.is_empty() {
                flagged += 1;
                // The resync's book (or empty guess): one more event.
                books.note_activity("A", now, &cfg);
            }
        }
        assert_eq!(flagged, 1);
        assert_eq!(books.silent(last + cfg.bucket * 64, &cfg).1, 0, "no longer active");
    }

    #[test]
    fn a_change_after_a_long_gap_starts_over() {
        let mut books = Books::new();
        let cfg = secs();
        let (_, last) = every_bucket(&mut books, 50, &cfg);
        books.note_activity("A", last + cfg.bucket * 200, &cfg);
        assert_eq!(books.silent(last + cfg.bucket * 300, &cfg), (vec![], 0));
    }

    #[test]
    fn remove_drops_the_book() {
        let mut books = Books::new();
        books.apply_snapshot("T", [], [], ts(1), ts(1)).unwrap();
        assert!(books.remove("T"));
        assert!(!books.remove("T"));
        assert!(books.minute_snapshots(ts(2)).next().is_none());
    }
}
