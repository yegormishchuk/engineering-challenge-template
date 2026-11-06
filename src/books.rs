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

/// How often a symbol's book changes (Extension 2). An event is one frame
/// that touched the symbol: a `price_change` can carry several entries for
/// it, which are one event.
#[derive(Debug, Clone, Copy)]
struct Activity {
    last: Instant,
    /// Moving average of the time between events, in seconds.
    mean_secs: f32,
    events: u32,
}

/// When a symbol counts as gone quiet: it was active (at least
/// `min_changes` events, mean gap under `active_below`) and nothing came for
/// `max(factor × mean gap, min_silence)`.
#[derive(Debug, Clone)]
pub struct SilenceConfig {
    /// How often a connection looks for silent symbols.
    pub check_every: Duration,
    pub min_changes: u32,
    pub active_below: Duration,
    pub factor: f64,
    pub min_silence: Duration,
    /// Most symbols resubscribed per check.
    pub max_per_check: usize,
}

impl Default for SilenceConfig {
    fn default() -> Self {
        Self {
            check_every: Duration::from_secs(5),
            min_changes: 10,
            active_below: Duration::from_secs(60),
            factor: 5.0,
            min_silence: Duration::from_secs(60),
            max_per_check: 100,
        }
    }
}

#[derive(Debug, Default)]
pub struct Books {
    books: HashMap<String, Entry>,
    stale: HashSet<String>,
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
                    activity: Activity {
                        last: now,
                        mean_secs: 0.0,
                        events: 0,
                    },
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

    /// Applies one level change (absolute quantity, `0` removes) and returns
    /// the `PriceLevelUpdate` to write. On error nothing changes.
    pub fn apply_level(
        &mut self,
        symbol: &str,
        side: Side,
        price: i64,
        quantity: i64,
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

    /// Records one activity event for `symbol` at `at` (the frame's receive
    /// instant). Repeats of the same instant are the same frame.
    pub fn note_activity(&mut self, symbol: &str, at: Instant) {
        let Some(entry) = self.books.get_mut(symbol) else {
            return;
        };
        let a = &mut entry.activity;
        if a.events > 0 && at == a.last {
            return;
        }
        if a.events > 0 {
            let gap = at.saturating_duration_since(a.last).as_secs_f32();
            a.mean_secs = if a.events == 1 { gap } else { 0.9 * a.mean_secs + 0.1 * gap };
        }
        a.last = at;
        a.events = a.events.saturating_add(1);
    }

    /// Live symbols that went quiet by `cfg` at `now` (sorted), and how many
    /// live symbols are active.
    pub fn silent(&self, now: Instant, cfg: &SilenceConfig) -> (Vec<String>, usize) {
        let mut silent = Vec::new();
        let mut active = 0;
        for (symbol, entry) in &self.books {
            let a = &entry.activity;
            if self.stale.contains(symbol)
                || a.events < cfg.min_changes
                || f64::from(a.mean_secs) >= cfg.active_below.as_secs_f64()
            {
                continue;
            }
            active += 1;
            let threshold = (cfg.factor * f64::from(a.mean_secs)).max(cfg.min_silence.as_secs_f64());
            if now.saturating_duration_since(a.last).as_secs_f64() > threshold {
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
        books.apply_level("T", Side::Buy, 100, 5, ts(2), ts(2)).unwrap();
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
        let OutMsg::Update(u) = books.apply_level("T", Side::Sell, 300, 7, ts(20), ts(21)).unwrap()
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
            books.apply_level("T", Side::Buy, 1, 1, ts(1), ts(1)),
            Err(BooksError::NoBaseline { symbol: "T".into() })
        );
        assert!(books.is_empty());
    }

    #[test]
    fn invalid_change_leaves_book_unchanged() {
        let mut books = Books::new();
        books.apply_snapshot("T", [(100, 1)], [], ts(1), ts(1)).unwrap();
        assert!(books.apply_level("T", Side::Buy, 100, -1, ts(2), ts(2)).is_err());
        assert!(books.apply_snapshot("T", [(100, -1)], [], ts(3), ts(3)).is_err());
        let book = books.get("T").unwrap();
        assert_eq!(book.bids().get(&100), Some(&1));
        assert_eq!(book.last_exchange_ts(), Some(ts(1)));
    }

    #[test]
    fn minute_snapshots_cover_every_symbol_with_last_exchange_time() {
        let mut books = Books::new();
        books.apply_snapshot("B", [(1, 1)], [], ts(5), ts(6)).unwrap();
        books.apply_snapshot("A", [], [(2, 2)], ts(7), ts(8)).unwrap();
        books.apply_level("A", Side::Sell, 2, 3, ts(9), ts(10)).unwrap();

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
            books.apply_level("T", Side::Buy, 100, 2, ts(2), ts(2)),
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

    /// A snapshot of `A`, then `n` activity events `gap` apart. Returns the
    /// last event's instant.
    fn active_feed(books: &mut Books, n: u32, gap: Duration) -> Instant {
        books.apply_snapshot("A", [], [], ts(1), ts(1)).unwrap();
        let t0 = Instant::now();
        for i in 0..n {
            books.note_activity("A", t0 + gap * i);
        }
        t0 + gap * (n - 1)
    }

    fn floor(min_silence: Duration) -> SilenceConfig {
        SilenceConfig {
            min_silence,
            ..SilenceConfig::default()
        }
    }

    #[test]
    fn silent_lists_an_active_symbol_quiet_for_five_mean_gaps() {
        let mut books = Books::new();
        let last = active_feed(&mut books, 12, Duration::from_secs(1));
        let cfg = floor(Duration::from_secs(3));
        assert_eq!(books.silent(last + Duration::from_millis(4900), &cfg), (vec![], 1));
        assert_eq!(books.silent(last + Duration::from_millis(5100), &cfg), (vec!["A".to_string()], 1));
    }

    #[test]
    fn min_silence_is_a_floor() {
        let mut books = Books::new();
        let last = active_feed(&mut books, 12, Duration::from_secs(1));
        let cfg = SilenceConfig::default();
        assert!(books.silent(last + Duration::from_secs(59), &cfg).0.is_empty());
        assert_eq!(books.silent(last + Duration::from_secs(61), &cfg).0, ["A"]);
    }

    #[test]
    fn a_rarely_changing_symbol_is_never_silent() {
        let mut books = Books::new();
        let last = active_feed(&mut books, 12, Duration::from_secs(120));
        assert_eq!(books.silent(last + Duration::from_secs(3600), &SilenceConfig::default()), (vec![], 0));
    }

    #[test]
    fn few_changes_are_not_enough() {
        let mut books = Books::new();
        let last = active_feed(&mut books, 5, Duration::from_secs(1));
        assert_eq!(books.silent(last + Duration::from_secs(600), &SilenceConfig::default()), (vec![], 0));
    }

    #[test]
    fn changes_in_one_frame_count_once() {
        let mut books = Books::new();
        books.apply_snapshot("A", [], [], ts(1), ts(1)).unwrap();
        let t0 = Instant::now();
        let mut last = t0;
        for i in 0..12 {
            last = t0 + Duration::from_secs(i);
            for _ in 0..3 {
                books.note_activity("A", last);
            }
        }
        let cfg = floor(Duration::from_secs(3));
        // Mean 1 s: a mean pulled toward 0 by repeats would flag it at 3 s.
        assert!(books.silent(last + Duration::from_millis(4900), &cfg).0.is_empty());
        assert_eq!(books.silent(last + Duration::from_millis(5100), &cfg).0, ["A"]);
    }

    #[test]
    fn stale_symbols_are_not_silent() {
        let mut books = Books::new();
        let last = active_feed(&mut books, 12, Duration::from_secs(1));
        books.mark_stale("A");
        assert!(books.silent(last + Duration::from_secs(600), &SilenceConfig::default()).0.is_empty());
    }

    #[test]
    fn a_market_that_stopped_trading_is_dropped_after_a_few_resyncs() {
        let mut books = Books::new();
        let mut last = active_feed(&mut books, 12, Duration::from_secs(1));
        let cfg = floor(Duration::from_secs(3));
        for _ in 0..15 {
            // Probe forward until it is flagged, as the periodic check would.
            let mut now = last;
            loop {
                now += Duration::from_millis(100);
                let (silent, active) = books.silent(now, &cfg);
                if active == 0 {
                    return;
                }
                if !silent.is_empty() {
                    break;
                }
            }
            // The resync's book arrives: one more activity event. Each long gap
            // raises the mean ~1.4x (weight 0.1, gap = 5x mean), so a market at
            // 1 s between changes needs ~12 rounds to stop counting as active.
            books.note_activity("A", now);
            last = now;
        }
        panic!("still active after 15 resyncs");
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
