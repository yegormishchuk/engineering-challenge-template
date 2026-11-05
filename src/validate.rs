//! Offline checks of a recorded file: replay every `PriceLevelUpdate` onto
//! the books built from `Snapshot`s, and compare the replayed book with
//! every later snapshot of the same symbol.
//!
//! The collector writes two kinds of snapshot that the format does not tell
//! apart: minute snapshots, built from the collector's own books, and
//! exchange books. A minute snapshot that does not match the replay means the
//! file is inconsistent (a lost or reordered message). An exchange book that
//! does not match is a change the exchange stream missed; the collector also
//! counts those as `mismatched_books`. Either way the snapshot then becomes
//! the replay baseline, just as it does for a backtest.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::book::{LevelMismatch, OrderBook};
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    pub snapshots: u64,
    pub updates: u64,
    pub symbols: usize,
    /// Snapshots of a symbol that already had a replayed book, so they could
    /// be compared.
    pub compared: u64,
    pub matched: u64,
    pub mismatched: u64,
    pub mismatched_levels: u64,
    /// Updates for a symbol with no snapshot before them: unusable for replay.
    pub updates_without_baseline: u64,
    /// Negative prices or quantities, or prices above 1 USDC.
    pub invalid_values: u64,
    /// Snapshots whose bids are not strictly descending or asks not strictly
    /// ascending.
    pub unsorted_snapshots: u64,
    /// Per symbol, a receive timestamp earlier than the previous one.
    pub time_reversals: u64,
    pub first_mismatches: Vec<(String, LevelMismatch)>,
}

impl Report {
    /// True when nothing indicates a broken file. Exchange-book mismatches
    /// alone do not fail a file, since they reflect the exchange stream; the
    /// caller compares `mismatched` with the collector's counter.
    pub fn file_is_consistent(&self) -> bool {
        self.updates_without_baseline == 0
            && self.invalid_values == 0
            && self.unsorted_snapshots == 0
            && self.time_reversals == 0
    }
}

const MAX_PRICE: i64 = 1_000_000;
const KEEP_MISMATCHES: usize = 10;

#[derive(Default)]
pub struct Validator {
    books: HashMap<String, OrderBook>,
    last_recv: HashMap<String, DateTime<Utc>>,
    report: Report,
}

impl Validator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&mut self, s: &SnapshotDomain) {
        self.report.snapshots += 1;
        self.check_time(&s.symbol, s.received_at);
        let bids: Vec<(i64, i64)> = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
        let asks: Vec<(i64, i64)> = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
        if !bids.windows(2).all(|w| w[0].0 > w[1].0) || !asks.windows(2).all(|w| w[0].0 < w[1].0) {
            self.report.unsorted_snapshots += 1;
        }
        let invalid = bids
            .iter()
            .chain(&asks)
            .filter(|&&(p, q)| !valid(p, q))
            .count();
        if invalid > 0 {
            self.report.invalid_values += invalid as u64;
            return;
        }
        match self.books.get_mut(&s.symbol) {
            Some(book) => {
                let mismatches = book
                    .check_and_replace(bids, asks, s.exchange_timestamp)
                    .expect("values were validated");
                self.report.compared += 1;
                if mismatches.is_empty() {
                    self.report.matched += 1;
                } else {
                    self.report.mismatched += 1;
                    self.report.mismatched_levels += mismatches.len() as u64;
                    if self.report.first_mismatches.len() < KEEP_MISMATCHES {
                        self.report
                            .first_mismatches
                            .push((s.symbol.clone(), mismatches[0]));
                    }
                }
            }
            None => {
                let mut book = OrderBook::new();
                book.replace(bids, asks, s.exchange_timestamp)
                    .expect("values were validated");
                self.books.insert(s.symbol.clone(), book);
            }
        }
    }

    pub fn update(&mut self, u: &PriceLevelUpdateDomain) {
        self.report.updates += 1;
        self.check_time(&u.symbol, u.backtest_timestamp);
        if !valid(u.price_level, u.quantity) {
            self.report.invalid_values += 1;
            return;
        }
        let Some(book) = self.books.get_mut(&u.symbol) else {
            self.report.updates_without_baseline += 1;
            return;
        };
        book.set_level(u.side, u.price_level, u.quantity, u.exchange_timestamp)
            .expect("values were validated");
    }

    pub fn finish(mut self) -> Report {
        self.report.symbols = self.books.len();
        self.report
    }

    fn check_time(&mut self, symbol: &str, recv: DateTime<Utc>) {
        if let Some(last) = self.last_recv.get_mut(symbol) {
            if recv < *last {
                self.report.time_reversals += 1;
            }
            *last = recv.max(*last);
        } else {
            self.last_recv.insert(symbol.to_owned(), recv);
        }
    }
}

fn valid(price: i64, quantity: i64) -> bool {
    (0..=MAX_PRICE).contains(&price) && quantity >= 0
}

/// For display: `Buy`/`Sell` and fixed point as decimals.
pub fn describe(symbol: &str, m: &LevelMismatch) -> String {
    let fmt = |q: Option<i64>| q.map_or("-".to_owned(), |q| format!("{:.6}", q as f64 / 1e6));
    let side = match m.side {
        Side::Buy => "bid",
        Side::Sell => "ask",
    };
    format!(
        "{symbol} {side} {:.6}: replayed {} vs snapshot {}",
        m.price as f64 / 1e6,
        fmt(m.local),
        fmt(m.exchange)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::level_update;

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    fn snap(symbol: &str, at: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> SnapshotDomain {
        let mut b = OrderBook::new();
        b.replace(bids.iter().copied(), asks.iter().copied(), ts(at)).unwrap();
        b.snapshot(symbol, ts(at), ts(at))
    }

    #[test]
    fn replayed_updates_match_the_next_snapshot() {
        let mut v = Validator::new();
        v.snapshot(&snap("A", 1, &[(400_000, 10)], &[(600_000, 20)]));
        v.update(&level_update("A", Side::Buy, 400_000, 15, ts(2), ts(2)));
        v.update(&level_update("A", Side::Sell, 600_000, 0, ts(3), ts(3)));
        v.snapshot(&snap("A", 4, &[(400_000, 15)], &[]));
        let r = v.finish();
        assert_eq!((r.snapshots, r.updates, r.symbols), (2, 2, 1));
        assert_eq!((r.compared, r.matched, r.mismatched), (1, 1, 0));
        assert!(r.file_is_consistent());
    }

    #[test]
    fn a_missing_update_shows_as_a_mismatch_and_the_snapshot_becomes_the_baseline() {
        let mut v = Validator::new();
        v.snapshot(&snap("A", 1, &[(400_000, 10)], &[]));
        // The update to 15 was lost.
        v.snapshot(&snap("A", 2, &[(400_000, 15)], &[]));
        v.update(&level_update("A", Side::Buy, 400_000, 16, ts(3), ts(3)));
        v.snapshot(&snap("A", 4, &[(400_000, 16)], &[]));
        let r = v.finish();
        assert_eq!((r.compared, r.matched, r.mismatched, r.mismatched_levels), (2, 1, 1, 1));
        assert_eq!(r.first_mismatches[0].1.local, Some(10));
        assert!(r.file_is_consistent(), "a mismatch alone is not a broken file");
    }

    #[test]
    fn structural_problems_make_the_file_inconsistent() {
        let mut v = Validator::new();
        v.update(&level_update("B", Side::Buy, 1, 1, ts(1), ts(1)));
        v.snapshot(&snap("A", 5, &[], &[]));
        v.update(&level_update("A", Side::Buy, 2_000_000, 1, ts(6), ts(6)));
        v.update(&level_update("A", Side::Buy, 1, 1, ts(4), ts(4)));
        let mut unsorted = snap("C", 1, &[(1, 1), (2, 1)], &[]);
        unsorted.bids.reverse();
        v.snapshot(&unsorted);
        let r = v.finish();
        assert_eq!(r.updates_without_baseline, 1);
        assert_eq!(r.invalid_values, 1);
        assert_eq!(r.time_reversals, 1);
        assert_eq!(r.unsorted_snapshots, 1);
        assert!(!r.file_is_consistent());
    }

    #[test]
    fn describe_is_readable() {
        let m = LevelMismatch {
            side: Side::Sell,
            price: 650_000,
            local: None,
            exchange: Some(6_153_845),
        };
        assert_eq!(describe("T", &m), "T ask 0.650000: replayed - vs snapshot 6.153845");
    }
}
