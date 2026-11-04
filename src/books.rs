//! The books owned by one connection, and the rules for turning exchange
//! events into output messages with the spec's timestamps (Section 5):
//!
//! | Message | `received_at` / `backtest_timestamp` | `exchange_timestamp` |
//! | --- | --- | --- |
//! | exchange snapshot | local receive time | snapshot time from the exchange |
//! | level change | local receive time | change time from the exchange |
//! | minute snapshot | emit time | last exchange time seen for the symbol |

use std::collections::HashMap;
use std::fmt;

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
}

#[derive(Debug, Default)]
pub struct Books {
    books: HashMap<String, OrderBook>,
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
        self.books.get(symbol)
    }

    pub fn contains(&self, symbol: &str) -> bool {
        self.books.contains_key(symbol)
    }

    /// Drops a symbol, e.g. after unsubscribing. Returns whether it existed.
    pub fn remove(&mut self, symbol: &str) -> bool {
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
            Some(book) => Some(
                book.check_and_replace(bids, asks, exchange_ts)
                    .map_err(book_err)?,
            ),
            None => {
                let mut book = OrderBook::new();
                book.replace(bids, asks, exchange_ts).map_err(book_err)?;
                self.books.insert(symbol.to_owned(), book);
                None
            }
        };
        let book = &self.books[symbol];
        Ok(SnapshotApplied {
            msg: OutMsg::Snapshot(book.snapshot(symbol, received_at, exchange_ts)),
            mismatches,
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
        let book = self.books.get_mut(symbol).ok_or_else(|| BooksError::NoBaseline {
            symbol: symbol.to_owned(),
        })?;
        book.set_level(side, price, quantity, exchange_ts)
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

    /// One `Snapshot` per symbol, for the minute timer, sorted by symbol so
    /// the output order is deterministic.
    pub fn minute_snapshots(&self, emitted_at: DateTime<Utc>) -> Vec<OutMsg> {
        let mut symbols: Vec<&String> = self.books.keys().collect();
        symbols.sort_unstable();
        symbols
            .into_iter()
            .map(|symbol| {
                let book = &self.books[symbol];
                let exchange_ts = book.last_exchange_ts().unwrap_or(emitted_at);
                OutMsg::Snapshot(book.snapshot(symbol, emitted_at, exchange_ts))
            })
            .collect()
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

        let snaps = books.minute_snapshots(ts(60_000));
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
    fn remove_drops_the_book() {
        let mut books = Books::new();
        books.apply_snapshot("T", [], [], ts(1), ts(1)).unwrap();
        assert!(books.remove("T"));
        assert!(!books.remove("T"));
        assert!(books.minute_snapshots(ts(2)).is_empty());
    }
}
