//! In-memory order book for one symbol, in the schema's fixed point.
//!
//! The book is exchange-agnostic: it stores absolute sizes per price level
//! and knows nothing about how an exchange encodes changes. It also builds the
//! template DTOs, so every message written for a symbol comes from one place.

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// Price → absolute quantity, both fixed point. Only non-zero quantities are
/// stored.
pub type Levels = BTreeMap<i64, i64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookError {
    NegativePrice { price: i64 },
    NegativeQuantity { price: i64, quantity: i64 },
    /// A delta took the quantity at `price` past `i64`.
    QuantityOverflow { price: i64 },
}

impl fmt::Display for BookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativePrice { price } => write!(f, "negative price {price}"),
            Self::NegativeQuantity { price, quantity } => {
                write!(f, "negative quantity {quantity} at price {price}")
            }
            Self::QuantityOverflow { price } => write!(f, "quantity overflow at price {price}"),
        }
    }
}

impl std::error::Error for BookError {}

/// One level where the local book and an exchange book disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelMismatch {
    pub side: Side,
    pub price: i64,
    /// Local quantity; `None` if the level is absent locally.
    pub local: Option<i64>,
    /// Exchange quantity; `None` if the level is absent at the exchange.
    pub exchange: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: Levels,
    asks: Levels,
    last_exchange_ts: Option<DateTime<Utc>>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bids(&self) -> &Levels {
        &self.bids
    }

    pub fn asks(&self) -> &Levels {
        &self.asks
    }

    /// The exchange timestamp of the last snapshot or change applied.
    pub fn last_exchange_ts(&self) -> Option<DateTime<Utc>> {
        self.last_exchange_ts
    }

    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.last_key_value().map(|(&p, &q)| (p, q))
    }

    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.first_key_value().map(|(&p, &q)| (p, q))
    }

    /// Sets one level to an absolute quantity; `0` removes it. Returns the
    /// previous quantity (`0` if the level did not exist).
    ///
    /// On error the book is unchanged.
    pub fn set_level(
        &mut self,
        side: Side,
        price: i64,
        quantity: i64,
        exchange_ts: DateTime<Utc>,
    ) -> Result<i64, BookError> {
        validate(price, quantity)?;
        let levels = self.side_mut(side);
        let previous = if quantity == 0 {
            levels.remove(&price)
        } else {
            levels.insert(price, quantity)
        };
        self.last_exchange_ts = Some(exchange_ts);
        Ok(previous.unwrap_or(0))
    }

    /// Replaces the whole book with an exchange snapshot. Zero quantities are
    /// dropped; a repeated price keeps the last quantity.
    ///
    /// On error the book is unchanged.
    pub fn replace(
        &mut self,
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
        exchange_ts: DateTime<Utc>,
    ) -> Result<(), BookError> {
        let bids = collect_levels(bids)?;
        let asks = collect_levels(asks)?;
        self.bids = bids;
        self.asks = asks;
        self.last_exchange_ts = Some(exchange_ts);
        Ok(())
    }

    /// Compares the book with an exchange snapshot, then replaces it. Returns
    /// every level that differed, bids first, each side by ascending price.
    ///
    /// Used at checkpoints (a `book` message arriving mid-stream): an empty
    /// result means no change was missed. On error the book is unchanged.
    pub fn check_and_replace(
        &mut self,
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
        exchange_ts: DateTime<Utc>,
    ) -> Result<Vec<LevelMismatch>, BookError> {
        let bids = collect_levels(bids)?;
        let asks = collect_levels(asks)?;
        let mut mismatches = diff(Side::Buy, &self.bids, &bids);
        mismatches.extend(diff(Side::Sell, &self.asks, &asks));
        self.bids = bids;
        self.asks = asks;
        self.last_exchange_ts = Some(exchange_ts);
        Ok(mismatches)
    }

    /// Builds a snapshot of the current book. Bids are ordered best (highest)
    /// first, asks best (lowest) first.
    pub fn snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut Levels {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}

/// Builds the update message for one level change.
pub fn level_update(
    symbol: &str,
    side: Side,
    price_level: i64,
    quantity: i64,
    backtest_timestamp: DateTime<Utc>,
    exchange_timestamp: DateTime<Utc>,
) -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        backtest_timestamp,
        exchange_timestamp,
        price_level,
        quantity,
        side,
        symbol: symbol.to_owned(),
    }
}

fn validate(price: i64, quantity: i64) -> Result<(), BookError> {
    if price < 0 {
        return Err(BookError::NegativePrice { price });
    }
    if quantity < 0 {
        return Err(BookError::NegativeQuantity { price, quantity });
    }
    Ok(())
}

fn collect_levels(levels: impl IntoIterator<Item = (i64, i64)>) -> Result<Levels, BookError> {
    let mut out = Levels::new();
    for (price, quantity) in levels {
        validate(price, quantity)?;
        if quantity == 0 {
            out.remove(&price);
        } else {
            out.insert(price, quantity);
        }
    }
    Ok(out)
}

fn diff(side: Side, local: &Levels, exchange: &Levels) -> Vec<LevelMismatch> {
    let mut out = Vec::new();
    let mut l = local.iter().peekable();
    let mut e = exchange.iter().peekable();
    loop {
        let (price, local_q, exchange_q) = match (l.peek(), e.peek()) {
            (None, None) => break,
            (Some(&(&p, &q)), None) => {
                l.next();
                (p, Some(q), None)
            }
            (None, Some(&(&p, &q))) => {
                e.next();
                (p, None, Some(q))
            }
            (Some(&(&lp, &lq)), Some(&(&ep, &eq))) => {
                if lp < ep {
                    l.next();
                    (lp, Some(lq), None)
                } else if ep < lp {
                    e.next();
                    (ep, None, Some(eq))
                } else {
                    l.next();
                    e.next();
                    (lp, Some(lq), Some(eq))
                }
            }
        };
        if local_q != exchange_q {
            out.push(LevelMismatch {
                side,
                price,
                local: local_q,
                exchange: exchange_q,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    fn book(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> OrderBook {
        let mut b = OrderBook::new();
        b.replace(bids.iter().copied(), asks.iter().copied(), ts(1)).unwrap();
        b
    }

    #[test]
    fn set_level_inserts_updates_and_removes() {
        let mut b = OrderBook::new();
        assert_eq!(b.set_level(Side::Buy, 500_000, 10, ts(1)), Ok(0));
        assert_eq!(b.set_level(Side::Buy, 500_000, 25, ts(2)), Ok(10));
        assert_eq!(b.bids().get(&500_000), Some(&25));
        assert_eq!(b.set_level(Side::Buy, 500_000, 0, ts(3)), Ok(25));
        assert!(b.bids().is_empty());
        assert_eq!(b.last_exchange_ts(), Some(ts(3)));
    }

    #[test]
    fn removing_a_missing_level_is_a_no_op() {
        let mut b = OrderBook::new();
        assert_eq!(b.set_level(Side::Sell, 400_000, 0, ts(1)), Ok(0));
        assert!(b.asks().is_empty());
    }

    #[test]
    fn sides_are_independent() {
        let mut b = OrderBook::new();
        b.set_level(Side::Buy, 400_000, 1, ts(1)).unwrap();
        b.set_level(Side::Sell, 400_000, 2, ts(1)).unwrap();
        assert_eq!(b.bids().get(&400_000), Some(&1));
        assert_eq!(b.asks().get(&400_000), Some(&2));
    }

    #[test]
    fn negative_values_are_rejected_and_leave_the_book_unchanged() {
        let mut b = book(&[(100, 1)], &[]);
        let before = b.clone();
        assert_eq!(
            b.set_level(Side::Buy, 100, -1, ts(9)),
            Err(BookError::NegativeQuantity { price: 100, quantity: -1 })
        );
        assert_eq!(
            b.set_level(Side::Sell, -5, 1, ts(9)),
            Err(BookError::NegativePrice { price: -5 })
        );
        assert!(b.replace([(1, 1)], [(2, -3)], ts(9)).is_err());
        assert_eq!(b, before);
    }

    #[test]
    fn replace_drops_zero_levels_and_keeps_last_duplicate() {
        let b = book(&[(100, 5), (200, 0), (100, 7)], &[(300, 1)]);
        assert_eq!(b.bids().iter().collect::<Vec<_>>(), [(&100, &7)]);
        assert_eq!(b.asks().len(), 1);
    }

    #[test]
    fn best_prices() {
        let b = book(&[(100, 1), (300, 2), (200, 3)], &[(700, 1), (500, 2), (600, 3)]);
        assert_eq!(b.best_bid(), Some((300, 2)));
        assert_eq!(b.best_ask(), Some((500, 2)));
        assert_eq!(OrderBook::new().best_bid(), None);
    }

    #[test]
    fn check_and_replace_reports_every_difference() {
        let mut b = book(&[(100, 1), (200, 2), (300, 3)], &[(500, 5)]);
        let mismatches = b
            .check_and_replace([(100, 1), (200, 9), (400, 4)], [(500, 5), (600, 6)], ts(5))
            .unwrap();
        let m = |side, price, local, exchange| LevelMismatch {
            side,
            price,
            local,
            exchange,
        };
        assert_eq!(
            mismatches,
            [
                m(Side::Buy, 200, Some(2), Some(9)),
                m(Side::Buy, 300, Some(3), None),
                m(Side::Buy, 400, None, Some(4)),
                m(Side::Sell, 600, None, Some(6)),
            ]
        );
        // The exchange book wins.
        let expected = book(&[(100, 1), (200, 9), (400, 4)], &[(500, 5), (600, 6)]);
        assert_eq!(b.bids(), expected.bids());
        assert_eq!(b.asks(), expected.asks());
        assert_eq!(b.last_exchange_ts(), Some(ts(5)));
    }

    #[test]
    fn check_and_replace_matches_after_updates() {
        let mut b = book(&[(100, 1)], &[(500, 5)]);
        b.set_level(Side::Buy, 100, 0, ts(2)).unwrap();
        b.set_level(Side::Buy, 150, 3, ts(3)).unwrap();
        b.set_level(Side::Sell, 500, 4, ts(4)).unwrap();
        let mismatches = b.check_and_replace([(150, 3)], [(500, 4)], ts(5)).unwrap();
        assert!(mismatches.is_empty());
    }

    #[test]
    fn snapshot_orders_best_first() {
        let b = book(&[(100, 1), (300, 3), (200, 2)], &[(700, 7), (500, 5)]);
        let s = b.snapshot("TOKEN", ts(10), ts(9));
        assert_eq!(s.symbol, "TOKEN");
        assert_eq!(s.received_at, ts(10));
        assert_eq!(s.exchange_timestamp, ts(9));
        let bids: Vec<_> = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
        let asks: Vec<_> = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
        assert_eq!(bids, [(300, 3), (200, 2), (100, 1)]);
        assert_eq!(asks, [(500, 5), (700, 7)]);
    }

    #[test]
    fn snapshot_and_update_round_trip_through_the_template_writer() {
        use crate::util::dtos::challenge_orderbook::AnyMessage;
        use crate::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

        let b = book(&[(400_000, 2_000_000)], &[(600_000, 3_500_000)]);
        let snapshot = b.snapshot("TOKEN", ts(10), ts(9));
        let update = level_update("TOKEN", Side::Sell, 600_000, 0, ts(12), ts(11));

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = AlignedSbeWriter::create(file.path(), 64 << 20).unwrap();
        writer.write_msg(&snapshot).unwrap();
        writer.write_msg(&update).unwrap();
        writer.flush().unwrap();
        drop(writer);

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let Some(AnyMessage::Snapshot(d)) = reader.read_msg().unwrap() else {
            panic!("expected snapshot");
        };
        assert_eq!(SnapshotDomain::try_from_decoder(d).unwrap(), snapshot);
        let Some(AnyMessage::PriceLevelUpdate(d)) = reader.read_msg().unwrap() else {
            panic!("expected update");
        };
        assert_eq!(PriceLevelUpdateDomain::try_from_decoder(d).unwrap(), update);
        assert!(reader.read_msg().unwrap().is_none());
    }
}
