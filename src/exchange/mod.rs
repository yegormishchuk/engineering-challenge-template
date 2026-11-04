//! Exchange-specific encoding and decoding. Everything above this module
//! (connections, books, writer) works only with [`Event`].

pub mod polymarket;

use chrono::{DateTime, Utc};

use crate::Side;

/// A normalized message from an exchange stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// Full book for one symbol. Prices and quantities are fixed point.
    Book {
        symbol: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        exchange_ts: DateTime<Utc>,
        hash: Option<String>,
    },
    /// One level set to an absolute quantity; `0` removes it.
    Level {
        symbol: String,
        side: Side,
        price: i64,
        quantity: i64,
        exchange_ts: DateTime<Utc>,
        hash: Option<String>,
    },
    /// A market was created; `symbols` are its order book ids.
    MarketAdded { market: String, symbols: Vec<String> },
    /// A market resolved; its symbols stop trading.
    MarketResolved { market: String, symbols: Vec<String> },
    TickSizeChanged { symbol: String, old: String, new: String },
    /// Heartbeat answer.
    Pong,
    /// A known message type that is not recorded (trades, best bid/ask), or
    /// an unknown one. Counted, not an error.
    Ignored { kind: String },
}
