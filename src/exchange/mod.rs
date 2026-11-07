//! Exchange-specific encoding and decoding. Everything above this module
//! (connections, books, writer) works only with [`Event`] and the
//! [`Exchange`] trait: plugging in another websocket exchange means
//! implementing [`Exchange`] and [`Codec`] (and, optionally, [`BookApi`]).

pub mod polymarket;

pub use polymarket::Polymarket;

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::fixed::ParseFixedError;

/// One websocket exchange.
///
/// Requirement on every exchange (the connection's barriers rely on it): the
/// server answers each [`ping_text`](Self::ping_text) in order, after every
/// frame it sent before receiving it.
pub trait Exchange: Send + Sync + std::fmt::Debug + 'static {
    fn name(&self) -> &'static str;
    fn ws_url(&self) -> &str;
    /// Text the client sends as an application heartbeat / barrier.
    fn ping_text(&self) -> &str;
    /// What the exchange can promise; enables exchange-specific logic in the connection.
    fn capabilities(&self) -> Capabilities;
    /// One codec per websocket session (it may keep state, e.g. sequence numbers).
    fn codec(&self) -> Box<dyn Codec>;
    /// Optional REST check of books assumed empty (Polymarket `POST /books`); None = no checks.
    fn book_api(&self) -> Option<&dyn BookApi> {
        None
    }
}

/// `Sync` as well as `Send`: the connection holding it is shared by
/// reference across `.await`s inside its own task.
pub trait Codec: Send + Sync {
    fn encode_initial_subscribe(&mut self, symbols: &[String]) -> String;
    fn encode_subscribe(&mut self, symbols: &[String]) -> String;
    fn encode_unsubscribe(&mut self, symbols: &[String]) -> String;
    /// A sequence gap must be returned as a `Failure` naming the symbols, so the connection resyncs them.
    fn decode(&mut self, text: &str) -> Result<Decoded, DecodeError>;
}

pub trait BookApi: Send + Sync {
    fn url(&self) -> &str;
    fn encode_request(&self, symbols: &[String]) -> String;
    fn parse_response(&self, body: &str) -> Result<HashMap<String, RestBook>, DecodeError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// The exchange sends no book for a symbol without orders (Polymarket): enables empty-book guesses.
    pub missing_book_means_empty: bool,
}

/// How an [`Event::Level`] quantity applies to the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelKind {
    /// The level's new quantity; `0` removes it.
    Absolute,
    /// Signed change to the level's current quantity (a missing level is 0).
    Delta,
}

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
    /// One level change; see [`LevelKind`].
    Level {
        symbol: String,
        side: Side,
        price: i64,
        quantity: i64,
        kind: LevelKind,
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

/// One book from a REST book check, in fixed point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestBook {
    pub bids: Vec<(i64, i64)>,
    pub asks: Vec<(i64, i64)>,
}

#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
    Number { field: &'static str, error: ParseFixedError },
    Timestamp(String),
    Side(String),
    /// A message skipped one or more sequence numbers: changes were missed.
    Sequence { expected: u64, got: u64 },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "bad JSON: {e}"),
            Self::Number { field, error } => write!(f, "{field}: {error}"),
            Self::Timestamp(s) => write!(f, "bad timestamp {s:?}"),
            Self::Side(s) => write!(f, "unknown side {s:?}"),
            Self::Sequence { expected, got } => write!(f, "sequence gap: expected {expected}, got {got}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// A book message that could not be decoded (or a sequence gap).
#[derive(Debug)]
pub struct Failure {
    pub kind: &'static str,
    /// The symbols the message was about, as far as they could be read;
    /// empty if unknown.
    pub symbols: Vec<String>,
    pub error: DecodeError,
}

/// Everything decoded from one frame. Messages are independent, so the good
/// ones are kept even when another message of the frame fails.
#[derive(Debug, Default)]
pub struct Decoded {
    pub events: Vec<Event>,
    pub failures: Vec<Failure>,
}
