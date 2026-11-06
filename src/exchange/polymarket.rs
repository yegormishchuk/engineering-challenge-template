//! Polymarket CLOB market channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).
//!
//! Formats were verified against live frames in M0 (spec, Section 4):
//! - a frame is one JSON object or a JSON array of objects, or the text `PONG`;
//! - `book` carries a full book; `price_change` carries absolute sizes;
//! - numbers are decimal strings, timestamps are millisecond strings.
//!
//! Decoding is strict only where the books depend on it. A `book` or
//! `price_change` that does not decode is reported as a [`Failure`] naming
//! the symbols it was about, so the caller can resync just those. Every
//! other message type is decoded leniently: a malformed one is ignored and
//! counted, never a reason to drop books.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use super::Event;
use crate::Side;
use crate::fixed::{ParseFixedError, parse_fixed};

pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Batch book endpoint, used to confirm that a token without a websocket
/// `book` really has no orders (it is left out of the response).
pub const BOOKS_URL: &str = "https://clob.polymarket.com/books";

/// Text frame the client sends every 10 s; the server answers `PONG`.
pub const PING: &str = "PING";
const PONG: &str = "PONG";

/// Exchange timestamps outside 2020-01-01 .. 2100-01-01 are rejected: they
/// are wrong, and one before 1970 cannot even be encoded in the schema.
const MIN_TS_MS: i64 = 1_577_836_800_000;
const MAX_TS_MS: i64 = 4_102_444_800_000;

/// First subscription on a fresh socket. `custom_feature_enabled` adds
/// `new_market` and `market_resolved` events, used for discovery.
pub fn encode_initial_subscribe(symbols: &[String]) -> String {
    json!({ "assets_ids": symbols, "type": "market", "custom_feature_enabled": true }).to_string()
}

/// Adds symbols to a socket that already sent its initial subscription.
pub fn encode_subscribe(symbols: &[String]) -> String {
    json!({ "assets_ids": symbols, "operation": "subscribe" }).to_string()
}

pub fn encode_unsubscribe(symbols: &[String]) -> String {
    json!({ "assets_ids": symbols, "operation": "unsubscribe" }).to_string()
}

/// Body for [`BOOKS_URL`].
pub fn encode_books_request(symbols: &[String]) -> String {
    Value::Array(symbols.iter().map(|s| json!({ "token_id": s })).collect()).to_string()
}

/// From a [`BOOKS_URL`] response, the symbols whose book has at least one
/// level. Tokens without orders are absent from the response.
pub fn symbols_with_orders(body: &str) -> Result<Vec<String>, DecodeError> {
    let books: Vec<Value> = serde_json::from_str(body).map_err(DecodeError::Json)?;
    Ok(books
        .iter()
        .filter(|b| {
            let non_empty = |side: &str| b[side].as_array().is_some_and(|a| !a.is_empty());
            non_empty("bids") || non_empty("asks")
        })
        .filter_map(|b| b["asset_id"].as_str().map(str::to_owned))
        .collect())
}

#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
    Number { field: &'static str, error: ParseFixedError },
    Timestamp(String),
    Side(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(e) => write!(f, "bad JSON: {e}"),
            Self::Number { field, error } => write!(f, "{field}: {error}"),
            Self::Timestamp(s) => write!(f, "bad timestamp {s:?}"),
            Self::Side(s) => write!(f, "unknown side {s:?}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// A `book` or `price_change` message that could not be decoded.
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

#[derive(Deserialize)]
struct BookMsg {
    asset_id: String,
    timestamp: String,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
}

#[derive(Deserialize)]
struct PriceChangeMsg {
    timestamp: String,
    price_changes: Vec<RawChange>,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct RawChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    #[serde(default)]
    hash: Option<String>,
}

fn number(field: &'static str, s: &str) -> Result<i64, DecodeError> {
    parse_fixed(s).map_err(|error| DecodeError::Number { field, error })
}

fn timestamp(s: &str) -> Result<DateTime<Utc>, DecodeError> {
    s.parse::<i64>()
        .ok()
        .filter(|ms| (MIN_TS_MS..MAX_TS_MS).contains(ms))
        .and_then(DateTime::from_timestamp_millis)
        .ok_or_else(|| DecodeError::Timestamp(s.to_owned()))
}

fn levels(raw: &[RawLevel]) -> Result<Vec<(i64, i64)>, DecodeError> {
    raw.iter()
        .map(|l| Ok((number("price", &l.price)?, number("size", &l.size)?)))
        .collect()
}

fn side(s: &str) -> Result<Side, DecodeError> {
    match s {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => Err(DecodeError::Side(other.to_owned())),
    }
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str().map(str::to_owned))
        .collect()
}

/// The symbols a book message is about, read without trusting the rest.
fn symbols_of(item: &Value) -> Vec<String> {
    if let Some(a) = item["asset_id"].as_str() {
        return vec![a.to_owned()];
    }
    let mut out: Vec<String> = item["price_changes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["asset_id"].as_str().map(str::to_owned))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn book(item: Value) -> Result<Event, DecodeError> {
    let m: BookMsg = serde_json::from_value(item).map_err(DecodeError::Json)?;
    Ok(Event::Book {
        bids: levels(&m.bids)?,
        asks: levels(&m.asks)?,
        exchange_ts: timestamp(&m.timestamp)?,
        symbol: m.asset_id,
        hash: m.hash,
    })
}

/// All entries of one `price_change`, or an error: the entries share one
/// timestamp, and a message is treated as a unit.
fn price_change(item: Value) -> Result<Vec<Event>, DecodeError> {
    let m: PriceChangeMsg = serde_json::from_value(item).map_err(DecodeError::Json)?;
    let exchange_ts = timestamp(&m.timestamp)?;
    m.price_changes
        .into_iter()
        .map(|c| {
            Ok(Event::Level {
                side: side(&c.side)?,
                price: number("price", &c.price)?,
                quantity: number("size", &c.size)?,
                symbol: c.asset_id,
                exchange_ts,
                hash: c.hash,
            })
        })
        .collect()
}

fn ignored(kind: &str) -> Event {
    Event::Ignored { kind: kind.to_owned() }
}

fn decode_item(item: Value, out: &mut Decoded) {
    let kind = item["event_type"].as_str().unwrap_or_default().to_owned();
    match kind.as_str() {
        "book" => {
            let symbols = symbols_of(&item);
            match book(item) {
                Ok(event) => out.events.push(event),
                Err(error) => out.failures.push(Failure { kind: "book", symbols, error }),
            }
        }
        "price_change" => {
            let symbols = symbols_of(&item);
            match price_change(item) {
                Ok(events) => out.events.extend(events),
                Err(error) => out.failures.push(Failure {
                    kind: "price_change",
                    symbols,
                    error,
                }),
            }
        }
        "new_market" | "market_resolved" => {
            let market = item["market"].as_str().or_else(|| item["condition_id"].as_str());
            let symbols = strings(&item["assets_ids"]);
            out.events.push(match market {
                Some(market) if !symbols.is_empty() => {
                    let market = market.to_owned();
                    if kind == "new_market" {
                        Event::MarketAdded { market, symbols }
                    } else {
                        Event::MarketResolved { market, symbols }
                    }
                }
                _ => ignored(&format!("malformed {kind}")),
            });
        }
        "tick_size_change" => {
            let field = |k: &str| item[k].as_str().map(str::to_owned);
            out.events.push(
                match (field("asset_id"), field("old_tick_size"), field("new_tick_size")) {
                    (Some(symbol), Some(old), Some(new)) => Event::TickSizeChanged { symbol, old, new },
                    _ => ignored("malformed tick_size_change"),
                },
            );
        }
        "last_trade_price" | "best_bid_ask" => out.events.push(ignored(&kind)),
        "" => out.events.push(ignored("no event_type")),
        _ => out.events.push(ignored("unknown")),
    }
}

/// Decodes one text frame. Returns `Err` only for a frame that looks like
/// JSON but does not parse: its contents, possibly book data, are unknown.
/// Text that is not JSON at all (a server notice, say) is ignored.
pub fn decode(text: &str) -> Result<Decoded, DecodeError> {
    let mut out = Decoded::default();
    if text == PONG {
        out.events.push(Event::Pong);
        return Ok(out);
    }
    if !matches!(text.trim_start().as_bytes().first(), Some(b'{' | b'[')) {
        out.events.push(ignored("non-JSON text"));
        return Ok(out);
    }
    let items = match serde_json::from_str(text).map_err(DecodeError::Json)? {
        Value::Array(items) => items,
        other => vec![other],
    };
    out.events.reserve(items.len());
    for item in items {
        decode_item(item, &mut out);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    /// Decodes a frame that must decode without failures.
    fn events(text: &str) -> Vec<Event> {
        let d = decode(text).unwrap();
        assert!(d.failures.is_empty(), "{:?}", d.failures);
        d.events
    }

    // Trimmed from frames captured on 2026-11-04.
    const BOOK_ARRAY: &str = r#"[{"market":"0x7d6b","asset_id":"2502","timestamp":"1793822768661","hash":"f8ce","bids":[{"price":"0.001","size":"9707.65"},{"price":"0.002","size":"10"}],"asks":[],"tick_size":"0.001","event_type":"book","last_trade_price":"0.001"},{"market":"0x7d6b","asset_id":"1025","timestamp":"1793822768661","hash":"aa11","bids":[],"asks":[{"price":"0.999","size":"9707.65"}],"tick_size":"0.001","event_type":"book","last_trade_price":"0.999"}]"#;
    const POST_TRADE_BOOK: &str = r#"{"market":"0xe3a2","asset_id":"6853","bids":[{"price":"0.64","size":"100"}],"asks":[{"price":"0.65","size":"6.153845"}],"hash":"917f","timestamp":"1793823205247","event_type":"book"}"#;
    const PRICE_CHANGE: &str = r#"{"market":"0x7d6b","price_changes":[{"asset_id":"2502","price":"0.5","size":"5805.25","side":"BUY","hash":"0fd0","best_bid":"0.999","best_ask":"1"},{"asset_id":"1025","price":"0.5","size":"0","side":"SELL","hash":"0fd1","best_bid":"0.001","best_ask":"0.002"}],"timestamp":"1793822781094","event_type":"price_change"}"#;
    const NEW_MARKET: &str = r#"{"id":"6694581","question":"Q","market":"0xc5e0","slug":"s","assets_ids":["3716","9597"],"outcomes":["Over","Under"],"event_message":{"id":"1"},"timestamp":"1793822780776","event_type":"new_market","tags":[],"condition_id":"0xc5e0","active":true,"clob_token_ids":["3716","9597"],"fee_schedule":{"rate":"0.05"}}"#;
    const TRADE: &str = r#"{"market":"0xe3a2","asset_id":"6853","price":"0.65","size":"6.153845","fee_rate_bps":"0","side":"BUY","timestamp":"1793823205266","event_type":"last_trade_price","transaction_hash":"0x8d"}"#;

    #[test]
    fn decodes_book_array() {
        let events = events(BOOK_ARRAY);
        assert_eq!(
            events[0],
            Event::Book {
                symbol: "2502".into(),
                bids: vec![(1_000, 9_707_650_000), (2_000, 10_000_000)],
                asks: vec![],
                exchange_ts: ts(1793822768661),
                hash: Some("f8ce".into()),
            }
        );
        assert!(matches!(&events[1], Event::Book { symbol, asks, .. } if symbol == "1025" && asks.len() == 1));
    }

    #[test]
    fn decodes_post_trade_book_without_optional_fields() {
        let events = events(POST_TRADE_BOOK);
        assert!(matches!(&events[..], [Event::Book { asks, .. }] if asks == &[(650_000, 6_153_845)]));
    }

    #[test]
    fn decodes_price_change_into_levels_sharing_the_timestamp() {
        assert_eq!(
            events(PRICE_CHANGE),
            [
                Event::Level {
                    symbol: "2502".into(),
                    side: Side::Buy,
                    price: 500_000,
                    quantity: 5_805_250_000,
                    exchange_ts: ts(1793822781094),
                    hash: Some("0fd0".into()),
                },
                Event::Level {
                    symbol: "1025".into(),
                    side: Side::Sell,
                    price: 500_000,
                    quantity: 0,
                    exchange_ts: ts(1793822781094),
                    hash: Some("0fd1".into()),
                },
            ]
        );
    }

    #[test]
    fn decodes_discovery_events() {
        assert_eq!(
            events(NEW_MARKET),
            [Event::MarketAdded {
                market: "0xc5e0".into(),
                symbols: vec!["3716".into(), "9597".into()],
            }]
        );
        let resolved = r#"{"event_type":"market_resolved","market":"0xc5e0","assets_ids":["3716","9597"],"winning_asset_id":"3716","timestamp":"1"}"#;
        assert!(matches!(&events(resolved)[..], [Event::MarketResolved { symbols, .. }] if symbols.len() == 2));
        let by_condition_id = r#"{"event_type":"market_resolved","condition_id":"0xc5e0","assets_ids":["3716"]}"#;
        assert!(matches!(&events(by_condition_id)[..], [Event::MarketResolved { market, .. }] if market == "0xc5e0"));
    }

    #[test]
    fn pong_trades_unknown_and_non_json_text_are_not_failures() {
        assert_eq!(events("PONG"), [Event::Pong]);
        assert_eq!(events(TRADE), [ignored("last_trade_price")]);
        assert_eq!(events(r#"{"event_type":"something_new","x":1}"#), [ignored("unknown")]);
        assert_eq!(events(r#"{"error":"bad request"}"#), [ignored("no event_type")]);
        assert_eq!(events("INVALID OPERATION"), [ignored("non-JSON text")]);
    }

    #[test]
    fn malformed_non_book_messages_are_ignored_not_failures() {
        assert_eq!(
            events(r#"{"event_type":"market_resolved","winning_asset_id":"1"}"#),
            [ignored("malformed market_resolved")]
        );
        assert_eq!(
            events(r#"{"event_type":"new_market","market":"m","assets_ids":[]}"#),
            [ignored("malformed new_market")]
        );
        assert_eq!(
            events(r#"{"event_type":"tick_size_change","asset_id":"1"}"#),
            [ignored("malformed tick_size_change")]
        );
    }

    #[test]
    fn a_bad_book_message_fails_alone_and_names_its_symbols() {
        let bad_size = PRICE_CHANGE.replace("\"0\",\"side\"", "\"1e3\",\"side\"");
        let frame = format!("[{POST_TRADE_BOOK},{bad_size}]");
        let d = decode(&frame).unwrap();
        assert_eq!(d.events.len(), 1, "the good book is kept");
        let [f] = &d.failures[..] else { panic!("{:?}", d.failures) };
        assert_eq!(f.kind, "price_change");
        assert_eq!(f.symbols, ["1025", "2502"], "every symbol of the message, none applied");
        assert!(matches!(f.error, DecodeError::Number { field: "size", .. }));

        let bad_side = PRICE_CHANGE.replace("SELL", "HOLD");
        assert!(matches!(decode(&bad_side).unwrap().failures[0].error, DecodeError::Side(_)));
        let bad_ts = PRICE_CHANGE.replace("1793822781094", "soon");
        assert!(matches!(decode(&bad_ts).unwrap().failures[0].error, DecodeError::Timestamp(_)));
        let missing = decode(r#"{"event_type":"book","asset_id":"1"}"#).unwrap();
        assert_eq!(missing.failures[0].symbols, ["1"]);
        assert!(missing.events.is_empty());
    }

    #[test]
    fn implausible_timestamps_are_rejected() {
        for bad in ["-1", "0", "1000", "99999999999999"] {
            let frame = PRICE_CHANGE.replace("1793822781094", bad);
            let d = decode(&frame).unwrap();
            assert!(
                matches!(d.failures[..], [Failure { error: DecodeError::Timestamp(_), .. }]),
                "{bad}"
            );
        }
    }

    #[test]
    fn broken_json_is_a_frame_error() {
        assert!(matches!(decode("{not json"), Err(DecodeError::Json(_))));
        assert!(matches!(decode("[{\"event_type\":\"book\""), Err(DecodeError::Json(_))));
    }

    #[test]
    fn subscription_messages() {
        let s = vec!["1".to_owned(), "2".to_owned()];
        let v: Value = serde_json::from_str(&encode_initial_subscribe(&s)).unwrap();
        assert_eq!(v, json!({"assets_ids":["1","2"],"type":"market","custom_feature_enabled":true}));
        let v: Value = serde_json::from_str(&encode_subscribe(&s)).unwrap();
        assert_eq!(v, json!({"assets_ids":["1","2"],"operation":"subscribe"}));
        let v: Value = serde_json::from_str(&encode_unsubscribe(&s)).unwrap();
        assert_eq!(v, json!({"assets_ids":["1","2"],"operation":"unsubscribe"}));
        let v: Value = serde_json::from_str(&encode_books_request(&s)).unwrap();
        assert_eq!(v, json!([{"token_id":"1"},{"token_id":"2"}]));
    }

    #[test]
    fn books_response_lists_only_tokens_with_orders() {
        let body = r#"[
            {"asset_id":"1","bids":[{"price":"0.1","size":"5"}],"asks":[]},
            {"asset_id":"2","bids":[],"asks":[]},
            {"asset_id":"3","bids":[],"asks":[{"price":"0.9","size":"1"}]}
        ]"#;
        assert_eq!(symbols_with_orders(body).unwrap(), ["1", "3"]);
        assert!(symbols_with_orders("{}").is_err());
    }
}
