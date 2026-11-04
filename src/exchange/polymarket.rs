//! Polymarket CLOB market channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).
//!
//! Formats were verified against live frames in M0 (spec, Section 4):
//! - a frame is one JSON object or a JSON array of objects, or the text `PONG`;
//! - `book` carries a full book; `price_change` carries absolute sizes;
//! - numbers are decimal strings, timestamps are millisecond strings.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use super::Event;
use crate::Side;
use crate::fixed::{ParseFixedError, parse_fixed};

pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Text frame the client sends every 10 s; the server answers `PONG`.
pub const PING: &str = "PING";
const PONG: &str = "PONG";

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

#[derive(Deserialize)]
#[serde(untagged)]
enum Frame {
    Many(Vec<Msg>),
    One(Msg),
}

#[derive(Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum Msg {
    Book {
        asset_id: String,
        timestamp: String,
        #[serde(default)]
        hash: Option<String>,
        #[serde(default)]
        bids: Vec<RawLevel>,
        #[serde(default)]
        asks: Vec<RawLevel>,
    },
    PriceChange {
        timestamp: String,
        price_changes: Vec<RawChange>,
    },
    NewMarket {
        market: String,
        #[serde(default)]
        assets_ids: Vec<String>,
    },
    MarketResolved {
        market: String,
        #[serde(default)]
        assets_ids: Vec<String>,
    },
    TickSizeChange {
        asset_id: String,
        old_tick_size: String,
        new_tick_size: String,
    },
    LastTradePrice {},
    BestBidAsk {},
    #[serde(other)]
    Unknown,
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

/// Decodes one text frame. Either every event of the frame decodes, or the
/// frame is rejected as a whole, so a caller never applies half a frame.
pub fn decode(text: &str) -> Result<Vec<Event>, DecodeError> {
    if text == PONG {
        return Ok(vec![Event::Pong]);
    }
    let msgs = match serde_json::from_str(text).map_err(DecodeError::Json)? {
        Frame::Many(msgs) => msgs,
        Frame::One(msg) => vec![msg],
    };
    let mut events = Vec::with_capacity(msgs.len());
    for msg in msgs {
        match msg {
            Msg::Book {
                asset_id,
                timestamp: ts,
                hash,
                bids,
                asks,
            } => events.push(Event::Book {
                symbol: asset_id,
                bids: levels(&bids)?,
                asks: levels(&asks)?,
                exchange_ts: timestamp(&ts)?,
                hash,
            }),
            Msg::PriceChange {
                timestamp: ts,
                price_changes,
            } => {
                let exchange_ts = timestamp(&ts)?;
                for c in price_changes {
                    events.push(Event::Level {
                        side: side(&c.side)?,
                        price: number("price", &c.price)?,
                        quantity: number("size", &c.size)?,
                        symbol: c.asset_id,
                        exchange_ts,
                        hash: c.hash,
                    });
                }
            }
            Msg::NewMarket { market, assets_ids } => events.push(Event::MarketAdded {
                market,
                symbols: assets_ids,
            }),
            Msg::MarketResolved { market, assets_ids } => events.push(Event::MarketResolved {
                market,
                symbols: assets_ids,
            }),
            Msg::TickSizeChange {
                asset_id,
                old_tick_size,
                new_tick_size,
            } => events.push(Event::TickSizeChanged {
                symbol: asset_id,
                old: old_tick_size,
                new: new_tick_size,
            }),
            Msg::LastTradePrice {} => events.push(Event::Ignored {
                kind: "last_trade_price".into(),
            }),
            Msg::BestBidAsk {} => events.push(Event::Ignored {
                kind: "best_bid_ask".into(),
            }),
            Msg::Unknown => events.push(Event::Ignored {
                kind: "unknown".into(),
            }),
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    // Trimmed from frames captured on 2026-11-04.
    const BOOK_ARRAY: &str = r#"[{"market":"0x7d6b","asset_id":"2502","timestamp":"1793822768661","hash":"f8ce","bids":[{"price":"0.001","size":"9707.65"},{"price":"0.002","size":"10"}],"asks":[],"tick_size":"0.001","event_type":"book","last_trade_price":"0.001"},{"market":"0x7d6b","asset_id":"1025","timestamp":"1793822768661","hash":"aa11","bids":[],"asks":[{"price":"0.999","size":"9707.65"}],"tick_size":"0.001","event_type":"book","last_trade_price":"0.999"}]"#;
    const POST_TRADE_BOOK: &str = r#"{"market":"0xe3a2","asset_id":"6853","bids":[{"price":"0.64","size":"100"}],"asks":[{"price":"0.65","size":"6.153845"}],"hash":"917f","timestamp":"1793823205247","event_type":"book"}"#;
    const PRICE_CHANGE: &str = r#"{"market":"0x7d6b","price_changes":[{"asset_id":"2502","price":"0.5","size":"5805.25","side":"BUY","hash":"0fd0","best_bid":"0.999","best_ask":"1"},{"asset_id":"1025","price":"0.5","size":"0","side":"SELL","hash":"0fd1","best_bid":"0.001","best_ask":"0.002"}],"timestamp":"1793822781094","event_type":"price_change"}"#;
    const NEW_MARKET: &str = r#"{"id":"6694581","question":"Q","market":"0xc5e0","slug":"s","assets_ids":["3716","9597"],"outcomes":["Over","Under"],"event_message":{"id":"1"},"timestamp":"1793822780776","event_type":"new_market","tags":[],"condition_id":"0xc5e0","active":true,"clob_token_ids":["3716","9597"],"fee_schedule":{"rate":"0.05"}}"#;
    const TRADE: &str = r#"{"market":"0xe3a2","asset_id":"6853","price":"0.65","size":"6.153845","fee_rate_bps":"0","side":"BUY","timestamp":"1793823205266","event_type":"last_trade_price","transaction_hash":"0x8d"}"#;

    #[test]
    fn decodes_book_array() {
        let events = decode(BOOK_ARRAY).unwrap();
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
        let events = decode(POST_TRADE_BOOK).unwrap();
        assert!(matches!(&events[..], [Event::Book { asks, .. }] if asks == &[(650_000, 6_153_845)]));
    }

    #[test]
    fn decodes_price_change_into_levels_sharing_the_timestamp() {
        let events = decode(PRICE_CHANGE).unwrap();
        assert_eq!(
            events,
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
            decode(NEW_MARKET).unwrap(),
            [Event::MarketAdded {
                market: "0xc5e0".into(),
                symbols: vec!["3716".into(), "9597".into()],
            }]
        );
        let resolved = r#"{"event_type":"market_resolved","market":"0xc5e0","assets_ids":["3716","9597"],"winning_asset_id":"3716","timestamp":"1"}"#;
        assert!(matches!(&decode(resolved).unwrap()[..], [Event::MarketResolved { symbols, .. }] if symbols.len() == 2));
    }

    #[test]
    fn pong_trades_and_unknown_types() {
        assert_eq!(decode("PONG").unwrap(), [Event::Pong]);
        assert_eq!(
            decode(TRADE).unwrap(),
            [Event::Ignored { kind: "last_trade_price".into() }]
        );
        assert_eq!(
            decode(r#"{"event_type":"something_new","x":1}"#).unwrap(),
            [Event::Ignored { kind: "unknown".into() }]
        );
    }

    #[test]
    fn a_bad_entry_rejects_the_whole_frame() {
        let bad_size = PRICE_CHANGE.replace("\"0\",\"side\"", "\"1e3\",\"side\"");
        assert!(matches!(decode(&bad_size), Err(DecodeError::Number { field: "size", .. })));
        let bad_side = PRICE_CHANGE.replace("SELL", "HOLD");
        assert!(matches!(decode(&bad_side), Err(DecodeError::Side(_))));
        let bad_ts = PRICE_CHANGE.replace("1793822781094", "soon");
        assert!(matches!(decode(&bad_ts), Err(DecodeError::Timestamp(_))));
        assert!(matches!(decode("{not json"), Err(DecodeError::Json(_))));
        // A known type missing a required field is an error, not "unknown".
        assert!(decode(r#"{"event_type":"book","asset_id":"1"}"#).is_err());
    }

    #[test]
    fn subscription_messages() {
        let s = vec!["1".to_owned(), "2".to_owned()];
        let v: serde_json::Value = serde_json::from_str(&encode_initial_subscribe(&s)).unwrap();
        assert_eq!(v, json!({"assets_ids":["1","2"],"type":"market","custom_feature_enabled":true}));
        let v: serde_json::Value = serde_json::from_str(&encode_subscribe(&s)).unwrap();
        assert_eq!(v, json!({"assets_ids":["1","2"],"operation":"subscribe"}));
        let v: serde_json::Value = serde_json::from_str(&encode_unsubscribe(&s)).unwrap();
        assert_eq!(v, json!({"assets_ids":["1","2"],"operation":"unsubscribe"}));
    }
}
