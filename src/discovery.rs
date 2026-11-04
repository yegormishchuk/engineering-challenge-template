//! Market discovery through the Gamma API.
//!
//! M3 uses a single full pass at startup; M4 adds periodic polling and the
//! diff against the current set.

use anyhow::{Context, bail};
use serde_json::Value;

pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketInfo {
    pub condition_id: String,
    /// Outcome token ids; each has its own order book.
    pub tokens: Vec<String>,
}

/// Gamma encodes some arrays as JSON strings (`"[\"1\", \"2\"]"`).
fn string_array(v: &Value) -> Vec<String> {
    let parsed;
    let arr = match v {
        Value::String(s) => {
            parsed = serde_json::from_str::<Value>(s).unwrap_or(Value::Null);
            &parsed
        }
        other => other,
    };
    arr.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str().map(str::to_owned))
        .collect()
}

/// The spec's tradable filter: an order book that accepts orders, not
/// closed, and not a create-your-own market (the closest thing Polymarket
/// has to Kalshi's excluded MVEs).
fn is_tradable(m: &Value) -> bool {
    m["enableOrderBook"] == true
        && m["acceptingOrders"] == true
        && m["closed"] == false
        && m["cyom"] != true
}

/// Parses one `/markets/keyset` page into tradable markets and the next
/// cursor (`None` on the last page).
pub fn parse_page(body: &str) -> anyhow::Result<(Vec<MarketInfo>, Option<String>)> {
    let page: Value = serde_json::from_str(body).context("market page is not JSON")?;
    let Some(markets) = page["markets"].as_array() else {
        bail!("market page has no `markets` array");
    };
    let tradable = markets
        .iter()
        .filter(|m| is_tradable(m))
        .filter_map(|m| {
            let tokens = string_array(&m["clobTokenIds"]);
            let condition_id = m["conditionId"].as_str()?.to_owned();
            (!tokens.is_empty()).then_some(MarketInfo { condition_id, tokens })
        })
        .collect();
    let cursor = page["next_cursor"]
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_owned);
    Ok((tradable, cursor))
}

/// Fetches every page of open markets (or up to `max_pages`) and returns the
/// tradable ones.
pub async fn fetch_tradable(
    http: &reqwest::Client,
    base_url: &str,
    max_pages: Option<usize>,
) -> anyhow::Result<Vec<MarketInfo>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut url = format!("{base_url}/markets/keyset?closed=false&limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&after_cursor=");
            url.push_str(c);
        }
        let resp = http.get(&url).send().await.with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            bail!("GET {url} returned {status}: {}", body.chars().take(200).collect::<String>());
        }
        let (markets, next) = parse_page(&body)?;
        out.extend(markets);
        pages += 1;
        cursor = next;
        if cursor.is_none() || max_pages.is_some_and(|max| pages >= max) {
            break;
        }
    }
    tracing::info!(pages, markets = out.len(), "discovery pass done");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_tradable_markets_and_parses_token_strings() {
        let body = r#"{"markets":[
            {"conditionId":"ok","enableOrderBook":true,"acceptingOrders":true,"closed":false,"cyom":false,"clobTokenIds":"[\"1\", \"2\"]"},
            {"conditionId":"no-book","enableOrderBook":false,"acceptingOrders":true,"closed":false,"clobTokenIds":"[\"3\"]"},
            {"conditionId":"not-accepting","enableOrderBook":true,"acceptingOrders":null,"closed":false,"clobTokenIds":"[\"4\"]"},
            {"conditionId":"closed","enableOrderBook":true,"acceptingOrders":true,"closed":true,"clobTokenIds":"[\"5\"]"},
            {"conditionId":"cyom","enableOrderBook":true,"acceptingOrders":true,"closed":false,"cyom":true,"clobTokenIds":"[\"6\"]"},
            {"conditionId":"array","enableOrderBook":true,"acceptingOrders":true,"closed":false,"clobTokenIds":["7","8"]},
            {"conditionId":"no-tokens","enableOrderBook":true,"acceptingOrders":true,"closed":false,"clobTokenIds":"[]"}
        ],"next_cursor":"abc"}"#;
        let (markets, cursor) = parse_page(body).unwrap();
        let ids: Vec<_> = markets.iter().map(|m| m.condition_id.as_str()).collect();
        assert_eq!(ids, ["ok", "array"]);
        assert_eq!(markets[0].tokens, ["1", "2"]);
        assert_eq!(markets[1].tokens, ["7", "8"]);
        assert_eq!(cursor.as_deref(), Some("abc"));
    }

    #[test]
    fn last_page_has_no_cursor() {
        let (_, cursor) = parse_page(r#"{"markets":[],"next_cursor":""}"#).unwrap();
        assert_eq!(cursor, None);
        let (_, cursor) = parse_page(r#"{"markets":[]}"#).unwrap();
        assert_eq!(cursor, None);
        assert!(parse_page(r#"{"oops":1}"#).is_err());
    }
}
