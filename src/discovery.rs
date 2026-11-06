//! Market discovery through the Gamma API, in three layers (spec, Section 4):
//!
//! 1. **Events**: `new_market` / `market_resolved` from the websocket, applied
//!    at once.
//! 2. **Incremental polls** (every ~30 s): the newest, recently closed and
//!    recently updated markets, each paged back only to the previous run.
//! 3. **Full passes** (startup, then every ~30 min): every open market. A full
//!    pass takes minutes, so it never overrides a change made after it began.
//!
//! [`Tracker`] holds the decision logic and is pure; [`run_poller`] does the
//! HTTP and sends [`Poll`] results to the main loop.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

/// One market as seen in a Gamma response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketObs {
    pub condition_id: String,
    /// Outcome token ids; each has its own order book.
    pub tokens: Vec<String>,
    pub tradable: bool,
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

fn parse_market(m: &Value) -> Option<MarketObs> {
    let condition_id = m["conditionId"].as_str()?.to_owned();
    let tokens = string_array(&m["clobTokenIds"]);
    let tradable = is_tradable(m) && !tokens.is_empty();
    Some(MarketObs {
        condition_id,
        tokens,
        tradable,
    })
}

/// Gamma timestamps come as RFC 3339 (`2026-11-04T21:32:32.91316Z`) or, for
/// `closedTime`, as `2026-11-04 21:37:36.268592+00`.
pub fn parse_gamma_time(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    let naive = s.strip_suffix("+00").unwrap_or(s);
    NaiveDateTime::parse_from_str(naive, "%Y-%m-%d %H:%M:%S%.f")
        .ok()
        .map(|t| t.and_utc())
}

/// An incremental query: markets ordered newest first by `field`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecentQuery {
    pub name: &'static str,
    pub closed: bool,
    pub field: &'static str,
}

pub const RECENT_QUERIES: [RecentQuery; 3] = [
    RecentQuery { name: "created", closed: false, field: "createdAt" },
    RecentQuery { name: "closed", closed: true, field: "closedTime" },
    RecentQuery { name: "updated", closed: false, field: "updatedAt" },
];

/// One parsed legacy `/markets` page.
pub struct RecentPage {
    pub markets: Vec<MarketObs>,
    /// The oldest `field` timestamp on the page.
    pub oldest: Option<DateTime<Utc>>,
    /// Entries on the page, including ones that did not parse as markets.
    pub len: usize,
}

/// Parses one legacy `/markets` page (a bare array).
pub fn parse_recent_page(body: &str, field: &str) -> anyhow::Result<RecentPage> {
    let page: Value = serde_json::from_str(body).context("market page is not JSON")?;
    let Some(markets) = page.as_array() else {
        bail!("market page is not an array");
    };
    let oldest = markets
        .iter()
        .filter_map(|m| m[field].as_str().and_then(parse_gamma_time))
        .min();
    Ok(RecentPage {
        markets: markets.iter().filter_map(parse_market).collect(),
        oldest,
        len: markets.len(),
    })
}

async fn get_text(http: &reqwest::Client, url: &str) -> anyhow::Result<String> {
    let resp = http.get(url).send().await.with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        bail!("GET {url} returned {status}: {}", body.chars().take(200).collect::<String>());
    }
    Ok(body)
}

/// Parses one `/events/keyset` page. Each event nests its markets; an open
/// event can contain closed markets, which come out as not tradable.
pub fn parse_events_page(body: &str) -> anyhow::Result<(Vec<MarketObs>, Option<String>)> {
    let page: Value = serde_json::from_str(body).context("event page is not JSON")?;
    let Some(events) = page["events"].as_array() else {
        bail!("event page has no `events` array");
    };
    let markets = events
        .iter()
        .flat_map(|e| e["markets"].as_array().into_iter().flatten())
        .filter_map(parse_market)
        .collect();
    let cursor = page["next_cursor"]
        .as_str()
        .filter(|c| !c.is_empty())
        .map(str::to_owned);
    Ok((markets, cursor))
}

/// Waits before each retry of a failed full-pass page. A pass is ~200 pages
/// over ~10 min; without retries one slow page (Gamma timed out mid-body
/// three times in one 30 min run on 2026-11-06) threw the pass away and
/// the next one started again from page 1.
const PAGE_RETRY_DELAYS: [Duration; 3] = [Duration::from_secs(1), Duration::from_secs(2), Duration::from_secs(4)];

/// One full-pass page, retried after [`PAGE_RETRY_DELAYS`].
async fn fetch_events_page(http: &reqwest::Client, url: &str) -> anyhow::Result<(Vec<MarketObs>, Option<String>)> {
    let mut delays = PAGE_RETRY_DELAYS.iter();
    loop {
        let result = async { parse_events_page(&get_text(http, url).await?) }.await;
        match (result, delays.next()) {
            (Ok(page), _) => return Ok(page),
            (Err(error), Some(&delay)) => {
                warn!(%error, ?delay, "full-pass page failed; retrying");
                tokio::time::sleep(delay).await;
            }
            (Err(error), None) => return Err(error),
        }
    }
}

/// Every open market (or up to `max_pages` pages), read through
/// `/events/keyset`: 100 events (`limit` is capped at 100), about 900–1400
/// markets per page, versus 100 per page from `/markets/keyset`. Each page is also sent on `pages` as it arrives, so
/// subscriptions start long before a pass that takes minutes is done. Fails
/// as a whole if any page still fails after its retries, so a partial pass
/// is never mistaken for the full set.
pub async fn fetch_full(
    http: &reqwest::Client,
    base_url: &str,
    max_pages: Option<usize>,
    pages: &mpsc::Sender<Poll>,
) -> anyhow::Result<Vec<MarketObs>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let mut count = 0;
    loop {
        let mut url = format!("{base_url}/events/keyset?closed=false&limit=1000");
        if let Some(c) = &cursor {
            url.push_str("&after_cursor=");
            url.push_str(c);
        }
        let observed_at = Utc::now();
        let (markets, next) = fetch_events_page(http, &url).await?;
        // A page is a fresh observation of its markets, like a recent poll.
        pages
            .send(Poll::Recent { query: "full-page", observed_at, markets: markets.clone() })
            .await
            .context("poll receiver gone")?;
        out.extend(markets);
        count += 1;
        cursor = next;
        if cursor.is_none() || max_pages.is_some_and(|max| count >= max) {
            break;
        }
    }
    Ok(out)
}

/// Markets ordered newest first by `query.field`, paging until the page's
/// oldest entry is before `since`, a short page, or `max_pages`.
pub async fn fetch_recent(
    http: &reqwest::Client,
    base_url: &str,
    query: RecentQuery,
    since: DateTime<Utc>,
    max_pages: usize,
) -> anyhow::Result<Vec<MarketObs>> {
    const PAGE: usize = 100;
    let mut out = Vec::new();
    for page in 0..max_pages {
        let url = format!(
            "{base_url}/markets?closed={}&order={}&ascending=false&limit={PAGE}&offset={}",
            query.closed,
            query.field,
            page * PAGE
        );
        let body = get_text(http, &url).await?;
        let page_data = parse_recent_page(&body, query.field)?;
        // Short before filtering: an entry that is not a market must not end
        // the paging early.
        let short = page_data.len < PAGE;
        out.extend(page_data.markets);
        if short || page_data.oldest.is_none_or(|t| t < since) {
            break;
        }
        if page + 1 == max_pages {
            warn!(query = query.name, pages = max_pages, "incremental poll hit its page limit");
        }
    }
    Ok(out)
}

/// What the poller found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Poll {
    /// Every open market; authoritative for markets unchanged since `started_at`.
    Full { started_at: DateTime<Utc>, markets: Vec<MarketObs> },
    /// Some markets, applied as observed, except markets changed at or after
    /// `observed_at` (when the fetch began): the response may predate them.
    Recent { query: &'static str, observed_at: DateTime<Utc>, markets: Vec<MarketObs> },
}

#[derive(Debug, Clone)]
pub struct PollerConfig {
    pub base_url: String,
    pub full_every: Duration,
    pub recent_every: Duration,
    /// Pages per incremental query, at most. The legacy `/markets` endpoint
    /// rejects offsets past ~2000 (HTTP 422), so more than 20 pages of 100
    /// cannot be read; the `updated` query often fills all of them, and the
    /// full pass catches what it misses.
    pub recent_max_pages: usize,
    /// Limit for full passes (testing).
    pub full_max_pages: Option<usize>,
    /// Overlap between incremental runs, so nothing falls between them.
    pub recent_overlap: TimeDelta,
    pub retry_after: Duration,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            base_url: GAMMA_URL.to_owned(),
            full_every: Duration::from_secs(30 * 60),
            recent_every: Duration::from_secs(30),
            recent_max_pages: 20,
            full_max_pages: None,
            recent_overlap: TimeDelta::seconds(60),
            retry_after: Duration::from_secs(30),
        }
    }
}

/// Runs full passes and incremental polls until `shutdown`, sending results
/// on `tx`. A failed full pass is retried after `retry_after`. Incremental
/// polls start at once, alongside the first pass. Full passes run in their
/// own task, so a slow pass does not delay incremental polls.
pub async fn run_poller(
    http: reqwest::Client,
    cfg: PollerConfig,
    tx: mpsc::Sender<Poll>,
    shutdown: CancellationToken,
) {
    let full = {
        let (http, cfg, tx, shutdown) = (http.clone(), cfg.clone(), tx.clone(), shutdown.clone());
        tokio::spawn(async move {
            loop {
                let started_at = Utc::now();
                let t = tokio::time::Instant::now();
                let result = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    r = fetch_full(&http, &cfg.base_url, cfg.full_max_pages, &tx) => r,
                };
                let wait = match result {
                    Ok(markets) => {
                        info!(markets = markets.len(), secs = t.elapsed().as_secs(), "full discovery pass done");
                        if tx.send(Poll::Full { started_at, markets }).await.is_err() {
                            return;
                        }
                        cfg.full_every
                    }
                    Err(error) => {
                        warn!(%error, "full discovery pass failed; keeping the current set");
                        cfg.retry_after
                    }
                };
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        })
    };

    // Incremental polls page back to the previous run's start (minus an
    // overlap). The first run reaches back to startup.
    let mut since = Utc::now() - cfg.recent_overlap;
    let mut every = tokio::time::interval(cfg.recent_every);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    every.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = every.tick() => {}
        }
        let run_started = Utc::now();
        let mut all_ok = true;
        for query in RECENT_QUERIES {
            let observed_at = Utc::now();
            let result = tokio::select! {
                _ = shutdown.cancelled() => break,
                r = fetch_recent(&http, &cfg.base_url, query, since, cfg.recent_max_pages) => r,
            };
            match result {
                Ok(markets) => {
                    if tx.send(Poll::Recent { query: query.name, observed_at, markets }).await.is_err() {
                        full.abort();
                        return;
                    }
                }
                Err(error) => {
                    all_ok = false;
                    warn!(query = query.name, %error, "incremental poll failed");
                }
            }
        }
        // On failure keep the old `since`, so the next run covers the gap.
        if all_ok {
            since = run_started - cfg.recent_overlap;
        }
    }
    let _ = full.await;
}

/// Token changes to apply to the shard manager.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diff {
    pub add: Vec<String>,
    pub remove: Vec<String>,
    pub markets_added: usize,
    pub markets_removed: usize,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Track at most this many markets (testing). Default: all.
    pub max_markets: Option<usize>,
    /// A full pass smaller than this share of the tracked set removes
    /// nothing: it is more likely a broken response than mass closing.
    pub min_full_ratio: f64,
    /// How long to remember why a market changed, so a slow full pass
    /// cannot undo a newer change. Longer than a full pass takes.
    pub memory: TimeDelta,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            max_markets: None,
            min_full_ratio: 0.5,
            memory: TimeDelta::hours(2),
        }
    }
}

/// The set of markets to record, and the reasons behind each change.
#[derive(Debug, Default)]
pub struct Tracker {
    cfg: TrackerConfig,
    tracked: HashMap<String, Vec<String>>,
    /// Last add or remove time per market, tracked or not.
    changed_at: HashMap<String, DateTime<Utc>>,
    /// When `changed_at` was last pruned of entries older than `memory`.
    pruned_at: Option<DateTime<Utc>>,
}

impl Tracker {
    pub fn new(cfg: TrackerConfig) -> Self {
        Self {
            cfg,
            ..Self::default()
        }
    }

    pub fn len(&self) -> usize {
        self.tracked.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tracked.is_empty()
    }

    pub fn tokens(&self) -> usize {
        self.tracked.values().map(Vec::len).sum()
    }

    pub fn is_tracked(&self, market: &str) -> bool {
        self.tracked.contains_key(market)
    }

    /// A `new_market` event. Subscribes at once; a later poll confirms or
    /// removes it.
    pub fn market_added(&mut self, market: &str, tokens: &[String], now: DateTime<Utc>) -> Diff {
        let mut diff = Diff::default();
        self.add(market, tokens, now, &mut diff);
        diff
    }

    /// A `market_resolved` event.
    pub fn market_resolved(&mut self, market: &str, now: DateTime<Utc>) -> Diff {
        let mut diff = Diff::default();
        self.remove(market, now, &mut diff);
        diff
    }

    pub fn apply(&mut self, poll: Poll, now: DateTime<Utc>) -> Diff {
        // On every poll, not only full passes: if full passes keep failing,
        // incremental polls would otherwise grow `changed_at` for good.
        if self.pruned_at.is_none_or(|p| now - p >= TimeDelta::minutes(1)) {
            let horizon = now - self.cfg.memory;
            self.changed_at.retain(|_, t| *t >= horizon);
            self.pruned_at = Some(now);
        }
        match poll {
            Poll::Recent { observed_at, markets, .. } => self.apply_recent(observed_at, markets, now),
            Poll::Full { started_at, markets } => self.apply_full(started_at, markets, now),
        }
    }

    fn apply_recent(&mut self, observed_at: DateTime<Utc>, markets: Vec<MarketObs>, now: DateTime<Utc>) -> Diff {
        let mut diff = Diff::default();
        for m in markets {
            // Changed (by an event or another poll) after this response was
            // requested: the response may not reflect it.
            if self.changed_at.get(&m.condition_id).is_some_and(|&c| c >= observed_at) {
                continue;
            }
            if m.tradable {
                self.add(&m.condition_id, &m.tokens, now, &mut diff);
            } else {
                self.remove(&m.condition_id, now, &mut diff);
            }
        }
        diff
    }

    fn apply_full(&mut self, started_at: DateTime<Utc>, markets: Vec<MarketObs>, now: DateTime<Utc>) -> Diff {
        // Anything changed since the pass began is newer than the pass.
        let newer = |t: &HashMap<String, DateTime<Utc>>, id: &str| t.get(id).is_some_and(|&c| c >= started_at);

        let tradable: HashMap<&str, &MarketObs> = markets
            .iter()
            .filter(|m| m.tradable)
            .map(|m| (m.condition_id.as_str(), m))
            .collect();
        let mut diff = Diff::default();

        let suspicious = !self.tracked.is_empty()
            && (tradable.len() as f64) < self.cfg.min_full_ratio * self.tracked.len() as f64;
        if suspicious {
            warn!(
                tradable = tradable.len(),
                tracked = self.tracked.len(),
                "full pass much smaller than the tracked set; not removing anything"
            );
        } else {
            let gone: Vec<String> = self
                .tracked
                .keys()
                .filter(|id| !tradable.contains_key(id.as_str()) && !newer(&self.changed_at, id))
                .cloned()
                .collect();
            for id in gone {
                self.remove(&id, now, &mut diff);
            }
        }
        // Keep Gamma's order, so `max_markets` picks the same markets each run.
        for m in markets.iter().filter(|m| m.tradable) {
            if !newer(&self.changed_at, &m.condition_id) {
                self.add(&m.condition_id, &m.tokens, now, &mut diff);
            }
        }
        diff
    }

    fn add(&mut self, market: &str, tokens: &[String], now: DateTime<Utc>, diff: &mut Diff) {
        if tokens.is_empty() || self.tracked.contains_key(market) {
            return;
        }
        if self.cfg.max_markets.is_some_and(|max| self.tracked.len() >= max) {
            return;
        }
        self.tracked.insert(market.to_owned(), tokens.to_vec());
        self.changed_at.insert(market.to_owned(), now);
        diff.add.extend_from_slice(tokens);
        diff.markets_added += 1;
    }

    fn remove(&mut self, market: &str, now: DateTime<Utc>, diff: &mut Diff) {
        self.changed_at.insert(market.to_owned(), now);
        if let Some(tokens) = self.tracked.remove(market) {
            diff.remove.extend(tokens);
            diff.markets_removed += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn obs(id: &str, tradable: bool) -> MarketObs {
        MarketObs {
            condition_id: id.into(),
            tokens: vec![format!("{id}-yes"), format!("{id}-no")],
            tradable,
        }
    }

    fn full(started_at: &str, ids: &[&str]) -> Poll {
        Poll::Full {
            started_at: t(started_at),
            markets: ids.iter().map(|id| obs(id, true)).collect(),
        }
    }

    /// An observation newer than every change.
    fn recent(markets: Vec<MarketObs>) -> Poll {
        recent_at(DateTime::<Utc>::MAX_UTC, markets)
    }

    fn recent_at(observed_at: DateTime<Utc>, markets: Vec<MarketObs>) -> Poll {
        Poll::Recent {
            query: "test",
            observed_at,
            markets,
        }
    }

    fn tracker() -> Tracker {
        Tracker::new(TrackerConfig::default())
    }

    /// A raw HTTP server that answers request `n` (from 0) with
    /// `respond(n)` = (status line, body). Returns its base URL.
    async fn http_server(respond: fn(usize) -> (&'static str, &'static str)) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let mut n = 0;
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0; 8192];
                let _ = stream.read(&mut buf).await;
                let (status, body) = respond(n);
                n += 1;
                let reply = format!(
                    "HTTP/1.1 {status}
Content-Type: application/json
Content-Length: {}
Connection: close

{body}",
                    body.len()
                );
                let _ = stream.write_all(reply.as_bytes()).await;
            }
        });
        url
    }

    const ONE_EVENT_PAGE: &str = r#"{"events":[{"markets":[{"conditionId":"c1","clobTokenIds":"[\"y\",\"n\"]","active":true,"closed":false,"acceptingOrders":true,"enableOrderBook":true}]}],"next_cursor":""}"#;

    #[tokio::test]
    async fn a_page_that_fails_once_is_retried_not_the_whole_pass() {
        let url = http_server(|n| if n == 0 { ("500 Internal Server Error", "busy") } else { ("200 OK", ONE_EVENT_PAGE) }).await;
        let (tx, mut rx) = mpsc::channel(8);
        let markets = fetch_full(&reqwest::Client::new(), &url, None, &tx).await.unwrap();
        assert_eq!(markets.len(), 1);
        assert!(rx.try_recv().is_ok(), "the page was also sent as it arrived");
    }

    #[tokio::test]
    async fn a_page_that_keeps_failing_fails_the_pass() {
        let url = http_server(|_| ("500 Internal Server Error", "down")).await;
        let (tx, _rx) = mpsc::channel(8);
        assert!(fetch_full(&reqwest::Client::new(), &url, None, &tx).await.is_err());
    }

    const T0: &str = "2026-11-05T10:00:00Z";
    const T1: &str = "2026-11-05T10:10:00Z";
    const T2: &str = "2026-11-05T10:20:00Z";
    const T3: &str = "2026-11-05T10:30:00Z";

    #[test]
    fn parses_tradability_and_token_strings() {
        let body = r#"[
            {"conditionId":"ok","enableOrderBook":true,"acceptingOrders":true,"closed":false,"cyom":false,"clobTokenIds":"[\"1\", \"2\"]"},
            {"conditionId":"no-book","enableOrderBook":false,"acceptingOrders":true,"closed":false,"clobTokenIds":"[\"3\"]"},
            {"conditionId":"not-accepting","enableOrderBook":true,"acceptingOrders":null,"closed":false,"clobTokenIds":"[\"4\"]"},
            {"conditionId":"closed","enableOrderBook":true,"acceptingOrders":true,"closed":true,"clobTokenIds":"[\"5\"]"},
            {"conditionId":"cyom","enableOrderBook":true,"acceptingOrders":true,"closed":false,"cyom":true,"clobTokenIds":"[\"6\"]"},
            {"conditionId":"array","enableOrderBook":true,"acceptingOrders":true,"closed":false,"clobTokenIds":["7","8"]},
            {"conditionId":"no-tokens","enableOrderBook":true,"acceptingOrders":true,"closed":false,"clobTokenIds":"[]"},
            {"noConditionId":true}
        ]"#;
        let markets: Vec<MarketObs> = serde_json::from_str::<Vec<Value>>(body)
            .unwrap()
            .iter()
            .filter_map(parse_market)
            .collect();
        let tradable: Vec<_> = markets
            .iter()
            .map(|m| (m.condition_id.as_str(), m.tradable))
            .collect();
        assert_eq!(
            tradable,
            [
                ("ok", true),
                ("no-book", false),
                ("not-accepting", false),
                ("closed", false),
                ("cyom", false),
                ("array", true),
                ("no-tokens", false),
            ]
        );
        assert_eq!(markets[0].tokens, ["1", "2"]);
        assert_eq!(markets[5].tokens, ["7", "8"]);
    }

    #[test]
    fn parses_markets_nested_in_events() {
        let body = r#"{"events":[
            {"id":"1","markets":[
                {"conditionId":"a","enableOrderBook":true,"acceptingOrders":true,"closed":false,"clobTokenIds":"[\"1\", \"2\"]"},
                {"conditionId":"b","enableOrderBook":true,"acceptingOrders":true,"closed":true,"clobTokenIds":"[\"3\"]"}
            ]},
            {"id":"2"},
            {"id":"3","markets":[{"conditionId":"c","enableOrderBook":true,"acceptingOrders":true,"closed":false,"clobTokenIds":"[\"4\"]"}]}
        ],"next_cursor":"n"}"#;
        let (markets, cursor) = parse_events_page(body).unwrap();
        let ids: Vec<_> = markets.iter().map(|m| (m.condition_id.as_str(), m.tradable)).collect();
        assert_eq!(ids, [("a", true), ("b", false), ("c", true)]);
        assert_eq!(cursor.as_deref(), Some("n"));
        assert!(parse_events_page(r#"{"markets":[]}"#).is_err());
    }

    #[test]
    fn parses_both_gamma_time_formats() {
        assert_eq!(parse_gamma_time("2026-11-04T21:32:32.91316Z"), Some(t("2026-11-04T21:32:32.91316Z")));
        assert_eq!(
            parse_gamma_time("2026-11-04 21:37:36.268592+00"),
            Some(t("2026-11-04T21:37:36.268592Z"))
        );
        assert_eq!(parse_gamma_time("2026-11-04 21:37:36+00"), Some(t("2026-11-04T21:37:36Z")));
        assert_eq!(parse_gamma_time("yesterday"), None);
    }

    #[test]
    fn recent_page_reports_its_oldest_timestamp() {
        let body = r#"[
            {"conditionId":"a","closedTime":"2026-11-04 21:37:36.2+00","closed":true,"clobTokenIds":"[\"1\"]"},
            {"conditionId":"b","closedTime":"2026-11-04 21:30:00+00","closed":true,"clobTokenIds":"[\"2\"]"},
            {"conditionId":"c","closedTime":null,"closed":true,"clobTokenIds":"[\"3\"]"}
        ]"#;
        let page = parse_recent_page(body, "closedTime").unwrap();
        assert_eq!(page.markets.len(), 3);
        assert!(page.markets.iter().all(|m| !m.tradable));
        assert_eq!(page.oldest, Some(t("2026-11-04T21:30:00Z")));
        let with_junk = r#"[{"conditionId":"a","clobTokenIds":"[\"1\"]"},{"noConditionId":true}]"#;
        let page = parse_recent_page(with_junk, "closedTime").unwrap();
        assert_eq!((page.markets.len(), page.len), (1, 2), "len counts every entry");
    }

    #[test]
    fn first_full_pass_adds_everything_tradable() {
        let mut tr = tracker();
        let mut poll = full(T0, &["a", "b"]);
        if let Poll::Full { markets, .. } = &mut poll {
            markets.push(obs("c", false));
        }
        let d = tr.apply(poll, t(T1));
        assert_eq!(d.add, ["a-yes", "a-no", "b-yes", "b-no"]);
        assert_eq!((d.markets_added, d.markets_removed), (2, 0));
        assert_eq!(tr.tokens(), 4);
    }

    #[test]
    fn full_pass_removes_markets_that_are_gone() {
        let mut tr = tracker();
        tr.apply(full(T0, &["a", "b"]), t(T1));
        let d = tr.apply(full(T2, &["a"]), t(T3));
        assert_eq!(d.remove, ["b-yes", "b-no"]);
        assert!(d.add.is_empty());
    }

    #[test]
    fn full_pass_does_not_undo_changes_made_while_it_ran() {
        let mut tr = tracker();
        tr.apply(full(T0, &["a", "b"]), t(T0));
        // While a pass that began at T1 runs: "new" is announced, "a" resolves.
        tr.market_added("new", &["n".into()], t(T2));
        tr.market_resolved("a", t(T2));
        // The pass still lists "a" and does not know "new".
        let d = tr.apply(full(T1, &["a", "b"]), t(T3));
        assert!(d.is_empty(), "{d:?}");
        assert!(tr.is_tracked("new"));
        assert!(!tr.is_tracked("a"));
        // A pass that began after both changes is authoritative again.
        let d = tr.apply(full(T3, &["a", "b"]), t(T3));
        assert_eq!(d.add, ["a-yes", "a-no"]);
        assert_eq!(d.remove, ["n"]);
    }

    #[test]
    fn suspiciously_small_full_pass_removes_nothing() {
        let mut tr = tracker();
        let ids: Vec<String> = (0..10).map(|i| format!("m{i}")).collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        tr.apply(full(T0, &refs), t(T0));
        let d = tr.apply(full(T1, &["m0", "m1", "new"]), t(T2));
        assert!(d.remove.is_empty());
        assert_eq!(d.add, ["new-yes", "new-no"], "additions still apply");
        assert_eq!(tr.len(), 11);
    }

    #[test]
    fn recent_polls_add_and_remove_as_observed() {
        let mut tr = tracker();
        tr.apply(full(T0, &["a"]), t(T0));
        let d = tr.apply(recent(vec![obs("a", false), obs("b", true), obs("c", false)]), t(T1));
        assert_eq!(d.add, ["b-yes", "b-no"]);
        assert_eq!(d.remove, ["a-yes", "a-no"]);
        assert_eq!((d.markets_added, d.markets_removed), (1, 1));
        assert!(tr.apply(recent(vec![obs("b", true)]), t(T2)).is_empty(), "already tracked");
    }

    #[test]
    fn an_observation_older_than_a_change_does_not_undo_it() {
        let mut tr = tracker();
        tr.apply(full(T0, &["a"]), t(T0));
        // A page fetched at T1 is applied at T3; meanwhile, at T2, "a"
        // resolved and "n" was announced.
        tr.market_resolved("a", t(T2));
        tr.market_added("n", &["n".into()], t(T2));
        let d = tr.apply(recent_at(t(T1), vec![obs("a", true), obs("n", false)]), t(T3));
        assert!(d.is_empty(), "{d:?}");
        assert!(!tr.is_tracked("a") && tr.is_tracked("n"));
        // An observation made after the change applies.
        let d = tr.apply(recent_at(t(T3), vec![obs("a", true)]), t(T3));
        assert_eq!(d.add, ["a-yes", "a-no"]);
    }

    #[test]
    fn change_times_are_forgotten_even_without_full_passes() {
        let mut tr = tracker();
        tr.apply(recent(vec![obs("x", false), obs("y", false)]), t(T0));
        assert_eq!(tr.changed_at.len(), 2);
        let later = t(T0) + TimeDelta::hours(3);
        tr.apply(recent(vec![]), later);
        assert!(tr.changed_at.is_empty());
    }

    #[test]
    fn events_add_and_remove_once() {
        let mut tr = tracker();
        let tokens = vec!["x".to_owned(), "y".to_owned()];
        assert_eq!(tr.market_added("m", &tokens, t(T0)).add, ["x", "y"]);
        assert!(tr.market_added("m", &tokens, t(T0)).is_empty());
        assert!(tr.market_added("empty", &[], t(T0)).is_empty());
        assert_eq!(tr.market_resolved("m", t(T1)).remove, ["x", "y"]);
        assert!(tr.market_resolved("m", t(T1)).is_empty());
    }

    #[test]
    fn max_markets_caps_additions_from_every_source() {
        let mut tr = Tracker::new(TrackerConfig {
            max_markets: Some(2),
            ..TrackerConfig::default()
        });
        tr.apply(full(T0, &["a", "b", "c"]), t(T0));
        assert_eq!(tr.len(), 2);
        assert!(tr.market_added("d", &["d".into()], t(T1)).is_empty());
        assert!(tr.apply(recent(vec![obs("e", true)]), t(T1)).is_empty());
        tr.market_resolved("a", t(T2));
        assert_eq!(tr.market_added("d", &["d".into()], t(T2)).add, ["d"]);
    }
}
