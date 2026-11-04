//! M0 probe for Polymarket: records raw market-channel frames for one market,
//! so message formats can be confirmed against live data before the collector
//! is written. No account or key is needed.
//!
//! ```text
//! capture [CONDITION_ID] [--seconds N] [--no-probe-resubscribe]
//! ```
//!
//! Output goes to `captures/` (git-ignored):
//! - `markets_page.json`: first Gamma page of open markets (raw body)
//! - `frames_<market>_<start>.jsonl`: one line per websocket frame,
//!   `{"recv_us":…,"kind":"text|ping|pong|close|binary","raw":…}`
//! - `rest_book_<token>.json`: CLOB REST books fetched at the end
//!
//! At the end the tool compares its own book, built from `book` and
//! `price_change` messages (treating `size` as the new absolute size), with
//! the REST book. A match supports the absolute-size reading.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::Utc;
use engineering_challenge::fixed::parse_fixed;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

const OUT_DIR: &str = "captures";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const CLOB: &str = "https://clob.polymarket.com";
const WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

struct Args {
    condition_id: Option<String>,
    seconds: u64,
    probe_resubscribe: bool,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args {
        condition_id: None,
        seconds: 60,
        probe_resubscribe: true,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--seconds" => {
                args.seconds = it
                    .next()
                    .context("--seconds needs a value")?
                    .parse()
                    .context("--seconds must be a whole number")?;
            }
            "--no-probe-resubscribe" => args.probe_resubscribe = false,
            "-h" | "--help" => {
                eprintln!("usage: capture [CONDITION_ID] [--seconds N] [--no-probe-resubscribe]");
                std::process::exit(0);
            }
            s if s.starts_with('-') => bail!("unknown flag {s}"),
            s => args.condition_id = Some(s.to_owned()),
        }
    }
    Ok(args)
}

struct Market {
    condition_id: String,
    question: String,
    tokens: Vec<(String, String)>, // (token id, outcome)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    fs::create_dir_all(OUT_DIR)?;
    let http = reqwest::Client::new();

    let market = discover(&http, args.condition_id.as_deref()).await?;
    eprintln!("market {} — {}", market.condition_id, market.question);
    for (token, outcome) in &market.tokens {
        eprintln!("  token {outcome}: {token}");
    }
    eprintln!("recording {} s from {WS}", args.seconds);
    record(&http, &market, args.seconds, args.probe_resubscribe).await
}

async fn get_json(http: &reqwest::Client, url: &str) -> anyhow::Result<(String, Value)> {
    let resp = http.get(url).send().await.with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        bail!("GET {url} returned {status}: {body}");
    }
    let value = serde_json::from_str(&body).with_context(|| format!("GET {url}: not JSON"))?;
    Ok((body, value))
}

/// Gamma encodes some arrays as JSON strings, e.g. `"[\"Yes\", \"No\"]"`.
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

/// Saves the first market page and returns the market to record: the given
/// condition id, or the open market with the most 24 h volume on that page.
async fn discover(http: &reqwest::Client, condition_id: Option<&str>) -> anyhow::Result<Market> {
    let url = match condition_id {
        Some(id) => format!("{GAMMA}/markets/keyset?condition_ids={id}&limit=1"),
        None => format!("{GAMMA}/markets/keyset?closed=false&limit=500"),
    };
    let (body, page) = get_json(http, &url).await?;
    fs::write(format!("{OUT_DIR}/markets_page.json"), &body)?;
    let markets = page["markets"].as_array().context("no `markets` array")?;
    eprintln!(
        "markets on first page: {}{}",
        markets.len(),
        if page["next_cursor"].as_str().is_some_and(|c| !c.is_empty()) {
            " (more pages: next_cursor set)"
        } else {
            ""
        }
    );
    let tradable = |m: &&Value| {
        m["enableOrderBook"] == true && m["acceptingOrders"] == true && m["closed"] == false
    };
    eprintln!(
        "  with enableOrderBook && acceptingOrders && !closed: {}",
        markets.iter().filter(tradable).count()
    );

    // 24 h volume is used only to pick an active market; float is fine here.
    let volume = |m: &Value| m["volume24hr"].as_f64().unwrap_or(0.0);
    let m = markets
        .iter()
        .filter(|m| condition_id.is_some() || tradable(m))
        .max_by(|a, b| volume(a).total_cmp(&volume(b)))
        .context("no tradable market found")?;

    let tokens = string_array(&m["clobTokenIds"]);
    let outcomes = string_array(&m["outcomes"]);
    if tokens.is_empty() {
        bail!("market has no clobTokenIds");
    }
    Ok(Market {
        condition_id: m["conditionId"].as_str().unwrap_or_default().to_owned(),
        question: m["question"].as_str().unwrap_or_default().to_owned(),
        tokens: tokens
            .into_iter()
            .enumerate()
            .map(|(i, t)| (t, outcomes.get(i).cloned().unwrap_or_else(|| format!("#{i}"))))
            .collect(),
    })
}

/// Price → size, both fixed point, for one side of one token.
type Levels = BTreeMap<i64, i64>;

#[derive(Default)]
struct LocalBook {
    bids: Levels,
    asks: Levels,
    last_hash: Option<String>,
    /// Recent (hash, bids, asks) states, newest last, to match a REST book
    /// taken at an arbitrary moment against the stream state with its hash.
    history: VecDeque<(String, Levels, Levels)>,
}

const HISTORY: usize = 200;

impl LocalBook {
    fn remember(&mut self, hash: Option<&str>) {
        self.last_hash = hash.map(str::to_owned);
        if let Some(hash) = hash {
            if self.history.len() == HISTORY {
                self.history.pop_front();
            }
            self.history
                .push_back((hash.to_owned(), self.bids.clone(), self.asks.clone()));
        }
    }
}

fn levels(v: &Value) -> anyhow::Result<Levels> {
    let mut out = Levels::new();
    for level in v.as_array().into_iter().flatten() {
        let price = parse_fixed(level["price"].as_str().context("level without price")?)?;
        let size = parse_fixed(level["size"].as_str().context("level without size")?)?;
        if size != 0 {
            out.insert(price, size);
        }
    }
    Ok(out)
}

#[derive(Default)]
struct Stats {
    by_type: BTreeMap<String, u64>,
    keys: BTreeMap<String, BTreeSet<String>>,
    change_keys: BTreeSet<String>,
    array_frames: u64,
    non_json: BTreeMap<String, u64>,
    sides: BTreeSet<String>,
    zero_size_changes: u64,
    books: BTreeMap<String, LocalBook>,
    book_msgs_after_start: u64,
    timestamps_backwards: u64,
    last_ts: Option<i64>,
    latency_ms: Vec<i64>,
    whole_second_ts: u64,
}

impl Stats {
    fn observe(&mut self, v: &Value, recv_us: i64) -> anyhow::Result<()> {
        let kind = v["event_type"].as_str().unwrap_or("<no event_type>").to_owned();
        *self.by_type.entry(kind.clone()).or_default() += 1;
        if let Some(obj) = v.as_object() {
            self.keys.entry(kind.clone()).or_default().extend(obj.keys().cloned());
        }
        // Only book data: `new_market` carries the market's creation time.
        let is_book_data = matches!(kind.as_str(), "book" | "price_change");
        if let Some(ts) = v["timestamp"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|_| is_book_data)
        {
            if self.last_ts.is_some_and(|last| ts < last) {
                self.timestamps_backwards += 1;
            }
            self.last_ts = Some(ts);
            self.latency_ms.push(recv_us / 1000 - ts);
            if ts % 1000 == 0 {
                self.whole_second_ts += 1;
            }
        }
        match kind.as_str() {
            "book" => {
                let asset = v["asset_id"].as_str().context("book without asset_id")?;
                if self.books.contains_key(asset) {
                    self.book_msgs_after_start += 1;
                }
                let book = self.books.entry(asset.to_owned()).or_default();
                book.bids = levels(&v["bids"])?;
                book.asks = levels(&v["asks"])?;
                book.remember(v["hash"].as_str());
            }
            "price_change" => {
                for change in v["price_changes"].as_array().into_iter().flatten() {
                    if let Some(obj) = change.as_object() {
                        self.change_keys.extend(obj.keys().cloned());
                    }
                    let side = change["side"].as_str().unwrap_or("?").to_owned();
                    self.sides.insert(side.clone());
                    let asset = change["asset_id"].as_str().context("change without asset_id")?;
                    let price = parse_fixed(change["price"].as_str().context("no price")?)?;
                    let size = parse_fixed(change["size"].as_str().context("no size")?)?;
                    if size == 0 {
                        self.zero_size_changes += 1;
                    }
                    let Some(book) = self.books.get_mut(asset) else {
                        eprintln!("price_change before book for {asset}");
                        continue;
                    };
                    let levels = match side.as_str() {
                        "BUY" => &mut book.bids,
                        "SELL" => &mut book.asks,
                        other => bail!("unknown side {other}"),
                    };
                    // Assumption under test: `size` is the new absolute size.
                    if size == 0 {
                        levels.remove(&price);
                    } else {
                        levels.insert(price, size);
                    }
                    book.remember(change["hash"].as_str());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn report(&self) {
        eprintln!("\n== summary ==");
        for (kind, n) in &self.by_type {
            eprintln!("{kind}: {n}");
            if let Some(keys) = self.keys.get(kind) {
                eprintln!("  fields: {}", join(keys));
            }
        }
        if !self.change_keys.is_empty() {
            eprintln!("price_changes[] fields: {}", join(&self.change_keys));
            eprintln!("sides seen: {}", join(&self.sides));
            eprintln!("changes with size 0 (level removed): {}", self.zero_size_changes);
        }
        eprintln!("frames that were JSON arrays: {}", self.array_frames);
        for (text, n) in &self.non_json {
            eprintln!("non-JSON text frame {text:?}: {n}");
        }
        eprintln!("book messages after the initial one: {}", self.book_msgs_after_start);
        eprintln!("exchange timestamps going backwards: {}", self.timestamps_backwards);
        eprintln!(
            "exchange timestamps on a whole second (…000 ms): {} of {}",
            self.whole_second_ts,
            self.latency_ms.len()
        );
        let mut lat = self.latency_ms.clone();
        lat.sort_unstable();
        if !lat.is_empty() {
            let pct = |p: f64| lat[((lat.len() - 1) as f64 * p) as usize];
            eprintln!(
                "recv − exchange timestamp (ms): p50 {} p90 {} p99 {} max {}",
                pct(0.5),
                pct(0.9),
                pct(0.99),
                lat[lat.len() - 1]
            );
        }
    }
}

fn join(keys: &BTreeSet<String>) -> String {
    keys.iter().map(String::as_str).collect::<Vec<_>>().join(", ")
}

async fn record(
    http: &reqwest::Client,
    market: &Market,
    seconds: u64,
    probe_resubscribe: bool,
) -> anyhow::Result<()> {
    let (mut ws, response) = tokio_tungstenite::connect_async(WS).await.context("connect")?;
    eprintln!("connected: HTTP {}", response.status());
    let assets: Vec<&str> = market.tokens.iter().map(|(t, _)| t.as_str()).collect();
    let subscribe = json!({ "assets_ids": assets, "type": "market", "custom_feature_enabled": true });
    ws.send(Message::text(subscribe.to_string())).await?;

    let started = Utc::now();
    let file_name = format!(
        "{OUT_DIR}/frames_{}_{}.jsonl",
        &market.condition_id[..market.condition_id.len().min(12)],
        started.format("%Y%m%dT%H%M%SZ")
    );
    let mut out = BufWriter::new(File::create(&file_name)?);
    let mut stats = Stats::default();

    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    let probe_at = tokio::time::sleep(Duration::from_secs(seconds / 2));
    tokio::pin!(probe_at);
    let mut probed = !probe_resubscribe;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.tick().await;
    // At the deadline, fetch REST books in the background and keep reading
    // for a few seconds, so the stream contains the state REST returned.
    let mut rest_task: Option<tokio::task::JoinHandle<anyhow::Result<Vec<RestBook>>>> = None;

    loop {
        let frame = tokio::select! {
            frame = ws.next() => frame,
            _ = &mut deadline => {
                if rest_task.is_some() {
                    break;
                }
                let (http, tokens) = (http.clone(), market.tokens.clone());
                rest_task = Some(tokio::spawn(async move { fetch_rest_books(&http, &tokens).await }));
                deadline.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(5));
                eprintln!("fetching REST books, reading 5 s more");
                continue;
            }
            _ = tokio::signal::ctrl_c() => { eprintln!("ctrl-c, stopping"); break }
            _ = ping.tick() => {
                ws.send(Message::text("PING")).await?;
                continue;
            }
            _ = &mut probe_at, if !probed => {
                probed = true;
                // Gap recovery will need a fresh book on demand. Check whether
                // unsubscribe + subscribe on a live socket sends a new `book`.
                eprintln!("probe: unsubscribe + subscribe");
                ws.send(Message::text(json!({ "assets_ids": assets, "operation": "unsubscribe" }).to_string())).await?;
                ws.send(Message::text(json!({ "assets_ids": assets, "operation": "subscribe" }).to_string())).await?;
                continue;
            }
        };
        // Stamp first, before any parsing.
        let recv_us = Utc::now().timestamp_micros();
        let Some(frame) = frame else {
            eprintln!("server closed the stream");
            break;
        };
        let (kind, raw) = match frame? {
            Message::Text(text) => {
                match serde_json::from_str::<Value>(&text) {
                    Ok(Value::Array(items)) => {
                        stats.array_frames += 1;
                        for item in &items {
                            stats.observe(item, recv_us)?;
                        }
                    }
                    Ok(v) => stats.observe(&v, recv_us)?,
                    Err(_) => *stats.non_json.entry(text.to_string()).or_default() += 1,
                }
                ("text", text.to_string())
            }
            Message::Ping(p) => ("ping", String::from_utf8_lossy(&p).into_owned()),
            Message::Pong(p) => ("pong", String::from_utf8_lossy(&p).into_owned()),
            Message::Close(c) => {
                eprintln!("close frame: {c:?}");
                ("close", format!("{c:?}"))
            }
            Message::Binary(b) => ("binary", format!("{} bytes", b.len())),
            Message::Frame(_) => continue,
        };
        writeln!(out, "{}", json!({ "recv_us": recv_us, "kind": kind, "raw": raw }))?;
    }

    let _ = ws.close(None).await;
    out.flush()?;
    stats.report();
    match rest_task {
        Some(task) => compare_with_rest(&task.await??, &stats)?,
        None => eprintln!("\nstopped before the REST check"),
    }
    eprintln!("frames written to {file_name}");
    Ok(())
}

struct RestBook {
    token: String,
    outcome: String,
    body: Value,
}

async fn fetch_rest_books(
    http: &reqwest::Client,
    tokens: &[(String, String)],
) -> anyhow::Result<Vec<RestBook>> {
    let mut out = Vec::new();
    for (token, outcome) in tokens {
        let (raw, body) = get_json(http, &format!("{CLOB}/book?token_id={token}")).await?;
        fs::write(format!("{OUT_DIR}/rest_book_{}.json", &token[..token.len().min(12)]), &raw)?;
        out.push(RestBook {
            token: token.clone(),
            outcome: outcome.clone(),
            body,
        });
    }
    Ok(out)
}

fn differing(mine: &Levels, theirs: &Levels) -> usize {
    mine.keys()
        .chain(theirs.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|p| mine.get(p) != theirs.get(p))
        .count()
}

/// Finds the stream state with the REST book's hash and compares levels. A
/// found hash with zero differing levels means the stream reproduced the
/// exchange book exactly at that point.
fn compare_with_rest(rest_books: &[RestBook], stats: &Stats) -> anyhow::Result<()> {
    eprintln!("\n== local book vs REST /book ==");
    for RestBook { token, outcome, body } in rest_books {
        let Some(local) = stats.books.get(token) else {
            eprintln!("{outcome}: no local book");
            continue;
        };
        let rest_hash = body["hash"].as_str().unwrap_or_default();
        let (bids, asks) = (levels(&body["bids"])?, levels(&body["asks"])?);
        let found = local.history.iter().rposition(|(h, _, _)| h == rest_hash);
        let (label, mine_bids, mine_asks) = match found {
            Some(i) => {
                let age = local.history.len() - 1 - i;
                let (_, b, a) = &local.history[i];
                (format!("REST hash found in stream ({age} states before the last)"), b, a)
            }
            None => (
                "REST hash NOT seen in stream; comparing with the last state".to_owned(),
                &local.bids,
                &local.asks,
            ),
        };
        eprintln!(
            "{outcome}: {label}; bids {}/{} levels differ {}, asks {}/{} levels differ {}",
            mine_bids.len(),
            bids.len(),
            differing(mine_bids, &bids),
            mine_asks.len(),
            asks.len(),
            differing(mine_asks, &asks),
        );
    }
    Ok(())
}
