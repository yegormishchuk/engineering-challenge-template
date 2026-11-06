//! Latency probe (E4): opens N websockets to the market channel, all
//! subscribed to the same tokens, and writes one CSV row per price change per
//! socket. Polymarket gives every socket the same `hash` for the same change,
//! so rows with the same (symbol, hash) can be compared across sockets.
//!
//! ```text
//! latency --out <csv> [--sockets 8] [--tokens 200] [--seconds 1800]
//!         [--tokens-file <path>] [--ports <lo>-<hi>] [--ip <addr>]... | --all-ips
//!         [--refresh <k>:<secs>] [--stagger-ms <n>]
//! ```
//!
//! Columns: `socket,session,local_port,remote_ip,connected_us,symbol,hash,recv_us,exchange_us`.
//! `recv_us` is taken right after the frame arrives, before parsing.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::Utc;
use clap::Parser;
use engineering_challenge::discovery::{GAMMA_URL, parse_market};
use engineering_challenge::exchange::Event;
use engineering_challenge::exchange::polymarket::{PING, WS_URL, decode, encode_initial_subscribe};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpSocket, lookup_host};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const HOST: &str = "ws-subscriptions-clob.polymarket.com";

#[derive(Parser)]
#[command(about = "Compare market-channel websockets: which socket gets each price change when")]
struct Args {
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = 8)]
    sockets: usize,
    #[arg(long, default_value_t = 200)]
    tokens: usize,
    #[arg(long, default_value_t = 1800)]
    seconds: u64,
    /// Reuse a token list (one per line). The list in use is written to <csv>.tokens.
    #[arg(long)]
    tokens_file: Option<PathBuf>,
    /// Bind socket i to local port lo+i.
    #[arg(long, value_parser = parse_ports)]
    ports: Option<RangeInclusive<u16>>,
    /// Connect sockets round-robin to these addresses.
    #[arg(long)]
    ip: Vec<Ipv4Addr>,
    /// Connect sockets round-robin to all resolved addresses.
    #[arg(long)]
    all_ips: bool,
    /// Sockets 0..k reconnect every <secs> seconds (age experiment).
    #[arg(long, value_parser = parse_refresh)]
    refresh: Option<(usize, Duration)>,
    #[arg(long, default_value_t = 200)]
    stagger_ms: u64,
}

fn parse_ports(s: &str) -> Result<RangeInclusive<u16>, String> {
    let (lo, hi) = s.split_once('-').ok_or("expected <lo>-<hi>")?;
    let lo: u16 = lo.parse().map_err(|e| format!("bad low port: {e}"))?;
    let hi: u16 = hi.parse().map_err(|e| format!("bad high port: {e}"))?;
    if lo > hi {
        return Err("low port is above high port".into());
    }
    Ok(lo..=hi)
}

fn parse_refresh(s: &str) -> Result<(usize, Duration), String> {
    let (k, secs) = s.split_once(':').ok_or("expected <k>:<secs>")?;
    let k: usize = k.parse().map_err(|e| format!("bad k: {e}"))?;
    let secs: u64 = secs.parse().map_err(|e| format!("bad secs: {e}"))?;
    Ok((k, Duration::from_secs(secs)))
}

/// What the rows of one websocket session have in common.
struct Conn {
    socket: usize,
    session: u32,
    local_port: u16,
    remote_ip: String,
    connected_us: i64,
}

/// One CSV line for a price change that carries a hash.
fn csv_row(c: &Conn, recv_us: i64, e: &Event) -> Option<String> {
    let Event::Level { symbol, exchange_ts, hash: Some(hash), .. } = e else {
        return None;
    };
    Some(format!(
        "{},{},{},{},{},{},{},{},{}",
        c.socket,
        c.session,
        c.local_port,
        c.remote_ip,
        c.connected_us,
        symbol,
        hash,
        recv_us,
        exchange_ts.timestamp_micros()
    ))
}

const HEADER: &str = "socket,session,local_port,remote_ip,connected_us,symbol,hash,recv_us,exchange_us";

async fn pick_tokens(args: &Args) -> anyhow::Result<Vec<String>> {
    if let Some(path) = &args.tokens_file {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        return Ok(text.lines().map(str::trim).filter(|l| !l.is_empty()).map(str::to_owned).collect());
    }
    let http = reqwest::Client::builder().gzip(true).build()?;
    let mut tokens = Vec::new();
    let mut offset = 0;
    while tokens.len() < args.tokens {
        let url = format!(
            "{GAMMA_URL}/markets?closed=false&active=true&order=volume24hr&ascending=false&limit=100&offset={offset}"
        );
        let page: Vec<Value> = http.get(&url).send().await?.error_for_status()?.json().await?;
        if page.is_empty() {
            break;
        }
        offset += page.len();
        for market in page.iter().filter_map(parse_market).filter(|m| m.tradable) {
            tokens.extend(market.tokens);
        }
    }
    tokens.truncate(args.tokens);
    Ok(tokens)
}

/// Where and how one socket connects.
struct Plan {
    socket: usize,
    ip: Ipv4Addr,
    port: Option<u16>,
    refresh: Option<Duration>,
}

/// One websocket session: runs until it fails, the refresh time passes or
/// the probe stops.
async fn session(
    plan: &Plan,
    n: u32,
    subscribe: &str,
    tx: &mpsc::UnboundedSender<String>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let tcp = TcpSocket::new_v4()?;
    tcp.set_reuseaddr(true)?;
    if let Some(port) = plan.port {
        tcp.bind((Ipv4Addr::UNSPECIFIED, port).into())?;
    }
    let stream = tokio::time::timeout(Duration::from_secs(10), tcp.connect(SocketAddr::from((plan.ip, 443))))
        .await
        .context("tcp connect timed out")??;
    let local_port = stream.local_addr()?.port();
    let (mut ws, _) = tokio_tungstenite::client_async_tls_with_config(WS_URL, stream, None, None)
        .await
        .context("websocket handshake")?;
    let conn = Conn {
        socket: plan.socket,
        session: n,
        local_port,
        remote_ip: plan.ip.to_string(),
        connected_us: Utc::now().timestamp_micros(),
    };
    eprintln!("socket {} session {n}: {} local port {local_port}", plan.socket, plan.ip);
    ws.send(Message::text(subscribe)).await?;

    let refresh_at = tokio::time::sleep(plan.refresh.unwrap_or(Duration::ZERO));
    tokio::pin!(refresh_at);
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.tick().await;
    loop {
        let frame = tokio::select! {
            frame = ws.next() => frame,
            _ = ping.tick() => { ws.send(Message::text(PING)).await?; continue; }
            _ = &mut refresh_at, if plan.refresh.is_some() => return Ok(()),
            _ = cancel.cancelled() => return Ok(()),
        };
        // Stamp first, before any parsing.
        let recv_us = Utc::now().timestamp_micros();
        match frame.context("stream closed")?? {
            Message::Text(text) => {
                if let Ok(decoded) = decode(&text) {
                    for row in decoded.events.iter().filter_map(|e| csv_row(&conn, recv_us, e)) {
                        let _ = tx.send(row);
                    }
                }
            }
            Message::Close(c) => bail!("close frame: {c:?}"),
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if let Some(ports) = &args.ports
        && ports.clone().count() < args.sockets
    {
        bail!("--ports has fewer ports than --sockets");
    }
    let tokens = Arc::new(pick_tokens(&args).await?);
    if tokens.is_empty() {
        bail!("no tokens");
    }
    let mut tokens_path = args.out.clone().into_os_string();
    tokens_path.push(".tokens");
    fs::write(&tokens_path, tokens.join("\n") + "\n")?;

    let resolved: Vec<Ipv4Addr> = lookup_host((HOST, 443))
        .await?
        .filter_map(|a| match a {
            SocketAddr::V4(a) => Some(*a.ip()),
            SocketAddr::V6(_) => None,
        })
        .collect();
    let ips = if !args.ip.is_empty() {
        args.ip.clone()
    } else if args.all_ips {
        resolved.clone()
    } else {
        resolved.iter().take(1).copied().collect()
    };
    if ips.is_empty() {
        bail!("no IPv4 address for {HOST}");
    }
    eprintln!("{} tokens, {} sockets, addresses {ips:?} (resolved {resolved:?})", tokens.len(), args.sockets);

    let subscribe: Arc<str> = encode_initial_subscribe(&tokens).into();
    let mut out = BufWriter::new(File::create(&args.out)?);
    writeln!(out, "{HEADER}")?;
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let cancel = CancellationToken::new();
    let mut tasks = Vec::new();
    for socket in 0..args.sockets {
        let plan = Plan {
            socket,
            ip: ips[socket % ips.len()],
            port: args.ports.as_ref().and_then(|p| p.clone().nth(socket)),
            refresh: args.refresh.filter(|(k, _)| socket < *k).map(|(_, d)| d),
        };
        let (tx, cancel, subscribe) = (tx.clone(), cancel.clone(), subscribe.clone());
        tasks.push(tokio::spawn(async move {
            let mut n = 0;
            while !cancel.is_cancelled() {
                if let Err(e) = session(&plan, n, &subscribe, &tx, &cancel).await {
                    eprintln!("socket {} session {n}: {e:#}", plan.socket);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = cancel.cancelled() => {}
                    }
                }
                n += 1;
            }
        }));
        tokio::time::sleep(Duration::from_millis(args.stagger_ms)).await;
    }
    drop(tx);

    let deadline = tokio::time::sleep(Duration::from_secs(args.seconds));
    tokio::pin!(deadline);
    let mut rows = 0u64;
    loop {
        tokio::select! {
            Some(row) = rx.recv() => { writeln!(out, "{row}")?; rows += 1; }
            _ = &mut deadline => break,
            _ = tokio::signal::ctrl_c() => { eprintln!("ctrl-c, stopping"); break }
        }
    }
    cancel.cancel();
    for t in tasks {
        let _ = t.await;
    }
    while let Ok(row) = rx.try_recv() {
        writeln!(out, "{row}")?;
        rows += 1;
    }
    out.flush()?;
    eprintln!("{rows} rows written to {}", args.out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use engineering_challenge::Side;

    #[test]
    fn parses_port_range() {
        assert_eq!(parse_ports("40000-40015").unwrap(), 40000..=40015);
        assert!(parse_ports("5-1").is_err());
        assert!(parse_ports("x").is_err());
    }

    #[test]
    fn parses_refresh() {
        assert_eq!(parse_refresh("4:600").unwrap(), (4, Duration::from_secs(600)));
        assert!(parse_refresh("4").is_err());
    }

    #[test]
    fn csv_row_format() {
        let c = Conn { socket: 2, session: 1, local_port: 40002, remote_ip: "1.2.3.4".into(), connected_us: 1_700_000_000_000_000 };
        let e = Event::Level {
            symbol: "tok".into(),
            side: Side::Buy,
            price: 500_000,
            quantity: 7,
            exchange_ts: Utc.timestamp_millis_opt(1_700_000_005_123).unwrap(),
            hash: Some("abc".into()),
        };
        assert_eq!(
            csv_row(&c, 1_700_000_005_200_000, &e).unwrap(),
            "2,1,40002,1.2.3.4,1700000000000000,tok,abc,1700000005200000,1700000005123000"
        );
        let no_hash = Event::Level { symbol: "tok".into(), side: Side::Buy, price: 1, quantity: 1, exchange_ts: Utc.timestamp_millis_opt(1_700_000_005_123).unwrap(), hash: None };
        assert!(csv_row(&c, 0, &no_hash).is_none());
    }
}
