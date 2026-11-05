//! Polymarket order book collector.
//!
//! SBE output goes to stdout (default) or `--output <path>`; logs and
//! counters go to stderr. See `docs/SPEC.md`.

use std::time::Duration;

use clap::Parser;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use chrono::Utc;
use engineering_challenge::clock;
use engineering_challenge::discovery::{self, Diff, Poll, PollerConfig, Tracker, TrackerConfig};
use engineering_challenge::exchange::Event;
use engineering_challenge::shard::ShardManager;
use engineering_challenge::writer::{self, Output};
use engineering_challenge::ws::connection::ConnectionConfig;

#[derive(Parser, Debug)]
#[command(about = "Records Polymarket order books in the challenge's SBE format")]
struct Args {
    /// Output: `-` or `/dev/stdout` for stdout, otherwise a file path. With
    /// stdout, redirect with `>`, not `>>`.
    #[arg(long, default_value = "-")]
    output: String,

    /// Most symbols (outcome tokens) per websocket connection.
    #[arg(long, default_value_t = 500)]
    max_per_connection: usize,

    /// Record at most this many markets (for testing). Default: all.
    #[arg(long)]
    max_markets: Option<usize>,

    /// Read at most this many Gamma event pages (~1400 markets each) per full pass
    /// (for testing). Default: all.
    #[arg(long)]
    max_pages: Option<usize>,

    /// Minutes between full discovery passes.
    #[arg(long, default_value_t = 30)]
    full_every_mins: u64,

    /// Seconds between incremental discovery polls.
    #[arg(long, default_value_t = 30)]
    recent_every_secs: u64,

    /// Writer channel capacity, in messages.
    #[arg(long, default_value_t = 1 << 16)]
    channel_capacity: usize,

    /// Seconds between counter logs.
    #[arg(long, default_value_t = 30)]
    stats_every: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let args = Args::parse();

    let output = Output::parse(&args.output);
    let (out, writer) = writer::spawn(&output, args.channel_capacity)?;
    info!(output = %output.path().display(), "writer started");

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("SIGINT: shutting down (press again to force)");
                shutdown.cancel();
            }
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("second SIGINT: exiting without flushing");
                std::process::exit(130);
            }
        }
    });

    let (tick_tx, ticks) = watch::channel(chrono::Utc::now());
    tokio::spawn(clock::run_minute_ticks(tick_tx, shutdown.clone()));

    let (discovery_tx, mut discovery_rx) = mpsc::unbounded_channel::<Event>();
    let mut shards = ShardManager::new(
        ConnectionConfig::default(),
        args.max_per_connection,
        out,
        ticks,
        shutdown.clone(),
        Some(discovery_tx),
    );

    let mut tracker = Tracker::new(TrackerConfig {
        max_markets: args.max_markets,
        ..TrackerConfig::default()
    });
    let (poll_tx, mut poll_rx) = mpsc::channel::<Poll>(64);
    let poller = tokio::spawn(discovery::run_poller(
        reqwest::Client::new(),
        PollerConfig {
            full_every: Duration::from_secs(args.full_every_mins * 60),
            recent_every: Duration::from_secs(args.recent_every_secs),
            full_max_pages: args.max_pages,
            ..PollerConfig::default()
        },
        poll_tx,
        shutdown.clone(),
    ));

    let mut stats_every = tokio::time::interval(Duration::from_secs(args.stats_every));
    stats_every.tick().await;
    let mut failed = false;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            gone = shards.wait_for_failure() => {
                error!(error = %gone, "writer stopped; shutting down");
                failed = true;
                shutdown.cancel();
                break;
            }
            Some(poll) = poll_rx.recv() => {
                let source = match &poll {
                    Poll::Full { .. } => "full",
                    Poll::Recent { query, .. } => query,
                };
                let diff = tracker.apply(poll, Utc::now());
                apply_diff(&mut shards, &tracker, source, diff);
            }
            Some(event) = discovery_rx.recv() => {
                let diff = match &event {
                    Event::MarketAdded { market, symbols } => tracker.market_added(market, symbols, Utc::now()),
                    Event::MarketResolved { market, .. } => tracker.market_resolved(market, Utc::now()),
                    _ => continue,
                };
                apply_diff(&mut shards, &tracker, "ws-event", diff);
            }
            _ = stats_every.tick() => {
                info!(markets = tracker.len(), symbols = shards.len(), "tracked");
                log_stats(&shards);
            }
        }
    }
    let _ = poller.await;

    log_stats(&shards);
    shards.join().await;
    let summary = writer.join()?;
    info!(?summary, "output flushed and closed");
    if failed {
        anyhow::bail!("collector stopped because the writer failed");
    }
    Ok(())
}

fn apply_diff(shards: &mut ShardManager, tracker: &Tracker, source: &str, diff: Diff) {
    if diff.is_empty() {
        return;
    }
    let removed = shards.remove(diff.remove);
    let added = shards.add(diff.add);
    info!(
        source,
        markets_added = diff.markets_added,
        markets_removed = diff.markets_removed,
        symbols_added = added,
        symbols_removed = removed,
        markets = tracker.len(),
        connections = shards.connections().len(),
        "subscriptions changed"
    );
}

fn log_stats(shards: &ShardManager) {
    use std::sync::atomic::Ordering::Relaxed;
    for (id, (symbols, s)) in shards.connections().iter().enumerate() {
        info!(
            conn = id,
            symbols,
            connects = s.connects.load(Relaxed),
            disconnects = s.disconnects.load(Relaxed),
            frames = s.frames.load(Relaxed),
            books = s.books.load(Relaxed),
            levels = s.levels.load(Relaxed),
            mismatched_books = s.mismatched_books.load(Relaxed),
            skipped_no_baseline = s.skipped_no_baseline.load(Relaxed),
            decode_errors = s.decode_errors.load(Relaxed),
            resyncs = s.resyncs.load(Relaxed),
            minute_snapshots = s.minute_snapshots.load(Relaxed),
            assumed_empty = s.assumed_empty.load(Relaxed),
            mb = s.bytes.load(Relaxed) / 1_000_000,
            busy_ms = s.busy_us.load(Relaxed) / 1000,
            max_frame_us = s.max_frame_us.load(Relaxed),
            "connection stats"
        );
    }
}
