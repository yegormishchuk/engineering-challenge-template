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
use engineering_challenge::ws::connection::{ConnectionConfig, VerifyConfig};

// glibc malloc fragments across tokio's threads: in 150 s full-scale runs
// RSS was ~7 KB per symbol with glibc, ~4.7 KB with jemalloc (live data is
// ~2 KB), and jemalloc handled frames fastest. glibc with 2 arenas used a
// little less memory but took ~60% longer per frame.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(about = "Records Polymarket order books in the challenge's SBE format")]
struct Args {
    /// Output: `-` or `/dev/stdout` for stdout, otherwise a file path. With
    /// stdout, redirect with `>`, not `>>`.
    #[arg(long, default_value = "-")]
    output: String,

    /// Most symbols (outcome tokens) per websocket connection. 5000 ran
    /// without errors live; all ~514k tokens then need about 100 sockets.
    #[arg(long, default_value_t = 5000)]
    max_per_connection: usize,

    /// Record at most this many markets (for testing). Default: all.
    #[arg(long)]
    max_markets: Option<usize>,

    /// Read at most this many Gamma event pages (~900–1400 markets each) per full pass
    /// (for testing). Default: all.
    #[arg(long)]
    max_pages: Option<usize>,

    /// Minutes between full discovery passes.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    full_every_mins: u64,

    /// Seconds between incremental discovery polls.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
    recent_every_secs: u64,

    /// Writer channel capacity, in messages.
    #[arg(long, default_value_t = 1 << 16)]
    channel_capacity: usize,

    /// Do not check books assumed empty against the REST batch endpoint.
    #[arg(long)]
    no_verify_empty: bool,

    /// Most REST checks of empty books per second, over all connections.
    #[arg(long, default_value_t = 10.0)]
    verify_rate: f64,

    /// Connections per shard, all subscribed to the same symbols; one writes
    /// at a time and the others take over when it fails (1 = no redundancy).
    /// Default 1 until hash-based loss detection stops misfiring live (see
    /// docs/NEXT.md, E1 status).
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..=4))]
    redundancy: u64,

    /// Seconds between counter logs.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..))]
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
    // SIGINT (Ctrl+C) and SIGTERM (`docker stop`, systemd) both shut down
    // cleanly; a second signal exits at once.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => info!("SIGINT: shutting down (send again to force)"),
                _ = sigterm.recv() => info!("SIGTERM: shutting down (send again to force)"),
            }
            shutdown.cancel();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            warn!("second signal: exiting without flushing");
            std::process::exit(130);
        }
    });

    let (tick_tx, ticks) = watch::channel(chrono::Utc::now());
    tokio::spawn(clock::run_minute_ticks(tick_tx, shutdown.clone()));

    // Every HTTP call has a deadline, so one stalled response cannot stop
    // discovery or the empty-book checks for good.
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()?;

    let (discovery_tx, mut discovery_rx) = mpsc::unbounded_channel::<Event>();
    let mut shards = ShardManager::new(
        ConnectionConfig {
            verify: (!args.no_verify_empty).then(|| VerifyConfig::polymarket(http.clone(), args.verify_rate)),
            ..ConnectionConfig::default()
        },
        args.max_per_connection,
        out,
        ticks,
        shutdown.clone(),
        Some(discovery_tx),
    )
    .with_redundancy(args.redundancy as usize);

    let mut tracker = Tracker::new(TrackerConfig {
        max_markets: args.max_markets,
        ..TrackerConfig::default()
    });
    let (poll_tx, mut poll_rx) = mpsc::channel::<Poll>(64);
    let poller = tokio::spawn(discovery::run_poller(
        http,
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
    let mut failed = None;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            failure = shards.wait_for_failure() => {
                error!(error = %failure, "fatal; shutting down");
                failed = Some(failure);
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
    // Closing the receiver ends any poller send still waiting for room.
    drop(poll_rx);
    let _ = poller.await;

    log_stats(&shards);
    shards.join().await;
    let summary = writer.join()?;
    info!(?summary, "output flushed and closed");
    if let Some(failure) = failed {
        anyhow::bail!("collector stopped: {failure}");
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
            mismatched_levels = s.mismatched_levels.load(Relaxed),
            skipped_no_baseline = s.skipped_no_baseline.load(Relaxed),
            decode_errors = s.decode_errors.load(Relaxed),
            resyncs = s.resyncs.load(Relaxed),
            minute_snapshots = s.minute_snapshots.load(Relaxed),
            assumed_empty = s.assumed_empty.load(Relaxed),
            empty_confirmed = s.empty_confirmed.load(Relaxed),
            empty_wrong = s.empty_wrong.load(Relaxed),
            empty_gave_up = s.empty_gave_up.load(Relaxed),
            verify_errors = s.verify_errors.load(Relaxed),
            ignored = s.ignored.load(Relaxed),
            dropped_during_resync = s.dropped_during_resync.load(Relaxed),
            gap_mismatched_books = s.gap_mismatched_books.load(Relaxed),
            switch_snapshots = s.switch_snapshots.load(Relaxed),
            switch_without_baseline = s.switch_without_baseline.load(Relaxed),
            mb = s.bytes.load(Relaxed) / 1_000_000,
            busy_ms = s.busy_us.load(Relaxed) / 1000,
            max_frame_us = s.max_frame_us.load(Relaxed),
            "connection stats"
        );
    }
    let now = std::time::Instant::now();
    for src in shards.sources() {
        let (lag_p50_ms, lag_p99_ms, lag_samples) = src.lag_stats(now).unwrap_or_default();
        info!(
            shard = src.shard,
            // Connection id of the source; absent if the lock was busy.
            source = src.try_source().map(|m| src.shard * src.members() + m),
            source_switches = src.switches.load(Relaxed),
            lost_on_source = src.lost_on_source.load(Relaxed),
            lost_on_peers = src.lost_on_peers.load(Relaxed),
            late_after_loss = src.late_after_loss.load(Relaxed),
            late_after_loss_max_ms = src.late_after_loss_max_ms.load(Relaxed),
            lag_p50_ms = lag_p50_ms.round() as i64,
            lag_p99_ms = lag_p99_ms.round() as i64,
            lag_samples,
            "shard source"
        );
    }
}
