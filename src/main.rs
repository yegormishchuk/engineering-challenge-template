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

use engineering_challenge::discovery;
use engineering_challenge::exchange::Event;
use engineering_challenge::shard::ShardManager;
use engineering_challenge::writer::{self, Output};
use engineering_challenge::ws::connection::ConnectionConfig;
use engineering_challenge::{clock, exchange};

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

    /// Read at most this many Gamma pages (100 markets each) at startup.
    #[arg(long)]
    max_pages: Option<usize>,

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

    let http = reqwest::Client::new();
    let markets = tokio::select! {
        _ = shutdown.cancelled() => None,
        m = discovery::fetch_tradable(&http, discovery::GAMMA_URL, args.max_pages) => Some(m?),
    };
    if let Some(mut markets) = markets {
        if let Some(max) = args.max_markets {
            markets.truncate(max);
        }
        let added = shards.add(markets.into_iter().flat_map(|m| m.tokens));
        info!(
            markets = args.max_markets.map_or("all".to_owned(), |m| m.to_string()),
            symbols = added,
            connections = shards.connections().len(),
            "subscribing"
        );
    }

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
            Some(event) = discovery_rx.recv() => log_discovery(&event),
            _ = stats_every.tick() => log_stats(&shards),
        }
    }

    log_stats(&shards);
    shards.join().await;
    let summary = writer.join()?;
    info!(?summary, "output flushed and closed");
    if failed {
        anyhow::bail!("collector stopped because the writer failed");
    }
    Ok(())
}

fn log_discovery(event: &exchange::Event) {
    // M4 acts on these; for now they are only logged.
    match event {
        Event::MarketAdded { market, symbols } => {
            info!(%market, symbols = symbols.len(), "new market announced")
        }
        Event::MarketResolved { market, .. } => info!(%market, "market resolved"),
        _ => {}
    }
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
            "connection stats"
        );
    }
}
