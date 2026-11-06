//! Spreads symbols across shards, at most `max_per_connection` each.
//!
//! A shard is `redundancy` connections subscribed to the same symbols (see
//! [`crate::ws::source`]); with redundancy 1 it is a single connection.
//! New symbols go to the least-loaded shard that has room; when every
//! shard is full a new one is started. Removing symbols frees room but
//! never closes a connection: an empty connection simply disconnects and
//! waits for work (see [`Connection::run`]).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::exchange::Event;
use crate::writer::{OutSender, WriterGone};
use crate::ws::connection::{Command, ConnStats, Connection, ConnectionConfig, Member};
use crate::ws::source::ShardSource;

/// Why the collector cannot go on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    WriterGone,
    Panicked(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WriterGone => f.write_str("writer thread has stopped"),
            Self::Panicked(e) => write!(f, "connection task panicked: {e}"),
        }
    }
}

/// One shard: its member connections and their symbol count.
struct Slot {
    txs: Vec<mpsc::UnboundedSender<Command>>,
    count: usize,
    stats: Vec<Arc<ConnStats>>,
    /// `None` with redundancy 1.
    source: Option<Arc<ShardSource>>,
}

pub struct ShardManager {
    cfg: ConnectionConfig,
    max_per_connection: usize,
    redundancy: usize,
    out: OutSender,
    ticks: watch::Receiver<DateTime<Utc>>,
    shutdown: CancellationToken,
    discovery: Option<mpsc::UnboundedSender<Event>>,
    slots: Vec<Slot>,
    assignment: HashMap<String, usize>,
    tasks: JoinSet<Result<(), WriterGone>>,
}

impl ShardManager {
    /// `discovery` receives `new_market` / `market_resolved` events from
    /// every connection. `new_market` is broadcast to every socket, so it
    /// arrives once per connection; the tracker ignores the repeats.
    /// `market_resolved` may go only to sockets subscribed to the market,
    /// which is why no connection is singled out.
    pub fn new(
        cfg: ConnectionConfig,
        max_per_connection: usize,
        out: OutSender,
        ticks: watch::Receiver<DateTime<Utc>>,
        shutdown: CancellationToken,
        discovery: Option<mpsc::UnboundedSender<Event>>,
    ) -> Self {
        assert!(max_per_connection > 0, "max_per_connection must be positive");
        Self {
            cfg,
            max_per_connection,
            redundancy: 1,
            out,
            ticks,
            shutdown,
            discovery,
            slots: Vec::new(),
            assignment: HashMap::new(),
            tasks: JoinSet::new(),
        }
    }

    /// Connections per shard (default 1). Call before adding symbols.
    pub fn with_redundancy(mut self, redundancy: usize) -> Self {
        assert!(redundancy > 0, "redundancy must be positive");
        assert!(self.slots.is_empty(), "set redundancy before adding symbols");
        self.redundancy = redundancy;
        self
    }

    pub fn len(&self) -> usize {
        self.assignment.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assignment.is_empty()
    }

    pub fn contains(&self, symbol: &str) -> bool {
        self.assignment.contains_key(symbol)
    }

    /// Per-connection symbol counts and counters, in connection order
    /// (shard by shard).
    pub fn connections(&self) -> Vec<(usize, Arc<ConnStats>)> {
        self.slots
            .iter()
            .flat_map(|s| s.stats.iter().map(move |st| (s.count, Arc::clone(st))))
            .collect()
    }

    /// Per shard with redundancy: its source state.
    pub fn sources(&self) -> Vec<Arc<ShardSource>> {
        self.slots.iter().filter_map(|s| s.source.clone()).collect()
    }

    /// Assigns and subscribes symbols not already assigned. Returns how many
    /// were new.
    pub fn add(&mut self, symbols: impl IntoIterator<Item = String>) -> usize {
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in symbols {
            if self.assignment.contains_key(&symbol) {
                continue;
            }
            let slot = self.pick_slot();
            self.slots[slot].count += 1;
            self.assignment.insert(symbol.clone(), slot);
            batches.entry(slot).or_default().push(symbol);
        }
        let added = batches.values().map(Vec::len).sum();
        for (slot, batch) in batches {
            // A send fails only if the task has ended, i.e. during shutdown.
            for tx in &self.slots[slot].txs {
                let _ = tx.send(Command::Add(batch.clone()));
            }
        }
        added
    }

    /// Unsubscribes symbols. Returns how many were assigned.
    pub fn remove(&mut self, symbols: impl IntoIterator<Item = String>) -> usize {
        let mut batches: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in symbols {
            if let Some(slot) = self.assignment.remove(&symbol) {
                self.slots[slot].count -= 1;
                batches.entry(slot).or_default().push(symbol);
            }
        }
        let removed = batches.values().map(Vec::len).sum();
        for (slot, batch) in batches {
            for tx in &self.slots[slot].txs {
                let _ = tx.send(Command::Remove(batch.clone()));
            }
        }
        removed
    }

    /// Waits for a connection to fail: the writer is gone, or a task
    /// panicked. A panicked connection's symbols would silently stop being
    /// recorded, so it is treated as fatal too. Never returns while all
    /// connections are healthy.
    pub async fn wait_for_failure(&mut self) -> Failure {
        loop {
            match self.tasks.join_next().await {
                Some(Ok(Err(WriterGone))) => return Failure::WriterGone,
                Some(Ok(Ok(()))) => {}
                Some(Err(e)) => return Failure::Panicked(e.to_string()),
                None => std::future::pending::<()>().await,
            }
        }
    }

    /// Closes every connection (they must already have seen `shutdown`) and
    /// waits for them. Dropping the manager's own [`OutSender`] here lets the
    /// writer finish once the connections are gone.
    pub async fn join(self) {
        let Self { slots, mut tasks, out, .. } = self;
        drop(slots);
        drop(out);
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(error = %e, "connection task panicked");
            }
        }
    }

    fn pick_slot(&mut self) -> usize {
        let best = self
            .slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.count < self.max_per_connection)
            .min_by_key(|(i, s)| (s.count, *i))
            .map(|(i, _)| i);
        best.unwrap_or_else(|| self.spawn_connection())
    }

    fn spawn_connection(&mut self) -> usize {
        let shard = self.slots.len();
        let source = (self.redundancy > 1).then(|| Arc::new(ShardSource::new(shard, self.redundancy)));
        let mut slot = Slot {
            txs: Vec::new(),
            count: 0,
            stats: Vec::new(),
            source: source.clone(),
        };
        for index in 0..self.redundancy {
            let (tx, rx) = mpsc::unbounded_channel();
            let stats = Arc::new(ConnStats::default());
            let mut conn = Connection::new(
                shard * self.redundancy + index,
                self.cfg.clone(),
                self.out.clone(),
                Arc::clone(&stats),
                self.discovery.clone(),
            );
            if let Some(shard) = &source {
                conn = conn.with_member(Member {
                    index,
                    shard: Arc::clone(shard),
                });
            }
            self.tasks
                .spawn(conn.run(rx, self.ticks.clone(), self.shutdown.clone()));
            slot.txs.push(tx);
            slot.stats.push(stats);
        }
        self.slots.push(slot);
        shard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{self, Output};

    /// A manager whose connections point at a closed port; they never
    /// connect, which is fine for testing assignment.
    fn manager(max: usize) -> (ShardManager, writer::WriterHandle, CancellationToken, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let (out, handle) = writer::spawn(&Output::File(dir.path().join("o.sbe")), 16).unwrap();
        let (_tx, ticks) = watch::channel(Utc::now());
        let shutdown = CancellationToken::new();
        let cfg = ConnectionConfig {
            url: "ws://127.0.0.1:9".into(),
            ..ConnectionConfig::default()
        };
        (ShardManager::new(cfg, max, out, ticks, shutdown.clone(), None), handle, shutdown, dir)
    }

    fn counts(m: &ShardManager) -> Vec<usize> {
        m.connections().into_iter().map(|(c, _)| c).collect()
    }

    fn syms(range: std::ops::Range<usize>) -> Vec<String> {
        range.map(|i| format!("s{i}")).collect()
    }

    #[tokio::test]
    async fn fills_connections_up_to_the_limit() {
        let (mut m, writer, shutdown, _dir) = manager(3);
        assert_eq!(m.add(syms(0..7)), 7);
        assert_eq!(counts(&m), [3, 3, 1]);
        assert_eq!(m.add(syms(0..7)), 0, "duplicates are ignored");
        assert_eq!(m.len(), 7);
        shutdown.cancel();
        m.join().await;
        writer.join().unwrap();
    }

    #[tokio::test]
    async fn removal_frees_room_that_is_reused_before_opening_more() {
        let (mut m, writer, shutdown, _dir) = manager(3);
        m.add(syms(0..6));
        assert_eq!(counts(&m), [3, 3]);
        assert_eq!(m.remove(["s0".to_string(), "s1".to_string(), "nope".to_string()]), 2);
        assert_eq!(counts(&m), [1, 3]);
        m.add(syms(10..13));
        assert_eq!(counts(&m), [3, 3, 1], "fills the emptier connection first");
        assert!(!m.contains("s0"));
        assert!(m.contains("s12"));
        shutdown.cancel();
        m.join().await;
        writer.join().unwrap();
    }

    #[tokio::test]
    async fn redundant_shards_have_every_member_and_count_once() {
        let (m, writer, shutdown, _dir) = manager(3);
        let mut m = m.with_redundancy(2);
        assert_eq!(m.add(syms(0..4)), 4);
        // Two shards (3 + 1 symbols), two connections each.
        assert_eq!(counts(&m), [3, 3, 1, 1]);
        assert_eq!(m.len(), 4);
        assert_eq!(m.sources().len(), 2);
        shutdown.cancel();
        m.join().await;
        writer.join().unwrap();
    }

    #[tokio::test]
    async fn new_symbols_go_to_the_least_loaded_connection() {
        let (mut m, writer, shutdown, _dir) = manager(10);
        m.add(syms(0..10));
        m.add(syms(10..15));
        assert_eq!(counts(&m), [10, 5]);
        m.remove(syms(0..8));
        m.add(syms(20..21));
        assert_eq!(counts(&m), [3, 5]);
        shutdown.cancel();
        m.join().await;
        writer.join().unwrap();
    }
}
