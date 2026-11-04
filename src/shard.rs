//! Spreads symbols across connections, at most `max_per_connection` each.
//!
//! New symbols go to the least-loaded connection that has room; when every
//! connection is full a new one is started. Removing symbols frees room but
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
use crate::ws::connection::{Command, ConnStats, Connection, ConnectionConfig};

struct Slot {
    tx: mpsc::UnboundedSender<Command>,
    count: usize,
    stats: Arc<ConnStats>,
}

pub struct ShardManager {
    cfg: ConnectionConfig,
    max_per_connection: usize,
    out: OutSender,
    ticks: watch::Receiver<DateTime<Utc>>,
    shutdown: CancellationToken,
    discovery: Option<mpsc::UnboundedSender<Event>>,
    slots: Vec<Slot>,
    assignment: HashMap<String, usize>,
    tasks: JoinSet<Result<(), WriterGone>>,
}

impl ShardManager {
    /// `discovery` receives `new_market` / `market_resolved` events from the
    /// first connection only (the exchange broadcasts them to every socket).
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
            out,
            ticks,
            shutdown,
            discovery,
            slots: Vec::new(),
            assignment: HashMap::new(),
            tasks: JoinSet::new(),
        }
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

    /// Per-connection symbol counts and counters, in connection order.
    pub fn connections(&self) -> Vec<(usize, Arc<ConnStats>)> {
        self.slots.iter().map(|s| (s.count, Arc::clone(&s.stats))).collect()
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
            let _ = self.slots[slot].tx.send(Command::Add(batch));
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
            let _ = self.slots[slot].tx.send(Command::Remove(batch));
        }
        removed
    }

    /// Waits for the first connection to fail with [`WriterGone`]. Never
    /// returns while all connections are healthy.
    pub async fn wait_for_failure(&mut self) -> WriterGone {
        loop {
            match self.tasks.join_next().await {
                Some(Ok(Err(gone))) => return gone,
                Some(Ok(Ok(()))) => {}
                Some(Err(e)) => tracing::error!(error = %e, "connection task panicked"),
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
        let id = self.slots.len();
        let (tx, rx) = mpsc::unbounded_channel();
        let stats = Arc::new(ConnStats::default());
        let discovery = if id == 0 { self.discovery.clone() } else { None };
        let conn = Connection::new(id, self.cfg.clone(), self.out.clone(), Arc::clone(&stats), discovery);
        self.tasks
            .spawn(conn.run(rx, self.ticks.clone(), self.shutdown.clone()));
        self.slots.push(Slot { tx, count: 0, stats });
        id
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
