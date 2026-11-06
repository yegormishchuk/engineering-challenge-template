//! Redundant connections (Extension 1): which connection of a shard writes.
//!
//! A shard is R connections subscribed to the same symbols, each with its own
//! books. Exactly one, the **source**, writes output; the others keep their
//! books current and drop their messages. Every message a member wants to
//! write goes through [`ShardSource::lock`], held while it checks whether it
//! is the source and sends, so a switch always falls between two messages,
//! never inside one.
//!
//! After a switch the new source first writes a snapshot of every symbol it
//! has a live book for (the members' books may differ), then its own
//! messages. It notices the switch through `epoch` at its next message.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};
use tracing::info;

#[derive(Debug)]
pub struct State {
    pub source: usize,
    /// Bumped on every switch.
    pub epoch: u64,
    /// Whether each member has a session open now.
    pub connected: Vec<bool>,
}

#[derive(Debug)]
pub struct ShardSource {
    pub shard: usize,
    state: Arc<Mutex<State>>,
    /// Live baselines per member, updated by each member as it goes; read
    /// without the lock to pick the best candidate.
    baselines: Vec<AtomicUsize>,
    /// Source switches in this shard.
    pub switches: AtomicU64,
}

impl ShardSource {
    pub fn new(shard: usize, members: usize) -> Self {
        assert!(members > 0);
        Self {
            shard,
            state: Arc::new(Mutex::new(State {
                source: 0,
                epoch: 0,
                connected: vec![false; members],
            })),
            baselines: (0..members).map(|_| AtomicUsize::new(0)).collect(),
            switches: AtomicU64::new(0),
        }
    }

    pub fn members(&self) -> usize {
        self.baselines.len()
    }

    pub async fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().await
    }

    /// The lock as an owned guard, held across a member's `&mut self` calls.
    pub async fn lock_owned(&self) -> OwnedMutexGuard<State> {
        Arc::clone(&self.state).lock_owned().await
    }

    /// Current source, for logs and stats.
    pub fn try_source(&self) -> Option<usize> {
        self.state.try_lock().ok().map(|s| s.source)
    }

    pub fn set_baselines(&self, member: usize, n: usize) {
        self.baselines[member].store(n, Ordering::Relaxed);
    }

    pub async fn connected(&self, member: usize) {
        self.lock().await.connected[member] = true;
    }

    /// A member's session ended. If it was the source, the connected member
    /// with the most live baselines takes over at once.
    pub async fn lost(&self, member: usize) {
        let mut st = self.lock().await;
        st.connected[member] = false;
        if st.source == member
            && let Some(best) = self.best_connected(&st, member)
        {
            self.switch(&mut st, best, "source lost its connection");
        }
    }

    /// Called by a non-source member about to write: takes over if the
    /// source is down and this member is up. Returns whether it is the
    /// source now.
    pub fn take_over_if_source_down(&self, st: &mut State, member: usize) -> bool {
        if st.source == member {
            return true;
        }
        if st.connected[st.source] || !st.connected[member] {
            return false;
        }
        self.switch(st, member, "source is down");
        true
    }

    fn best_connected(&self, st: &State, except: usize) -> Option<usize> {
        (0..st.connected.len())
            .filter(|&m| m != except && st.connected[m])
            .max_by_key(|&m| (self.baselines[m].load(Ordering::Relaxed), std::cmp::Reverse(m)))
    }

    pub fn switch(&self, st: &mut State, to: usize, reason: &str) {
        let from = st.source;
        st.source = to;
        st.epoch += 1;
        self.switches.fetch_add(1, Ordering::Relaxed);
        info!(shard = self.shard, from, to, reason, "source switched");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lost_source_hands_over_to_the_best_connected_member() {
        let s = ShardSource::new(0, 3);
        for m in 0..3 {
            s.connected(m).await;
        }
        s.set_baselines(1, 10);
        s.set_baselines(2, 50);
        s.lost(0).await;
        let st = s.lock().await;
        assert_eq!((st.source, st.epoch), (2, 1));
    }

    #[tokio::test]
    async fn nobody_to_hand_over_to_keeps_the_source() {
        let s = ShardSource::new(0, 2);
        s.connected(0).await;
        s.lost(0).await;
        let mut st = s.lock().await;
        assert_eq!((st.source, st.epoch), (0, 0));
        // A member that comes up later takes over at its next message.
        assert!(!s.take_over_if_source_down(&mut st, 1), "member 1 is not connected yet");
        st.connected[1] = true;
        assert!(s.take_over_if_source_down(&mut st, 1));
        assert_eq!((st.source, st.epoch), (1, 1));
    }

    #[tokio::test]
    async fn a_connected_source_is_not_taken_over() {
        let s = ShardSource::new(0, 2);
        s.connected(0).await;
        s.connected(1).await;
        let mut st = s.lock().await;
        assert!(!s.take_over_if_source_down(&mut st, 1));
        assert_eq!(st.epoch, 0);
    }
}
