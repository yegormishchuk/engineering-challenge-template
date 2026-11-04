//! The minute snapshot timer.
//!
//! Ticks fire on wall-clock minute boundaries (`hh:mm:00.000` UTC), so
//! snapshots from every connection and every run line up, which makes
//! backtests easier to reason about. The tick value is the boundary itself;
//! each connection stamps its snapshots with the time it actually builds
//! them.

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// The first minute boundary strictly after `now`.
pub fn next_minute(now: DateTime<Utc>) -> DateTime<Utc> {
    let floor = now
        .duration_trunc(TimeDelta::minutes(1))
        .expect("minute truncation is in range");
    floor + TimeDelta::minutes(1)
}

/// The boundary to wait for next: after `now`, and strictly after the last
/// boundary sent. The second condition matters because tokio's timer can wake
/// slightly before the wall clock reaches the boundary; without it the same
/// boundary would fire twice.
pub fn next_tick(now: DateTime<Utc>, last_sent: Option<DateTime<Utc>>) -> DateTime<Utc> {
    next_minute(last_sent.map_or(now, |last| now.max(last)))
}

/// Sends each minute boundary on `tx` until `shutdown` is cancelled or every
/// receiver is gone. Receivers use [`watch::Receiver::changed`]; a slow
/// receiver sees only the latest tick, which is what a snapshot needs.
pub async fn run_minute_ticks(tx: watch::Sender<DateTime<Utc>>, shutdown: CancellationToken) {
    let mut last_sent = None;
    loop {
        let target = next_tick(Utc::now(), last_sent);
        // Sleep on tokio's clock; recompute from the wall clock each round so
        // drift and clock adjustments never accumulate.
        let wait = (target - Utc::now()).to_std().unwrap_or_default();
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        if tx.send(target).is_err() {
            return;
        }
        last_sent = Some(target);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn next_minute_is_strictly_after() {
        assert_eq!(next_minute(at("2026-11-05T10:15:42.123Z")), at("2026-11-05T10:16:00Z"));
        assert_eq!(next_minute(at("2026-11-05T10:15:00Z")), at("2026-11-05T10:16:00Z"));
        assert_eq!(next_minute(at("2026-11-05T23:59:59.999Z")), at("2026-11-06T00:00:00Z"));
    }

    #[test]
    fn early_wake_does_not_repeat_a_boundary() {
        let boundary = at("2026-11-05T10:16:00Z");
        // Woke 0.4 ms before the boundary the wall clock reports.
        let early = at("2026-11-05T10:15:59.9996Z");
        assert_eq!(next_tick(early, None), boundary, "first tick");
        assert_eq!(next_tick(early, Some(boundary)), at("2026-11-05T10:17:00Z"));
        // On time or late: the following boundary.
        assert_eq!(next_tick(boundary, Some(boundary)), at("2026-11-05T10:17:00Z"));
        assert_eq!(
            next_tick(at("2026-11-05T10:18:30Z"), Some(boundary)),
            at("2026-11-05T10:19:00Z"),
            "after a long stall, skips missed boundaries instead of bursting"
        );
    }

    #[tokio::test]
    async fn stops_on_shutdown() {
        let (tx, _rx) = watch::channel(Utc::now());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_minute_ticks(tx, shutdown.clone()));
        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("ticker stops promptly")
            .unwrap();
    }

    #[tokio::test]
    async fn stops_when_receivers_are_gone() {
        let (tx, rx) = watch::channel(Utc::now());
        drop(rx);
        let shutdown = CancellationToken::new();
        // With no receivers the first send fails; it happens at the next
        // minute boundary, so only check that the task is still parked now
        // and exits on cancel.
        let task = tokio::spawn(run_minute_ticks(tx, shutdown.clone()));
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        shutdown.cancel();
        task.await.unwrap();
    }
}
