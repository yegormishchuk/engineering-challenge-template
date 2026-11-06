//! The single writer thread that owns the template's [`AlignedSbeWriter`].
//!
//! Producers (connection tasks) send [`OutMsg`]s through a bounded tokio
//! channel: [`OutSender::send`] awaits when the channel is full, so
//! backpressure suspends only the sending task and never blocks a runtime
//! worker thread (other connections keep reading their sockets). The writer
//! thread drains the channel in order and flushes once every sender is gone,
//! which is how shutdown reaches it: drop all [`OutSender`]s, then call
//! [`WriterHandle::join`].
//!
//! A message that cannot be encoded (for example a timestamp before 1970) is
//! skipped and counted; it never costs the rest of the buffered chunk. An I/O
//! error is fatal: the writer still tries to flush what it holds, then stops.
//!
//! The writer never flushes on a timer: every `flush()` pads the chunk to the
//! full 64 MiB, so periodic flushes would mostly write zeros (see the spec,
//! Section 6).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;

use anyhow::Context;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// Chunk size the template reader expects; any other value makes files
/// unreadable through `AlignedSbeReader::open`.
pub const CHUNK_SIZE: usize = 64 << 20;

/// Where the SBE stream goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// `/dev/stdout`. Redirect with `>`, not `>>`: opening `/dev/stdout`
    /// truncates and starts at offset 0 regardless of the shell's mode.
    Stdout,
    File(PathBuf),
}

impl Output {
    /// `-` or `/dev/stdout` means stdout; anything else is a file path.
    pub fn parse(arg: &str) -> Self {
        match arg {
            "-" | "/dev/stdout" => Self::Stdout,
            path => Self::File(PathBuf::from(path)),
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            Self::Stdout => Path::new("/dev/stdout"),
            Self::File(path) => path,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutMsg {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

/// Counters shared between producers and the writer thread.
#[derive(Debug, Default)]
pub struct WriterStats {
    pub snapshots: AtomicU64,
    pub updates: AtomicU64,
    /// Sends that found the channel full and had to wait.
    pub backpressure: AtomicU64,
    /// Messages skipped because they could not be encoded.
    pub encode_errors: AtomicU64,
}

/// The writer thread has stopped (it failed, or shutdown is under way).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterGone;

impl std::fmt::Display for WriterGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("writer thread has stopped")
    }
}

impl std::error::Error for WriterGone {}

/// Cloneable producer handle.
#[derive(Clone)]
pub struct OutSender {
    tx: Sender<OutMsg>,
    stats: Arc<WriterStats>,
}

impl OutSender {
    /// Queues a message, never dropping it. If the channel is full this
    /// counts and logs the backpressure, then waits for room.
    pub async fn send(&self, msg: OutMsg) -> Result<(), WriterGone> {
        match self.tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(msg)) => {
                self.note_backpressure();
                self.tx.send(msg).await.map_err(|_| WriterGone)
            }
            Err(TrySendError::Closed(_)) => Err(WriterGone),
        }
    }

    /// [`Self::send`] for plain threads. Panics if called inside an async
    /// context; use `send` there.
    pub fn send_blocking(&self, msg: OutMsg) -> Result<(), WriterGone> {
        match self.tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(msg)) => {
                self.note_backpressure();
                self.tx.blocking_send(msg).map_err(|_| WriterGone)
            }
            Err(TrySendError::Closed(_)) => Err(WriterGone),
        }
    }

    fn note_backpressure(&self) {
        let n = self.stats.backpressure.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_power_of_two() {
            tracing::warn!(count = n, "writer channel full; producer is waiting");
        }
    }

    pub fn stats(&self) -> &WriterStats {
        &self.stats
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterSummary {
    pub snapshots: u64,
    pub updates: u64,
    pub backpressure: u64,
    pub encode_errors: u64,
}

pub struct WriterHandle {
    thread: JoinHandle<anyhow::Result<()>>,
    stats: Arc<WriterStats>,
}

impl WriterHandle {
    /// Waits for the writer to drain and flush. Returns only after every
    /// [`OutSender`] has been dropped.
    pub fn join(self) -> anyhow::Result<WriterSummary> {
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
        Ok(WriterSummary {
            snapshots: self.stats.snapshots.load(Ordering::Relaxed),
            updates: self.stats.updates.load(Ordering::Relaxed),
            backpressure: self.stats.backpressure.load(Ordering::Relaxed),
            encode_errors: self.stats.encode_errors.load(Ordering::Relaxed),
        })
    }
}

/// Opens the output and starts the writer thread. The output is opened here,
/// on the caller's thread, so a bad path fails before anything connects.
pub fn spawn(output: &Output, capacity: usize) -> anyhow::Result<(OutSender, WriterHandle)> {
    spawn_with_chunk(output, capacity, CHUNK_SIZE)
}

fn spawn_with_chunk(
    output: &Output,
    capacity: usize,
    chunk: usize,
) -> anyhow::Result<(OutSender, WriterHandle)> {
    let path = output.path().to_owned();
    let writer = AlignedSbeWriter::create(&path, chunk)
        .with_context(|| format!("opening output {}", path.display()))?;
    let (tx, rx) = mpsc::channel(capacity);
    let stats = Arc::new(WriterStats::default());
    let thread_stats = Arc::clone(&stats);
    let thread = std::thread::Builder::new()
        .name("sbe-writer".into())
        .spawn(move || run(writer, rx, &thread_stats))
        .context("spawning writer thread")?;
    Ok((OutSender { tx, stats: Arc::clone(&stats) }, WriterHandle { thread, stats }))
}

fn run(mut writer: AlignedSbeWriter, mut rx: Receiver<OutMsg>, stats: &WriterStats) -> anyhow::Result<()> {
    // `blocking_recv` returns `None` once every sender is dropped and the
    // queue is empty.
    while let Some(msg) = rx.blocking_recv() {
        let (result, counter) = match &msg {
            OutMsg::Snapshot(s) => (writer.write_msg(s), &stats.snapshots),
            OutMsg::Update(u) => (writer.write_msg(u), &stats.updates),
        };
        match result {
            Ok(()) => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.downcast_ref::<std::io::Error>().is_some() => {
                // Disk or pipe failure. Keep what can still be kept.
                if let Err(flush) = writer.flush() {
                    tracing::error!(error = %flush, "flush after a write error also failed");
                }
                return Err(error.context("writing output"));
            }
            Err(error) => {
                // The message could not be encoded; the buffer is unchanged.
                let n = stats.encode_errors.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_power_of_two() {
                    tracing::error!(%error, count = n, message = ?msg, "skipping a message that cannot be encoded");
                }
            }
        }
    }
    writer.flush().context("flushing output")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::book::level_update;
    use crate::util::dtos::challenge_orderbook::{AnyMessage, SnapshotBidsEntryDomain};
    use crate::util::{AlignedSbeReader, OrderbookSchema};
    use chrono::DateTime;

    fn ts(ms: i64) -> chrono::DateTime<chrono::Utc> {
        DateTime::from_timestamp_millis(ms).unwrap()
    }

    fn snapshot(symbol: &str) -> SnapshotDomain {
        SnapshotDomain {
            received_at: ts(2),
            exchange_timestamp: ts(1),
            bids: vec![SnapshotBidsEntryDomain {
                price_level: 500_000,
                quantity: 1_000_000,
            }],
            asks: vec![],
            symbol: symbol.to_owned(),
        }
    }

    fn read_all(path: &Path) -> Vec<OutMsg> {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
        let mut out = Vec::new();
        while let Some(msg) = reader.read_msg().unwrap() {
            out.push(match msg {
                AnyMessage::Snapshot(d) => OutMsg::Snapshot(SnapshotDomain::try_from_decoder(d).unwrap()),
                AnyMessage::PriceLevelUpdate(d) => {
                    OutMsg::Update(PriceLevelUpdateDomain::try_from_decoder(d).unwrap())
                }
                _ => panic!("unknown message"),
            });
        }
        out
    }

    #[test]
    fn output_parse() {
        assert_eq!(Output::parse("-"), Output::Stdout);
        assert_eq!(Output::parse("/dev/stdout"), Output::Stdout);
        assert_eq!(Output::parse("out.sbe"), Output::File("out.sbe".into()));
        assert_eq!(Output::Stdout.path(), Path::new("/dev/stdout"));
    }

    #[test]
    fn writes_in_order_from_several_producers_and_flushes_on_last_drop() {
        let dir = tempfile::tempdir().unwrap();
        let output = Output::File(dir.path().join("out.sbe"));
        let (tx, handle) = spawn(&output, 4).unwrap();

        let producers: Vec<_> = ["A", "B"]
            .into_iter()
            .map(|symbol| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    tx.send_blocking(OutMsg::Snapshot(snapshot(symbol))).unwrap();
                    for i in 0..100 {
                        let u = level_update(symbol, Side::Buy, 500_000, i, ts(10 + i), ts(10 + i));
                        tx.send_blocking(OutMsg::Update(u)).unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        for p in producers {
            p.join().unwrap();
        }
        let summary = handle.join().unwrap();
        assert_eq!((summary.snapshots, summary.updates), (2, 200));

        let msgs = read_all(output.path());
        assert_eq!(msgs.len(), 202);
        // Per producer, messages keep their send order.
        for symbol in ["A", "B"] {
            let quantities: Vec<i64> = msgs
                .iter()
                .filter_map(|m| match m {
                    OutMsg::Update(u) if u.symbol == symbol => Some(u.quantity),
                    _ => None,
                })
                .collect();
            assert_eq!(quantities, (0..100).collect::<Vec<_>>());
        }
    }

    #[test]
    fn backpressure_blocks_instead_of_dropping() {
        let dir = tempfile::tempdir().unwrap();
        let output = Output::File(dir.path().join("out.sbe"));
        // Capacity 1 with a fast producer forces the full-channel path.
        let (tx, handle) = spawn(&output, 1).unwrap();
        for i in 0..2_000 {
            tx.send_blocking(OutMsg::Update(level_update("A", Side::Sell, 1, i, ts(i), ts(i))))
                .unwrap();
        }
        drop(tx);
        let summary = handle.join().unwrap();
        assert_eq!(summary.updates, 2_000);
        assert_eq!(read_all(output.path()).len(), 2_000);
    }

    #[test]
    fn messages_spanning_several_chunks_are_all_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let output = Output::File(dir.path().join("out.sbe"));
        // The reader only opens 64 MiB chunks, so write enough to fill more
        // than one.
        use crate::util::dtos::SbeEncode;
        let sample = level_update("SYM", Side::Buy, 1, 0, ts(0), ts(0));
        let len = SbeEncode::encoded_length_with_header(&sample).unwrap() as i64;
        let n = (CHUNK_SIZE as i64 / len) * 3 / 2;
        let (tx, handle) = spawn(&output, 1 << 16).unwrap();
        for i in 0..n {
            tx.send_blocking(OutMsg::Update(level_update("SYM", Side::Buy, 1, i, ts(i), ts(i))))
                .unwrap();
        }
        drop(tx);
        handle.join().unwrap();
        let len = std::fs::metadata(output.path()).unwrap().len();
        assert!(len >= 2 * CHUNK_SIZE as u64, "expected at least two chunks, got {len} bytes");
        assert_eq!(len % CHUNK_SIZE as u64, 0);
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(output.path()).unwrap();
        let mut count = 0;
        while reader.read_msg().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, n);
    }

    #[test]
    fn io_error_stops_the_writer_and_senders_see_writer_gone() {
        // Writes to /dev/full fail with ENOSPC; a small chunk makes the first
        // chunk write happen quickly.
        let output = Output::File("/dev/full".into());
        let (tx, handle) = spawn_with_chunk(&output, 1, 256).unwrap();
        let mut result = Ok(());
        for i in 0..10_000 {
            result = tx.send_blocking(OutMsg::Update(level_update("A", Side::Buy, 1, i, ts(i), ts(i))));
            if result.is_err() {
                break;
            }
        }
        assert_eq!(result, Err(WriterGone));
        drop(tx);
        let err = handle.join().unwrap_err();
        assert!(format!("{err:#}").contains("writing output"), "{err:#}");
    }

    #[test]
    fn unencodable_message_is_skipped_and_the_rest_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let output = Output::File(dir.path().join("out.sbe"));
        let (tx, handle) = spawn(&output, 16).unwrap();
        tx.send_blocking(OutMsg::Snapshot(snapshot("A"))).unwrap();
        // Before 1970: the schema stores unsigned microseconds.
        let bad = level_update("A", Side::Buy, 1, 1, ts(-5), ts(-5));
        tx.send_blocking(OutMsg::Update(bad)).unwrap();
        tx.send_blocking(OutMsg::Update(level_update("A", Side::Buy, 1, 2, ts(7), ts(7))))
            .unwrap();
        drop(tx);
        let summary = handle.join().unwrap();
        assert_eq!((summary.snapshots, summary.updates, summary.encode_errors), (1, 1, 1));
        assert_eq!(read_all(output.path()).len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn async_send_waits_without_blocking_the_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let output = Output::File(dir.path().join("out.sbe"));
        let (tx, handle) = spawn(&output, 1).unwrap();
        // With one worker thread, a blocking send would starve this task.
        let ticker = tokio::spawn(async {
            for _ in 0..20 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });
        for i in 0..5_000 {
            tx.send(OutMsg::Update(level_update("A", Side::Buy, 1, i, ts(i), ts(i))))
                .await
                .unwrap();
        }
        ticker.await.unwrap();
        drop(tx);
        let summary = tokio::task::spawn_blocking(move || handle.join()).await.unwrap().unwrap();
        assert_eq!(summary.updates, 5_000);
    }

    #[test]
    fn bad_output_path_fails_before_spawning() {
        let output = Output::File("/nonexistent-dir/out.sbe".into());
        assert!(spawn(&output, 1).is_err());
    }
}
