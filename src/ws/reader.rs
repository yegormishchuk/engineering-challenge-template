//! A per-socket reader task: stamps each frame the moment it is read and
//! queues it, so that a busy consumer cannot delay the receive time.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use futures_util::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::ws::connection::ConnStats;

pub type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsSink = SplitSink<Ws, Message>;

/// Receive times shared by everything that stamps for one socket.
pub struct RecvClock {
    last: Mutex<DateTime<Utc>>,
}

impl RecvClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            last: Mutex::new(DateTime::<Utc>::MIN_UTC),
        })
    }

    /// The wall-clock time for output, never earlier than the previous one
    /// taken. The host clock can step back (WSL2 resyncs with Windows: a
    /// 386 ms step was seen live), and a symbol's receive times must not go
    /// backwards.
    pub fn stamp(&self) -> DateTime<Utc> {
        self.stamp_with(|t| t)
    }

    /// Takes the stamp and runs `f` while holding the lock. Whatever `f`
    /// enqueues is in the queue before any later `stamp()` returns.
    pub fn stamp_with<R>(&self, f: impl FnOnce(DateTime<Utc>) -> R) -> R {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        *last = Utc::now().max(*last);
        f(*last)
    }
}

/// One item read from the socket. `Err` is always the last item: the error
/// text, or "stream ended".
pub struct Frame {
    pub received_at: DateTime<Utc>,
    pub at: Instant,
    pub msg: Result<Message, String>,
}

/// Reads `stream` until it ends or fails (the `Err` item is sent last), or
/// until `tx` is closed.
pub fn spawn_reader(
    mut stream: SplitStream<Ws>,
    clock: Arc<RecvClock>,
    tx: mpsc::UnboundedSender<Frame>,
    stats: Arc<ConnStats>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let msg = match stream.next().await {
                Some(Ok(m)) => Ok(m),
                Some(Err(e)) => Err(e.to_string()),
                None => Err("stream ended".to_string()),
            };
            if let Ok(Message::Text(t)) = &msg {
                stats.frames.fetch_add(1, Ordering::Relaxed);
                stats.bytes.fetch_add(t.len() as u64, Ordering::Relaxed);
            }
            let last = msg.is_err();
            let sent = clock.stamp_with(|received_at| {
                tx.send(Frame {
                    received_at,
                    at: Instant::now(),
                    msg,
                })
            });
            if last || sent.is_err() {
                return;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::SinkExt;
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn stamps_never_go_back() {
        let clock = RecvClock::new();
        let mut last = clock.stamp();
        for _ in 0..1000 {
            let t = clock.stamp();
            assert!(t >= last);
            last = t;
        }
    }

    #[test]
    fn a_stamp_waits_for_an_earlier_frame_to_be_queued() {
        let clock = RecvClock::new();
        let queue = Arc::new(Mutex::new(Vec::new()));
        let a = {
            let (clock, queue) = (Arc::clone(&clock), Arc::clone(&queue));
            std::thread::spawn(move || {
                clock.stamp_with(|t| {
                    std::thread::sleep(Duration::from_millis(50));
                    queue.lock().unwrap().push(t);
                    t
                })
            })
        };
        std::thread::sleep(Duration::from_millis(10));
        let t_b = clock.stamp();
        assert_eq!(queue.lock().unwrap().len(), 1);
        assert!(t_b >= a.join().unwrap());
    }

    #[tokio::test]
    async fn reader_forwards_frames_in_order_then_the_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.send(Message::text("a")).await.unwrap();
            ws.send(Message::text("b")).await.unwrap();
        });
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (_sink, stream) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let stats = Arc::new(ConnStats::default());
        spawn_reader(stream, RecvClock::new(), tx, Arc::clone(&stats));

        let mut frames = Vec::new();
        while let Some(f) = rx.recv().await {
            frames.push(f);
        }
        assert_eq!(frames.len(), 3);
        assert!(matches!(&frames[0].msg, Ok(Message::Text(t)) if t.as_str() == "a"));
        assert!(matches!(&frames[1].msg, Ok(Message::Text(t)) if t.as_str() == "b"));
        assert!(frames[2].msg.is_err());
        assert!(frames.windows(2).all(|w| w[0].received_at <= w[1].received_at));
        assert_eq!(stats.frames.load(Ordering::Relaxed), 2);
        assert_eq!(stats.bytes.load(Ordering::Relaxed), 2);
    }
}
