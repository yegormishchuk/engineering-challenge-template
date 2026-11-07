# Polymarket order book collector

A Rust service that subscribes to every tradable Polymarket market, keeps a
live order book per outcome token, and records every price-level change
plus a full snapshot of every book once a minute, in the SBE format of the
Voluntas engineering challenge.

The challenge is written for Kalshi and allows Polymarket for candidates
without a Kalshi account; this solution targets Polymarket, whose market
channel is public. This repository is based on the
[challenge template](https://github.com/voluntas-io/engineering-challenge-template):
the SBE schema, generated DTOs, `AlignedSbeWriter`/`AlignedSbeReader` and
`Side` (`src/util/`, `src/side.rs`, `sbe_schemas/`) come from it unchanged.

## Challenge coverage

Every run below except `diagA` ended with `validate` = `CONSISTENT`,
`receive time reversals` 0 and `updates without baseline` 0 (`diagA` wrote
to `/dev/null`, so there is no file to validate). The run artefacts (`.log`,
`.validate`, `.mem`, `.info`) are in `~/runs/<name>.*` on the test host
(WSL2); `python3 scripts/summarize.py ~/runs/<name>.log` regenerates a
summary. "Mismatched" is `mismatched_books` per million levels (exchange
books that differed from the local book, Log counters). The commands are
under [Evidence runs](#evidence-runs).

| # | Requirement | How it is met | Evidence |
| --- | --- | --- | --- |
| R1 | Connect to Kalshi, or Polymarket without a Kalshi account | Polymarket: no Kalshi account. Public market channel, no key | `full30g` |
| R2 | Subscribe to all live symbols, no MVEs | Gamma discovery of every tradable market, `cyom` excluded ([What it does](#what-it-does), [Design](#design)) | `full30g`: 486k symbols |
| R3 | Maintain an order book per symbol | One book per token, owned by one connection ([Sharding](#design)) | `full30g`, `r2-25k-30m` |
| R4 | Subscribe to new symbols, unsubscribe from ones no longer trading | Three discovery layers feeding one tracker ([Design](#design)) | `e2-90m-25k`: 3 full passes, 37 `subscriptions changed` lines |
| R5 | Spread symbols over several websockets | 5000 tokens per socket, least-loaded first ([Design](#design)) | `full30g`: ~100 sockets |
| R6 | Deltas to stdout, a full snapshot every minute, with the template helpers | `PriceLevelUpdate` per change, `Snapshot` per book each minute, through `AlignedSbeWriter` ([Output format](#output-format)) | all runs |
| R7 | On SIGINT close the websockets and the output file | Close frames, queue drained, file flushed, exit 0 ([Shutdown](#shutdown)) | every run ends with `output flushed and closed` |
| Q1 | Data complete | Exchange books checked against the local book, resync on any loss ([Checkpoints](#design), [Redundancy](#redundancy), [Validating a file](#validating-a-file)) | `r2-25k-30m`: 0.71 mismatched; `e2-90m-25k`: 0.38; `full30g`: 3.1 but 395 disconnects (see below) |
| Q2 | Data time-accurate | Receive stamp taken by a reader task when the frame is read, before parsing and queueing; per-token monotonic; exact minute snapshots ([Time stamps](#design)) | Accuracy evidence: test `receive_times_do_not_wait_for_a_full_writer` (stamps within 50 ms of arrival while the writer is held full). Before the change (`full30e`) writer backpressure stalled reads for ~1 s. Reversals 0 in all validated runs proves ordering only, not accuracy. At full scale on this host the stamps are true receive times, but arrival itself is delayed by the link (PONGs up to 47.6 s late); `full30g` `max_queue` p50 1.1 s is processing delay, not stamp error. A starved reader task (CPU) could still delay stamps |
| E1 | Redundant websockets, snapshot on source switch | `--redundancy` ([Redundancy](#redundancy)) | `e1-r1` / `e1-r2` (10k tokens, 30 min, two forced drops by iptables): R=2 switched source on each drop (`source lost its connection`), 3.70 mismatched vs 4.07 for R=1 |
| E2 | Subscription monitoring, periodic socket recycling | Quiet-token check and `--recycle-hours` ([Subscription monitoring](#subscription-monitoring)) | `e2-90m-25k` (90 min, recycle every 15 min): 110 recycles, 0 skipped, 74 silent resyncs, 16,906 active symbols, 0.38 mismatched, 11 disconnects |
| E3 | Generic encode/decode per exchange | `Exchange` / `Codec` / `BookApi` traits in `src/exchange/mod.rs`, Polymarket behind them ([Adding an exchange](#adding-an-exchange)). The second exchange is **test-only** (no Kalshi account): a mock with a different protocol, sequence numbers and signed deltas. Discovery stays per exchange (the `Poll` / `Tracker` interface) | tests in `src/ws/connection/tests.rs`: `a_second_exchange_records_books_and_deltas`, `a_sequence_gap_resyncs_the_symbol`, `a_negative_delta_resyncs_the_symbol`, `mock_exchange_makes_no_empty_guesses`, `deltas_after_a_gap_start_only_one_resync` |
| E4 | Latency experiments, findings in a markdown file | `src/bin/latency.rs` probe, `scripts/latency_report.py`; findings in [LATENCY.md](LATENCY.md). In one line: best-of-N sockets cuts tail delay; source port and server IP showed no stable gain; host bandwidth appears to be the main limit | [LATENCY.md](LATENCY.md) |

### Completeness at full scale depends on the link

The full 30-minute run (`full30g`, `--redundancy 1`, 486k symbols) was
consistent, with 3.1 mismatched books per million levels, a 1.16 GiB peak
RSS and 18.4 GB/h of output. It also had 395 disconnects (255 with no
`PONG`, 132 resets). They are consistent with the test host's link cap,
but the evidence is circumstantial:

- The link measured ~3–5 MB/s to a CDN (3.0 MB/s over 6 streams). The
  collector received ~4.8–5.1 MB/s from Polymarket, which matches a link cap.
- The market channel needs more than 5 MB/s even at half scale: `diagB`
  (240k tokens) received 5.15 MB/s and had 23 disconnects in 12 min. A
  smaller load fits: `r2-25k-30m` (50k tokens, R=2) used 2.9 MB/s and
  had 1 disconnect in 30 min.
- Polymarket does not negotiate permessage-deflate, so the volume cannot
  be reduced on the client.
- Output to `/dev/null` instead of a file (`diagA`) did not remove the
  disconnects (66 in 11.8 min, ~5.6/min) but `full30g` had ~13/min, so the
  disk/writer path may contribute.
- `full30g` did not meet the plan's T1b thresholds: 255 no-`PONG`
  disconnects and `max_queue_us` p99 2.3 s.
- The redundancy run (`r2-25k-30m`) used 50k tokens, not full scale, because
  R=2 at full scale needs twice the bandwidth.

A disconnect costs a gap, handled as any reconnect (stale books,
`gap_mismatched_books`); the file stays consistent. **Deployment
requirement (estimated, not measured):** at least ~10 MB/s of inbound
bandwidth for full scale at `--redundancy 1`, and about twice that at
`--redundancy 2`, since each member receives the full stream.

### Evidence runs

All runs except `diagA` use `scripts/run.sh <name> --duration <secs> --
<flags>` on commit `41671a7` or later; `diagA` was run as `timeout -s INT 720
~/runs/collector-41671a7 --redundancy 1 --recycle-hours 0 --output /dev/null`. `--max-markets 25000` is 50k tokens.

| Run | Flags | Duration | Key result |
| --- | --- | --- | --- |
| `full30g` | `--redundancy 1 --recycle-hours 0` | 30 min | 486k symbols, 3.1 mismatched, 395 disconnects (link), peak RSS 1.16 GiB, 18.4 GB/h |
| `diagA` | as `full30g`, with `--output /dev/null` | 11.8 min | 66 disconnects at 5.1 MB/s |
| `diagB` | as `full30g`, with `--max-markets 120000` | 12 min | 23 disconnects at 5.15 MB/s |
| `r2-25k-30m` | `--max-markets 25000 --redundancy 2 --recycle-hours 0` | 30 min | 1 disconnect, 0.71 mismatched, 2.9 MB/s, peak RSS 0.72 GiB |
| `e1-r1` | `--max-markets 5000 --redundancy 1` | 30 min | 1 disconnect, 4.07 mismatched |
| `e1-r2` | `--max-markets 5000 --redundancy 2`, plus iptables DROP of the sockets for 40 s at ~10 and ~20 min | 30 min | source switch on each drop, 3.70 mismatched |
| `e2-90m-25k` | `--max-markets 25000 --redundancy 2 --recycle-hours 0.25` | 90 min | 110 recycles (0 skipped), 3 full passes, 74 silent resyncs, 0.38 mismatched, 11 disconnects |

## What it does

- Discovers every tradable market from Polymarket's Gamma API
  (`enableOrderBook && acceptingOrders && !closed`, `cyom` markets excluded),
  about 257k markets / 514k tokens, and keeps the set current while running.
- Spreads the tokens over many websocket connections (5000 per socket by
  default, ~100 sockets at full scale).
- Writes a `Snapshot` for every book the exchange sends, a
  `PriceLevelUpdate` for every level change, and a `Snapshot` of every book
  at each wall-clock minute (`hh:mm:00` UTC), to stdout or a file.
- Checks completeness as it goes: every book the exchange sends mid-stream is
  compared with the locally built book, and any lost data is followed by a
  fresh exchange book before more updates are written for that token.
- Shuts down cleanly on SIGINT or SIGTERM: closes the websockets, flushes and
  closes the output.

## Requirements

- Linux or WSL2. The template's reader uses `std::os::unix`, so the crate
  does not build on Windows.
- Rust ≥ 1.85 (edition 2024).
- A C compiler and `make` (jemalloc is built from source):
  `sudo apt install build-essential`.
- Network access to `gamma-api.polymarket.com`, `clob.polymarket.com` and
  `ws-subscriptions-clob.polymarket.com`. No account or API key.

In WSL with the repository under `/mnt/<drive>`, put the build output on the
Linux filesystem; it is much faster:

```bash
export CARGO_TARGET_DIR=~/target/engineering-challenge
```

## Build

```bash
cargo build --release
cargo test           # ~150 tests, incl. mock websocket/HTTP servers; no network
```

Binaries: `engineering-challenge` (the collector), `validate` (file
checker), `capture` and `latency` (research probes, not needed to run;
see [LATENCY.md](LATENCY.md)).

## Run

```bash
BIN=$CARGO_TARGET_DIR/release   # or target/release

# A 5-minute small test: 200 markets (400 tokens), first discovery page only.
timeout -s INT 300 $BIN/engineering-challenge --max-markets 200 --max-pages 1 --output small.sbe
$BIN/validate small.sbe

# Everything, to stdout (redirect with `>`, never `>>`).
$BIN/engineering-challenge > polymarket.sbe 2> collector.log

# Everything, to a file.
$BIN/engineering-challenge --output polymarket.sbe 2> collector.log
```

`timeout` exits with 124 when it had to send the signal; the collector
itself shut down cleanly if the log ends with `output flushed and closed`.

Logs and counters go to stderr, never to stdout, so they cannot corrupt the
SBE stream. Set the log level with `RUST_LOG` (default `info`; `debug` adds
per-symbol detail).

**`>` not `>>`:** stdout output opens `/dev/stdout`, which truncates the
target and writes from offset 0 whatever the shell's mode, so appending to an
existing file would overwrite it. Use a new file for every run.

`scripts/run.sh <name> --duration <secs> [-- <args>]` is a wrapper for
measured runs: it copies the binary, runs it with output
`$RUN_SBE_DIR/<name>.sbe` (default `/mnt/d/runs`: under WSL, files in the
Linux home grow a virtual disk on C: that never shrinks), samples memory
every 30 s, stops it with SIGINT, and validates. Logs stay in `~/runs/`;
`scripts/summarize.py ~/runs/<name>.log` summarizes one.

### Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--output <path>` | `-` | `-` or `/dev/stdout` = stdout; otherwise a file (truncated) |
| `--max-per-connection <n>` | 5000 | Most tokens per websocket |
| `--max-markets <n>` | all | Track at most n markets (testing) |
| `--max-pages <n>` | all | Read at most n Gamma event pages (~900–1400 markets each) per full pass (testing) |
| `--full-every-mins <n>` | 30 | Minutes between full discovery passes |
| `--recent-every-secs <n>` | 30 | Seconds between incremental discovery polls |
| `--channel-capacity <n>` | 65536 | Writer queue, in messages |
| `--no-verify-empty` | off | Skip the REST checks of books assumed empty (guesses are then never verified) |
| `--verify-rate <r>` | 10 | REST checks per second, over all connections |
| `--redundancy <r>` | 2 | Connections per shard, all subscribed to the same tokens; one writes (1–4; 1 = no redundancy; each member about doubles memory) |
| `--recycle-hours <h>` | 6 | Reconnect every socket once per this many hours, one at a time (0 = never; see Subscription monitoring) |
| `--stats-every <secs>` | 30 | Seconds between counter logs |

## Output format

The file is the template's format: SBE messages (schema id 1, version 2)
written by `AlignedSbeWriter` in 64 MiB chunks, read with
`AlignedSbeReader::<OrderbookSchema>::open`. Two message types:

| Message | Fields | Written when |
| --- | --- | --- |
| `Snapshot` | `received_at`, `exchange_timestamp`, `bids[]`, `asks[]` (`price_level`, `quantity`), `symbol` | the exchange sends a `book` (on subscribe, after a resubscribe, after trades); a token is assumed empty (below); every minute for every token with a live book |
| `PriceLevelUpdate` | `backtest_timestamp`, `exchange_timestamp`, `price_level`, `quantity`, `side`, `symbol` | every entry of every `price_change` |

- **Symbol** = the Polymarket token id (a long decimal string). A binary
  market has two tokens, Yes and No, whose books mirror each other (a Yes ask
  at `p` is a No bid at `1 − p`); both are recorded as published.
- **Fixed point:** prices and quantities are `i64`, `1_000_000` = 1 USDC or
  1 share. Parsed from Polymarket's decimal strings without floating point.
- **Quantities are absolute**: an update sets the level to `quantity`;
  `0` removes it. Bids in a snapshot are best (highest) first, asks best
  (lowest) first. `side`: `Buy` = bid, `Sell` = ask.
- **Timestamps** (microseconds since the epoch):

  | Field | Value |
  | --- | --- |
  | `PriceLevelUpdate.backtest_timestamp` | local receive time of the frame, taken when the reader task reads it from the socket (before parsing or queueing) |
  | `PriceLevelUpdate.exchange_timestamp` | `price_change.timestamp` |
  | `Snapshot.received_at` | receive time (exchange book) or emit time (minute snapshot) |
  | `Snapshot.exchange_timestamp` | `book.timestamp`; for a minute snapshot, the last exchange time seen for the token |
  | assumed-empty `Snapshot` | local time for both (the exchange sent nothing) |

- **Replay rule:** a reader starts a token's book at its first `Snapshot`
  and applies updates in file order. Every later `Snapshot` replaces the
  book. No update is ever written for a token before its first snapshot.

## Shutdown

SIGINT (Ctrl+C) or SIGTERM (`docker stop`, systemd): discovery stops, every
connection sends a websocket Close, the writer drains its queue, flushes, and
closes the file; the process exits 0. A second signal exits at once without
flushing.

A crash or `kill -9` loses everything since the last full chunk, up to
64 MiB of messages: the template writer only writes whole 64 MiB chunks, and
the template reader only reads 64 MiB chunks, so flushing more often would
mean writing mostly zero padding. Run two instances on separate hosts if a
crash must not lose data (see Redundancy).

## Validating a file

```bash
validate polymarket.sbe
```

It replays every update onto the books built from snapshots and compares the
replayed book with every later snapshot. Exit code 0 = consistent, 1 = not,
2 = read error. Output:

| Line | Meaning |
| --- | --- |
| `symbols`, `snapshots`, `updates` | counts in the file |
| `snapshots compared (matched, mismatched, levels)` | later snapshots compared with the replayed book |
| `updates without baseline` | updates before any snapshot of their token: must be 0 |
| `invalid values` | negative values or prices above 1 USDC: must be 0 |
| `unsorted snapshots` | levels out of order: must be 0 |
| `receive time reversals` | a token's receive time going backwards: must be 0 |
| `file size problem` | shown only if the file is empty or not a whole number of 64 MiB chunks (killed before its first flush, or truncated): the reader silently stops at a partial chunk, so such a file is never `CONSISTENT` |
| `result` | `CONSISTENT` if the four "must be 0" lines are 0 and the size is whole chunks |

`mismatched` is not a file error by itself: when the exchange sends a book
that differs from the locally built one, the collector writes it as is (it is
the truth from then on) and counts it. So `mismatched` should equal the sum,
over connections, of `mismatched_books + gap_mismatched_books` from the
collector's last stats lines. Any extra means the file lost or reordered
messages. (A token removed and later re-added can add one.) With
`--redundancy` above 1 this holds per source only: a switch snapshot that
differs from what the old source wrote also counts as `mismatched`, so
compare with `--redundancy 1` when checking completeness this way.

## Log counters

Every `--stats-every` seconds and at shutdown, the collector logs
`tracked markets=… symbols=…` and one `connection stats` line per connection.
Counters are totals since start.

| Counter | Meaning |
| --- | --- |
| `conn`, `symbols` | connection number; tokens assigned to it now |
| `connects`, `disconnects` | sessions opened; sessions lost (reason in a `connection lost` warning). `connects − disconnects` is 1 while connected |
| `frames`, `mb` | websocket text frames and megabytes received |
| `books` | exchange `book` messages applied (each written as a `Snapshot`) |
| `levels` | level changes applied (each written as a `PriceLevelUpdate`) |
| `mismatched_books`, `mismatched_levels` | exchange books that differed from the live local book, and the differing levels: data the stream lost. Should be ~0 |
| `gap_mismatched_books` | first book after a gap (reconnect or resync) that differed from the book before the gap: the market moved while we could not see it. Not a loss |
| `skipped_no_baseline` | level changes skipped because the token had no book yet (e.g. a sibling token's change before its own subscription) |
| `decode_errors` | frames or messages that did not decode; their tokens are resynced |
| `resyncs` | resubscriptions to get a fresh book (after a decode error, invalid data, or a wrong empty guess) |
| `dropped_during_resync` | messages dropped because their token was being resynced (they may predate the unsubscribe) |
| `minute_snapshots` | minute snapshots written |
| `assumed_empty` | tokens given an empty book because Polymarket sent none (see below) |
| `empty_confirmed`, `empty_wrong` | REST checks that confirmed an empty guess / showed resting orders the stream never sent (the token is then resynced) |
| `empty_gave_up` | tokens no longer checked after 3 wrong results |
| `verify_errors` | failed REST check requests (retried with backoff) |
| `ignored` | messages not recorded (`last_trade_price`, `best_bid_ask`, …) |
| `busy_ms`, `max_frame_us` | time spent handling frames, total and longest single frame |
| `queue_ms`, `max_queue_us` | delay between a frame being read by the reader task (its receive time) and the connection starting to process it: total in ms, longest single delay in µs |
| `max_queued_frames` | largest number of frames read but not yet processed. A `frame backlog growing` warning is logged once per new power of two (1024, 2048, 4096, …) per session |
| `switch_snapshots`, `switch_without_baseline` | snapshots this connection wrote on becoming its shard's source; symbols it had no live book for then (nothing is written for them until their next book) |
| `lock_wait_ms`, `max_lock_wait_us` | time waiting for the shard's output lock, total and longest |
| `silent_resyncs` | active tokens resubscribed because they went quiet (see Subscription monitoring) |
| `active_symbols` | tokens counted as active (changed in ≥ 45 of 50 minutes) at the last silence check (gauge) |
| `recycles`, `recycles_skipped` | planned reconnects done (not counted in `disconnects`); ones skipped because the source had no peer with its books ready |

With `--redundancy` above 1, a `shard source` line per shard follows:
`source` (connection id writing now), `source_switches`, `lost_on_source` /
`lost_on_peers` (changes a member never received, by hash, see
Redundancy), `late_after_loss` and `late_after_loss_max_ms` (changes
declared lost that arrived later after all: ideally 0; observed values
are in Known limitations), `lag_p50_ms` / `lag_p99_ms` / `lag_samples` (how much
later the source receives a change than the first member that does, last
30 s; negative = the source is ahead).

Other lines: `subscriptions changed` (discovery added/removed markets;
`source` says which layer), `full discovery pass done`, `writer channel full;
producer is waiting` (logged at counts 1, 2, 4, …), and at shutdown
`output flushed and closed summary=WriterSummary { snapshots, updates,
backpressure, encode_errors }`.

## Design

**Discovery** runs in three layers feeding one tracker. Websocket
`new_market` / `market_resolved` events add and remove markets at once.
Every 30 s, Gamma `/markets` sorted by `createdAt`, `closedTime` and
`updatedAt` catches what the events miss. Every 30 minutes a full pass over
`/events/keyset` (~210 pages, ~10 min) is the authoritative set: each page is
applied as it arrives (so a fresh start subscribes within seconds), and at
the end markets the pass did not list are removed, except ones changed after
the pass began. A pass listing under half the tracked markets removes
nothing.

**Exchange layer.** Only `src/exchange/`, discovery (`src/discovery.rs`) and
`VerifyConfig::polymarket` (the REST check defaults) know Polymarket. The core
(connections, books, shards, writer) speaks to it through the `Exchange`
trait (URL, heartbeat text, capabilities, a per-socket `Codec`, optional
REST book check) and sees only normalized `Event`s. See
[Adding an exchange](#adding-an-exchange).

**Sharding.** Each token is assigned to the least-loaded connection with
room (≤ `--max-per-connection`); a new connection opens when all are full.
A connection owns its tokens' books and is the only producer of their
messages, so books need no locks. Each socket has a second, small reader task
that does nothing but read frames, stamp them and queue them (unbounded) for
the connection task, so a slow writer or a long handler cannot delay receive
times. One writer thread owns the
`AlignedSbeWriter` and drains a bounded queue; a full queue suspends only the
sending connection, never drops.

**Pacing.** Subscribing makes the server push a book per token at once; 500
tokens in one message overflowed its send buffer ("slow consumer"). Each
connection subscribes 100 tokens every 250 ms.

**Barriers.** Polymarket has no sequence numbers, but it answers the text
`PING`, and the `PONG` comes after every frame it sent before the `PING`
(assumed, not proven; see Known limitations). The
collector numbers its `PING`s and uses the matching `PONG` as a barrier:
after an unsubscribe (for a resync or removal), data for the token is dropped
until the barrier, so nothing from the old subscription leaks into the new
one; after a subscribe batch, a token whose book has not come by the barrier
is not going to get one.

**Empty books.** Polymarket sends no `book` for a token without orders (~43%
of tokens), yet its first order arrives as a `price_change` that needs a
baseline. Such a token gets an empty baseline once its subscription's barrier
is answered and 5 s have passed, or at once when a change for it arrives
with an exchange time ≥ 1.5 s after the subscription. Each guess is checked
against `POST /books` (REST); if resting orders stay missing locally for 5 s,
the token is resynced.

**Checkpoints.** Every exchange `book` is compared with the local book before
replacing it (`mismatched_books`). After a reconnect or resync the old book
is kept as *stale*: not used as a baseline or in minute snapshots, but
compared with the next book (`gap_mismatched_books`). Receive timestamps are
taken by the reader task, before parsing, and no decision depends on how fast
frames are processed.

**Memory.** The collector uses jemalloc: glibc malloc fragmented badly
across tokio's threads (about 7 KB per token against ~2 KB of live data).
RSS is not the live heap, and with redundancy 2 and recycling it grew for
a reason that was not a leak: transparent huge pages. The collector now
tells jemalloc not to use them (`thp:never`, built into the binary;
`_RJEM_MALLOC_CONF` overrides it). The measurements, run by run:

- **e2-90m-25k** (90 min, `--max-markets 25000 --redundancy 2
  --recycle-hours 0.25`, before the change): RSS went from 426 MB at 9 min
  to 904 MB at 87 min, in steps, with a 1.1 GB high-water mark during
  discovery passes. It was flat at ~915 MB for the last 10 min. That one
  plateau is all we know about the long run: nothing longer was measured.
- **t12-recycle** (15 min, `--max-markets 5000 --redundancy 2
  --recycle-hours 0.05`, temporary jemalloc gauges): over 16 recycles the
  allocated heap stayed at 144–147 MB and RSS at 262–278 MB.
- **t12-full** (15 min, `--max-markets 25000 --redundancy 2
  --recycle-hours 0.25 --full-every-mins 5`: 17 recycles, 2 full passes,
  same gauges): the allocated heap went back to ~290–297 MB after each pass
  (a pass adds ~190 MB while 412k Gamma markets are parsed). jemalloc's
  resident memory stayed at 352–380 MB between passes, while RSS rose from
  445 to 560 MB.
- **t12-thp-never** (same flags as t12-full, with
  `_RJEM_MALLOC_CONF=thp:never`): RSS after the passes was 399, 402, 429,
  417 and 418 MB at 239, 389, 689, 839 and 900 s. With huge pages
  (t12-full) it was 438, 467, 518, 523 and 544 MB at the same times.

Why: WSL2 sets `transparent_hugepage=always`, so the kernel backs a 2 MB
range with one huge page on its first touch. Touching one byte per 2 MB of
a 128 MB mapping cost 126 MB of RSS. As the heap spreads over more 2 MB
ranges after bursts (discovery passes, book bursts after reconnects), more
of them become huge pages that jemalloc counts as mostly empty.

One structure grows with load, but only over a fixed time window. The
redundancy loss check keeps one record per change for 35 s plus the
reader's PONG lag: 145–178k records in t12-full. Its hash map keeps its
peak capacity (~465k slots, ~40 MB).

**Time stamps.** The receive time of a frame is taken when the reader task
reads it from the socket, not when the connection processes it; the delay
between the two is `queue_ms` / `max_queue_us`. Each connection's output
times never go backwards: the WSL2 clock stepped back 386 ms in one run, so
a stamp is never earlier than the connection's previous one (the reader
stamps from one clock shared with the connection). A minute snapshot stamped
T reflects every frame read before T and none read after: before writing
it, the connection first processes the queued frames read before T. The
same holds for the other outputs written on a timer (empty guesses,
shard checks, shutdown, recycle). Frames read before a drop or shutdown are
still processed and written. A connection is lost when no `PONG` has been
read for 30 s after a `PING` was sent: the oldest `PING` sent after the last
`PONG` read is more than 30 s old. Not when the oldest unanswered `PING` is:
with the download at its cap (~4.8–5.1 MB/s observed; the host link, ~3–5 MB/s
measured to a CDN, is the cap) the server still answers every `PING`
exactly once (counted), but up to 47 s later, while earlier
`PONG`s keep arriving (live probe: 30 sockets, 1560 of 1560 answered); that
rule dropped 765 healthy sockets in 18 min, and each reconnect's book burst
added to the backlog.

## Redundancy

A **shard** is `--redundancy` connections (default 2) subscribed to the
same tokens, each with its own books. One of them, the **source**, writes;
the others keep their books current and drop their output. Every write goes
through a lock shared by the shard, so a switch falls between two messages,
never inside one.

The source changes when:

- **its connection is lost** (closed, error, or no `PONG` for 30 s): the
  connected member with the most live books takes over at once; if none is
  connected, the first member to connect takes over;
- **it loses a change**: Polymarket attaches a `hash` of the resulting book
  to every change, and every socket on the same token gets the same hashes
  (checked live: 61,044 of 61,044 on 6000 tokens). A hash a peer received
  but the source has not, 30 s later, while the source's book for that
  token was live, is a lost change: the source hands over and resyncs the
  token. The wait is long because sockets differ a lot in delivery time
  (the same change arrived up to 4 s apart between two sockets, one socket
  stayed 4–17 s behind in collector runs), and `PONG`s are not queued
  behind changes, so a barrier cannot tell late from lost;
- **it lags**: if over 30 s the source received changes a median of more
  than 250 ms after a peer (at least 100 samples), the faster peer takes
  over.

Apart from a lost connection, there is at most one switch a minute.

On becoming the source, a member first writes a snapshot of every token it
has a live book for, as its books were before its next frame, then its own
messages. A reader therefore always has a baseline, and the file replays
consistently across switches. Receive times after a switch are raised to
the last one written, so a token's times never go backwards when the new
source's clock reading of a change is earlier than the old one's.

**Process crashes** are not covered by redundancy inside one process. Run
two collector instances on separate hosts (or at least separate machines'
network paths), each writing its own file; if one crashes, the other's file
is complete. Choosing between them afterwards is a matter of `validate` and
the counters.

## Subscription monitoring

An exchange can stop sending a subscription without saying so, or slow down
a socket it has been serving for hours. Two mechanisms handle that, at every
`--redundancy`:

- **Quiet tokens.** Each connection records, per token, which of the last
  64 minutes had a change (a 64-bit mask). Polymarket markets change in
  bursts (many changes within milliseconds, then minutes of nothing), so
  gaps between changes say little: a first version based on them
  resynced 190k times in 30 minutes. A token is *active* if it changed in
  at least 45 of the 50 minutes before the last 10; an active token with
  no change in the last 10 minutes is resubscribed (unsubscribe, fresh
  book, compared with the old one), at most 100 per connection per 5 s
  check. The fresh book (or empty guess) counts as a change, and the 10
  empty minutes keep a market that really stopped below "active", so it is
  resubscribed once. Nothing is active in the first hour after start.
  With redundancy, the hash check (Redundancy) catches a token silent on
  one socket much sooner; this check also covers a token silent on every
  socket.
- **Recycling.** Every connection is reconnected once per
  `--recycle-hours` (default 6), one at a time, spread evenly over the
  period. The next one waits until the previous one is back with books for
  99% of its tokens (or 10 minutes have passed). A source first hands over
  to a peer that has books for 99% of the tokens; if there is none, the
  recycle is skipped until the next round. With `--redundancy 1` there is
  no peer: a recycle is a short gap, recorded like any reconnect (stale
  books, `gap_mismatched_books`).

## Adding an exchange

The service is generic over the websocket layer (Extension 3). A second
exchange supplies two things:

1. **An `Exchange` impl** (`src/exchange/mod.rs`): `ws_url`, `ping_text`
   (the application heartbeat), `capabilities`, `codec()` and optionally
   `book_api()`. The `Codec`, one per socket so it can keep state, encodes
   subscribe / unsubscribe messages and decodes each text frame into
   `Event`s: `Book` (full book), `Level` (`LevelKind::Absolute` sets a
   quantity, `LevelKind::Delta` adds a signed change), `Pong` (the
   heartbeat's answer), `MarketAdded` / `MarketResolved`, `TickSizeChanged` (logged only),
   `Ignored`. A
   message it cannot decode, or a **sequence gap**, is returned as a
   `Failure` naming its symbols; the connection then resyncs exactly those.
   Set `missing_book_means_empty` only if the exchange sends no book for a
   symbol without orders (Polymarket): it enables the empty-book guesses,
   checked over REST if `book_api()` is given and verification is on (not
   `--no-verify-empty`). Pass the impl as
   `ConnectionConfig::exchange` in `main.rs`.
2. **A discovery poller** that sends `Poll`s to the `Tracker` (as
   `discovery.rs` does with Polymarket's Gamma API). Discovery is
   exchange-specific; the core only sees market adds and removals.

**Requirement:** the server must answer each heartbeat in order, after every
frame it sent before receiving it. Every barrier relies on this (see
Design). An exchange whose heartbeat cannot do this needs another barrier.

The core then gives, unchanged: sharding over connections, pacing,
reconnects and resyncs with stale-book comparison, the output timestamps
and `AlignedSbeWriter` output (the replay rule is the same: every
`PriceLevelUpdate` carries the resulting absolute quantity, also for
deltas, and a delta below zero resyncs the symbol), minute snapshots,
redundancy, quiet-token monitoring, recycling, counters and validation.
Redundant loss and lag detection compare the optional `hash` of each event;
without hashes the source changes only when its connection drops.

The tests (`src/ws/connection/tests.rs`) plug in a mock second exchange with
a different protocol: `hb` / `hb-ack` heartbeats, `{"op":"sub",...}`
messages, per-symbol sequence numbers and signed deltas.

## Measured limits

Measured live in November 2026.

| | |
| --- | --- |
| Tradable markets / tokens | ~257k / ~514k (415k open markets) |
| Tokens per socket | 5000 fine (`--max-per-connection` default) |
| Sockets at once | 300 fine |
| Full discovery pass | ~10 min, ~210 pages |
| Subscribe → book | p50 0.27 s, p99 0.74 s |
| Receive − exchange time | p50 ~0.4 s, p99 ~0.75 s (includes clock offset) |
| Memory | ~4 KB per token at R=1; `full30g` (486k symbols, R=1): peak RSS 1.16 GiB. Each extra redundancy member about doubles it |
| Writer | ~790k snapshots/s through queue and thread; average load at 204k tokens ~14k messages/s |
| Output | ~9 GB/h at 204k tokens; 18.4 GB/h at full scale (`full30g`) |
| Inbound | ~4.8–5.1 MB/s observed on the test host, whose link is the cap; the full channel needs more (see Challenge coverage) |
| REST checks | 10 requests/s × 200 tokens each (default) |

## Known limitations

- The `hash` Polymarket attaches to books and changes cannot verify each
  change: it covers a `last_trade_price` that is not in the stream. It is
  only usable as an opaque state id.
- Assumed-empty baselines use local time for both timestamps.
- The change-path empty guess compares exchange time with local time; a host
  clock more than 1.5 s behind the exchange can make guesses early (the next
  book or the REST check corrects them).
- Full-scale completeness needs inbound bandwidth the test host lacked (see
  Challenge coverage): there, disconnects and the gaps they cause are the
  main loss of continuity, though every file validated.
- A crash loses up to one 64 MiB chunk (see Shutdown).
- `--output -` writes through `/dev/stdout`: redirect with `>`, not `>>`.
- Barriers assume the server answers each `PING` exactly once and in order.
  Once is verified (every `PING` answered exactly once, counted live, also
  with 30 sockets on a saturated link); order is assumed. The `PING`s are
  matched first-in first-out, so k lost `PONG`s make every later barrier
  pass k `PING`s late (safe, only slower); an extra one would pass them
  early. Under a saturated link a session whose first `PONG` is more than
  30 s late is dropped at once (no `PONG` read yet, so the oldest-`PING`
  rule applies), and each reconnect's book burst adds load. The `PONG` timeout does not depend on
  it. A `PONG` comes
  after every `book` sent before the `PING`, but it can overtake queued
  `price_change` data.
- `late_after_loss` was not 0: 0–4 per shard (max ~32 s late) in
  `r2-25k-30m`, and 228 in `e1-r2`, where the iptables blackhole makes TCP
  retransmit late data after the loss was declared. The 30 s loss window is
  slightly short under load.
- After a failover to a lagging member, its switch snapshot can be seconds
  older than what the old source wrote; the following changes catch up,
  stamped no earlier than the switch.
- Gamma's `/markets` rejects offsets past ~2000, so an incremental query
  reads at most 20 pages; the full pass catches the rest.
- REST `/book` hashes include the request time, so REST books cannot be
  matched to stream states by hash.

## Template background

The repository uses the
[FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for binary messages. The schema is written in a custom YAML format
([`challenge_orderbook.yaml`](sbe_schemas/engineering_challenge/challenge_orderbook.yaml)),
turned into FIX SBE XML and then into Rust code by `cargo build`. Timestamps
use a custom field so the Rust structs hold `chrono::DateTime` while SBE
stores a `uint64` of microseconds since the Unix epoch.
