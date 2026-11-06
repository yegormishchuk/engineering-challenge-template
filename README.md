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
cargo test           # ~90 tests, incl. mock websocket/HTTP servers; no network
```

Binaries: `engineering-challenge` (the collector), `validate` (file
checker), `capture` (research probe, not needed to run).

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
`~/runs/<name>.sbe`, samples memory every 30 s, stops it with SIGINT, and
validates. `scripts/summarize.py ~/runs/<name>.log` summarizes the log.

### Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--output <path>` | `-` | `-` or `/dev/stdout` = stdout; otherwise a file (truncated) |
| `--max-per-connection <n>` | 5000 | Most tokens per websocket |
| `--max-markets <n>` | all | Track at most n markets (testing) |
| `--max-pages <n>` | all | Read at most n Gamma event pages (~1400 markets each) per full pass (testing) |
| `--full-every-mins <n>` | 30 | Minutes between full discovery passes |
| `--recent-every-secs <n>` | 30 | Seconds between incremental discovery polls |
| `--channel-capacity <n>` | 65536 | Writer queue, in messages |
| `--no-verify-empty` | off | Skip REST checks of books assumed empty |
| `--verify-rate <r>` | 10 | REST checks per second, over all connections |
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
  | `PriceLevelUpdate.backtest_timestamp` | local receive time of the frame, taken before parsing |
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
crash must not lose data.

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
| `result` | `CONSISTENT` if the four "must be 0" lines are 0 |

`mismatched` is not a file error by itself: when the exchange sends a book
that differs from the locally built one, the collector writes it as is (it is
the truth from then on) and counts it. So `mismatched` should equal the sum,
over connections, of `mismatched_books + gap_mismatched_books` from the
collector's last stats lines. Any extra means the file lost or reordered
messages. (A token removed and later re-added can add one.)

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

**Sharding.** Each token is assigned to the least-loaded connection with
room (≤ `--max-per-connection`); a new connection opens when all are full.
A connection owns its tokens' books and is the only producer of their
messages, so books need no locks. One writer thread owns the
`AlignedSbeWriter` and drains a bounded queue; a full queue suspends only the
sending connection, never drops.

**Pacing.** Subscribing makes the server push a book per token at once; 500
tokens in one message overflowed its send buffer ("slow consumer"). Each
connection subscribes 100 tokens every 250 ms.

**Barriers.** Polymarket has no sequence numbers, but it answers the text
`PING` in order, after every frame it sent before it (verified live). The
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
taken before parsing, and no decision depends on how fast frames are read.

**Memory.** The collector uses jemalloc: glibc malloc fragmented badly
across tokio's threads (about 7 KB per token against ~2 KB of live data).

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
| Memory | ~4 KB per token (≈ 2 GB projected at 514k) |
| Writer | ~790k snapshots/s through queue and thread; average load at 204k tokens ~14k messages/s |
| Output | ~9 GB/h at 204k tokens |
| REST checks | 10 requests/s × 200 tokens each (default) |

## Known limitations

- The `hash` Polymarket attaches to books and changes cannot verify each
  change: it covers a `last_trade_price` that is not in the stream. It is
  only usable as an opaque state id.
- Assumed-empty baselines use local time for both timestamps.
- The change-path empty guess compares exchange time with local time; a host
  clock more than 1.5 s behind the exchange can make guesses early (the next
  book or the REST check corrects them).
- A crash loses up to one 64 MiB chunk (see Shutdown).
- `--output -` writes through `/dev/stdout`: redirect with `>`, not `>>`.
- Barriers assume the server answers each `PING` exactly once and in order
  (verified live; a violation would shift barriers by one).
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
