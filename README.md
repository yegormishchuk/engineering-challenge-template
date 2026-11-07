# Polymarket order book collector

A Rust service that subscribes to every tradable Polymarket market, keeps a
live order book per outcome token, and records every price-level change plus
a full snapshot of every book once a minute, in the SBE format of the
[Voluntas engineering challenge](https://voluntas.io/engineering-challenge).

The challenge targets Kalshi and allows Polymarket for candidates without a
Kalshi account; this solution uses Polymarket (public market channel, no key).
The SBE schema, generated DTOs, `AlignedSbeWriter`/`AlignedSbeReader` and
`Side` come unchanged from the
[challenge template](https://github.com/voluntas-io/engineering-challenge-template).

## 1. Usage

**Requirements:** Linux or WSL2 (the template reader uses `std::os::unix`),
Rust ≥ 1.85, `build-essential` (jemalloc is built from source). No account or
API key.

```bash
cargo build --release
cargo test            # ~150 tests, mock websocket/HTTP servers, no network

# Small 5-minute test: 200 markets, first discovery page only
timeout -s INT 300 target/release/engineering-challenge \
    --max-markets 200 --max-pages 1 --output small.sbe
target/release/validate small.sbe

# Everything, to stdout (use `>`, not `>>`) or to a file
target/release/engineering-challenge > polymarket.sbe 2> collector.log
target/release/engineering-challenge --output polymarket.sbe 2> collector.log
```

Stop it with Ctrl+C (SIGINT) or SIGTERM. The log ends with
`output flushed and closed` after a clean shutdown.

Useful flags: `--output <path>` (default stdout), `--redundancy <1-4>`
(default 2), `--recycle-hours <h>` (default 6, 0 = off),
`--max-per-connection <n>` (default 5000), `--max-markets <n>` and
`--max-pages <n>` (for testing). `--help` lists the rest.

### The result

The output is one SBE file in the template's format (64 MiB chunks, read with
`AlignedSbeReader::<OrderbookSchema>`), with two message types:

- `Snapshot`: written when the exchange sends a book, and for every live book
  at each wall-clock minute (`hh:mm:00` UTC).
- `PriceLevelUpdate`: one per level change. Quantities are absolute (`0`
  removes the level).

Symbols are Polymarket token ids. Prices and quantities are `i64` fixed point
(`1_000_000` = 1 USDC / 1 share). Receive timestamps are taken when the frame
is read from the socket, before parsing or queueing. Logs and counters go to
stderr only.

`validate <file>` replays every update onto the books and compares the result
with every later snapshot. It prints `CONSISTENT` (exit 0) when there are no
updates without a baseline, no invalid values, no receive-time reversals and
the file is whole chunks.

### Numbers (measured November 2026, WSL2 test host)

| | |
| --- | --- |
| Tradable markets / tokens | ~257k / ~514k |
| Sockets at full scale | ~100 (5000 tokens each) |
| Full 30-min run (`--redundancy 1`) | 486k symbols, `CONSISTENT`, 3.1 mismatched books per million levels, peak RSS 1.16 GiB, 18.4 GB/h output |
| 50k tokens, `--redundancy 2`, 30 min | 1 disconnect, 0.71 mismatched per million levels, 0.72 GiB |
| 50k tokens, 90 min with recycling every 15 min | 110 recycles, 74 silent-token resyncs, 0.38 mismatched per million levels |
| Subscribe → first book | p50 0.27 s, p99 0.74 s |
| Inbound traffic | ~5 MB/s observed, capped by the test host's link |

Every validated run was `CONSISTENT` with 0 receive-time reversals and 0
updates without a baseline. "Mismatched" counts exchange books that differed
from the locally built book, i.e. data the stream lost.

## 2. Structure

```
src/
  main.rs              CLI, wiring, signal handling
  discovery.rs         market discovery (Gamma API + websocket events)
  shard.rs             spreads tokens over connections
  ws/connection.rs     one websocket: subscribe, heartbeat, resync, monitoring
  ws/reader.rs         per-socket reader task that timestamps frames
  ws/source.rs         redundancy: which connection of a shard writes
  exchange/mod.rs      Exchange / Codec traits and normalized Events
  exchange/polymarket.rs  the only Polymarket-specific encode/decode
  book.rs, books.rs    order books and event → output message rules
  clock.rs             minute snapshot timer
  writer.rs            single writer thread owning AlignedSbeWriter
  validate.rs          offline file checker
  fixed.rs             decimal strings → fixed point
  util/, side.rs       from the template
  bin/validate.rs      file checker binary
  bin/latency.rs, bin/capture.rs  research probes (E4, format checks)
scripts/
  run.sh               measured run: memory sampling, SIGINT, validate
  summarize.py         summarizes a collector log
  latency_runs.sh, latency_report.py, ntp_offset.py   E4 experiments
LATENCY.md             latency findings (Extension 4)
```

## 3. Challenge requirements

**Connect to the exchange.** Polymarket's public CLOB market channel, since
there is no Kalshi account. No API key is needed. Kalshi is **not
implemented**.

**Subscribe to all live markets, no MVEs.** Discovery lists every market with
`enableOrderBook && acceptingOrders && !closed` from the Gamma API and
excludes `cyom` markets (Polymarket's combo markets, the closest equivalent to
MVEs).

**One order book per symbol.** Each token has a book owned by exactly one
connection, so books need no locks. Polymarket sends no book for a token
without orders; such tokens get an empty baseline, which is then checked over
REST and resynced if wrong.

**Discover new symbols, drop finished ones.** Three layers: websocket
`new_market` / `market_resolved` events (immediate), a Gamma poll every 30 s
for recently created/closed/updated markets, and a full pass every 30 min that
is the authoritative set.

**Spread over multiple websockets.** At most 5000 tokens per socket, least
loaded first, subscribing 100 tokens every 250 ms (bigger batches made the
server drop the socket as a slow consumer).

**Deltas to stdout and a snapshot every minute, with the template helpers.**
All output goes through `AlignedSbeWriter` on one writer thread with a
bounded queue. A full queue pauses the producer, it never drops messages.

**Clean SIGINT.** Discovery stops, every socket sends a Close frame, the
writer drains its queue, flushes and closes the file, exit code 0. A second
signal exits at once.

**Completeness and time accuracy.** Every exchange book is compared with the
local book; after any gap (reconnect, decode error) the token is resynced and
no update is written before a fresh baseline. Polymarket has no sequence
numbers, so numbered `PING`/`PONG` pairs act as barriers between old and new
subscriptions. Timestamps come from a dedicated reader task per socket, so a
slow writer cannot delay them, and never go backwards per token.

**Not fully met:** at full scale the test host's link (~5 MB/s) was too slow
for the whole market channel, so the full run had 395 disconnects in 30 min.
Each one is a gap that is handled correctly (the file stays consistent), but
data during the gap is missing. Estimated requirement: ~10 MB/s inbound at
`--redundancy 1`, double that at 2. Also, a crash or `kill -9` loses up to the
last 64 MiB chunk, because the template writer only writes whole chunks.

### Extension 1: Redundant websockets

`--redundancy R` (default 2) subscribes R sockets to the same tokens; one of
them, the source, writes. The source changes when its socket drops (silent
disconnects are caught by a 30 s `PONG` timeout), when it misses a change a
peer received (Polymarket attaches the same `hash` to a change on every
socket), or when it lags a peer by more than 250 ms median. The new source
first writes a snapshot of all its books. Tested with forced iptables drops:
the source switched on each drop.

**Not covered:** process crashes. Redundancy is inside one process; the
answer is two instances on separate hosts, each writing its own file.

### Extension 2: Subscription monitoring

Each connection tracks per token which of the last 64 minutes had a change.
A token that changed in at least 45 of 50 minutes and then went silent for 10
is resubscribed. Polymarket updates come in bursts, so simple rate thresholds
caused far too many resyncs. Every socket is also fully reconnected once per
`--recycle-hours`, one at a time; with redundancy the source hands over to a
ready peer first, so a recycle causes no gap.

### Extension 3: Generic design

Connections, books, shards, redundancy, monitoring and the writer only see the
`Exchange` / `Codec` traits and normalized `Event`s (`src/exchange/mod.rs`).
Only `src/exchange/polymarket.rs` and discovery know Polymarket. Sequence
numbers and signed deltas are supported by the core.

**Partly done:** there is no second real exchange (no Kalshi account). The
abstraction is proven by a mock exchange in the tests with a different
protocol, sequence numbers and delta updates. Discovery stays per exchange.

### Extension 4: Latency

`src/bin/latency.rs` opens many sockets on the same tokens and records when
each one receives each change. Findings are in [LATENCY.md](LATENCY.md). In
short: taking the earliest of several sockets cuts tail latency; source port
and server IP choice showed no stable gain; host bandwidth was the main limit.
