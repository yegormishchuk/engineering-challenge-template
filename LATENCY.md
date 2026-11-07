# Latency experiments (Extension 4)

The challenge asks how to get a faster feed: connecting first after
maintenance, opening many connections and keeping a fast one, or changing the
TCP source port. This file sums up what we measured against Polymarket's
market channel and what it means for the collector.

## Summary

- **Best of N sockets** is the only technique with a real effect. At the
  median it gains little (3.7 ms in a normal run, 19 ms in a degraded one),
  but it removes a single socket's tail (p99 0.5–4.7 s behind the earliest
  socket) and catches changes one socket misses (up to 46 % on one bad
  socket). The fastest socket changes over time, so the useful form is "keep
  several, take the first arrival". The collector already does this with
  `--redundancy` (Extension 1).
- **Source port, server IP, connection age:** no stable gain.
- **Bandwidth** appears to be the main limit on this host (~5 MB/s). This is
  inferred from the collector runs, not measured by the probe.

No collector code changed because of these experiments.

## Method

- **Host:** Windows 11 + WSL2, one public IP. The server is CloudFront in
  front of Polymarket.
- **Probe:** `src/bin/latency.rs` opens 16 sockets subscribed to the same 40
  busy tokens and logs when each socket receives each change. Polymarket gives
  the same `hash` to a change on every socket, so arrivals can be matched.
  The probe used ~1–2 MB/s, below the link limit, so the link itself was not
  what was being measured.
- **Metric:** *recv − earliest*, how much later a socket got a change than
  the first socket. It uses one local clock only. recv − exchange timestamp
  was not used for decisions, because the WSL clock drifted ~190 ms during
  the runs.
- **Decision rule:** a technique counts as a gain if it improves the median
  by ≥ 20 ms **and** the same setting is fastest in ≥ 80 % of 5-minute
  windows.
- **Report:** `scripts/latency_report.py`. All runs were on 2026-11-07,
  driven by `scripts/latency_runs.sh`.

## Results

| Experiment | Setup | Result | Verdict |
| --- | --- | --- | --- |
| L1: best of N | 16 sockets, 30 min | Single socket p50 3.72 ms behind the earliest (range 0.26–8.55), p99 0.53–4.68 s. Best of 2: 0.45 ms; best of 4: 0.11 ms. The fastest socket won only 4 of 7 windows | Below the median bar; large tail and completeness gain |
| L2: source port | 16 fixed ports 40000–40015, then 55000–55015, 15 min each | Low range: all ports within 7.7 ms. High range: 7 scattered sockets were 42–79 ms behind, the rest 1–12 ms. The slowness belonged to individual connections, not port numbers. The best port won 75 % of windows | No gain |
| L3: server IP | 16 sockets over the 4 CloudFront IPs, 30 min | IPs within 1.3 ms at the median (6.1–7.4 ms). p99 differences came from single slow sockets. The best IP won 57 % of windows | No gain |
| L4: connection age | 8 sockets reconnecting every 10 min vs 8 never closed by the probe, 2 h | Same median for < 5 min and 5–30 min old connections (18.6 vs 18.8 ms). This run was degraded: 30 server drops, p99 23–174 s, 3–13 % of changes missed per socket. Here best of 16 covered 100 % vs 94 % for one socket | No gain |
| L5: clock | NTP samples before/after each run | WSL clock was 109–296 ms ahead and drifted ~190 ms in 1.5 h. Corrected, exchange timestamp → us is ~150–180 ms (± ~70 ms NTP uncertainty) | Absolute latency needs a synced clock |

A 2-minute smoke run (4 sockets) had one socket that got only 54 % of the
changes and was 18.5 s behind at the median, with no reconnect. A single socket
cannot see this (it looks like a quiet market), but a second socket on the
same tokens can. This is why best of N matters for completeness as well as
latency.

## Bandwidth

These numbers come from the collector runs, not from the probe:

- The host link is ~3–5 MB/s (a CDN test reached 3.0 MB/s; the collector
  received ~4.8–5.1 MB/s).
- The full market channel needs more than that. At full scale `PONG`s
  arrived up to 47.6 s late and sockets were dropped.
- Polymarket does not negotiate permessage-deflate, so the client cannot
  reduce the volume.

When the link is full, every socket queues behind it and no choice of port,
IP or socket helps. The first lever is a host with more bandwidth close to
the CloudFront edge (e.g. a cloud VM in the same region).

## Not tried

**Connecting first after scheduled maintenance.** Polymarket announces
maintenance ad hoc (Telegram/Discord, status page); there is no regular
window, and none happened during the measurements. L4 also showed that fresh
sockets are not consistently faster.

## Reproducing

```bash
bash scripts/latency_runs.sh   # in WSL from the repo root, ~2 h 45 min
```

It builds the probe, picks the tokens, runs L1–L4 and writes `.csv`, `.err`,
`.ntp` and `.report` files to `~/runs/`. Markets change all the time, so
absolute numbers differ between reruns. The comparisons within a run are what
should repeat.
