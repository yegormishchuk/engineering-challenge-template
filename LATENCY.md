# Latency experiments (Extension 4)

The challenge asks how to get faster connections from an exchange:
"being the first to connect after their schedule maintenance, or spamming
connections until you find a fast one, or changing your source port on
TCP". This file records what we measured against Polymarket's market
channel (`wss://ws-subscriptions-clob.polymarket.com/ws/market`), how, and
what we changed in the collector because of it.

Short answer: on this host the fastest of several sockets beats a single
socket by only 3.7 ms at the median in a normal run (19 ms in a degraded
one), but by hundreds of milliseconds to seconds in the tail, and it also
protects completeness. No source port, server IP or connection age gave a
stable gain. The biggest limit is the host's bandwidth, not the choice of
connection.

## Setup

| Item | Value |
| --- | --- |
| Host | Windows 11, WSL2 (Ubuntu 24.04.2, kernel 5.15.167.4-microsoft-standard-WSL2, 16 vCPU, 7.8 GB RAM), one public IP; location not recorded in the run files (NTP round trip to time.google.com ~140–150 ms) |
| Server | CloudFront in front of Polymarket: `getent ahosts` gave 4 IPv4 addresses per lookup (`13.227.65.{9,34,63,109}` for L1/L3, `13.226.209.{109,112,124,125}` for L2/L4; DNS answers changed between runs) |
| Probe | `src/bin/latency.rs` at commit `9a45757`, copied to `~/runs/latency-9a45757`. Later commits changed only token picking (`--tokens` now limits a `--tokens-file`; Gamma tokens are deduplicated); the runs here pass `--tokens-file` without `--tokens`, so they behave the same at HEAD |
| Tokens | 40 tokens, the top tradable markets by 24 h volume from Gamma at 2026-11-07 05:03 UTC, picked by the budget probe and reused by every run (`lat-budget.csv.tokens`, same md5 in every run) |
| Clock | WSL system clock; offset measured with `scripts/ntp_offset.py --server time.google.com` (SNTP, 8 samples, median) before and after each run |
| Dates | 2026-11-07, 05:03–08:37 UTC (smoke run 2026-11-06 ~23:20 UTC) |
| Driver | `scripts/latency_runs.sh` (all runs, one after another, nothing else on the IP) |

### What is measured

Every socket subscribes to the same 40 tokens. Polymarket gives every
socket the same `hash` for the same price change, so a change is
identified by `(symbol, hash)` and its arrival can be compared across
sockets. The probe writes one CSV row per change per socket with
`recv_us` (taken when the frame arrives, before parsing) and the
exchange's `timestamp` (ms).

`scripts/latency_report.py` computes:

- **recv − earliest**: how much later a socket got a change than the
  first socket that got it. This needs no clock correction (one clock).
  It is the main metric.
- **best of k**: for random k-subsets of the 16 sockets, the p50 of
  (first arrival within the subset − earliest overall), median over
  subsets. `vs_exchange` is the same with recv − exchange timestamp.
- **stability**: the fastest socket (lowest p50) in each 5-minute window.
- **missed%**: changes some socket saw but this one did not.

### Budget

16 sockets × 40 tokens was chosen by a 2-minute budget probe
(`lat-budget`, 1.4 MB/s) so the whole probe stays at ~1–2 MB/s, well
below the link (see [Bandwidth](#bandwidth)); otherwise all sockets
queue behind one bottleneck and the comparison measures the link. The
measured eth0 receive rates (`rx_kBps` line in each `.err`): L1 1847 kB/s,
L2 2100 and 1498 kB/s, L3 920 kB/s. L4's driver was stopped before it
wrote the line; from its row rate (17,322,080 rows / 7200 s = 2406
rows/s) and the ~2.3–2.5 rows per kB of the other runs, L4 was about
1.0 MB/s (an estimate).

### Decision rule

The rule used for every decision below: a technique counts as a gain only if it
improves the median recv − earliest by **at least 20 ms** and the same
setting is fastest in **at least 80 %** of the 5-minute windows.

## L1: best of N sockets ("spamming connections")

**Question.** How much does the fastest of N sockets buy for N = 2, 4,
8, 16, and does the fastest socket stay fastest?

**Method.** 16 sockets to the same server IP, 30 minutes, same 40
tokens.

```bash
latency-9a45757 --out ~/runs/lat-best.csv --tokens-file ~/runs/lat-budget.csv.tokens --sockets 16 --seconds 1800
python3 scripts/latency_report.py ~/runs/lat-best.csv --sockets 16 --k 1,2,4,8,16
```

**Numbers** (`lat-best.report`; 496,907 changes; 8,388,758 rows):

```
1. per socket: recv - earliest (ms)
socket     seen  missed%   p50    p90    p99  first%
     0   496891     0.0   5.58 305.64 4681.68     4.8
     1   494011     0.6   6.44 224.76 1451.19     1.9
     2   496865     0.0   6.03 221.86 2768.21     2.3
     3   496613     0.1   6.35 251.34 2088.00     2.0
     4   495675     0.2   1.31 118.99 809.58    10.2
     5   496835     0.0   3.24 247.48 2538.61     9.0
     6   496703     0.0   0.34 122.61 690.12    11.6
     7   496709     0.0   2.64 109.47 713.89     5.6
     8   492165     1.0   4.21 208.50 2060.09     4.9
     9   496599     0.1   0.26  97.91 528.15    14.6
    10   496575     0.1   5.73 152.47 803.71     2.4
    11   495407     0.3   2.20 171.53 1841.34     6.8
    12   496525     0.1   8.55 281.36 1644.53     1.5
    13   495497     0.3   0.31 136.11 1204.07    14.1
    14   493331     0.7   5.03 209.80 1770.15     3.4
    15   496475     0.1   2.00 152.90 996.58     5.5

2. best of k (median over random k-subsets of p50, ms)
     k  subsets  covered%  vs_earliest  vs_exchange(offset 0.0 ms)
     1       16      99.9         3.72       336.61
     2      120     100.0         0.45       327.13
     4      200     100.0         0.11       323.40
     8      200     100.0         0.00       321.38
    16        1     100.0         0.00       319.85

3. stability, 5-minute windows: 7 windows
fastest by p50: 9 9 9 9 13 13 4
same fastest as previous (adjacent) window: 66.7%
```

Six sockets were dropped once by the server and reconnected (`.err`:
socket 13 "slow consumer: send buffer full", socket 11 "server shutting
down", sockets 1, 4, 8, 14 reset without a close).

**Conclusion.**

- **Median:** a single socket is 3.72 ms behind the earliest (median over
  sockets; range 0.26–8.55 ms). Two sockets cut that to 0.45 ms, four to
  0.11 ms. On `vs_exchange` the gain from 1 to 16 sockets is 16.8 ms
  (336.61 → 319.85). Both are below the 20 ms bar.
- **Tail:** this is where the sockets differ. Per socket, p90 is 98–306 ms
  and p99 is 0.53–4.68 s behind the earliest. With all 16 sockets the
  delay to the earliest is 0 by definition, so best-of-16 removes the
  whole tail of a single socket (p99 median over sockets ≈ 1.5 s). The
  report does not give tails for k = 2–8.
- **Stability:** socket 9 was fastest in 4 of 7 windows (57 %), then 13,
  then 4. Picking "the fast one" once and keeping it does not work: the
  fast socket changes within tens of minutes.
- **Verdict:** best-of-N does not pass the median rule, but it is the only
  technique here with a real effect: it cuts tail delay and covers
  changes a single socket misses (up to 1.0 % here, much more in the smoke
  run below). The useful form is not "find the fast socket and keep it"
  but "keep several and take each change from whichever delivers first",
  or switch away from one that falls behind.

### Completeness across sockets (smoke run)

The earlier smoke run (4 sockets × 50 tokens, 120 s, one server IP) had
one bad socket:

```bash
python3 scripts/latency_report.py ~/runs/lat-smoke.csv --sockets 4 --k 1,2,4
```

```
socket     seen  missed%   p50    p90    p99  first%
     0    18338    46.1 18451.85 46887.64 53641.46     0.0
     1    33916     0.3  31.68 584.69 1624.82    27.6
     2    33106     2.7   0.99 619.54 1557.36    40.1
     3    33960     0.2  12.31 493.30 1068.59    33.7
```

Socket 0 got only 54 % of the changes (18,338 of 34,014) and the ones it
got were 18.5 s behind at the median, 53.6 s at p99, without a reconnect
(one session per socket in the CSV). The server can thin out and delay one connection while
others on the same IP are fine. A single socket cannot notice this (it
looks like a quiet market); a second socket on the same tokens can. So
best-of-N protects completeness as well as latency.

## L2: local source port

**Question.** Does the local TCP source port matter (e.g. through flow
hashing in load balancers)?

**Method.** 16 sockets bound to fixed local ports 40000–40015, 15
minutes; then 16 sockets on 55000–55015, 15 minutes. The two runs ran one
after the other, so market activity differs (279,679 vs 185,201
changes); compare the spread within a run, not lo vs hi.

```bash
latency-9a45757 --out ~/runs/lat-port-lo.csv --tokens-file ~/runs/lat-budget.csv.tokens --sockets 16 --ports 40000-40015 --seconds 900
latency-9a45757 --out ~/runs/lat-port-hi.csv --tokens-file ~/runs/lat-budget.csv.tokens --sockets 16 --ports 55000-55015 --seconds 900
python3 scripts/latency_report.py ~/runs/lat-port-lo.csv --sockets 16 --k 1,2,4,8,16 --group-by port
python3 scripts/latency_report.py ~/runs/lat-port-hi.csv --sockets 16 --k 1,2,4,8,16 --group-by port
```

**Numbers** (`lat-port-lo.report`, `lat-port-hi.report`, section 4;
recv − earliest in ms):

| port | n | p50 | p99 | | port | n | p50 | p99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 40000 | 279677 | 7.95 | 3114.00 | | 55000 | 185201 | 45.48 | 4261.53 |
| 40001 | 279669 | 2.70 | 2394.42 | | 55001 | 185171 | 42.04 | 5609.12 |
| 40002 | 279233 | 5.01 | 2504.09 | | 55002 | 185171 | 1.23 | 1630.24 |
| 40003 | 279609 | 2.04 | 2460.45 | | 55003 | 185139 | 2.92 | 1352.55 |
| 40004 | 279549 | 2.56 | 3730.78 | | 55004 | 185117 | 2.27 | 1513.75 |
| 40005 | 279487 | 0.34 | 614.79 | | 55005 | 185113 | 41.96 | 5212.94 |
| 40006 | 279449 | 6.54 | 2334.76 | | 55006 | 185009 | 4.74 | 1399.35 |
| 40007 | 279445 | 5.79 | 4793.49 | | 55007 | 184837 | 71.92 | 8944.81 |
| 40008 | 279419 | 5.33 | 1364.23 | | 55008 | 184659 | 9.47 | 1712.17 |
| 40009 | 279401 | 6.37 | 1958.58 | | 55009 | 184041 | 11.96 | 3278.25 |
| 40010 | 279345 | 5.82 | 2664.16 | | 55010 | 184557 | 51.33 | 8259.03 |
| 40011 | 279245 | 2.55 | 2346.77 | | 55011 | 184393 | 57.27 | 10315.95 |
| 40012 | 279195 | 3.88 | 2145.81 | | 55012 | 183943 | 78.68 | 5131.70 |
| 40013 | 279185 | 0.22 | 826.53 | | 55013 | 183617 | 2.86 | 1456.48 |
| 40014 | 279185 | 0.56 | 753.72 | | 55014 | 183493 | 2.46 | 1847.98 |
| 40015 | 279143 | 4.08 | 2504.93 | | 55015 | 181213 | 3.04 | 2302.73 |

Best of k and stability:

| k | lo vs_earliest | lo vs_exchange | hi vs_earliest | hi vs_exchange |
| --- | --- | --- | --- | --- |
| 1 | 3.98 | 369.79 | 10.71 | 423.15 |
| 2 | 0.34 | 358.15 | 1.38 | 378.57 |
| 4 | 0.11 | 352.03 | 0.12 | 364.39 |
| 8 | 0.00 | 349.24 | 0.00 | 357.74 |
| 16 | 0.00 | 347.57 | 0.00 | 354.51 |

Fastest socket per 5-minute window: lo `13 13 5 13` (ports 40013, 40013,
40005, 40013); hi `2 2 2 14` (ports 55002 three times, then 55014).

**Conclusion.**

- In the low range all ports are within 0.22–7.95 ms at the median: a
  7.7 ms spread, below the bar.
- In the high range seven sockets (55000, 55001, 55005, 55007, 55010,
  55011, 55012) were 42–79 ms behind at the median and 4–10 s at p99,
  while the other nine were 1.2–12 ms. The slow ports are not a block
  (55002–55004 are fast, 55005 slow, 55006 fast), so this is not "low
  vs high" but individual connections falling behind, as in L1 and the
  smoke run. With one connection per port the data cannot tell a port
  effect from a connection effect, and nothing points to the port
  number.
- The fastest port won 3 of 4 windows in each run (75 %), below the 80 %
  bar. Choosing the best port of the run (40013 or 55002) over the median
  socket gains 3.8 ms (lo) and 9.5 ms (hi), below 20 ms. A port number
  is also not something one can carry from one run to the next: the
  connection, not the number, was fast.
- Sockets 2 (lo), 9 and 15 (hi) reconnected on the same bound port after
  a server drop; no effect of the port on drops is visible.
- **Verdict:** no gain from the source port.

## L3: server IP

**Question.** Do the 4 CloudFront addresses DNS returns differ?

**Method.** 16 sockets, round-robin over all 4 resolved IPv4 addresses
(4 sockets each), 30 minutes.

```bash
getent ahosts ws-subscriptions-clob.polymarket.com > ~/runs/lat-ip.hosts
latency-9a45757 --out ~/runs/lat-ip.csv --tokens-file ~/runs/lat-budget.csv.tokens --sockets 16 --all-ips --seconds 1800
python3 scripts/latency_report.py ~/runs/lat-ip.csv --sockets 16 --k 1,2,4,8,16 --group-by ip
```

**Numbers** (`lat-ip.report`; 230,979 changes):

```
4. by ip: recv - earliest (ms)
              ip        n     p50     p99
   13.227.65.109   923582    6.77 1647.86
    13.227.65.34   921356    7.39 6519.12
    13.227.65.63   923636    6.29 4843.91
     13.227.65.9   923444    6.11 6861.63

3. stability, 5-minute windows: 7 windows
fastest by p50: 4 4 7 4 4 11 13
```

Sockets 0, 4, 8, 12 are on .63; 1, 5, 9, 13 on .109; 2, 6, 10, 14 on
.34; 3, 7, 11, 15 on .9 (`lat-ip.err`). Per socket, the slow ones were
socket 2 (.34, p50 13.83 ms, p99 7518 ms), 8 (.63, 11.45 / 5779), 12 (.63,
5.86 / 4898) and 15 (.9, 13.64 / 7732); the others had p99 1.1–2.2 s.

**Conclusion.**

- The four IPs are within 1.3 ms of each other at the median (6.11–7.39
  ms). The p99 differences (1.6 s vs 4.8–6.9 s) come from single slow
  sockets (2, 8, 12, 15), spread over three of the four IPs, not from an
  IP as a whole; .109 just happened to get no slow socket.
- The fastest socket by window was on .63 in 4 of 7 windows (57 %), on .9
  twice and .109 once. Below the 80 % bar.
- **Verdict:** no gain from choosing a server IP. All four are the same
  CloudFront edge site as far as this host can see.

## L4: connection age (fresh vs old sockets)

**Question.** Is a fresh socket faster than an hours-old one, and is a
socket fast right after it reconnects?

**Method.** 16 sockets, 2 hours. Sockets 0–7 close and reconnect every
10 minutes (`--refresh 8:600`); sockets 8–15 are never closed by the
probe. The report groups each row by the age of its connection.

```bash
latency-9a45757 --out ~/runs/lat-age.csv --tokens-file ~/runs/lat-budget.csv.tokens --sockets 16 --refresh 8:600 --seconds 7200
python3 scripts/latency_report.py ~/runs/lat-age.csv --sockets 16 --k 1,2,4,8,16 --group-by age
```

**Numbers** (`lat-age.report`; 1,116,301 changes; 17,322,080 rows):

```
1. per socket: recv - earliest (ms)
socket     seen  missed%   p50    p90    p99  first%
     0  1052517     5.7  14.57 9010.26 27640.68     9.8
     1  1062795     4.8  24.88 13100.61 105544.94     8.8
     2  1066145     4.5  19.08 12045.85 71848.14     6.3
     3  1047517     6.2  27.43 11217.31 33824.88     4.7
     4  1067233     4.4  15.19 4490.21 117376.25     7.5
     5  1033791     7.4   7.67 1521.67 23429.41    10.1
     6  1056575     5.4  20.54 5159.83 44047.47     5.9
     7  1058203     5.2  22.84 7938.40 25506.88     5.5
     8  1078489     3.4  35.16 31916.31 148220.78     3.8
     9  1036763     7.1  66.71 13138.43 143219.07     5.2
    10  1028551     7.9  15.29 17304.58 36090.49     6.1
    11   981431    12.1   6.58 3063.80 26993.45     9.4
    12   971989    12.9  16.91 6077.81 115666.20     4.6
    13  1011497     9.4  34.87 7075.68 174063.50     4.4
    14  1057091     5.3  18.97 5307.06 90522.63     6.1
    15   979189    12.3   8.29 4938.90 100661.88     9.9

2. best of k (median over random k-subsets of p50, ms)
     k  subsets  covered%  vs_earliest  vs_exchange(offset 0.0 ms)
     1       16      94.1        19.03       548.14
     2      120      95.8         4.54       515.36
     4      200      97.5         0.59       500.07
     8      200      98.9         0.00       491.39
    16        1     100.0         0.00       486.31

3. stability, 5-minute windows: 25 windows
fastest by p50: 9 0 4 4 5 11 10 0 9 15 1 1 9 4 4 15 2 0 0 0 6 6 1 4 4
same fastest as previous (adjacent) window: 29.2%

4. by age: recv - earliest (ms)
             age        n     p50     p99
             <5m  5975494   18.57 57870.50
           5-30m  9836172   18.84 50337.86
          30-60m   700430   10.40 127592.32
            >60m    77680   22.01 579090.50
```

The "old" sockets did not stay old: the server dropped them too. Sockets
8–15 had 2–6 sessions each (22 server drops), sockets 0–7 had 12–13
sessions (12 planned refreshes, plus 8 server drops). Of the 30 server
drops, 28 were resets without a close frame and 2 were "server shutting
down". The `.err` lines have no timestamps, but their order shows the
server drops of sockets 0–7 in their sessions 7–8 (70–90 minutes in),
interleaved with drops of the old sockets: a burst hitting both groups. So only 0.4 % of rows (77,680) come from
connections older than an hour.

This run was much worse than the others: p90 1.5–32 s and p99 23–174 s
behind the earliest, 3.4–12.9 % of changes missed per socket, against
p99 ≤ 4.7 s and ≤ 1 % in L1. The traffic was lower (2406 rows/s vs 4650
in L1), so this was not our link filling up; the reset bursts point to
the server side. One caveat: `latency_report.py` for L2/L3 ran in WSL
during the first ~35 minutes of this run (one core, ≤ 1.5 GB, of 16 vCPU
and 7.8 GB).

**Conclusion.**

- **Age:** connections under 5 minutes old and 5–30 minutes old have the
  same median delay (18.57 vs 18.84 ms). 30–60 minutes is lower (10.40
  ms) and over 60 minutes higher (22.01 ms), but both rest on few rows
  (4 % and 0.4 %) from the few old connections that survived. No age
  effect at the median; no sign that a socket is fast right after it
  connects. Per socket, the refreshed group (0–7) had p50 7.67–27.43 ms,
  the never-refreshed group (8–15) 6.58–66.71 ms.
- **Stability:** a refreshed socket (0–7) was fastest in 18 of 25
  windows (72 %), the never-refreshed group in 7. The group is not a
  single setting, and even as a group it is below the 80 % bar; its
  median is no better.
- **Best of N** mattered more in this bad run: a single socket was 19.03
  ms behind at the median and saw only 94.1 % of the changes; 2 sockets
  4.54 ms / 95.8 %, 16 sockets 100 %. On `vs_exchange` the gain from 1 to
  16 sockets is 61.8 ms (548.14 → 486.31).
- **Verdict:** no gain from connection age or from reconnecting
  regularly. Refreshing did not reduce misses either: refreshed sockets
  missed 4.4–7.4 % of the changes, never-refreshed ones 3.4–12.9 %.

## L5: clock offset (how much of recv − exchange is the clock)

The planned extra run with a host sleep in the middle (`lat-clock`) was
cancelled for time. What we have are the NTP samples before and after
each run (`<name>.ntp`, `scripts/ntp_offset.py --server time.google.com`,
offset = local − server, positive = local clock ahead):

| Run | UTC | offset before (ms) | offset after (ms) | NTP round trip (ms) | best-of-16 `vs_exchange` p50 (ms) | minus mean offset (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| L1 lat-best | 05:06–05:36 | 108.6 | 166.1 | 139–147 | 319.85 | ≈ 182 |
| L2 lat-port-lo | 05:36–05:51 | 155.2 | 206.5 | 145–151 | 347.57 | ≈ 167 |
| L2 lat-port-hi | 05:51–06:06 | 203.7 | 207.0 | 147–178 | 354.51 | ≈ 149 |
| L3 lat-ip | 06:07–06:37 | 212.4 | 267.3 | 146–156 | 391.55 | ≈ 152 |
| L4 lat-age | 06:37–08:37 | 296.0 | (not taken) | 144 | 486.31 | ≤ 190 (offset only before) |

**Conclusion.**

- The WSL clock ran ahead of time.google.com and drifted by ~190 ms in
  1.5 hours (108.6 ms at 05:06 → 296.0 ms at 06:37 UTC). The raw
  recv − exchange rose over the same runs (320 → 392 ms for best-of-16)
  mostly because of this
  drift, not because the network got slower: after subtracting the mean
  offset of each run, best-of-16 recv − exchange is ~150–180 ms in each
  of L1–L3 (L4: at most ~190 ms, with only the start offset).
- The NTP round trip is ~140–150 ms, so each offset is uncertain by up to
  about ±70 ms (half the round trip, if the path is asymmetric), and two
  samples taken about a minute apart (end of L3, start of L4) differ by
  29 ms. The corrected numbers are good to that much; they are fine for "~150–180 ms
  from the exchange's timestamp to us", not for comparing runs a few ms
  apart.
- The exchange timestamp has 1 ms resolution and is Polymarket's own; we
  do not know where in their pipeline it is taken. recv − exchange
  includes Polymarket's internal delay, CloudFront and the internet path,
  and we cannot split them.
- This is why every comparison above uses recv − earliest, which needs
  only the local clock. For absolute latency, WSL's clock must be
  measured during the run (or synced, e.g. `chrony`).

## Bandwidth

These numbers were measured during the collector work, not with the
probe (see also README, "Completeness at full scale depends on the
link"), but they set the limit for every technique above.

- The host link is ~3–5 MB/s in total. A kernel.org CDN download reached
  3.0 MB/s over 6 streams and 0.4 MB/s over 1 stream; the collector,
  with ~50–100 sockets to CloudFront, received ~4.8–5.1 MB/s.
- The full market channel (~514k tokens) needs more than 5 MB/s. At full
  scale the queues grew until PONGs arrived up to 47.6 s behind the data
  and sockets were dropped for missing PONGs.
- Polymarket does not negotiate permessage-deflate: a Python `websockets`
  client offering `compression="deflate"` got no
  `Sec-WebSocket-Extensions` in the reply. The volume cannot be cut on
  the client side.
- The top 200 tokens alone are ~440 KB/s per socket, so even a modest
  best-of-N over the busiest markets multiplies a large stream.

**Conclusion.** When the link is full, every socket queues behind it and
no choice of port, IP or socket helps; latency is then set by the
bandwidth. For latency on this host, the first lever is a host with more
bandwidth close to the CloudFront edge (e.g. a cloud VM in the same
region), not a cleverer connection.

## What helped / what did not

Gains are medians of recv − earliest; "stable" is the 80 % window rule.

| Technique | p50 gain (ms) | Tail gain | Stable | Verdict |
| --- | --- | --- | --- | --- |
| Best of N sockets (L1, L4) | L1: 3.3 (k 1→2), 3.7 (k 1→16), `vs_exchange` 16.8; L4: 14.5 (k 1→2), 19.0 (k 1→16), `vs_exchange` 61.8 | Large: single-socket p99 0.5–4.7 s (L1), 23–174 s (L4) → 0 with all 16; also covers changes one socket misses (≤ 1 % in L1, 3–13 % in L4, 46 % on one smoke socket) | No single fastest socket (57 % of windows) | Below the median bar; useful for tail and completeness, as "take the first arrival" / switch away from a lagging socket |
| Source port (L2) | 3.8 (lo), 9.5 (hi), best port vs median socket | None attributable to the port; slow sockets were scattered | No (75 %, 75 %) | No gain |
| Server IP (L3) | ≤ 1.3 between IPs | None; p99 differences came from single sockets | No (57 %) | No gain |
| Connection age (L4) | none: < 5 min 18.57 vs 5–30 min 18.84 | None; the tail was set by server-side drops on old and fresh sockets alike | No (refreshed group 72 % of windows) | No gain |
| Clock correction (L5) | n/a (changes the absolute number by the offset, 109–296 ms) | n/a | Offset drifted ~190 ms in 1.5 h | Needed for absolute latency; comparisons use recv − earliest |
| More bandwidth near the edge | not measured | — | — | Expected main lever (link full at ~5 MB/s) |

## Applied to the collector

Per technique, against the rule (≥ 20 ms median gain and the same
setting fastest in ≥ 80 % of 5-minute windows):

- **Best of N:** does not pass the median bar (3.7 ms in L1, 19.0 ms in
  L4) nor the window bar (no socket fastest in more than 57 % of
  windows), but it is the technique that helps (tail, completeness). No new code: Extension 1
  already does this. `--redundancy` (default 2) keeps several
  connections on the same tokens; the source hands over when it loses
  a change a peer got, or when it lags its peers by a median above
  250 ms over 30 s (`src/ws/source.rs`, `DetectConfig`). That covers the
  smoke-run socket that fell 18 s behind and lost 46 % of the changes.
- **Source port:** fails both parts of the rule. No code.
- **Server IP:** fails both parts. No code.
- **Connection age:** fails the median part (no difference between age
  groups) and the window part (72 %). No code. The collector's periodic
  recycling (`--recycle-hours`, default 6 h, one socket at a time,
  Extension 2) exists to catch silently dead subscriptions, not for
  latency; L4 gives no reason to recycle more often.
- **Clock:** no code; documented above.

So no collector code changed for Extension 4.

## Not tried, and why

**Being first to connect after scheduled maintenance.** One web search
(2026-11-07) found Polymarket's matching-engine docs
(docs.polymarket.com/trading/matching-engine), which say planned
restarts and maintenance are announced in their Telegram and Discord
channels, ideally ~2 days ahead, and that the CLOB API answers
HTTP 425 during an engine restart; status.polymarket.com is named for
exact start times during upgrades (the changelog lists the CLOB V2
upgrade on 2026-04-28 with ~1 hour of downtime). There is no regular
published window to aim at, and none fell in our measuring time, so we
could not test it. Our probe also shows why it may not matter much here:
the server dropped and we reconnected sockets during every run (6 in L1,
1 and 2 in L2, 1 in L3; see the `.err` files) and fresh sockets were not
consistently faster (L4).

## Reproducing

```bash
bash scripts/latency_runs.sh   # in WSL, from the repo root; ~2 h 45 min of probing plus reports
```

It builds the probe from the checkout, runs the budget probe (which picks
the tokens), L1–L4 one after another, and writes every `.csv`, `.err`,
`.ntp` and `.report` into `~/runs/`. The market changes every minute, so
a rerun gives different numbers; the comparisons within a run are what
should repeat. The smoke run (4 sockets, 50 tokens, 120 s) used an
earlier probe build and is not part of the script.
