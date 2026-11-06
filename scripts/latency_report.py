#!/usr/bin/env python3
"""Compare websocket latency from the `latency` probe's CSV.

A change is identified by (symbol, hash). Per socket only the first sighting
of a change counts. Not every socket sees every change, so every statistic is
computed over the changes that socket (or subset) actually saw.

usage: latency_report.py <csv> [--offset-ms X] [--k 1,2,4,8,16]
                         [--window-mins 5] [--group-by port|ip|age]
"""
import argparse
import csv
import itertools
import random
import statistics
import sys
from collections import defaultdict, namedtuple

Row = namedtuple("Row", "socket session port ip connected_us symbol hash recv_us exchange_us")

MIN_US = 60_000_000
AGE_BUCKETS = ["<5m", "5-30m", "30-60m", ">60m"]


def load(path):
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            yield Row(int(r["socket"]), int(r["session"]), int(r["local_port"]), r["remote_ip"],
                      int(r["connected_us"]), r["symbol"], r["hash"], int(r["recv_us"]), int(r["exchange_us"]))


def dedupe(rows):
    """{(symbol, hash): {socket: Row with the smallest recv_us}}"""
    changes = defaultdict(dict)
    for r in rows:
        seen = changes[(r.symbol, r.hash)]
        if r.socket not in seen or r.recv_us < seen[r.socket].recv_us:
            seen[r.socket] = r
    return changes


def earliest(seen):
    return min(r.recv_us for r in seen.values())


def percentile(values, p):
    """Linear interpolation between closest ranks."""
    v = sorted(values)
    if not v:
        return float("nan")
    pos = (len(v) - 1) * p / 100
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def sockets_of(changes):
    return sorted({s for seen in changes.values() for s in seen})


def deltas_by_socket(changes):
    """socket -> list of recv - earliest (microseconds), over changes it saw."""
    out = defaultdict(list)
    for seen in changes.values():
        e = earliest(seen)
        for s, r in seen.items():
            out[s].append(r.recv_us - e)
    return out


def missed_share(changes, socket):
    return 1 - sum(socket in seen for seen in changes.values()) / len(changes)


def rel_to_earliest(seen, subset, offset_us):
    """min recv over subset minus the earliest over all sockets; None if the subset never saw it."""
    got = [seen[s].recv_us for s in subset if s in seen]
    return min(got) - earliest(seen) if got else None


def absolute(seen, subset, offset_us):
    """min recv over subset minus exchange time and clock offset."""
    got = [seen[s].recv_us for s in subset if s in seen]
    if not got:
        return None
    exchange = next(iter(seen.values())).exchange_us
    return min(got) - exchange - offset_us


def best_of_k(changes, sockets, k, metric, offset_us=0, n=200, seed=1):
    """Median over random k-subsets (all if fewer than n) of the p50 of metric. Returns (value, subsets used)."""
    k = min(k, len(sockets))
    combos = list(itertools.combinations(sockets, k)) if _comb(len(sockets), k) <= n else None
    if combos is None:
        rng = random.Random(seed)
        combos = [tuple(rng.sample(sockets, k)) for _ in range(n)]
    p50s = []
    for subset in combos:
        vals = [v for seen in changes.values() if (v := metric(seen, subset, offset_us)) is not None]
        if vals:
            p50s.append(percentile(vals, 50))
    return (statistics.median(p50s) if p50s else float("nan")), len(combos)


def _comb(n, k):
    from math import comb
    return comb(n, k)


def age_bucket(age_us):
    m = age_us / MIN_US
    if m < 5:
        return AGE_BUCKETS[0]
    if m < 30:
        return AGE_BUCKETS[1]
    if m < 60:
        return AGE_BUCKETS[2]
    return AGE_BUCKETS[3]


def stability(changes, window_mins):
    """Fastest socket (lowest p50 of recv - earliest) per window, and the % of windows
    whose fastest equals the previous window's."""
    win = int(window_mins * MIN_US)
    per = defaultdict(lambda: defaultdict(list))
    for seen in changes.values():
        e = earliest(seen)
        for s, r in seen.items():
            per[e // win][s].append(r.recv_us - e)
    fastest = []
    for w in sorted(per):
        fastest.append(min(per[w], key=lambda s: (percentile(per[w][s], 50), s)))
    if len(fastest) < 2:
        return fastest, float("nan")
    same = sum(a == b for a, b in zip(fastest, fastest[1:]))
    return fastest, 100 * same / (len(fastest) - 1)


def group_deltas(changes, group_by):
    out = defaultdict(list)
    for seen in changes.values():
        e = earliest(seen)
        for r in seen.values():
            key = {"port": r.port, "ip": r.ip, "age": age_bucket(r.recv_us - r.connected_us)}[group_by]
            out[key].append(r.recv_us - e)
    return out


def ms(us):
    return f"{us / 1000:.2f}"


def report(changes, args, out=sys.stdout):
    p = lambda *a: print(*a, file=out)
    socks = sockets_of(changes)
    offset_us = args.offset_ms * 1000
    total = len(changes)
    d = deltas_by_socket(changes)
    wins = defaultdict(int)
    for seen in changes.values():
        e = earliest(seen)
        for s, r in seen.items():
            if r.recv_us == e:
                wins[s] += 1

    p(f"changes seen by any socket: {total}")
    p("\n1. per socket: recv - earliest (ms)")
    p("socket     seen  missed%   p50    p90    p99  first%")
    for s in socks:
        p(f"{s:>6} {len(d[s]):>8} {100 * missed_share(changes, s):>7.1f} {ms(percentile(d[s], 50)):>6} "
          f"{ms(percentile(d[s], 90)):>6} {ms(percentile(d[s], 99)):>6} {100 * wins[s] / len(d[s]):>7.1f}")

    ks = [int(x) for x in args.k.split(",")]
    p("\n2. best of k (median over random k-subsets of p50, ms)")
    p("     k  subsets  vs_earliest  vs_exchange(offset %.1f ms)" % args.offset_ms)
    for k in ks:
        if k > len(socks):
            continue
        rel, n = best_of_k(changes, socks, k, rel_to_earliest)
        ab, _ = best_of_k(changes, socks, k, absolute, offset_us)
        p(f"{k:>6} {n:>8} {ms(rel):>12} {ms(ab):>12}")

    fastest, same = stability(changes, args.window_mins)
    p(f"\n3. stability, {args.window_mins:g}-minute windows: {len(fastest)} windows")
    p("fastest by p50: " + " ".join(map(str, fastest)))
    p(f"same fastest as previous window: {same:.1f}%")

    if args.group_by:
        g = group_deltas(changes, args.group_by)
        keys = AGE_BUCKETS if args.group_by == "age" else sorted(g)
        p(f"\n4. by {args.group_by}: recv - earliest (ms)")
        p(f"{args.group_by:>16} {'n':>8} {'p50':>7} {'p99':>7}")
        for key in keys:
            if g.get(key):
                p(f"{key!s:>16} {len(g[key]):>8} {ms(percentile(g[key], 50)):>7} {ms(percentile(g[key], 99)):>7}")

    p(f"\n5. absolute: recv - exchange - {args.offset_ms:g} ms offset (ms)")
    p("socket    p50      p90      p99")
    for s in socks:
        v = [r.recv_us - r.exchange_us - offset_us for seen in changes.values() for r in [seen.get(s)] if r]
        p(f"{s:>6} {ms(percentile(v, 50)):>8} {ms(percentile(v, 90)):>8} {ms(percentile(v, 99)):>8}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv")
    ap.add_argument("--offset-ms", type=float, default=0.0, help="local - server clock offset (ntp_offset.py)")
    ap.add_argument("--k", default="1,2,4,8,16")
    ap.add_argument("--window-mins", type=float, default=5)
    ap.add_argument("--group-by", choices=["port", "ip", "age"])
    args = ap.parse_args()
    changes = dedupe(load(args.csv))
    if not changes:
        sys.exit("no rows")
    report(changes, args)


if __name__ == "__main__":
    main()
