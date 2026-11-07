#!/usr/bin/env python3
"""Compare websocket latency from the `latency` probe's CSV.

A change is identified by (symbol, hash). Per socket only the first sighting
of a change counts. Not every socket sees every change, so every statistic is
computed over the changes that socket (or subset) actually saw.

usage: latency_report.py <csv> [--offset-ms X] [--k 1,2,4,8,16]
                         [--window-mins 5] [--group-by port|ip|age] [--sockets N]
"""
import argparse
import csv
import itertools
import random
import statistics
import sys
from array import array
from bisect import bisect_left
from collections import defaultdict, namedtuple
from math import comb
from operator import eq, sub

Row = namedtuple("Row", "socket session port ip connected_us symbol hash recv_us exchange_us")

MIN_US = 60_000_000
AGE_BUCKETS = ["<5m", "5-30m", "30-60m", ">60m"]
UNSEEN = 1 << 62  # recv_us of a change a socket never saw; anything >= HALF derived from it is "not seen"
HALF = UNSEEN >> 1


class Changes:
    """Every change (symbol, hash) as a dense index c, with per-socket columns holding that socket's
    first sighting (smallest recv_us): recv_us, exchange_us and an id into the table of
    (session, port, ip, connected_us). A few bytes per cell instead of a Python object per row."""

    def __init__(self):
        self.index = {}
        self.symbols = {}
        self.n = 0
        self.cols = {}  # socket -> (recv array, exchange array, conn-id array)
        self.conns = []
        self.conn_ids = {}
        self.ex1 = array("q")  # exchange_us of the first row of each change
        self.uniform = True  # every first sighting of a change carries that change's ex1
        self.earliest = array("q")
        self._absent = {}

    def __len__(self):
        return self.n

    def add(self, socket, session, port, ip, connected_us, symbol, hash_, recv_us, exchange_us):
        key = (self.symbols.setdefault(symbol, symbol), hash_)
        c = self.index.get(key)
        if c is None:
            c = self.index[key] = self.n
            self.n += 1
            self.ex1.append(exchange_us)
            for recv, exch, conn in self.cols.values():
                recv.append(UNSEEN)
                exch.append(0)
                conn.append(0)
        col = self.cols.get(socket)
        if col is None:
            col = self.cols[socket] = (array("q", [UNSEEN]) * self.n, array("q", [0]) * self.n,
                                       array("I", [0]) * self.n)
        recv, exch, conn = col
        if recv_us < recv[c]:
            recv[c] = recv_us
            exch[c] = exchange_us
            if exchange_us != self.ex1[c]:
                self.uniform = False
            ckey = (session, port, ip, connected_us)
            cid = self.conn_ids.get(ckey)
            if cid is None:
                cid = self.conn_ids[ckey] = len(self.conns)
                self.conns.append(ckey)
            conn[c] = cid

    def finish(self):
        """Call once after the last add."""
        recvs = [c[0] for c in self.cols.values()]
        if len(recvs) == 1:
            self.earliest = array("q", recvs[0])
        elif recvs:
            self.earliest = array("q", map(min, *recvs))

    def sockets(self):
        return sorted(self.cols)

    def recv_col(self, socket):
        if socket in self.cols:
            return self.cols[socket][0]
        return self._absent.setdefault((socket, "recv"), array("q", [UNSEEN]) * self.n)

    def exch_col(self, socket):
        if socket in self.cols:
            return self.cols[socket][1]
        return self._absent.setdefault((socket, "exch"), array("q", [0]) * self.n)

    def seen_count(self, socket):
        col = self.recv_col(socket)
        return len(col) - col.count(UNSEEN)

    def __getitem__(self, key):
        """{socket: Row} of the first sightings of one change (slow; for tests and spot checks)."""
        c = self.index[key]
        return {s: Row(s, *self.conns[conn[c]], key[0], key[1], recv[c], exch[c])
                for s, (recv, exch, conn) in sorted(self.cols.items()) if recv[c] < UNSEEN}

    def subset_values(self, subset, absolute_metric, offset_us):
        """Sorted per-change values for a socket subset, with the unseen ones (>= HALF) at the end,
        and how many changes the subset saw. Relative: min recv over the subset minus the earliest
        over all sockets. Absolute: that min row's recv - exchange - offset."""
        cols = [self.recv_col(s) for s in subset]
        best = cols[0] if len(cols) == 1 else map(min, *cols)
        if not absolute_metric:
            vals = sorted(map(sub, best, self.earliest))
        elif self.uniform:
            vals = list(map(sub, best, self.ex1))
            if offset_us:
                vals = [v - offset_us for v in vals]
            vals.sort()
        else:
            exch = [self.exch_col(s) for s in subset]
            vals = []
            for c in range(self.n):
                m, x = UNSEEN, 0
                for r, e in zip(cols, exch):
                    if r[c] < m:
                        m, x = r[c], e[c]
                vals.append(m - x - offset_us)
            vals.sort()
        return vals, bisect_left(vals, HALF)


def load_changes(path):
    """Stream the CSV into a Changes without keeping any row."""
    ch = Changes()
    with open(path, newline="") as f:
        rd = csv.reader(f)
        header = next(rd, None)
        if header is None:
            return ch
        i = [header.index(c) for c in ("socket", "session", "local_port", "remote_ip", "connected_us",
                                       "symbol", "hash", "recv_us", "exchange_us")]
        add = ch.add
        for r in rd:
            add(int(r[i[0]]), int(r[i[1]]), int(r[i[2]]), r[i[3]], int(r[i[4]]), r[i[5]], r[i[6]],
                int(r[i[7]]), int(r[i[8]]))
    ch.finish()
    return ch


def dedupe(rows):
    """Changes from an iterable of Row, keeping each socket's smallest recv_us per change."""
    ch = Changes()
    for r in rows:
        ch.add(*r)
    ch.finish()
    return ch


def earliest(seen):
    return min(r.recv_us for r in seen.values())


def percentile(values, p):
    """Linear interpolation between closest ranks."""
    return percentile_sorted(sorted(values), p)


def percentile_sorted(v, p, n=None):
    """percentile() of the first n (default all) items of the already sorted v."""
    n = len(v) if n is None else n
    if not n:
        return float("nan")
    pos = (n - 1) * p / 100
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def sockets_of(changes, declared=0):
    """Every socket with a row, plus sockets 0..declared-1 (a socket that saw
    nothing writes no row)."""
    return sorted(set(changes.cols) | set(range(declared)))


def socket_deltas(changes, socket):
    """Sorted recv - earliest (microseconds) over the changes this socket saw."""
    v = sorted(map(sub, changes.recv_col(socket), changes.earliest))
    del v[bisect_left(v, HALF):]
    return v


def missed_share(changes, socket):
    return 1 - changes.seen_count(socket) / len(changes)


def rel_to_earliest(seen, subset, offset_us):
    """min recv over subset minus the earliest over all sockets; None if the subset never saw it.
    Per-change definition; best_of_k computes it column-wise (Changes.subset_values)."""
    got = [seen[s].recv_us for s in subset if s in seen]
    return min(got) - earliest(seen) if got else None


def absolute(seen, subset, offset_us):
    """min recv over subset minus that row's exchange time and the clock offset."""
    got = [seen[s] for s in subset if s in seen]
    if not got:
        return None
    best = min(got, key=lambda r: r.recv_us)
    return best.recv_us - best.exchange_us - offset_us


def best_of_k(changes, sockets, k, metric, offset_us=0, n=200, seed=1):
    """Median over random k-subsets (all if fewer than n) of the p50 of metric (rel_to_earliest or
    absolute). Returns (value, subsets used, median share of changes the subset saw);
    changes no socket of a subset saw are left out of its p50."""
    k = min(k, len(sockets))
    combos = list(itertools.combinations(sockets, k)) if comb(len(sockets), k) <= n else None
    if combos is None:
        rng = random.Random(seed)
        combos = [tuple(rng.sample(sockets, k)) for _ in range(n)]
    p50s = []
    covered = []
    for subset in combos:
        vals, seen = changes.subset_values(subset, metric is absolute, offset_us)
        covered.append(seen / len(changes))
        if seen:
            p50s.append(percentile_sorted(vals, 50, seen))
    return (statistics.median(p50s) if p50s else float("nan")), len(combos), statistics.median(covered)


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
    """Fastest socket (lowest p50 of recv - earliest) per non-empty window, and the % of
    windows whose fastest equals the previous window's. Only adjacent windows are
    compared: a window after an empty one has no previous. A tie in p50 goes to the
    lowest socket id."""
    win = int(window_mins * MIN_US)
    per = defaultdict(lambda: defaultdict(lambda: array("q")))
    for s in changes.sockets():
        for v, e in zip(changes.recv_col(s), changes.earliest):
            if v < UNSEEN:
                per[e // win][s].append(v - e)
    windows = sorted(per)
    fastest = [min(per[w], key=lambda s: (percentile(per[w][s], 50), s)) for w in windows]
    pairs = [(a, b) for (wa, a), (wb, b) in zip(zip(windows, fastest), zip(windows[1:], fastest[1:]))
             if wb == wa + 1]
    if not pairs:
        return fastest, float("nan")
    return fastest, 100 * sum(a == b for a, b in pairs) / len(pairs)


def group_deltas(changes, group_by):
    out = defaultdict(lambda: array("q"))
    for s in changes.sockets():
        recv, _, conn = changes.cols[s]
        for v, e, k in zip(recv, changes.earliest, conn):
            if v < UNSEEN:
                _, port, ip, connected_us = changes.conns[k]
                key = age_bucket(v - connected_us) if group_by == "age" else port if group_by == "port" else ip
                out[key].append(v - e)
    return out


def ms(us):
    return f"{us / 1000:.2f}"


def report(changes, args, out=sys.stdout):
    p = lambda *a: print(*a, file=out)
    socks = sockets_of(changes, args.sockets)
    offset_us = args.offset_ms * 1000
    total = len(changes)

    p(f"changes seen by any socket: {total}")
    p("\n1. per socket: recv - earliest (ms)")
    p("socket     seen  missed%   p50    p90    p99  first%")
    for s in socks:
        d = socket_deltas(changes, s)
        first = sum(map(eq, changes.recv_col(s), changes.earliest))
        p(f"{s:>6} {len(d):>8} {100 * missed_share(changes, s):>7.1f} {ms(percentile_sorted(d, 50)):>6} "
          f"{ms(percentile_sorted(d, 90)):>6} {ms(percentile_sorted(d, 99)):>6} "
          f"{100 * first / len(d) if d else 0:>7.1f}")

    ks = [int(x) for x in args.k.split(",")]
    p("\n2. best of k (median over random k-subsets of p50, ms)")
    p(f"     k  subsets  covered%  vs_earliest  vs_exchange(offset {args.offset_ms:.1f} ms)")
    for k in ks:
        if k > len(socks):
            continue
        rel, n, covered = best_of_k(changes, socks, k, rel_to_earliest)
        ab, _, _ = best_of_k(changes, socks, k, absolute, offset_us)
        p(f"{k:>6} {n:>8} {100 * covered:>9.1f} {ms(rel):>12} {ms(ab):>12}")

    fastest, same = stability(changes, args.window_mins)
    p(f"\n3. stability, {args.window_mins:g}-minute windows: {len(fastest)} windows")
    p("fastest by p50: " + " ".join(map(str, fastest)))
    p(f"same fastest as previous (adjacent) window: {same:.1f}%")

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
        v = list(map(sub, changes.recv_col(s), changes.exch_col(s)))
        if offset_us:
            v = [x - offset_us for x in v]
        v.sort()
        n = bisect_left(v, HALF)
        p(f"{s:>6} {ms(percentile_sorted(v, 50, n)):>8} {ms(percentile_sorted(v, 90, n)):>8} "
          f"{ms(percentile_sorted(v, 99, n)):>8}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv")
    ap.add_argument("--offset-ms", type=float, default=0.0, help="local - server clock offset (ntp_offset.py)")
    ap.add_argument("--k", default="1,2,4,8,16")
    ap.add_argument("--window-mins", type=float, default=5)
    ap.add_argument("--group-by", choices=["port", "ip", "age"])
    ap.add_argument("--sockets", type=int, default=0,
                    help="the probe's socket count: sockets 0..N-1 are listed even with no rows")
    args = ap.parse_args()
    changes = load_changes(args.csv)
    if not len(changes):
        sys.exit("no rows")
    report(changes, args)


if __name__ == "__main__":
    main()
