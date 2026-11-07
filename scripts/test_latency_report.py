import argparse
import csv
import importlib.util
import io
import os
import random
import tempfile
import unittest
from collections import defaultdict

_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "latency_report.py")
_spec = importlib.util.spec_from_file_location("latency_report", _path)
lr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lr)

MS = 1000  # microseconds per millisecond
MIN = 60_000_000


def row(socket, symbol, h, recv_ms, exchange_ms=0, session=0, port=40000, ip="1.1.1.1", connected_us=0):
    return lr.Row(socket, session, port, ip, connected_us, symbol, h, recv_ms * MS, exchange_ms * MS)


class ReportTests(unittest.TestCase):
    def test_earliest_is_per_change(self):
        rows = [
            row(0, "s", "a", 100), row(1, "s", "a", 105),
            row(0, "s", "b", 220), row(1, "s", "b", 210),
            row(0, "t", "c", 300), row(1, "t", "c", 300),
            row(0, "s", "a", 130),  # duplicate on socket 0: first sighting counts
        ]
        changes = lr.dedupe(rows)
        self.assertEqual(len(changes), 3)
        self.assertEqual(lr.socket_deltas(changes, 0), [0, 0, 10 * MS])
        self.assertEqual(lr.socket_deltas(changes, 1), [0, 0, 5 * MS])

    def test_best_of_k_ignores_changes_a_socket_never_saw(self):
        rows = [
            row(0, "s", "x", 100), row(1, "s", "x", 100),
            row(0, "s", "y", 200), row(1, "s", "y", 200),
            row(0, "s", "z", 300), row(1, "s", "z", 310),
        ]
        # socket 1 misses y.
        rows = [r for r in rows if not (r.socket == 1 and r.hash == "y")]
        changes = lr.dedupe(rows)
        self.assertAlmostEqual(lr.missed_share(changes, 1), 1 / 3)
        self.assertEqual(lr.missed_share(changes, 0), 0)
        # best-of-1 for socket 1: changes x (0) and z (10ms) only -> p50 = 5ms
        self.assertEqual(lr.best_of_k(changes, [1], 1, lr.rel_to_earliest)[0], 5 * MS)
        # socket 0 saw all: deltas 0,0,0
        self.assertEqual(lr.best_of_k(changes, [0], 1, lr.rel_to_earliest)[0], 0)
        # both sockets: min recv equals earliest everywhere
        self.assertEqual(lr.best_of_k(changes, [0, 1], 2, lr.rel_to_earliest)[0], 0)

    def test_best_of_k_reports_the_share_of_changes_covered(self):
        # socket 0 saw x and y, socket 1 only z: each single socket covers 2/3 or 1/3.
        changes = lr.dedupe([row(0, "s", "x", 100), row(0, "s", "y", 200), row(1, "s", "z", 300)])
        self.assertAlmostEqual(lr.best_of_k(changes, [0], 1, lr.rel_to_earliest)[2], 2 / 3)
        self.assertAlmostEqual(lr.best_of_k(changes, [1], 1, lr.rel_to_earliest)[2], 1 / 3)
        self.assertEqual(lr.best_of_k(changes, [0, 1], 2, lr.rel_to_earliest)[2], 1)

    def test_absolute_uses_the_exchange_time_of_the_fastest_row(self):
        seen = lr.dedupe([row(0, "s", "x", 110, exchange_ms=50), row(1, "s", "x", 100, exchange_ms=90)])[("s", "x")]
        self.assertEqual(lr.absolute(seen, [0, 1], 0), 10 * MS)
        self.assertEqual(lr.absolute(seen, [0], 2 * MS), 58 * MS)

    def test_a_socket_without_rows_is_listed_as_missing_everything(self):
        changes = lr.dedupe([row(0, "s", "x", 100), row(1, "s", "x", 105)])
        self.assertEqual(lr.sockets_of(changes), [0, 1])
        self.assertEqual(lr.sockets_of(changes, 3), [0, 1, 2])
        args = argparse.Namespace(offset_ms=0.0, k="1,2", window_mins=5, group_by=None, sockets=3)
        out = io.StringIO()
        lr.report(changes, args, out)
        line = next(l for l in out.getvalue().splitlines() if l.split()[:2] == ["2", "0"])
        self.assertEqual(line.split()[2], "100.0")

    def test_age_bucket_boundaries(self):
        self.assertEqual(lr.age_bucket(5 * MIN - 1), "<5m")
        self.assertEqual(lr.age_bucket(5 * MIN), "5-30m")
        self.assertEqual(lr.age_bucket(30 * MIN - 1), "5-30m")
        self.assertEqual(lr.age_bucket(30 * MIN), "30-60m")
        self.assertEqual(lr.age_bucket(60 * MIN - 1), "30-60m")
        self.assertEqual(lr.age_bucket(60 * MIN), ">60m")

    def test_age_buckets(self):
        self.assertEqual(lr.age_bucket(1 * MIN), "<5m")
        self.assertEqual(lr.age_bucket(20 * MIN), "5-30m")
        self.assertEqual(lr.age_bucket(120 * MIN), ">60m")
        self.assertEqual(lr.age_bucket(45 * MIN), "30-60m")

    def test_percentile(self):
        self.assertEqual(lr.percentile([1, 2, 3, 4, 5], 50), 3)
        self.assertEqual(lr.percentile([0, 10], 50), 5)
        self.assertEqual(lr.percentile([7], 99), 7)

    def test_stability(self):
        # window = 1 min; socket 0 fastest in windows 0,1; socket 1 in window 2
        rows = []
        for w, (fast, slow) in enumerate([(0, 1), (0, 1), (1, 0)]):
            t = w * 60_000
            rows.append(row(fast, "s", f"h{w}", t + 1000))
            rows.append(row(slow, "s", f"h{w}", t + 1010))
        changes = lr.dedupe(rows)
        fastest, same_pct = lr.stability(changes, 1)
        self.assertEqual(fastest, [0, 0, 1])
        self.assertEqual(same_pct, 50.0)

    def test_stability_skips_pairs_across_an_empty_window(self):
        # windows 0, 1 and 3 (2 is empty): only 0-1 is a pair, and it differs.
        rows = []
        for w, (fast, slow) in [(0, (0, 1)), (1, (1, 0)), (3, (1, 0))]:
            t = w * 60_000
            rows.append(row(fast, "s", f"h{w}", t + 1000))
            rows.append(row(slow, "s", f"h{w}", t + 1010))
        fastest, same_pct = lr.stability(lr.dedupe(rows), 1)
        self.assertEqual(fastest, [0, 1, 1])
        self.assertEqual(same_pct, 0.0)

    def test_stability_breaks_ties_by_lowest_socket(self):
        changes = lr.dedupe([row(1, "s", "x", 1000), row(0, "s", "x", 1000)])
        self.assertEqual(lr.stability(changes, 1)[0], [0])


def reference_dedupe(rows):
    """The original dict-of-dicts dedupe: {(symbol, hash): {socket: Row with the smallest recv_us}}"""
    changes = defaultdict(dict)
    for r in rows:
        seen = changes[(r.symbol, r.hash)]
        if r.socket not in seen or r.recv_us < seen[r.socket].recv_us:
            seen[r.socket] = r
    return changes


class CompactLoadTests(unittest.TestCase):
    def test_compact_load_matches_reference(self):
        rng = random.Random(7)
        rows = [lr.Row(rng.randrange(4), rng.randrange(3), 40000 + rng.randrange(3), f"10.0.0.{rng.randrange(2)}",
                       rng.randrange(1000), f"sym{rng.randrange(3)}", f"h{rng.randrange(20)}",
                       1_700_000_000_000_000 + rng.randrange(10**6), 1_699_999_999_000_000 + rng.randrange(10**3))
                for _ in range(300)]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "l.csv")
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow("socket,session,local_port,remote_ip,connected_us,symbol,hash,recv_us,exchange_us".split(","))
                w.writerows(rows)
            new = lr.load_changes(path)
        ref = reference_dedupe(rows)
        self.assertEqual(len(new), len(ref))
        for key, seen in ref.items():
            self.assertEqual(new[key], seen)
        self.assertEqual(lr.sockets_of(new), sorted({s for seen in ref.values() for s in seen}))
        for s in range(5):
            self.assertEqual(lr.socket_deltas(new, s), sorted(r.recv_us - min(x.recv_us for x in seen.values())
                                                              for seen in ref.values() for r in [seen.get(s)] if r))

    def test_absolute_when_exchange_times_differ_between_sockets(self):
        # same change, different exchange_us per socket: the fastest row's own exchange time counts.
        changes = lr.dedupe([row(0, "s", "x", 110, exchange_ms=50), row(1, "s", "x", 100, exchange_ms=90),
                             row(0, "s", "y", 200, exchange_ms=150), row(1, "s", "y", 230, exchange_ms=150)])
        self.assertFalse(changes.uniform)
        # x: 100 - 90 = 10ms; y: 200 - 150 = 50ms -> p50 30ms
        self.assertEqual(lr.best_of_k(changes, [0, 1], 2, lr.absolute)[0], 30 * MS)


if __name__ == "__main__":
    unittest.main()
