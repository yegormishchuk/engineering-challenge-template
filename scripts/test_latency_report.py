import importlib.util
import os
import unittest

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
        d = lr.deltas_by_socket(changes)
        self.assertEqual(sorted(d[0]), [0, 0, 10 * MS])
        self.assertEqual(sorted(d[1]), [0, 0, 5 * MS])

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


if __name__ == "__main__":
    unittest.main()
