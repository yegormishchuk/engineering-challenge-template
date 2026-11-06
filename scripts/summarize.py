#!/usr/bin/env python3
"""Summarizes a collector run.

    scripts/summarize.py ~/runs/<name>.log

Reads the log (ANSI codes stripped) and, if present next to it,
<name>.mem, <name>.info and <name>.validate from scripts/run.sh.
"""

import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*m")
LINE = re.compile(r"^(\S+)\s+(\w+)\s+(\S+?):\s(.*)$")
FIELD = re.compile(r"(\w+)=(\S+)")


def parse(path):
    """Yields (time, level, message, fields, raw rest) per log line."""
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            m = LINE.match(ANSI.sub("", raw.rstrip("\n")))
            if not m:
                continue
            ts, level, _target, rest = m.groups()
            # The message is the text before the first `key=` field.
            first = FIELD.search(rest)
            msg = (rest[: first.start()] if first else rest).strip()
            fields = dict(FIELD.findall(rest[first.start():])) if first else {}
            yield ts, level, msg, fields, rest


def when(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def spread(rows, n=12):
    """About n rows evenly spaced, always including the last."""
    picked = rows[:: max(1, len(rows) // n)]
    return picked if picked[-1:] == rows[-1:] else picked + rows[-1:]


def num(v):
    try:
        return int(v)
    except ValueError:
        return None


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    log = Path(sys.argv[1])
    base = log.with_suffix("")

    conn_last = {}
    tracked = []
    lost = Counter()
    connect_failed = Counter()
    warnings = Counter()
    errors = Counter()
    full_passes = []
    changes = 0
    backpressure_warn = 0
    summary = None
    first = last = None

    for ts, level, msg, fields, rest in parse(log):
        t = when(ts)
        first = first or t
        last = t
        if msg == "connection stats":
            conn_last[fields["conn"]] = fields
        elif msg == "tracked":
            tracked.append((t, int(fields["markets"]), int(fields["symbols"])))
        elif msg == "subscriptions changed":
            changes += 1
        elif msg == "full discovery pass done":
            full_passes.append((t, fields.get("markets"), fields.get("secs")))
        elif msg == "output flushed and closed":
            summary = rest.split("summary=", 1)[-1]
        if level == "WARN":
            warnings[msg] += 1
            if msg == "connection lost":
                # Group "no PONG for 31.8s" and the like.
                lost[re.sub(r"\d+(\.\d+)?s\b", "<n>s", rest.split("reason=", 1)[-1])] += 1
            elif msg in ("connect failed", "connect timed out"):
                connect_failed[rest.split("error=", 1)[-1] if "error=" in rest else msg] += 1
            elif msg.startswith("writer channel full"):
                backpressure_warn = max(backpressure_warn, int(fields["count"]))
        elif level == "ERROR":
            errors[msg] += 1

    if first is None:
        sys.exit(f"{log}: no log lines")
    secs = (last - first).total_seconds()
    print(f"log:            {log}")
    print(f"span:           {first:%F %T} .. {last:%T} UTC ({secs / 60:.1f} min)")

    info = base.with_suffix(".info")
    output_bytes = None
    if info.exists():
        print("\n== run info")
        for line in info.read_text().splitlines():
            print(f"  {line}")
            if line.startswith("output_bytes:"):
                output_bytes = int(line.split()[1])
            if line.startswith("elapsed_s:"):
                secs = int(line.split()[1])

    print(f"\n== connection stats (last line of each of {len(conn_last)} connections)")
    totals = Counter()
    for fields in conn_last.values():
        for k, v in fields.items():
            if k != "conn" and num(v) is not None:
                totals[k] = max(totals[k], num(v)) if k.startswith("max_") else totals[k] + num(v)
    for k, v in totals.items():
        print(f"  {k:24} {v:>14,}")
    levels = totals["levels"]
    if levels:
        per_m = totals["mismatched_books"] / levels * 1e6
        print(f"  {'mismatched_books / 1M levels':28} {per_m:10.2f}")
    print(f"  {'mismatched + gap_mismatched':28} {totals['mismatched_books'] + totals['gap_mismatched_books']:10,}")
    down = [c for c, f in conn_last.items() if int(f["connects"]) - int(f["disconnects"]) != 1]
    print(f"  connections with connects - disconnects != 1: {down or 'none'}")

    print(f"\n== tracked over time ({len(tracked)} samples)")
    for t, markets, symbols in spread(tracked):
        print(f"  {t:%T}  markets {markets:>8,}  symbols {symbols:>8,}")
    if tracked:
        print(f"  max symbols {max(s for _, _, s in tracked):,}")
    print(f"  'subscriptions changed' lines: {changes}")

    print(f"\n== full discovery passes: {len(full_passes)}")
    for t, markets, s in full_passes:
        print(f"  {t:%T}  markets {markets}  secs {s}")

    print(f"\n== disconnect reasons ({sum(lost.values())})")
    for reason, n in lost.most_common(10):
        print(f"  {n:6}  {reason}")
    if connect_failed:
        print(f"  connect failures ({sum(connect_failed.values())}):")
        for reason, n in connect_failed.most_common(5):
            print(f"  {n:6}  {reason}")

    print("\n== top warnings")
    for msg, n in warnings.most_common(10):
        print(f"  {n:8}  {msg}")
    for msg, n in errors.most_common(10):
        print(f"  {n:8}  ERROR {msg}")

    print("\n== writer")
    print(f"  summary: {summary or '(no shutdown line)'}")
    print(f"  highest backpressure count logged: {backpressure_warn}")
    if output_bytes is not None and secs > 0:
        print(f"  output: {output_bytes / 1e9:.2f} GB, {output_bytes / secs * 3600 / 1e9:.2f} GB/h")

    mem = base.with_suffix(".mem")
    if mem.exists():
        rows = [l.split() for l in mem.read_text().splitlines() if l and not l.startswith("#")]
        if rows:
            peak_rss = max(int(r[2]) for r in rows)
            hwm = max(int(r[3]) for r in rows)
            min_avail = min(int(r[4]) for r in rows)
            print("\n== memory")
            print(f"  peak VmRSS {peak_rss / 2**20:.2f} GiB, VmHWM {hwm / 2**20:.2f} GiB, "
                  f"min MemAvailable {min_avail / 2**20:.2f} GiB ({len(rows)} samples)")
            for r in spread(rows):
                print(f"  {r[0]}  +{int(r[1]):>5}s  rss {int(r[2]) / 2**20:6.2f} GiB  "
                      f"avail {int(r[4]) / 2**20:6.2f} GiB")

    report = base.with_suffix(".validate")
    if report.exists():
        print("\n== validate")
        for line in report.read_text().splitlines():
            print(f"  {line}")


if __name__ == "__main__":
    main()
