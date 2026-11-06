#!/usr/bin/env python3
"""Measure the local clock offset against an NTP server (SNTP, UDP 123).

offset_ms is local - server (positive = local clock ahead), median of the
samples. delay_ms is the median round trip. Run it in the same environment
as the probe (WSL), since the Windows and WSL clocks differ.

usage: ntp_offset.py [--server pool.ntp.org] [--samples 8]
"""
import argparse
import shutil
import socket
import statistics
import struct
import subprocess
import time

NTP_EPOCH = 2208988800  # seconds from 1900 to 1970


def ntp_time(data, i):
    sec, frac = struct.unpack("!II", data[i:i + 8])
    return sec - NTP_EPOCH + frac / 2**32


def sample(server):
    """One exchange: (offset_s, delay_s) with offset = local - server."""
    packet = b"\x23" + 47 * b"\0"  # v4, client mode
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(3)
        t0 = time.time()
        s.sendto(packet, (server, 123))
        data, _ = s.recvfrom(512)
        t3 = time.time()
    t1, t2 = ntp_time(data, 32), ntp_time(data, 40)  # server receive, transmit
    return ((t0 - t1) + (t3 - t2)) / 2, (t3 - t0) - (t2 - t1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="pool.ntp.org")
    ap.add_argument("--samples", type=int, default=8)
    args = ap.parse_args()
    server = socket.gethostbyname(args.server)  # same server for every sample
    results = []
    for _ in range(args.samples):
        try:
            results.append(sample(server))
        except OSError as e:
            print(f"sample failed: {e}")
        time.sleep(0.2)
    if not results:
        raise SystemExit("no NTP replies")
    print(f"server={args.server} ({server}) samples={len(results)}")
    print(f"offset_ms={statistics.median(o for o, _ in results) * 1000:.3f}")
    print(f"delay_ms={statistics.median(d for _, d in results) * 1000:.3f}")
    if shutil.which("chronyc"):
        subprocess.run(["chronyc", "tracking"])


if __name__ == "__main__":
    main()
