#!/usr/bin/env bash
# The E4 latency experiments behind LATENCY.md, one after another.
#
#   bash scripts/latency_runs.sh            # from the repo root, in WSL
#
# Builds the `latency` probe from this checkout and copies it to
# ~/runs/latency-<rev> (a later rebuild cannot touch a running binary).
# The token list comes from a 2-minute budget probe (40 tokens by 24 h
# volume from Gamma) and is reused by every run, so all runs watch the same
# markets. Each run takes an NTP sample before and after (<name>.ntp) and
# appends the eth0 receive rate to <name>.err. Run nothing else on the same
# IP meanwhile (no collector): the link is only ~3-5 MB/s.
#
# Files in ~/runs/: <name>.csv (one row per change per socket), .err
# (per-socket counters, reconnects, rx_kBps), .ntp, .report.
# Total: ~2 h 45 min of probing, then ~1 h of reports (latency_report.py,
# up to ~1.5 GB RSS on the 3 GB lat-age CSV).
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
runs=~/runs
mkdir -p "$runs"
cd "$repo"

source ~/.cargo/env
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-~/target/engineering-challenge}
cargo build --release --bin latency
bin=$runs/latency-$(git rev-parse --short HEAD)
cp "$CARGO_TARGET_DIR/release/latency" "$bin"

tokens=$runs/lat-budget.csv.tokens

ntp() { python3 "$repo/scripts/ntp_offset.py" --server time.google.com; }

run() {
    local name=$1; shift
    ntp > "$runs/$name.ntp" 2>&1
    local rx0 t0 rx1 t1
    rx0=$(cat /sys/class/net/eth0/statistics/rx_bytes); t0=$(date +%s)
    "$bin" --out "$runs/$name.csv" "$@" 2> "$runs/$name.err"
    rx1=$(cat /sys/class/net/eth0/statistics/rx_bytes); t1=$(date +%s)
    echo "rx_kBps=$(( (rx1 - rx0) / (t1 - t0) / 1000 )) secs=$((t1 - t0)) args=$*" >> "$runs/$name.err"
    ntp >> "$runs/$name.ntp" 2>&1
    echo "$(date -u +%FT%TZ) $name done" >> "$runs/lat-all.progress"
}

report() {
    local name=$1; shift
    python3 "$repo/scripts/latency_report.py" "$runs/$name.csv" --sockets 16 "$@" > "$runs/$name.report"
}

# Budget probe: picks the tokens (written to lat-budget.csv.tokens).
run lat-budget  --sockets 16 --tokens 40 --seconds 120

getent ahosts ws-subscriptions-clob.polymarket.com > "$runs/lat-ip.hosts"
run lat-best    --tokens-file "$tokens" --sockets 16 --seconds 1800
run lat-port-lo --tokens-file "$tokens" --sockets 16 --ports 40000-40015 --seconds 900
run lat-port-hi --tokens-file "$tokens" --sockets 16 --ports 55000-55015 --seconds 900
run lat-ip      --tokens-file "$tokens" --sockets 16 --all-ips --seconds 1800
run lat-age     --tokens-file "$tokens" --sockets 16 --refresh 8:600 --seconds 7200
echo ALL_DONE >> "$runs/lat-all.progress"

report lat-best    --k 1,2,4,8,16
report lat-port-lo --k 1,2,4,8,16 --group-by port
report lat-port-hi --k 1,2,4,8,16 --group-by port
report lat-ip      --k 1,2,4,8,16 --group-by ip
report lat-age     --k 1,2,4,8,16 --group-by age
