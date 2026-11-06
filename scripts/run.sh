#!/usr/bin/env bash
# Reproducible collector run with memory sampling and validation.
#
#   scripts/run.sh <name> --duration <secs> [-- <collector args>]
#
# Builds the release binaries, copies them to ~/runs/ (so a later rebuild
# cannot touch a running binary), starts the collector directly, samples its
# memory every 30 s, sends SIGINT after <secs>, then validates the output.
#
# Output: $RUN_SBE_DIR/<name>.sbe (default /mnt/d/runs, unless the collector
# args give --output). Not in ~/runs: the WSL virtual disk lives on C: and
# never shrinks; ~3 GB per full-scale run filled C: on 2026-11-06.
#
# Files in ~/runs/:
#   <name>.log       collector stderr
#   <name>.mem       samples: utc elapsed_s vmrss_kb vmhwm_kb memavailable_kb
#   <name>.info      commit, args, start/end, exit code, output size
#   <name>.validate  validate output
#
# Ctrl+C (or SIGTERM) on this script stops the collector early and still
# validates. For long runs start it with nohup:
#   nohup scripts/run.sh full30 --duration 1800 > ~/runs/full30.out 2>&1 &
set -euo pipefail

usage() { echo "usage: $0 <name> --duration <secs> [-- <collector args>]" >&2; exit 2; }

[[ $# -ge 1 ]] || usage
name=$1; shift
duration=
while [[ $# -gt 0 ]]; do
    case $1 in
        --duration) duration=${2:?}; shift 2 ;;
        --) shift; break ;;
        *) usage ;;
    esac
done
[[ $duration =~ ^[0-9]+$ ]] || usage
args=("$@")

runs=~/runs
mkdir -p "$runs"
log=$runs/$name.log
mem=$runs/$name.mem
info=$runs/$name.info
report=$runs/$name.validate

# Output: the collector's --output if given, else $RUN_SBE_DIR/<name>.sbe. It is
# taken out of the args and passed back as an absolute path, so the check
# below, the collector (run from the repo) and validate all see one file.
out=
rest=()
for ((i = 0; i < ${#args[@]}; i++)); do
    case ${args[i]} in
        --output) out=${args[i+1]:-}; i=$((i + 1)) ;;
        --output=*) out=${args[i]#--output=} ;;
        *) rest+=("${args[i]}") ;;
    esac
done
sbe_dir=${RUN_SBE_DIR:-/mnt/d/runs}
mkdir -p "$sbe_dir"
[[ -n $out ]] || out=$sbe_dir/$name.sbe
case $out in -|/dev/stdout) echo "run.sh needs a file output, not stdout" >&2; exit 2 ;; esac
out=${out/#\~/$HOME}
[[ $out == /* ]] || out=$PWD/$out
args=("${rest[@]}" --output "$out")
for f in "$log" "$out"; do
    [[ -e $f ]] && { echo "$f exists; pick another name" >&2; exit 2; }
done

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
source ~/.cargo/env 2>/dev/null || true
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-~/target/engineering-challenge}
cargo build --release -q --bin engineering-challenge --bin validate

rev=$(git rev-parse --short HEAD)
# autocrlf: the checkout may come from Windows git (CRLF); only content counts.
[[ -z $(git -c core.autocrlf=true -c core.filemode=false status --porcelain \
    --untracked-files=no -- src Cargo.toml Cargo.lock) ]] || rev=$rev-dirty
bin=$runs/collector-$rev
validate=$runs/validate-$rev
# A dirty tree may differ from an earlier copy with the same name.
if [[ ! -e $bin || $rev == *-dirty ]]; then
    cp "$CARGO_TARGET_DIR/release/engineering-challenge" "$bin.tmp" && mv -f "$bin.tmp" "$bin"
    cp "$CARGO_TARGET_DIR/release/validate" "$validate.tmp" && mv -f "$validate.tmp" "$validate"
fi

started=$(date -u +%FT%TZ)
start_s=$(date +%s)
{
    echo "name: $name"
    echo "commit: $rev"
    echo "binary: $bin"
    echo "args: ${args[*]}"
    echo "duration_s: $duration"
    echo "start: $started"
} > "$info"

# Own session: a terminal Ctrl+C reaches only this script, which then sends
# exactly one SIGINT (a second one would make the collector skip the flush).
setsid "$bin" "${args[@]}" 2> "$log" &
pid=$!
echo "pid: $pid" >> "$info"
echo "collector pid $pid; log $log; output $out" >&2

sample() {
    local now rss hwm avail
    now=$(date -u +%FT%TZ) || return 0
    rss=$(awk '/^VmRSS:/ {print $2}' /proc/$pid/status 2>/dev/null) || return 0
    hwm=$(awk '/^VmHWM:/ {print $2}' /proc/$pid/status 2>/dev/null) || return 0
    [[ -n $rss ]] || return 0
    avail=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
    echo "$now $(( $(date +%s) - start_s )) $rss $hwm $avail" >> "$mem"
}
echo "# utc elapsed_s vmrss_kb vmhwm_kb memavailable_kb" > "$mem"

# Ctrl+C / SIGTERM here only ends the wait early.
stop_early=0
trap 'stop_early=1' INT TERM

deadline=$((start_s + duration))
while kill -0 "$pid" 2>/dev/null && (( stop_early == 0 )); do
    sample
    left=$((deadline - $(date +%s)))
    (( left > 0 )) || break
    sleep $(( left < 30 ? left : 30 )) &
    wait $! || true
done

if kill -0 "$pid" 2>/dev/null; then
    sample
    echo "stopping: SIGINT $(date -u +%FT%TZ)" >> "$info"
    kill -INT "$pid" || true
    # Sample through the shutdown drain; force after 120 s.
    for _ in $(seq 120); do
        kill -0 "$pid" 2>/dev/null || break
        sample
        sleep 1 || true  # a second Ctrl+C here must not end the script
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "collector still running 120 s after SIGINT; sending SIGINT again" | tee -a "$info" >&2
        kill -INT "$pid" || true
    fi
else
    echo "collector exited before the deadline" | tee -a "$info" >&2
fi
trap - INT TERM
set +e
wait "$pid"
code=$?
set -e
{
    echo "exit_code: $code"
    echo "end: $(date -u +%FT%TZ)"
    echo "elapsed_s: $(( $(date +%s) - start_s ))"
    echo "output_bytes: $(stat -c %s "$out" 2>/dev/null || echo 0)"
} >> "$info"

echo "validating $out" >&2
set +e
"$validate" "$out" > "$report" 2>&1
vcode=$?
set -e
echo "validate_exit: $vcode" >> "$info"
cat "$info" >&2
tail -n 12 "$report" >&2
exit "$code"
