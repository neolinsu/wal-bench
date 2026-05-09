#!/usr/bin/env bash
#
# trace_write_syscall.sh — Trace the kernel call graph inside write() for wal-bench
# at different concurrency levels using ftrace's function_graph tracer.
#
# Runs concurrency 1..6 pinned to worker cores 4..9, so concurrency c uses
# cores 4..(3+c).  This isolates benchmark threads from system activity on
# cores 0-3 and scales core count with concurrency.
#
# Usage:
#   sudo ./scripts/trace_write_syscall.sh [OPTIONS]
#
# Options:
#   --dir DIR                   WAL directory (default: ~/tmp/wal-trace)
#   --writes N                  Writes per task (default: 500)
#   --depth N                   Max call graph depth (default: 20)
#   --out DIR                   Output directory for traces (default: ./trace-out)
#   --binary PATH               Path to wal-bench binary (default: ./target/release/wal-bench)
#   --core-base N               First worker core (default: 4)
#   --max-conc N                Run concurrency 1..N (default: 2)
#   --tracing-thresh US         ftrace: only record functions >= this many us (default: 2000)
#   --buffer-size-kb N          ftrace: per-CPU buffer size in KB (default: 65536)
#   --mode MODE                 wal-bench -m (default: std)
#   --no-direct                 Disable O_DIRECT (default: enabled)
#   --sync-mode MODE            wal-bench --sync-mode (default: dsync)
#   --record-size N             wal-bench min/max record size in bytes (default: 512)
#   --spinup-delay SEC          Sleep after bench launch before listing TIDs (default: 0.3)
#   --slow-thresh-us N          Summary: list functions taking >= this many us (default: 500)
#   --very-slow-thresh-ms N     Summary: header label for slow-write section (default: 6)
#
# Requires: root (for ftrace), wal-bench binary already built.
#
set -euo pipefail

# --- defaults ---
WAL_DIR="$HOME/tmp/wal-trace"
WRITES=500
DEPTH=20
OUT_DIR="./trace-out"
BINARY="./target/release/wal-bench"
CORE_BASE=4        # first worker core
MAX_CONC=2         # concurrency 1..MAX_CONC, cores CORE_BASE..(CORE_BASE+MAX_CONC-1)
TRACING_THRESH_US=2000   # ftrace: only record functions exceeding this many us
BUFFER_SIZE_KB=65536     # ftrace: per-CPU buffer size
BENCH_MODE=std           # wal-bench -m
DIRECT_FLAG="--direct"   # set empty via --no-direct to disable O_DIRECT
SYNC_MODE=dsync          # wal-bench --sync-mode
RECORD_SIZE=512          # wal-bench --min/--max-record-size
SPINUP_DELAY=0.3         # seconds between bench launch and TID enumeration
SLOW_THRESH_US=500       # summary: list functions taking >= this many us
VERY_SLOW_THRESH_MS=6    # summary: header label for slow-write section

# --- parse args ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir)                 WAL_DIR="$2";              shift 2 ;;
        --writes)              WRITES="$2";               shift 2 ;;
        --depth)               DEPTH="$2";                shift 2 ;;
        --out)                 OUT_DIR="$2";              shift 2 ;;
        --binary)              BINARY="$2";               shift 2 ;;
        --core-base)           CORE_BASE="$2";            shift 2 ;;
        --max-conc)            MAX_CONC="$2";             shift 2 ;;
        --tracing-thresh)      TRACING_THRESH_US="$2";    shift 2 ;;
        --buffer-size-kb)      BUFFER_SIZE_KB="$2";       shift 2 ;;
        --mode)                BENCH_MODE="$2";           shift 2 ;;
        --no-direct)           DIRECT_FLAG="";            shift 1 ;;
        --sync-mode)           SYNC_MODE="$2";            shift 2 ;;
        --record-size)         RECORD_SIZE="$2";          shift 2 ;;
        --spinup-delay)        SPINUP_DELAY="$2";         shift 2 ;;
        --slow-thresh-us)      SLOW_THRESH_US="$2";       shift 2 ;;
        --very-slow-thresh-ms) VERY_SLOW_THRESH_MS="$2";  shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

TRACEFS="/sys/kernel/debug/tracing"

if [[ ! -d "$TRACEFS" ]]; then
    TRACEFS="/sys/kernel/tracing"
fi

if [[ ! -d "$TRACEFS" ]]; then
    echo "ERROR: Cannot find tracefs. Are you root?" >&2
    exit 1
fi

if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: wal-bench binary not found at $BINARY" >&2
    echo "       Run 'cargo build --release' first." >&2
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: This script must be run as root (for ftrace)." >&2
    exit 1
fi

mkdir -p "$OUT_DIR" "$WAL_DIR"

# --- helper: reset ftrace state ---
reset_ftrace() {
    echo 0 > "$TRACEFS/tracing_on"
    echo nop > "$TRACEFS/current_tracer"
    echo > "$TRACEFS/set_graph_function"
    echo > "$TRACEFS/set_ftrace_pid"
    echo > "$TRACEFS/set_ftrace_filter"
    echo > "$TRACEFS/trace"
    echo 0 > "$TRACEFS/max_graph_depth" 2>/dev/null || true
    echo 0 > "$TRACEFS/tracing_thresh"
}

# --- helper: setup ftrace for function_graph on ksys_write ---
setup_ftrace() {
    local pid=$1
    local depth=$2

    echo function_graph > "$TRACEFS/current_tracer"

    echo funcgraph-duration > "$TRACEFS/trace_options"
    echo funcgraph-proc > "$TRACEFS/trace_options"
    echo funcgraph-abstime > "$TRACEFS/trace_options"

    echo ksys_write > "$TRACEFS/set_graph_function"

    echo "$pid" > "$TRACEFS/set_ftrace_pid"

    echo "$depth" > "$TRACEFS/max_graph_depth"

    # only record functions exceeding the threshold (microseconds)
    echo "$TRACING_THRESH_US" > "$TRACEFS/tracing_thresh"

    # per-CPU buffer
    echo "$BUFFER_SIZE_KB" > "$TRACEFS/buffer_size_kb"

    echo > "$TRACEFS/trace"
}

# --- run one benchmark and capture trace ---
run_trace() {
    local conc=$1
    local cores_str=$2
    local label="c${conc}_cores${cores_str//,/-}"
    local trace_file="$OUT_DIR/write_trace_${label}.out"
    local summary_file="$OUT_DIR/write_trace_${label}_summary.txt"

    echo "=== Concurrency $conc | worker-cores $cores_str ==="
    echo "    Trace output: $trace_file"

    reset_ftrace

    $BINARY \
        --dir "$WAL_DIR" \
        --cleanup \
        -n "$WRITES" \
        -c "$conc" \
        -m "$BENCH_MODE" \
        $DIRECT_FLAG \
        --sync-mode "$SYNC_MODE" \
        --max-record-size "$RECORD_SIZE" \
        --min-record-size "$RECORD_SIZE" \
        --worker-cores "$cores_str" &
    local bench_pid=$!

    echo "    wal-bench PID: $bench_pid"

    setup_ftrace "$bench_pid" "$DEPTH"

    # wait for worker threads to spin up, then add their TIDs
    sleep "$SPINUP_DELAY"
    local tids
    tids=$(ls "/proc/$bench_pid/task/" 2>/dev/null || echo "$bench_pid")
    for tid in $tids; do
        echo "$tid" >> "$TRACEFS/set_ftrace_pid"
    done
    echo "    Tracing PIDs/TIDs: $(cat "$TRACEFS/set_ftrace_pid" | tr '\n' ' ')"

    echo 1 > "$TRACEFS/tracing_on"

    wait "$bench_pid" 2>/dev/null || true

    echo 0 > "$TRACEFS/tracing_on"

    cat "$TRACEFS/trace" > "$trace_file"
    local trace_lines
    trace_lines=$(wc -l < "$trace_file")
    echo "    Captured $trace_lines trace lines"

    # summary: slow writes
    {
        echo "=== Concurrency $conc | worker-cores $cores_str: slow writes (>${VERY_SLOW_THRESH_MS}ms) ==="
        echo ""
        grep -E '^\s*[0-9]+\.[0-9]+ us\s+\|\s+\}' "$trace_file" | head -5 || true
        echo ""
        echo "--- Functions taking >${SLOW_THRESH_US}us ---"
        awk -v t="$SLOW_THRESH_US" '
            /^[[:space:]]*[0-9]+\.[0-9]+ us/ { if ($1+0      >= t) print; next }
            /^[[:space:]]*[0-9]+\.[0-9]+ ms/ { if ($1*1000   >= t) print; next }
        ' "$trace_file" || true
    } > "$summary_file" 2>/dev/null

    echo "    Summary: $summary_file"
    echo ""
}

# --- main ---
echo "============================================="
echo "  ftrace write() call graph tracer"
echo "============================================="
echo "  WAL dir:       $WAL_DIR"
echo "  Core range:    ${CORE_BASE}..$(( CORE_BASE + MAX_CONC - 1 ))"
echo "  Concurrency:   1..${MAX_CONC}"
echo "  Writes/task:   $WRITES"
echo "  Graph depth:   $DEPTH"
echo "  Output dir:    $OUT_DIR"
echo "  Binary:        $BINARY"
echo "  Mode:          $BENCH_MODE"
echo "  O_DIRECT:      ${DIRECT_FLAG:+on}${DIRECT_FLAG:-off}"
echo "  Sync mode:     $SYNC_MODE"
echo "  Record size:   $RECORD_SIZE"
echo "  ftrace thresh: ${TRACING_THRESH_US} us"
echo "  ftrace buffer: ${BUFFER_SIZE_KB} KB"
echo "  Spinup delay:  ${SPINUP_DELAY} s"
echo "  Slow thresh:   ${SLOW_THRESH_US} us"
echo "  Very slow:     ${VERY_SLOW_THRESH_MS} ms"
echo "============================================="
echo ""

for conc in $(seq 1 "$MAX_CONC"); do
    last_core=$(( CORE_BASE + conc - 1 ))
    if (( conc == 1 )); then
        cores_str="$CORE_BASE"
    else
        cores_str="${CORE_BASE}-${last_core}"
    fi
    run_trace "$conc" "$cores_str"
done

# --- final cleanup ---
reset_ftrace

echo "============================================="
echo "  Done. Trace files in $OUT_DIR/"
echo "============================================="
echo ""
echo "Useful analysis commands:"
echo ""
echo "  # View a trace:"
echo "  less $OUT_DIR/write_trace_c1_cores4.out"
echo ""
echo "  # Find the slowest ksys_write calls:"
echo "  grep -E '(us|ms)\s+\|\s+\} /\* ksys_write \*/' $OUT_DIR/write_trace_c1_cores4.out | sort -t'|' -k1 -rn | head -20"
echo ""
echo "  # Compare across concurrency levels:"
echo "  for f in $OUT_DIR/write_trace_c*_summary.txt; do echo \"--- \$f ---\"; head -5 \"\$f\"; echo; done"
