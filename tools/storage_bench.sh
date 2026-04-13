#!/usr/bin/env bash
# storage_bench.sh — run a single BenchmarkStorage sub-benchmark with iostat capture.
#
# Usage:
#   ./tools/storage_bench.sh <bench_name> [benchtime] [iostat_dev]
#
# Examples:
#   ./tools/storage_bench.sh vols20_w40_r2000_blk64KiB
#   ./tools/storage_bench.sh vols20_w40_r2000_blk128KiB 4x nvme0n1p3
#
# Outputs (written to internal/ss/storage/testcase/):
#   bench_<name>.txt   — full go test -v output
#   iostat_<name>.txt  — iostat -x at 1s intervals during the run

set -euo pipefail

BENCH_NAME="${1:?Usage: $0 <bench_name> [benchtime] [iostat_dev]}"
BENCHTIME="${2:-4x}"
IOSTAT_DEV="${3:-nvme0n1p3}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUTDIR="$REPO_ROOT/internal/ss/storage/testcase"
mkdir -p "$OUTDIR"

BENCH_OUT="$OUTDIR/bench_${BENCH_NAME}.txt"
IOSTAT_OUT="$OUTDIR/iostat_${BENCH_NAME}.txt"

echo "=== storage_bench: $BENCH_NAME (benchtime=$BENCHTIME, dev=$IOSTAT_DEV) ===" | tee "$BENCH_OUT"
echo "Started: $(date)" | tee -a "$BENCH_OUT"
echo ""

# Start iostat in background.
echo "=== iostat -x $IOSTAT_DEV 1 — started $(date) ===" > "$IOSTAT_OUT"
iostat -x "$IOSTAT_DEV" 1 >> "$IOSTAT_OUT" &
IOSTAT_PID=$!

cleanup() {
    kill "$IOSTAT_PID" 2>/dev/null || true
    wait "$IOSTAT_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Run the benchmark from the repo root so module resolution works.
cd "$REPO_ROOT"
go test ./internal/ss/storage \
    -run '^$' \
    -bench "BenchmarkStorage/${BENCH_NAME}" \
    -benchtime="$BENCHTIME" \
    -timeout=24h \
    -v 2>&1 | tee -a "$BENCH_OUT"

echo "" | tee -a "$BENCH_OUT"
echo "Finished: $(date)" | tee -a "$BENCH_OUT"
echo "Outputs:"
echo "  bench:  $BENCH_OUT"
echo "  iostat: $IOSTAT_OUT"
