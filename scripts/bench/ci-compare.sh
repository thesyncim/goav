#!/usr/bin/env bash
# Advisory CI benchmark comparison. It runs a small stable benchmark subset on
# a base ref and the current checkout, then writes benchstat output under
# bench-results/. Timing is never a pass/fail gate; compile/runtime failures
# still fail because the artifact would otherwise be meaningless.
#
# Usage:
#   scripts/bench/ci-compare.sh origin/main
#   BENCH_COMPARE_COUNT=5 scripts/bench/ci-compare.sh FETCH_HEAD
set -euo pipefail

cd "$(dirname "$0")/../.."

base_ref="${1:-origin/main}"
outdir="${BENCH_COMPARE_OUT:-bench-results}"
bench_re="${BENCH_COMPARE_REGEX:-Benchmark(RecordPackets|DecodeEncode|Resample|BranchFanout)}"
benchtime="${BENCH_COMPARE_TIME:-1x}"
count="${BENCH_COMPARE_COUNT:-3}"
: "${CGO_ENABLED:=0}"
export CGO_ENABLED

mkdir -p "$outdir"
outdir_abs="$(cd "$outdir" && pwd)"
base_out="$outdir_abs/bench-base.txt"
current_out="$outdir_abs/bench-current.txt"
stat_out="$outdir_abs/benchstat-pr-vs-base.txt"
worktree="$(mktemp -d)"

cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || rm -rf "$worktree"
}
trap cleanup EXIT

benchstat_bin="$(command -v benchstat || true)"
if [ -z "$benchstat_bin" ]; then
  go install golang.org/x/perf/cmd/benchstat@latest
  benchstat_bin="$(go env GOPATH)/bin/benchstat"
fi

git worktree add --detach "$worktree" "$base_ref" >/dev/null

(
  cd "$worktree"
  go test -run '^$' -bench "$bench_re" -benchmem -benchtime "$benchtime" -count "$count" . > "$base_out"
)

go test -run '^$' -bench "$bench_re" -benchmem -benchtime "$benchtime" -count "$count" . > "$current_out"

if "$benchstat_bin" "$base_out" "$current_out" > "$stat_out"; then
  cat "$stat_out"
else
  {
    echo "benchstat comparison unavailable"
    echo "base: $base_ref"
  } | tee "$stat_out"
fi

echo "saved: $base_out"
echo "saved: $current_out"
echo "saved: $stat_out"
