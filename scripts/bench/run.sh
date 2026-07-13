#!/usr/bin/env bash
# Runs the goav benchmark suite with -benchmem and saves the timestamped output
# under bench-results/ for later benchstat comparison (scripts/bench/compare.sh).
#
# Usage:
#   scripts/bench/run.sh                 # whole module, 6 repetitions
#   BENCH_COUNT=10 scripts/bench/run.sh  # more repetitions (better benchstat CIs)
#   scripts/bench/run.sh -bench Mix .    # extra `go test` args narrow the run
#
# Timing numbers are machine-dependent, so the meaningful timing comparison is
# old-vs-new output from the SAME machine. Committed reference JSON covers only
# allocation and byte ceilings; see scripts/bench/baseline.sh.
set -euo pipefail

cd "$(dirname "$0")/../.."
mkdir -p bench-results
out="bench-results/bench-$(date +%Y%m%d-%H%M%S).txt"

args=("$@")
if [ ${#args[@]} -eq 0 ]; then
  args=(./...)
fi

go test -run '^$' -bench . -benchmem -count "${BENCH_COUNT:-6}" "${args[@]}" | tee "$out"
echo
echo "saved: $out"
