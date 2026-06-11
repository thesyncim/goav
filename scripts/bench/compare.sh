#!/usr/bin/env bash
# Compares two saved benchmark runs (scripts/bench/run.sh output) with benchstat.
#
# Prerequisite:
#   go install golang.org/x/perf/cmd/benchstat@latest
#
# Usage:
#   scripts/bench/compare.sh bench-results/bench-OLD.txt bench-results/bench-NEW.txt
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 old.txt new.txt" >&2
  exit 2
fi

if ! command -v benchstat >/dev/null 2>&1; then
  echo "benchstat not found; install it with:" >&2
  echo "  go install golang.org/x/perf/cmd/benchstat@latest" >&2
  exit 1
fi

benchstat "$1" "$2"
