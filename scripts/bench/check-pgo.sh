#!/usr/bin/env bash
# Validate that the checked-in PGO profile still matches the benchmark suite it
# was generated from.
set -euo pipefail

cd "$(dirname "$0")/../.."

profile="${PGO_PROFILE:-default.pgo}"
meta="${PGO_META:-default.pgo.meta}"

if [ ! -s "$profile" ]; then
  echo "$profile is missing or empty; run scripts/bench/pgo.sh" >&2
  exit 1
fi
if [ ! -s "$meta" ]; then
  echo "$meta is missing or empty; run scripts/bench/pgo.sh" >&2
  exit 1
fi

meta_value() {
  awk -F= -v key="$1" '$1 == key {sub(/^[^=]*=/, ""); print; found=1} END {exit found ? 0 : 1}' "$meta"
}

want_bench_hash="$(meta_value bench_test_sha256)"
want_script_hash="$(meta_value pgo_script_sha256)"
got_bench_hash="$(shasum -a 256 bench_test.go | awk '{print $1}')"
got_script_hash="$(shasum -a 256 scripts/bench/pgo.sh | awk '{print $1}')"

if [ "$got_bench_hash" != "$want_bench_hash" ]; then
  echo "bench_test.go changed since $profile was generated; run scripts/bench/pgo.sh" >&2
  echo "want: $want_bench_hash" >&2
  echo " got: $got_bench_hash" >&2
  exit 1
fi

if [ "$got_script_hash" != "$want_script_hash" ]; then
  echo "scripts/bench/pgo.sh changed since $profile was generated; run scripts/bench/pgo.sh" >&2
  echo "want: $want_script_hash" >&2
  echo " got: $got_script_hash" >&2
  exit 1
fi

if ! go tool pprof -top "$profile" >/dev/null; then
  echo "$profile is not a readable Go CPU profile" >&2
  exit 1
fi

echo "$profile is fresh for $(meta_value package) $(meta_value bench_regex)"
