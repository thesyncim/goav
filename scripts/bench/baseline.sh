#!/usr/bin/env bash
# Generate or check the committed benchmark baseline ratchet.
#
# Usage:
#   scripts/bench/baseline.sh generate   # refresh bench-results/reference/*.json
#   scripts/bench/baseline.sh check      # CI/local allocation + B/op ratchet
set -euo pipefail

# Benchmarks build with the Go SIMD experiment, matching CI. Set GOEXPERIMENT=
# (empty) to measure the scalar build importers get by default.
: "${GOEXPERIMENT=simd}"
export GOEXPERIMENT

cd "$(dirname "$0")/../.."

mode="${1:-check}"
reference_dir="${BENCH_BASELINE_DIR:-bench-results/reference}"
tmp_dir="${BENCH_BASELINE_TMP:-$(mktemp -d)}"
benchtime="${BENCH_BASELINE_TIME:-100x}"
count="${BENCH_BASELINE_COUNT:-1}"
bytes_slack_ratio="${BENCH_BASELINE_BYTES_SLACK_RATIO:-0.05}"
bytes_slack_min="${BENCH_BASELINE_BYTES_SLACK_MIN:-1024}"
bytes_cold_ratio="${BENCH_BASELINE_BYTES_COLD_RATIO:-0.25}"
allocs_slack="${BENCH_BASELINE_ALLOCS_SLACK:-0}"
allocs_slack_ratio="${BENCH_BASELINE_ALLOCS_SLACK_RATIO:-0.05}"
allocs_slack_min="${BENCH_BASELINE_ALLOCS_SLACK_MIN:-4}"
allocs_cold_at="${BENCH_BASELINE_ALLOCS_COLD_AT:-100}"
allocs_cold_ratio="${BENCH_BASELINE_ALLOCS_COLD_RATIO:-0.5}"
allocs_cold_min="${BENCH_BASELINE_ALLOCS_COLD_MIN:-64}"
: "${CGO_ENABLED:=0}"
export CGO_ENABLED

root_regex="${BENCH_BASELINE_ROOT_REGEX:-Benchmark(RecordPackets|RemuxPackets|DecodeToFrameSink|DecodeEncode|Resample|Resize|BranchFanout|SharedMuxGroup|Mix|Composite|SelectPassthrough|LatencyRecordPackets|SustainedRecordMemory|SourcePush|AttachDetachUnderLoad|SoakRecordDrift|LiveRoomSync)$}"
pipeline_regex="${BENCH_BASELINE_PIPELINE_REGEX:-Benchmark(DirectFanout|BufferedFanout)$}"
soak_regex="${BENCH_BASELINE_SOAK_REGEX:-Benchmark(SoakRecordDrift|LiveRoomSync|AttachDetachUnderLoad)$}"
# The packetize hot path lives in the rtpav nested module, run from its own
# directory.
rtpav_regex="${BENCH_BASELINE_RTPAV_REGEX:-Benchmark(Packetize|Send)$}"
# Transport stream packetizing, the hot path of every TS and SRT output.
ts_regex="${BENCH_BASELINE_TS_REGEX:-Benchmark(MuxPacketize|MuxPacketizeCBR)$}"

cleanup() {
  if [ -z "${BENCH_BASELINE_TMP:-}" ]; then
    rm -rf "$tmp_dir"
  fi
}
trap cleanup EXIT

mkdir -p "$reference_dir" "$tmp_dir"

run_suite() {
  local kind="$1"
  local pkg="$2"
  local regex="$3"
  local dir="${4:-.}"
  local out="$tmp_dir/${kind}.txt"
  local target="$pkg"
  if [ "$dir" != . ]; then
    target=.
  fi
  (cd "$dir" && CGO_ENABLED="$CGO_ENABLED" go test -run '^$' -bench "$regex" -benchmem -benchtime "$benchtime" -count "$count" "$target") > "$out"
  echo "$out"
}

collect_suite() {
  local kind="$1"
  local pkg="$2"
  local regex="$3"
  local out
  out="$(run_suite "$kind" "$pkg" "$regex" "${4:-.}")"
  go run ./scripts/bench/internal/benchbaseline collect \
    -kind "$kind" \
    -package "$pkg" \
    -bench-regex "$regex" \
    -benchtime "$benchtime" \
    -count "$count" \
    -bytes-slack-ratio "$bytes_slack_ratio" \
    -bytes-slack-min "$bytes_slack_min" \
    -bytes-cold-ratio "$bytes_cold_ratio" \
    -allocs-slack "$allocs_slack" \
    -allocs-slack-ratio "$allocs_slack_ratio" \
    -allocs-slack-min "$allocs_slack_min" \
    -allocs-cold-at "$allocs_cold_at" \
    -allocs-cold-ratio "$allocs_cold_ratio" \
    -allocs-cold-min "$allocs_cold_min" \
    -source "$out" \
    -source-label "scripts/bench/baseline.sh $kind $pkg" \
    -out "$reference_dir/${kind}.json"
  echo "wrote $reference_dir/${kind}.json"
}

check_suite() {
  local kind="$1"
  local pkg="$2"
  local regex="$3"
  local dir="${4:-.}"
  local baseline="$reference_dir/${kind}.json"
  if [ ! -s "$baseline" ]; then
    echo "$baseline is missing; run scripts/bench/baseline.sh generate" >&2
    exit 1
  fi
  local out
  out="$(run_suite "$kind" "$pkg" "$regex" "$dir")"
  go run ./scripts/bench/internal/benchbaseline check -baseline "$baseline" -current "$out"
}

case "$mode" in
  generate)
    collect_suite root . "$root_regex"
    collect_suite pipeline ./pipeline "$pipeline_regex"
    collect_suite soak . "$soak_regex"
    collect_suite rtpav ./rtpav "$rtpav_regex" rtpav
    collect_suite ts ./container/ts "$ts_regex"
    ;;
  check)
    check_suite root . "$root_regex"
    check_suite pipeline ./pipeline "$pipeline_regex"
    check_suite soak . "$soak_regex"
    check_suite rtpav ./rtpav "$rtpav_regex" rtpav
    check_suite ts ./container/ts "$ts_regex"
    ;;
  *)
    echo "usage: scripts/bench/baseline.sh [generate|check]" >&2
    exit 2
    ;;
esac
