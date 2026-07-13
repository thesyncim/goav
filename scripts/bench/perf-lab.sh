#!/usr/bin/env bash
# Runs the performance-lab smoke benchmarks and saves the output under the
# checked bench-results/ layout. The Go benchmarks report p50/p95/p99,
# heap/runtime memory, live-room sync drift/drop smoke,
# pressure/control/fanout/container smoke, real Opus and video codec
# (VP8/VP9/AV1 encode, VP8 decode) throughput, soak-drift, and live-room
# attach/detach churn harnesses; OS time output adds max RSS where the
# platform's /usr/bin/time exposes it.
#
# Usage:
#   scripts/bench/perf-lab.sh
#   PERF_BENCHTIME=500x scripts/bench/perf-lab.sh
#   PERF_RELEASE_QUALITY=true PERF_BENCHTIME=2000x PERF_SOAK_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_BENCHTIME=1h scripts/bench/perf-lab.sh
#   PERF_RELEASE_QUALITY=true PERF_GO_TEST_TIMEOUT=2h PERF_BENCHTIME=2000x PERF_SOAK_BENCHTIME=1h PERF_LIVE_ROOM_CHURN_BENCHTIME=1h scripts/bench/perf-lab.sh
set -euo pipefail

cd "$(dirname "$0")/../.."
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
machine="$(hostname -s 2>/dev/null || hostname || echo unknown)-$(uname -s)-$(uname -m)"
machine="${machine//[^A-Za-z0-9._-]/_}"
baseline_dir="bench-results/baseline/${stamp}"
latency_dir="bench-results/latency"
rss_dir="bench-results/rss"
soak_dir="bench-results/soak"
pressure_dir="bench-results/pressure"
control_dir="bench-results/control"
fanout_dir="bench-results/fanout"
live_sync_dir="bench-results/live-sync"
container_dir="bench-results/container"
pprof_dir="bench-results/pprof/perf-lab-${stamp}"
manifest_dir="bench-results/manifest"
mkdir -p "${baseline_dir}" "${latency_dir}" "${rss_dir}" "${soak_dir}" "${pressure_dir}" "${control_dir}" "${fanout_dir}" "${live_sync_dir}" "${container_dir}" "${pprof_dir}" "${manifest_dir}"
out="${baseline_dir}/${machine}.txt"
legacy="bench-results/perf-lab-${stamp}.txt"
latency_json="${latency_dir}/record-packets-${stamp}.json"
rss_json="${rss_dir}/sustained-record-memory-${stamp}.json"
soak_json="${soak_dir}/record-drift-${stamp}.json"
live_room_churn_soak_json="${soak_dir}/live-room-churn-${stamp}.json"
pressure_json="${pressure_dir}/source-push-${stamp}.json"
control_json="${control_dir}/attach-detach-${stamp}.json"
fanout_json="${fanout_dir}/fanout-sweep-${stamp}.json"
live_sync_json="${live_sync_dir}/live-room-sync-${stamp}.json"
container_json="${container_dir}/container-corpus-${stamp}.json"
manifest_json="${manifest_dir}/perf-lab-${stamp}.json"
benchtime="${PERF_BENCHTIME:-100x}"
soak_benchtime="${PERF_SOAK_BENCHTIME:-$benchtime}"
live_room_churn_benchtime="${PERF_LIVE_ROOM_CHURN_BENCHTIME:-$soak_benchtime}"
live_room_churn_interval="${PERF_LIVE_ROOM_CHURN_INTERVAL:-}"
release_quality="${PERF_RELEASE_QUALITY:-false}"
go_test_timeout="${PERF_GO_TEST_TIMEOUT:-10m}"
if [ "$release_quality" != "true" ] && [ "$release_quality" != "false" ]; then
  echo "PERF_RELEASE_QUALITY must be true or false" >&2
  exit 2
fi
if [ "$release_quality" = "true" ]; then
  if [ -z "${PERF_SOAK_BENCHTIME:-}" ] || [ -z "${PERF_LIVE_ROOM_CHURN_BENCHTIME:-}" ]; then
    echo "PERF_RELEASE_QUALITY=true requires PERF_SOAK_BENCHTIME and PERF_LIVE_ROOM_CHURN_BENCHTIME" >&2
    exit 2
  fi
  case "$soak_benchtime" in
    *x)
      echo "PERF_SOAK_BENCHTIME must be a duration such as 1h for PERF_RELEASE_QUALITY=true" >&2
      exit 2
      ;;
  esac
  case "$live_room_churn_benchtime" in
    *x)
      echo "PERF_LIVE_ROOM_CHURN_BENCHTIME must be a duration such as 1h for PERF_RELEASE_QUALITY=true" >&2
      exit 2
      ;;
  esac
  if [ -z "${PERF_GO_TEST_TIMEOUT:-}" ]; then
    go_test_timeout=0
  fi
  if [ -z "$live_room_churn_interval" ]; then
    live_room_churn_interval=100ms
  fi
fi
: "${CGO_ENABLED:=0}"
export CGO_ENABLED
go_version="$(go version)"
goos="$(go env GOOS)"
goarch="$(go env GOARCH)"
git_commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git status --porcelain --untracked-files=all 2>/dev/null || true)" ]; then
  git_dirty=true
else
  git_dirty=false
fi

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_string() {
  printf '"%s"' "$(json_escape "$1")"
}

{
  echo "goav performance lab"
  echo "date: ${stamp}"
  echo "machine: ${machine}"
  echo "benchtime: ${benchtime}"
  echo "soak_benchtime: ${soak_benchtime}"
  echo "live_room_churn_benchtime: ${live_room_churn_benchtime}"
  echo "live_room_churn_interval: ${live_room_churn_interval:-none}"
  echo "go_test_timeout: ${go_test_timeout}"
  echo "go_version: ${go_version}"
  echo "goos: ${goos}"
  echo "goarch: ${goarch}"
  echo "cgo_enabled: ${CGO_ENABLED}"
  echo "git_commit: ${git_commit}"
  echo "git_dirty: ${git_dirty}"
  echo "release_quality: ${release_quality}"
  echo "manifest: ${manifest_json}"
  echo "pprof_dir: ${pprof_dir}"
  echo
  go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkLatencyRecordPackets|BenchmarkLiveRoomSync|BenchmarkSustainedRecordMemory|BenchmarkRealOpus(Encode|Decode)$' \
    -benchmem -benchtime "${benchtime}" \
    -cpuprofile "${pprof_dir}/cpu.out" -memprofile "${pprof_dir}/mem.out" .
  echo
  echo "real video codec throughput:"
  go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkReal(VP8|VP9|H264|AV1)Encode$|BenchmarkRealVP8Decode$' \
    -benchmem -benchtime "${benchtime}" .
  echo
  echo "soak drift (run with a large PERF_SOAK_BENCHTIME for an extended-stability artifact):"
  if [ "$release_quality" = "true" ]; then
    GOAV_PERF_RECORD_DRIFT_SOAK_DURATION="${soak_benchtime}" \
      go test -timeout "${go_test_timeout}" -run '^TestPerformanceLabRecordDriftSoak$' -count=1 -v .
  else
    go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkSoakRecordDrift$' \
      -benchmem -benchtime "${soak_benchtime}" .
  fi
  echo
  echo "pressure and hot control:"
  go test -timeout "${go_test_timeout}" -run '^$' -bench 'Benchmark(SourcePush|AttachDetachUnderLoad)$' \
    -benchmem -benchtime "${benchtime}" .
  echo
  echo "fanout sweep:"
  go test -timeout "${go_test_timeout}" -run '^$' -bench 'Benchmark(DirectFanout|DirectFanoutParallel|BufferedFanout)$' \
    -benchmem -benchtime "${benchtime}" ./pipeline
  echo
  echo "container corpus smoke:"
  go test -timeout "${go_test_timeout}" -run '^$' -bench 'Benchmark(Read|Write).*Corpus|BenchmarkExternal.*FieldCorpusScan$' \
    -benchmem -benchtime "${benchtime}" ./container/matroska ./container/webm
  echo
  echo "os memory wrapper:"
  if [ "$(uname -s)" = "Darwin" ]; then
    /usr/bin/time -l go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkSustainedRecordMemory$' -benchmem -benchtime "${benchtime}" .
  elif /usr/bin/time -v true >/dev/null 2>&1; then
    /usr/bin/time -v go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkSustainedRecordMemory$' -benchmem -benchtime "${benchtime}" .
  else
    echo "/usr/bin/time does not expose RSS on this platform"
  fi
  echo
  echo "live-room attach/detach churn soak with os memory wrapper:"
  if [ "$(uname -s)" = "Darwin" ]; then
    if [ "$release_quality" = "true" ]; then
      /usr/bin/time -l env GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION="${live_room_churn_benchtime}" GOAV_PERF_LIVE_ROOM_CHURN_INTERVAL="${live_room_churn_interval}" \
        go test -timeout "${go_test_timeout}" -run '^TestPerformanceLabLiveRoomAttachDetachSoak$' -count=1 -v .
    else
      /usr/bin/time -l go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkLiveRoomAttachDetachSoak$' -benchmem -benchtime "${live_room_churn_benchtime}" .
    fi
  elif /usr/bin/time -v true >/dev/null 2>&1; then
    if [ "$release_quality" = "true" ]; then
      /usr/bin/time -v env GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION="${live_room_churn_benchtime}" GOAV_PERF_LIVE_ROOM_CHURN_INTERVAL="${live_room_churn_interval}" \
        go test -timeout "${go_test_timeout}" -run '^TestPerformanceLabLiveRoomAttachDetachSoak$' -count=1 -v .
    else
      /usr/bin/time -v go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkLiveRoomAttachDetachSoak$' -benchmem -benchtime "${live_room_churn_benchtime}" .
    fi
  else
    if [ "$release_quality" = "true" ]; then
      GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION="${live_room_churn_benchtime}" GOAV_PERF_LIVE_ROOM_CHURN_INTERVAL="${live_room_churn_interval}" \
        go test -timeout "${go_test_timeout}" -run '^TestPerformanceLabLiveRoomAttachDetachSoak$' -count=1 -v .
    else
      go test -timeout "${go_test_timeout}" -run '^$' -bench 'BenchmarkLiveRoomAttachDetachSoak$' -benchmem -benchtime "${live_room_churn_benchtime}" .
    fi
    echo "/usr/bin/time does not expose RSS on this platform"
  fi
} 2>&1 | tee "$out"
cp "$out" "$legacy"

metric_value() {
  local bench="$1"
  local metric="$2"
  awk -v bench="$bench" -v metric="$metric" '
    $1 ~ "^" bench "(-[0-9]+)?$" {
      for (i = 1; i <= NF; i++) {
        if ($i == metric) {
          print $(i - 1)
          exit
        }
      }
    }
  ' "$out"
}

line_metric() {
  local prefix="$1"
  local metric="$2"
  awk -v prefix="$prefix" -v metric="$metric" '
    index($0, prefix) {
      for (i = 1; i <= NF; i++) {
        split($i, parts, "=")
        if (parts[1] == metric) {
          print parts[2]
          exit
        }
      }
    }
  ' "$out"
}

json_number() {
  if [ -n "$1" ]; then
    printf '%s' "$1"
  else
    printf 'null'
  fi
}

latency_ns="$(metric_value BenchmarkLatencyRecordPackets ns/op)"
p50_ns="$(metric_value BenchmarkLatencyRecordPackets p50_ns)"
p95_ns="$(metric_value BenchmarkLatencyRecordPackets p95_ns)"
p99_ns="$(metric_value BenchmarkLatencyRecordPackets p99_ns)"
cat > "$latency_json" <<EOF
{
  "scenario": "record-packets",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "ns_per_op": $(json_number "$latency_ns"),
  "p50_ns": $(json_number "$p50_ns"),
  "p95_ns": $(json_number "$p95_ns"),
  "p99_ns": $(json_number "$p99_ns")
}
EOF

heap_live_b="$(metric_value BenchmarkSustainedRecordMemory heap_live_B)"
runtime_sys_b="$(metric_value BenchmarkSustainedRecordMemory runtime_sys_B)"
max_rss_b="$(awk '/maximum resident set size/ {print $1; exit}' "$out")"
if [ -z "$max_rss_b" ]; then
  max_rss_kb="$(awk -F: '/Maximum resident set size/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' "$out")"
  if [ -n "$max_rss_kb" ]; then
    max_rss_b=$((max_rss_kb * 1024))
  fi
fi
cat > "$rss_json" <<EOF
{
  "scenario": "sustained-record-memory",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "heap_live_B": $(json_number "$heap_live_b"),
  "runtime_sys_B": $(json_number "$runtime_sys_b"),
  "max_rss_B": $(json_number "$max_rss_b")
}
EOF

soak_duration_ns="$(line_metric goav_perf_record_drift_soak duration_ns)"
soak_packets="$(line_metric goav_perf_record_drift_soak packets)"
soak_ns="$(line_metric goav_perf_record_drift_soak ns_per_packet)"
soak_allocs=""
soak_heap_drift_b="$(line_metric goav_perf_record_drift_soak heap_drift_B)"
soak_gc_cycles="$(line_metric goav_perf_record_drift_soak gc_cycles)"
soak_gc_pause_total_ns="$(line_metric goav_perf_record_drift_soak gc_pause_total_ns)"
if [ -z "$soak_ns" ]; then
  soak_ns="$(metric_value BenchmarkSoakRecordDrift ns/op)"
  soak_allocs="$(metric_value BenchmarkSoakRecordDrift allocs/op)"
  soak_heap_drift_b="$(metric_value BenchmarkSoakRecordDrift heap_drift_B)"
  soak_gc_cycles="$(metric_value BenchmarkSoakRecordDrift gc_cycles)"
  soak_gc_pause_total_ns="$(metric_value BenchmarkSoakRecordDrift gc_pause_total_ns)"
fi
cat > "$soak_json" <<EOF
{
  "scenario": "record-drift-soak",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "benchtime": "${soak_benchtime}",
  "duration_ns": $(json_number "$soak_duration_ns"),
  "packets": $(json_number "$soak_packets"),
  "ns_per_op": $(json_number "$soak_ns"),
  "allocs_per_op": $(json_number "$soak_allocs"),
  "heap_drift_B": $(json_number "$soak_heap_drift_b"),
  "gc_cycles": $(json_number "$soak_gc_cycles"),
  "gc_pause_total_ns": $(json_number "$soak_gc_pause_total_ns")
}
EOF

live_churn_duration_ns="$(line_metric goav_perf_live_room_churn_soak duration_ns)"
live_churn_operations="$(line_metric goav_perf_live_room_churn_soak operations)"
live_churn_interval_ns="$(line_metric goav_perf_live_room_churn_soak churn_interval_ns)"
live_churn_ns="$(line_metric goav_perf_live_room_churn_soak ns_per_op)"
live_churn_allocs=""
live_churn_p50="$(line_metric goav_perf_live_room_churn_soak p50_ns)"
live_churn_p95="$(line_metric goav_perf_live_room_churn_soak p95_ns)"
live_churn_p99="$(line_metric goav_perf_live_room_churn_soak p99_ns)"
live_churn_source_drops="$(line_metric goav_perf_live_room_churn_soak source_drops)"
live_churn_sync_drops="$(line_metric goav_perf_live_room_churn_soak sync_drops)"
live_churn_graph_drops="$(line_metric goav_perf_live_room_churn_soak graph_drops)"
live_churn_delivered="$(line_metric goav_perf_live_room_churn_soak delivered)"
live_churn_max_drift="$(line_metric goav_perf_live_room_churn_soak max_drift_ns)"
if [ -z "$live_churn_ns" ]; then
  live_churn_ns="$(metric_value BenchmarkLiveRoomAttachDetachSoak ns/op)"
  live_churn_allocs="$(metric_value BenchmarkLiveRoomAttachDetachSoak allocs/op)"
  live_churn_p50="$(metric_value BenchmarkLiveRoomAttachDetachSoak p50_ns)"
  live_churn_p95="$(metric_value BenchmarkLiveRoomAttachDetachSoak p95_ns)"
  live_churn_p99="$(metric_value BenchmarkLiveRoomAttachDetachSoak p99_ns)"
  live_churn_source_drops="$(metric_value BenchmarkLiveRoomAttachDetachSoak source_drops)"
  live_churn_sync_drops="$(metric_value BenchmarkLiveRoomAttachDetachSoak sync_drops)"
  live_churn_graph_drops="$(metric_value BenchmarkLiveRoomAttachDetachSoak graph_drops)"
  live_churn_delivered="$(metric_value BenchmarkLiveRoomAttachDetachSoak delivered)"
  live_churn_max_drift="$(metric_value BenchmarkLiveRoomAttachDetachSoak max_drift_ns)"
fi
live_churn_max_rss_b="$(awk '/maximum resident set size/ {value=$1} END {print value}' "$out")"
if [ -z "$live_churn_max_rss_b" ]; then
  live_churn_max_rss_kb="$(awk -F: '/Maximum resident set size/ {gsub(/^[ \t]+/, "", $2); value=$2} END {print value}' "$out")"
  if [ -n "$live_churn_max_rss_kb" ]; then
    live_churn_max_rss_b=$((live_churn_max_rss_kb * 1024))
  fi
fi
cat > "$live_room_churn_soak_json" <<EOF
{
  "scenario": "live-room-attach-detach-soak",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "benchtime": "${live_room_churn_benchtime}",
  "churn_interval": "${live_room_churn_interval:-}",
  "churn_interval_ns": $(json_number "$live_churn_interval_ns"),
  "duration_ns": $(json_number "$live_churn_duration_ns"),
  "operations": $(json_number "$live_churn_operations"),
  "ns_per_op": $(json_number "$live_churn_ns"),
  "allocs_per_op": $(json_number "$live_churn_allocs"),
  "p50_ns": $(json_number "$live_churn_p50"),
  "p95_ns": $(json_number "$live_churn_p95"),
  "p99_ns": $(json_number "$live_churn_p99"),
  "source_drops": $(json_number "$live_churn_source_drops"),
  "sync_drops": $(json_number "$live_churn_sync_drops"),
  "graph_drops": $(json_number "$live_churn_graph_drops"),
  "delivered": $(json_number "$live_churn_delivered"),
  "max_drift_ns": $(json_number "$live_churn_max_drift"),
  "max_rss_B": $(json_number "$live_churn_max_rss_b")
}
EOF

source_push_drop_ns="$(metric_value BenchmarkSourcePush/dropping ns/op)"
source_push_block_ns="$(metric_value BenchmarkSourcePush/blocking ns/op)"
source_push_drop_allocs="$(metric_value BenchmarkSourcePush/dropping allocs/op)"
source_push_block_allocs="$(metric_value BenchmarkSourcePush/blocking allocs/op)"
cat > "$pressure_json" <<EOF
{
  "scenario": "source-push-pressure",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "drop_oldest_ns_per_op": $(json_number "$source_push_drop_ns"),
  "blocking_ns_per_op": $(json_number "$source_push_block_ns"),
  "drop_oldest_allocs_per_op": $(json_number "$source_push_drop_allocs"),
  "blocking_allocs_per_op": $(json_number "$source_push_block_allocs")
}
EOF

attach_detach_ns="$(metric_value BenchmarkAttachDetachUnderLoad ns/op)"
attach_detach_allocs="$(metric_value BenchmarkAttachDetachUnderLoad allocs/op)"
cat > "$control_json" <<EOF
{
  "scenario": "attach-detach-under-load",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "ns_per_op": $(json_number "$attach_detach_ns"),
  "allocs_per_op": $(json_number "$attach_detach_allocs")
}
EOF

fanout_direct_1="$(metric_value BenchmarkDirectFanout/N=1 ns/op)"
fanout_direct_8="$(metric_value BenchmarkDirectFanout/N=8 ns/op)"
fanout_direct_64="$(metric_value BenchmarkDirectFanout/N=64 ns/op)"
fanout_direct_512="$(metric_value BenchmarkDirectFanout/N=512 ns/op)"
fanout_buffered_shared_1="$(metric_value BenchmarkBufferedFanout/shared/N=1 ns/op)"
fanout_buffered_shared_512="$(metric_value BenchmarkBufferedFanout/shared/N=512 ns/op)"
cat > "$fanout_json" <<EOF
{
  "scenario": "fanout-sweep",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "direct_n1_ns_per_op": $(json_number "$fanout_direct_1"),
  "direct_n8_ns_per_op": $(json_number "$fanout_direct_8"),
  "direct_n64_ns_per_op": $(json_number "$fanout_direct_64"),
  "direct_n512_ns_per_op": $(json_number "$fanout_direct_512"),
  "buffered_shared_n1_ns_per_op": $(json_number "$fanout_buffered_shared_1"),
  "buffered_shared_n512_ns_per_op": $(json_number "$fanout_buffered_shared_512")
}
EOF

live_sync_ns="$(metric_value BenchmarkLiveRoomSync ns/op)"
live_sync_p50="$(metric_value BenchmarkLiveRoomSync p50_ns)"
live_sync_p95="$(metric_value BenchmarkLiveRoomSync p95_ns)"
live_sync_p99="$(metric_value BenchmarkLiveRoomSync p99_ns)"
live_sync_allocs="$(metric_value BenchmarkLiveRoomSync allocs/op)"
live_sync_source_drops="$(metric_value BenchmarkLiveRoomSync source_drops)"
live_sync_sync_drops="$(metric_value BenchmarkLiveRoomSync sync_drops)"
live_sync_delivered="$(metric_value BenchmarkLiveRoomSync delivered)"
live_sync_max_drift="$(metric_value BenchmarkLiveRoomSync max_drift_ns)"
cat > "$live_sync_json" <<EOF
{
  "scenario": "live-room-sync",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "ns_per_op": $(json_number "$live_sync_ns"),
  "p50_ns": $(json_number "$live_sync_p50"),
  "p95_ns": $(json_number "$live_sync_p95"),
  "p99_ns": $(json_number "$live_sync_p99"),
  "allocs_per_op": $(json_number "$live_sync_allocs"),
  "source_drops": $(json_number "$live_sync_source_drops"),
  "sync_drops": $(json_number "$live_sync_sync_drops"),
  "delivered": $(json_number "$live_sync_delivered"),
  "max_drift_ns": $(json_number "$live_sync_max_drift")
}
EOF

matroska_read_ns="$(metric_value BenchmarkReadWebRTCCorpus ns/op)"
matroska_write_ns="$(metric_value BenchmarkWriteWebRTCCorpus ns/op)"
webm_read_ns="$(metric_value BenchmarkReadWebMCorpus ns/op)"
webm_write_ns="$(metric_value BenchmarkWriteWebMCorpus ns/op)"
matroska_field_ns="$(metric_value BenchmarkExternalMatroskaFieldCorpusScan ns/op)"
webm_field_ns="$(metric_value BenchmarkExternalWebMFieldCorpusScan ns/op)"
cat > "$container_json" <<EOF
{
  "scenario": "container-corpus",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "source": "${out}",
  "matroska_webrtc_read_ns_per_op": $(json_number "$matroska_read_ns"),
  "matroska_webrtc_write_ns_per_op": $(json_number "$matroska_write_ns"),
  "webm_read_ns_per_op": $(json_number "$webm_read_ns"),
  "webm_write_ns_per_op": $(json_number "$webm_write_ns"),
  "matroska_field_corpus_ns_per_op": $(json_number "$matroska_field_ns"),
  "webm_field_corpus_ns_per_op": $(json_number "$webm_field_ns")
}
EOF

cat > "$manifest_json" <<EOF
{
  "schema": 1,
  "kind": "goav-perf-lab-manifest",
  "stamp": "${stamp}",
  "machine": "${machine}",
  "go_version": $(json_string "$go_version"),
  "goos": "${goos}",
  "goarch": "${goarch}",
  "cgo_enabled": "${CGO_ENABLED}",
  "git_commit": "${git_commit}",
  "git_dirty": ${git_dirty},
  "benchtime": "${benchtime}",
  "soak_benchtime": "${soak_benchtime}",
  "live_room_churn_benchtime": "${live_room_churn_benchtime}",
  "live_room_churn_interval": "${live_room_churn_interval:-}",
  "go_test_timeout": "${go_test_timeout}",
  "release_quality": ${release_quality},
  "release_quality_note": "Set PERF_RELEASE_QUALITY=true only for a maintainer-run reference-hardware pass whose generated artifacts are attached to the release candidate.",
  "artifacts": {
    "baseline_transcript": "${out}",
    "legacy_transcript": "${legacy}",
    "latency": "${latency_json}",
    "rss": "${rss_json}",
    "soak": "${soak_json}",
    "live_room_churn_soak": "${live_room_churn_soak_json}",
    "pressure": "${pressure_json}",
    "control": "${control_json}",
    "fanout": "${fanout_json}",
    "live_sync": "${live_sync_json}",
    "container": "${container_json}",
    "cpu_profile": "${pprof_dir}/cpu.out",
    "mem_profile": "${pprof_dir}/mem.out"
  }
}
EOF

echo
echo "saved: $out"
echo "legacy: $legacy"
echo "latency: $latency_json"
echo "rss: $rss_json"
echo "soak: $soak_json"
echo "live-room-churn-soak: $live_room_churn_soak_json"
echo "pressure: $pressure_json"
echo "control: $control_json"
echo "fanout: $fanout_json"
echo "live-sync: $live_sync_json"
echo "container: $container_json"
echo "manifest: $manifest_json"
echo "profiles: ${pprof_dir}/cpu.out ${pprof_dir}/mem.out"
