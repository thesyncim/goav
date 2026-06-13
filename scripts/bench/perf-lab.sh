#!/usr/bin/env bash
# Runs the performance-lab smoke benchmarks and saves the output under the
# checked bench-results/ layout. The Go benchmarks report p50/p95/p99,
# heap/runtime memory, pressure/control/fanout/container smoke, and real Opus
# throughput; OS time output adds max RSS where the platform's /usr/bin/time
# exposes it.
#
# Usage:
#   scripts/bench/perf-lab.sh
#   PERF_BENCHTIME=500x scripts/bench/perf-lab.sh
set -euo pipefail

cd "$(dirname "$0")/../.."
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
machine="$(hostname -s 2>/dev/null || hostname || echo unknown)-$(uname -s)-$(uname -m)"
machine="${machine//[^A-Za-z0-9._-]/_}"
baseline_dir="bench-results/baseline/${stamp}"
latency_dir="bench-results/latency"
rss_dir="bench-results/rss"
pressure_dir="bench-results/pressure"
control_dir="bench-results/control"
fanout_dir="bench-results/fanout"
container_dir="bench-results/container"
pprof_dir="bench-results/pprof/perf-lab-${stamp}"
mkdir -p "${baseline_dir}" "${latency_dir}" "${rss_dir}" "${pressure_dir}" "${control_dir}" "${fanout_dir}" "${container_dir}" "${pprof_dir}"
out="${baseline_dir}/${machine}.txt"
legacy="bench-results/perf-lab-${stamp}.txt"
latency_json="${latency_dir}/record-packets-${stamp}.json"
rss_json="${rss_dir}/sustained-record-memory-${stamp}.json"
pressure_json="${pressure_dir}/source-push-${stamp}.json"
control_json="${control_dir}/attach-detach-${stamp}.json"
fanout_json="${fanout_dir}/fanout-sweep-${stamp}.json"
container_json="${container_dir}/container-corpus-${stamp}.json"
benchtime="${PERF_BENCHTIME:-100x}"
: "${CGO_ENABLED:=0}"
export CGO_ENABLED

{
  echo "goav performance lab"
  echo "date: ${stamp}"
  echo "machine: ${machine}"
  echo "benchtime: ${benchtime}"
  echo "pprof_dir: ${pprof_dir}"
  echo
  go test -run '^$' -bench 'BenchmarkLatencyRecordPackets|BenchmarkSustainedRecordMemory|BenchmarkRealOpus(Encode|Decode)$' \
    -benchmem -benchtime "${benchtime}" \
    -cpuprofile "${pprof_dir}/cpu.out" -memprofile "${pprof_dir}/mem.out" .
  echo
  echo "pressure and hot control:"
  go test -run '^$' -bench 'Benchmark(SourcePush|AttachDetachUnderLoad)$' \
    -benchmem -benchtime "${benchtime}" .
  echo
  echo "fanout sweep:"
  go test -run '^$' -bench 'Benchmark(DirectFanout|DirectFanoutParallel|BufferedFanout)$' \
    -benchmem -benchtime "${benchtime}" ./pipeline
  echo
  echo "container corpus smoke:"
  go test -run '^$' -bench 'Benchmark(Read|Write).*Corpus|BenchmarkExternal.*FieldCorpusScan$' \
    -benchmem -benchtime "${benchtime}" ./container/matroska ./container/webm
  echo
  echo "os memory wrapper:"
  if [ "$(uname -s)" = "Darwin" ]; then
    /usr/bin/time -l go test -run '^$' -bench 'BenchmarkSustainedRecordMemory$' -benchmem -benchtime "${benchtime}" .
  elif /usr/bin/time -v true >/dev/null 2>&1; then
    /usr/bin/time -v go test -run '^$' -bench 'BenchmarkSustainedRecordMemory$' -benchmem -benchtime "${benchtime}" .
  else
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

echo
echo "saved: $out"
echo "legacy: $legacy"
echo "latency: $latency_json"
echo "rss: $rss_json"
echo "pressure: $pressure_json"
echo "control: $control_json"
echo "fanout: $fanout_json"
echo "container: $container_json"
echo "profiles: ${pprof_dir}/cpu.out ${pprof_dir}/mem.out"
