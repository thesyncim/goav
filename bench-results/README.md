# Benchmark Results

This directory is the performance-lab artifact landing zone. Generated result
files are ignored by git; this README keeps the expected layout visible for CI,
release rehearsals, and local benchmark runs. Treat the layout as the contract:
numbers belong in generated artifacts, not committed prose.

## Layout

```text
bench-results/
  reference/root.json                    # committed alloc/B-op ratchet: root suite
  reference/pipeline.json                # committed alloc/B-op ratchet: deterministic fanout rows
  reference/soak.json                    # committed alloc/B-op ratchet: soak/control smoke rows
  baseline/<timestamp>/<machine>.txt      # full benchmark and RSS transcript
  latency/<scenario>-<timestamp>.json     # p50/p95/p99 and ns/op summary
  rss/<scenario>-<timestamp>.json         # heap/sys/max-RSS summary
  soak/<scenario>-<timestamp>.json        # record drift and live-room churn soak summaries
  pressure/<scenario>-<timestamp>.json    # drop/backpressure SourcePush summary
  control/<scenario>-<timestamp>.json     # hot attach/detach summary
  fanout/<scenario>-<timestamp>.json      # 1/8/64/512 fanout sweep summary
  live-sync/<scenario>-<timestamp>.json   # live-room sync drift/drop summary
  container/<scenario>-<timestamp>.json   # Matroska/WebM corpus smoke summary
  pprof/<scenario>-<timestamp>/           # cpu.out and mem.out profiles
  manifest/perf-lab-<timestamp>.json       # host/toolchain/git provenance + artifact index
  bench-*.txt                             # scripts/bench/run.sh outputs
  benchstat-*.txt                         # scripts/bench/compare.sh outputs
```

`scripts/bench/perf-lab.sh` writes the `baseline`, `latency`, `rss`,
`soak`, `pressure`, `control`, `fanout`, `live-sync`, `container`, `pprof`, and
`manifest` directories, including CPU and memory profiles for the perf-lab
subset. The manifest records host/toolchain/git provenance, the selected
general benchtime, the separate soak benchtimes (`PERF_SOAK_BENCHTIME` and
`PERF_LIVE_ROOM_CHURN_BENCHTIME`), the Go test timeout (`PERF_GO_TEST_TIMEOUT`,
or `0` by default in release-quality mode), the `PERF_RELEASE_QUALITY` flag,
and every generated artifact path. CI runs the lab with a smoke
`PERF_BENCHTIME=1x` and uploads the generated manifest and JSON summaries;
release evidence should use longer same-machine runs with
`PERF_RELEASE_QUALITY=true` plus explicit long soak benchtimes, then attach the
generated manifest plus files to the release or PR instead of committing
machine-dependent numbers. Release-quality mode requires explicit
duration-based `PERF_SOAK_BENCHTIME` and `PERF_LIVE_ROOM_CHURN_BENCHTIME`
values so fixed-count smoke runs cannot be mislabelled as release evidence,
and defaults `PERF_LIVE_ROOM_CHURN_INTERVAL` to `100ms` so the long churn soak
measures sustained attach/detach behavior rather than an unpaced allocation
storm; set `PERF_GO_TEST_TIMEOUT` only when you want a bounded watchdog around
the long run.

The soak directory contains `record-drift-<timestamp>.json` for heap drift,
GC cycles, and GC pause, plus `live-room-churn-<timestamp>.json` for the
combined live-room sync + attach/detach churn scenario (p50/p95/p99,
source/sync/graph drops, delivered messages, max A/V drift, and `max_rss_B`
from the OS memory wrapper when the host exposes it). The churn summary also
records the paced `churn_interval` and `churn_interval_ns` fields. In
release-quality mode the script runs wall-clock-controlled tests
`TestPerformanceLabRecordDriftSoak` and
`TestPerformanceLabLiveRoomAttachDetachSoak` by passing
`GOAV_PERF_RECORD_DRIFT_SOAK_DURATION` and
`GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION`, plus
`GOAV_PERF_LIVE_ROOM_CHURN_INTERVAL` for the paced churn interval; smoke mode
keeps using the benchmark rows so CI stays quick.

`bench-results/reference/*.json` is the committed D5 ratchet. Refresh it with
`scripts/bench/baseline.sh generate`; check the current tree with
`scripts/bench/baseline.sh check`. The JSON records benchmark rows as
provenance, but CI enforces only `allocs/op` and `B/op` ceilings. Wall-clock
metrics remain advisory because shared runners and local machines are not
stable reference hardware.

`scripts/bench/pgo.sh` is the exception: it refreshes the checked-in
`default.pgo` and `default.pgo.meta` workflow artifacts, and
`scripts/bench/check-pgo.sh` verifies that their benchmark-suite hashes still
match the current tree.
