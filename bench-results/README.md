# Benchmark Results

This directory is the performance-lab artifact landing zone. Generated result
files are ignored by git; this README keeps the expected layout visible for CI,
release rehearsals, and local benchmark runs.

## Layout

```text
bench-results/
  baseline/<timestamp>/<machine>.txt      # full benchmark and RSS transcript
  latency/<scenario>-<timestamp>.json     # p50/p95/p99 and ns/op summary
  rss/<scenario>-<timestamp>.json         # heap/sys/max-RSS summary
  pressure/<scenario>-<timestamp>.json    # drop/backpressure SourcePush summary
  control/<scenario>-<timestamp>.json     # hot attach/detach summary
  fanout/<scenario>-<timestamp>.json      # 1/8/64/512 fanout sweep summary
  container/<scenario>-<timestamp>.json   # Matroska/WebM corpus smoke summary
  pprof/<scenario>-<timestamp>/           # cpu.out and mem.out profiles
  bench-*.txt                             # scripts/bench/run.sh outputs
  benchstat-*.txt                         # scripts/bench/compare.sh outputs
```

`scripts/bench/perf-lab.sh` writes the `baseline`, `latency`, `rss`,
`pressure`, `control`, `fanout`, `container`, and `pprof` directories,
including CPU and memory profiles for the perf-lab subset. CI runs it with a
smoke `PERF_BENCHTIME=1x`; release evidence should use longer same-machine
runs and attach the generated files to the release or PR instead of committing
machine-dependent numbers.
