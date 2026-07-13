# Performance

Rule: this repo does not claim performance it cannot prove. Read this page as a
trust boundary, not a marketing sheet.

Every statement here is either **proven** (a named test enforces it in CI),
**measured** (a named benchmark reports it, numbers vary by machine), or **not
proven** (explicitly listed). Anything else in the docs is design intent,
worded as intent.

## Design intent

`goav` treats construction and steady-state media flow differently. Cold paths
may allocate because they happen before or around media delivery: runtime/graph
construction, registry setup, recipe planning and validation, codec
open/configuration, format probing, runtime attach.

Hot paths must keep allocation explicit and bounded:

- per RTP packet
- per depacketized packet
- per decoded/encoded frame
- per mux/demux packet
- per direct pipeline message
- per `source.Push` delivery, which currently allocates one independent
  message wrapper so buffered or retaining emitters cannot observe later emits
  mutating earlier messages

Once running, lower-level stages reuse caller-owned result structs, frame
planes, packet buffers, and scratch storage, and take no per-message mutex
(per-node atomics plus atomically-swapped routing snapshots; mutexes on cold
paths only). The intended shape is one cold-path executable `WorkPlan` and
runtime `WorkPatch`; packet, frame, event, and mux/demux loops must not route
through fluent recipe objects or workflow-specific compiler dispatch.

Rules for hot-path code:

- Prefer `Into` methods that fill caller-owned result structs; reset before
  reuse. Preallocate result slices and frame plane buffers; return capacity
  errors instead of appending beyond capacity.
- Avoid `fmt`, map writes, closure allocation, and error wrapping.
- Keep recipe, flow, branch, tap, destination, and codec abstractions cold-path
  only; do not dispatch through them for each packet or frame.
- Fanout shares payload references unless an explicit policy requires a copy.
- Optional `runconfig.WithMediaPools(true)` installs per-runtime, never-global
  scratch pools for runtime-owned metadata maps and join frame-plane slices.
  The pool is only for values whose lifetime is known to the runtime; public
  event metadata can escape through watchers and is not returned to it.

## Proven: allocation pins

These `testing.AllocsPerRun` guards run in plain `go test ./...` and fail
loudly on regression. Zero means zero allocations per message after warm-up;
a ceiling means the path allocates today and the pin stops the cost from
silently growing.

| Path | Test | Enforced |
|---|---|---|
| Direct graph pass-through (source->stage->sink) | `pipeline.TestGraphDirectRunAllocs` | 0 |
| Buffered fanout steady path (emit -> slot bind -> worker deliver -> slot release, immutable payload, 1->2 fanout) | `pipeline.TestGraphBufferedSteadyEmitAllocs` | 0 |
| Buffered borrowed-packet fanout steady path (1/8/64/512 targets, one graph-owned refcounted copy) | `pipeline.TestBufferedFanoutRefcountAllocs` | 0 |
| Drop-policy decision | `pipeline.TestDropControllerDecideAllocs` | 0 |
| Message/scratch resets | `pipeline.TestMessageAndScratchResetAllocs`, `av.TestCoreResetAllocs`, `av.TestTimeBaseHelpersAllocs` | 0 |
| `source.Push.Packet` / `source.Push.Frame` delivery | `goav.TestSourcePushDeliveryAllocs` | <=1 |
| `component.SinkFunc` delivery from `source.Push` | `goav.TestSinkFuncDeliveryAllocs` | <=1 |
| Select active-arm passthrough (frame/packet) | `goav.TestSelectorPassthroughAllocs` | 0 |
| Audio mix join, per step (2 and 8 arms) | `goav.TestAudioMixStepAllocs` | 0 |
| Video composite join, per step (2 I420 arms) | `goav.TestVideoCompositeStepAllocs` | 0 |
| Runtime media pools (metadata maps; 1-plane audio and 3-plane I420 slices) | `goav.TestMetadataPoolAllocs`, `goav.TestFramePlanePoolAllocs` | 0 |
| Codec decode/encode stages | `codec.TestDecoderStageAllocs`, `codec.TestEncoderStageAllocs` | 0 |
| Format demux/mux stages | `format.TestDemuxSourceAllocs`, `format.TestMuxStageAllocs`, `format.TestFormatResultResetAllocs` | 0 |
| Filter stage (resize/resample) | `filter.TestStageAllocs`, `adapters/resize.TestFilterAllocs`, `adapters/resample.TestFilterAllocs` | 0 |
| RTP receive loop, depacketizers (Opus/VP8/VP9/H264/AV1), sequence/jitter, feedback scratch | `rtpav.TestSourceStartAllocs`, `Test*DepacketizerAllocs`, `TestSequenceDetectorAllocs`, `TestJitterRingAllocs`, `TestFeedbackResultAllocs` | 0 |
| IVF / Annex B read+write; gopus/govpx/goav1/goh264 adapter hot paths | per-adapter `*Allocs` tests (see `docs/ADAPTERS.md`) | 0 |
| Matroska/WebM steady-state packet write/read | allocation guards in `container/matroska`, `container/webm` | 0 |

## Measured: benchmarks

Benchmarks never run under plain `go test`. Run them deliberately, on the same
machine when comparing two revisions:

```sh
scripts/bench/run.sh                  # full suite, -benchmem, saved to bench-results/
scripts/bench/compare.sh old new      # benchstat old-vs-new (same machine!)
scripts/bench/pgo.sh                  # refresh default.pgo + default.pgo.meta
scripts/bench/baseline.sh check       # committed alloc/B-op baseline ratchet
scripts/bench/perf-lab.sh             # latency quantiles, heap/sys, RSS wrapper
scripts/bench/ci-compare.sh BASE_REF  # CI PR-vs-base benchstat artifact
```

`compare.sh` needs `go install golang.org/x/perf/cmd/benchstat@latest`.
Timing numbers are machine-dependent, so only same-machine old-vs-new timing
comparison is meaningful. The repo does commit
`bench-results/reference/*.json` as a narrower D5 ratchet: CI enforces only
`allocs/op` and `B/op` ceilings from those files, while wall-clock metrics in
the JSON remain provenance, not claims. Refresh those ceilings intentionally
with `scripts/bench/baseline.sh generate`.

The canonical-workload suite (root `bench_test.go`) runs the public recipe
grammar against the deterministic `goavtest` runtime: fake passthrough codecs
and fake byte-faithful containers (so numbers include the fake's serialization
cost, not a real codec's), plus the **real** bundled resize/resample filters.
Each benchmark builds its task untimed, then pushes exactly `b.N` messages:
ns/op and allocs/op are per-message steady state. The perf lab adds explicit
p50/p95/p99 metrics for the packet-record path; other benchmark rows still use
ns/op as their steady-state timing proxy.

One allocation is honest arithmetic, not noise: every push-fed workload pays
exactly one steady-state allocation per message — the deliberately heap-owned
`pipeline.Message` that gives each delivery independent ownership so a
retaining emitter can never observe a later push mutating an earlier message.
That safety cost is pinned at a ceiling of one by
`TestSourcePushDeliveryAllocs`; everything downstream of the source is
allocation-free per message. `BenchmarkRemuxPackets` has no push producer and
measures 0 allocs/op end to end. The committed ratchet
(`bench-results/reference/root.json`, `-benchtime=100x`) also rounds one-time
cold-start costs into a few rows (Mix 2–3, Composite 2, SelectPassthrough 2);
at `-benchtime=1s` those converge to the per-push allocation alone.

| Benchmark | Workload |
|---|---|
| `BenchmarkRecordPackets` | RTP-style record: packet source -> Copy -> fake-container file (1 alloc/op steady state: the pinned per-push message) |
| `BenchmarkRemuxPackets` | file->file packet remux (demux -> Copy -> mux, 0 allocs/op measured) |
| `BenchmarkDecodeToFrameSink` | packets -> decode (fake) -> frame sink (1 alloc/op steady state) |
| `BenchmarkDecodeEncode` | decode -> re-encode (fake) -> sink (1 alloc/op steady state) |
| `BenchmarkResample` | real bundled filter, 44.1kHz stereo -> 48kHz mono (1 alloc/op steady state) |
| `BenchmarkResampleS16Kernel`, `BenchmarkResampleS16KernelDurations` | adapter-local S16 resample inner-loop benches: rate conversion, channel remap, equal-rate copy, and duration scaling |
| `BenchmarkResize` | real bundled filter, 320x180 -> 160x90 I420 (1 alloc/op steady state) |
| `BenchmarkScalePlaneNearestKernel`, `BenchmarkScaleI420Kernel` | adapter-local I420 nearest-neighbor resize inner-loop benches: luma/chroma scale, passthrough row copy, fill crop, and whole-I420 half-scale |
| `BenchmarkBranchFanout/branches=2,8` | one decode, N planned branches to sinks (1 alloc/op steady state) |
| `BenchmarkSharedMuxGroup` | audio+video chains sharing one mux destination (1 alloc/op steady state) |
| `BenchmarkMix/arms=2,8` | N-arm audio mix on a blocking buffered graph (1 alloc/op steady state per push) |
| `BenchmarkMixS16Kernel` | audio mix inner-loop kernel microbench; arm64/two-arm rows use NEON `SQADD`, amd64/two-arm rows use SSE2 `PADDSW`, and other rows fall back to scalar until measured asm kernels earn dispatch |
| `BenchmarkComposite` | 2-arm video composite (1 alloc/op steady state per push) |
| `BenchmarkCopyPlaneKernel`, `BenchmarkCompositeI420BlitKernel` | root-local I420 composite blit benches: clipped row copy, inside-canvas row copy, and two-arm canvas paint |
| `BenchmarkSelectPassthrough` | one-of-N selector forwarding the active arm (1 alloc/op steady state per push) |
| `BenchmarkAttachDetachUnderLoad` | runtime branch attach+detach per op while live traffic flows (a cold-path control operation, measured against load) |
| `BenchmarkLiveRoomAttachDetachSoak` | live-room sync plus attach/detach churn: packet taps stay live while monitor branches attach/detach; reports p50/p95/p99, source/sync/graph drops, delivered messages, max A/V drift, and `max_rss_B` in the perf-lab JSON when the OS memory wrapper exposes it |
| `BenchmarkSourcePush/dropping,blocking` | the flow-control hot path: source.Push into a DropOldest vs Blocking queue (bounded by `TestSourcePushDeliveryAllocs`) |
| `BenchmarkLatencyRecordPackets` | packet-record path with p50/p95/p99 `source.Push.Packet` acceptance metrics |
| `BenchmarkSustainedRecordMemory` | bounded packet-record memory smoke, reporting live heap and runtime-reserved memory |
| `BenchmarkRealOpusEncode` / `BenchmarkRealOpusDecode` | standard Opus adapter throughput path, not the goavtest fake codec |
| `BenchmarkRealVP8Encode` / `BenchmarkRealVP9Encode` / `BenchmarkRealAV1Encode` | standard video encoder throughput (real `govpx`/`goav1` adapters, deterministic I420 frames) |
| `BenchmarkRealH264Encode` | skips: the bundle ships H.264 decode-only, so there is no encoder to measure (honest gap, not a fake number) |
| `BenchmarkRealVP8Decode` | standard VP8 decoder throughput, decoding a real keyframe captured from the encoder |
| `BenchmarkSoakRecordDrift` | soak harness: long offline-paced record reporting heap drift, GC cycles, and total GC pause; run with a large `-benchtime` for an extended-stability artifact |

Local D4 kernel note (not a release claim): on Apple M4 Max
(`darwin/arm64`, `go test -run '^$' -bench BenchmarkMixS16Kernel -benchmem
-benchtime=300ms -count=3`), the two-arm S16 selected row measured about
38.6-41.3 ns/op vs about 1.76-1.88 us/op for the scalar row, both at
0 allocs/op. The same host can execute the amd64 test binary under
VirtualApple/Rosetta; with `GOARCH=amd64` the `amd64/paddsw2` row measured
about 41.0-44.9 ns/op vs about 1.85-1.87 us/op for scalar, both at
0 allocs/op.

The composite/resample/resize D4 decision is scalar-retain until a future asm
prototype beats the measured scalar rows by at least 1.5x. The current scalar
baseline includes no-clipping composite row-copy, equal-rate resample
copy/channel-remap, and integer-scale nearest-neighbor resize fast paths. On
the same arm64 host (`-benchtime=300ms -count=3`), representative rows measured:
`BenchmarkCompositeI420BlitKernel/two_arms/160x90` about 1.4-1.5 us/op,
`BenchmarkResampleS16Kernel/stereo44100_to_mono48000/20ms` about
4.8-5.1 us/op, and `BenchmarkScaleI420Kernel/320x180_to_half` about
10.1-10.5 us/op, all at 0 allocs/op. The amd64 VirtualApple runs measured
about 2.0 us/op, 8.9-10.0 us/op, and 15.4-15.8 us/op for those same rows,
also at 0 allocs/op.

The `pipeline` package adds the executor-level fanout sweeps
(`BenchmarkDirectFanout`, `BenchmarkDirectFanoutParallel`,
`BenchmarkBufferedFanout` over shared/copy/refcount modes and 1/8/64/512
targets). The committed baseline ratchet covers the deterministic
`BenchmarkDirectFanout` and `BenchmarkBufferedFanout` rows; the parallel row
stays an advisory multi-core scaling signal. The
`container/matroska` + `container/webm` add demux/remux throughput benches
(`BenchmarkReadWebRTCCorpus`, `BenchmarkReadWebMCorpus`, and write/read
variants) with optional external-tool comparisons (ffprobe/ffmpeg/mkvmerge,
skipped when not installed).

`scripts/bench/perf-lab.sh` runs the performance-lab subset and saves a
timestamped artifact set under the checked layout in
[`bench-results/README.md`](../bench-results/README.md):

- `bench-results/baseline/<timestamp>/<machine>.txt` — full transcript
- `bench-results/latency/<scenario>-<timestamp>.json` — p50/p95/p99 summaries
- `bench-results/rss/<scenario>-<timestamp>.json` — heap/sys/RSS summaries
- `bench-results/soak/<scenario>-<timestamp>.json` — heap drift / GC summaries
  and live-room attach/detach churn
- `bench-results/pressure/<scenario>-<timestamp>.json` — drop/backpressure
  smoke
- `bench-results/control/<scenario>-<timestamp>.json` — attach/detach under
  load
- `bench-results/fanout/<scenario>-<timestamp>.json` — 1/8/64/512 fanout
- `bench-results/live-sync/<scenario>-<timestamp>.json` — live-room sync
  latency, drift, and drop smoke
- `bench-results/container/<scenario>-<timestamp>.json` — Matroska/WebM corpus
  smoke
- `bench-results/pprof/<scenario>-<timestamp>/cpu.out` plus `mem.out` —
  profiles
- `bench-results/manifest/perf-lab-<timestamp>.json` — host/toolchain/git
  provenance and the generated-artifact index

The Go benchmarks report latency quantiles, heap/sys metrics,
drop/backpressure costs, attach/detach under load, fanout sweep costs, real
Opus encode/decode throughput, live-room sync drift/drops
(`BenchmarkLiveRoomSync`), live-room attach/detach churn
(`BenchmarkLiveRoomAttachDetachSoak`), and container corpus smoke. Optional
external field-corpus benches stay skipped unless `GOAV_MATROSKA_FIELD_CORPUS`
or `GOAV_WEBM_FIELD_CORPUS` is set. The script wraps the memory benchmark with
`/usr/bin/time` on Linux and macOS so max RSS lands in the artifact when the
host exposes it.

CI runs a `PERF_BENCHTIME=1x` smoke to catch bit rot and uploads the generated
benchmark artifacts, soak summaries, and manifest with `release_quality=false`.
Serious performance claims still need same-machine, longer-run artifacts
attached to a release with `PERF_RELEASE_QUALITY=true`; use
`PERF_SOAK_BENCHTIME` and `PERF_LIVE_ROOM_CHURN_BENCHTIME` to make the two
soak scenarios long-running without stretching every throughput smoke
benchmark. Release-quality mode defaults `PERF_GO_TEST_TIMEOUT` to `0` so
duration-based soaks are not killed by Go's default test timeout; set it
explicitly when you want a bounded watchdog. For those release-quality soaks,
the script runs `TestPerformanceLabRecordDriftSoak` and
`TestPerformanceLabLiveRoomAttachDetachSoak` with
`GOAV_PERF_RECORD_DRIFT_SOAK_DURATION` and
`GOAV_PERF_LIVE_ROOM_CHURN_SOAK_DURATION`, so the artifact duration is
wall-clock controlled instead of subject to Go benchmark calibration.
`PERF_LIVE_ROOM_CHURN_INTERVAL` defaults to `100ms` in release-quality mode
and is recorded as `churn_interval` / `churn_interval_ns` in the churn JSON.

On pull requests, CI also runs `scripts/bench/ci-compare.sh` against the PR base
commit and uploads `bench-base.txt`, `bench-current.txt`, and
`benchstat-pr-vs-base.txt`. This is an advisory same-runner comparison for a
small benchmark subset; it catches obvious drift and gives reviewers a
benchstat table, but it is not a release-quality performance claim.

`default.pgo` is generated from the canonical root benchmark suite by
`scripts/bench/pgo.sh`. The companion `default.pgo.meta` records the benchmark
regex, duration, Go version, and hashes of `bench_test.go` plus the generator
script. CI runs `scripts/bench/check-pgo.sh` so benchmark-suite changes fail
until the checked-in profile is refreshed. Treat the profile as a workflow
artifact, not a performance claim; measured deltas still need same-machine
before/after evidence.

`bench-results/reference/{root,pipeline,soak}.json` is generated by
`scripts/bench/baseline.sh generate` and checked by
`scripts/bench/baseline.sh check` in CI. This ratchet fails on allocation or
byte growth beyond the recorded ceilings. It deliberately ignores `ns/op`,
latency percentile, RSS, and throughput metrics; those remain advisory unless a
release attaches same-machine artifacts.

## Experimental

Performance characteristics that exist but are expected to change; treat the
current numbers as snapshots, not contracts:

- **Buffered mutable frame/owned-payload fanout**: borrowed packet fanout is
  refcounted after one graph-owned copy (`BenchmarkBufferedFanout/refcount` and
  `TestGraphBufferedBorrowedPacketFanoutSharesOneGraphCopy`); owned packets,
  owned frames, and defensive `CopyAlways` paths still copy per target to
  preserve branch-local mutation isolation.
- **Runtime attach under load**: `BenchmarkAttachDetachUnderLoad` measures a
  cold-path control operation; its cost is dominated by planning and is not a
  data-plane figure.
- **Performance lab smoke**: the latency, memory, pressure, control, fanout,
  live-sync, container, and real-Opus rows prove the harness shape, not production
  percentiles, soak behavior, or reference-hardware throughput at realistic
  durations. The committed baseline ratchet makes only `allocs/op` and `B/op`
  ceilings pass/fail.
- **PR benchstat artifact**: `scripts/bench/ci-compare.sh` compares a small
  subset against the PR base on the same runner. It is useful review signal,
  not a pass/fail timing gate.

## Not proven

Stated plainly so the docs never imply otherwise:

- **Published tail-latency baselines** under representative long runs. The
  percentile harness exists, but the committed allocation/byte ratchet and CI
  smoke numbers are not production latency evidence.
- **RSS under sustained load** (1h/6h). The perf-lab script can capture
  `max_rss_B` for the memory smoke and live-room churn soak when run with long
  benchtimes, but no release-candidate long-run RSS artifact is committed here.
- **Real-codec throughput on reference hardware**. The harnesses now exist
  (`BenchmarkReal{VP8,VP9,AV1}Encode`, `BenchmarkRealVP8Decode`,
  `BenchmarkRealOpus*`), so per-codec throughput is now *measurable and
  reproducible*; it becomes a *claim* only once same-machine artifacts are
  committed. H.264 encode is not measurable here — the bundle is decode-only.
- **Sustained-load soak** (hours-long stability, fragmentation, drift). The
  `BenchmarkSoakRecordDrift` and `BenchmarkLiveRoomAttachDetachSoak` harnesses
  exist and report drift / GC / p99 / drop / RSS fields; in release-quality
  mode their wall-clock test counterparts produce the artifacts from
  `PERF_SOAK_BENCHTIME` plus `PERF_LIVE_ROOM_CHURN_BENCHTIME`, with the churn
  cadence controlled by `PERF_LIVE_ROOM_CHURN_INTERVAL`. Those files are not
  release-candidate evidence until attached to a release.
- **Multi-core scaling** targets: `BenchmarkDirectFanoutParallel` measures
  scaling but no specific ratio is promised.
- Comparative leadership claims. Comparative claims require committed
  methodology and reproducible numbers; the only comparative data in-repo is
  the optional external-tool container benches.

## Profiling

The benchmarks are the profiling entry points:

```sh
go test -run '^$' -bench BenchmarkRemuxPackets -benchmem \
  -cpuprofile cpu.out -memprofile mem.out .
go tool pprof cpu.out
go tool pprof -alloc_space mem.out
```

Use `-benchtime=Nx` for a fixed message count and `-cpu 1,2,4,8` on the
pipeline fanout benches for contention analysis.

## Releasing artifacts

`scripts/bench/perf-lab.sh` generates the JSON artifact set (latency
percentiles, RSS/heap, pressure, control, fanout, real Opus and video codec
throughput, soak drift, live-room attach/detach churn, and a manifest) into
`bench-results/`. Those files are machine-specific and `.gitignored` by design
— a number is only evidence on the machine that produced it. To release an
artifact, run the harness on the target hardware and attach the output to the
release, naming the host and Go version:

```sh
PERF_RELEASE_QUALITY=true \
  PERF_BENCHTIME=2000x \
  PERF_SOAK_BENCHTIME=1h \
  PERF_LIVE_ROOM_CHURN_BENCHTIME=1h \
  scripts/bench/perf-lab.sh
```

A throughput, tail-latency, soak, or scaling claim is only valid with the
matching release manifest and artifacts; until then PERFORMANCE.md lists it
under "Not proven". `PERF_RELEASE_QUALITY=true` requires explicit
duration-based `PERF_SOAK_BENCHTIME` and `PERF_LIVE_ROOM_CHURN_BENCHTIME`
values so a fixed-count smoke cannot be mislabeled as release evidence.
Release-quality mode records `PERF_GO_TEST_TIMEOUT` in the manifest and
defaults it to `0` unless the maintainer supplies a bounded watchdog.
`scripts/bench/baseline.sh generate` is only for intentionally refreshing CI
allocation/byte ceilings.
