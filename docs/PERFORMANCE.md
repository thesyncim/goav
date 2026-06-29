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
paths only). The intended shape is
one cold-path executable `WorkPlan` and runtime `WorkPatch`;
packet, frame, event, and mux/demux loops must not route
through fluent recipe objects or workflow-specific compiler dispatch.

Rules for hot-path code:

- Prefer `Into` methods that fill caller-owned result structs; reset before
  reuse. Preallocate result slices and frame plane buffers; return capacity
  errors instead of appending beyond capacity.
- Avoid `fmt`, map writes, closure allocation, and error wrapping.
- Keep recipe, flow, branch, tap, destination, and codec abstractions cold-path
  only; do not dispatch through them for each packet or frame.
- Fanout shares payload references unless an explicit policy requires a copy.

## Proven: allocation pins

These `testing.AllocsPerRun` guards run in plain `go test ./...` and fail
loudly on regression. Zero means zero allocations per message after warm-up;
a ceiling means the path allocates today and the pin stops the cost from
silently growing.

| Path | Test | Enforced |
|---|---|---|
| Direct graph pass-through (source->stage->sink) | `pipeline.TestGraphDirectRunAllocs` | 0 |
| Buffered fanout steady path (emit -> slot bind -> worker deliver -> slot release, immutable payload, 1->2 fanout) | `pipeline.TestGraphBufferedSteadyEmitAllocs` | 0 |
| Drop-policy decision | `pipeline.TestDropControllerDecideAllocs` | 0 |
| Message/scratch resets | `pipeline.TestMessageAndScratchResetAllocs`, `av.TestCoreResetAllocs`, `av.TestTimeBaseHelpersAllocs` | 0 |
| `source.Push.Packet` / `source.Push.Frame` delivery | `goav.TestSourcePushDeliveryAllocs` | <=1 |
| `component.SinkFunc` delivery from `source.Push` | `goav.TestSinkFuncDeliveryAllocs` | <=1 |
| Select active-arm passthrough (frame/packet) | `goav.TestSelectorPassthroughAllocs` | 0 |
| Audio mix join, per step (2 and 8 arms) | `goav.TestAudioMixStepAllocs` | 0 |
| Video composite join, per step (2 I420 arms) | `goav.TestVideoCompositeStepAllocs` | 0 |
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
scripts/bench/perf-lab.sh             # latency quantiles, heap/sys, RSS wrapper
scripts/bench/ci-compare.sh BASE_REF  # CI PR-vs-base benchstat artifact
```

`compare.sh` needs `go install golang.org/x/perf/cmd/benchstat@latest`.
No baseline numbers are committed: they are machine-dependent, so only
same-machine old-vs-new comparison is meaningful.

The canonical-workload suite (root `bench_test.go`) runs the public recipe
grammar against the deterministic `goavtest` runtime: fake passthrough codecs
and fake byte-faithful containers (so numbers include the fake's serialization
cost, not a real codec's), plus the **real** bundled resize/resample filters.
Each benchmark builds its task untimed, then pushes exactly `b.N` messages:
ns/op and allocs/op are per-message steady state. The perf lab adds explicit
p50/p95/p99 metrics for the packet-record path; other benchmark rows still use
ns/op as their steady-state timing proxy.

| Benchmark | Workload |
|---|---|
| `BenchmarkRecordPackets` | RTP-style record: packet source -> Copy -> fake-container file (0 allocs/op measured) |
| `BenchmarkRemuxPackets` | file->file packet remux (demux -> Copy -> mux, 0 allocs/op measured) |
| `BenchmarkDecodeToFrameSink` | packets -> decode (fake) -> frame sink (0 allocs/op measured) |
| `BenchmarkDecodeEncode` | decode -> re-encode (fake) -> sink (0 allocs/op measured) |
| `BenchmarkResample` | real bundled filter, 44.1kHz stereo -> 48kHz mono (0 allocs/op measured) |
| `BenchmarkResize` | real bundled filter, 320x180 -> 160x90 I420 (0 allocs/op measured) |
| `BenchmarkBranchFanout/branches=2,8` | one decode, N planned branches to sinks (0 allocs/op measured) |
| `BenchmarkSharedMuxGroup` | audio+video chains sharing one mux destination (0 allocs/op measured) |
| `BenchmarkMix/arms=2,8` | N-arm audio mix on a blocking buffered graph (0 allocs/op measured) |
| `BenchmarkComposite` | 2-arm video composite (0 allocs/op measured) |
| `BenchmarkSelectPassthrough` | one-of-N selector forwarding the active arm (0 allocs/op measured) |
| `BenchmarkAttachDetachUnderLoad` | runtime branch attach+detach per op while live traffic flows (a cold-path control operation, measured against load) |
| `BenchmarkSourcePush/dropping,blocking` | the flow-control hot path: source.Push into a DropOldest vs Blocking queue (bounded by `TestSourcePushDeliveryAllocs`) |
| `BenchmarkLatencyRecordPackets` | packet-record path with p50/p95/p99 `source.Push.Packet` acceptance metrics |
| `BenchmarkSustainedRecordMemory` | bounded packet-record memory smoke, reporting live heap and runtime-reserved memory |
| `BenchmarkRealOpusEncode` / `BenchmarkRealOpusDecode` | standard Opus adapter throughput path, not the goavtest fake codec |
| `BenchmarkRealVP8Encode` / `BenchmarkRealVP9Encode` / `BenchmarkRealAV1Encode` | standard video encoder throughput (real `govpx`/`goav1` adapters, deterministic I420 frames) |
| `BenchmarkRealH264Encode` | skips: the bundle ships H.264 decode-only, so there is no encoder to measure (honest gap, not a fake number) |
| `BenchmarkRealVP8Decode` | standard VP8 decoder throughput, decoding a real keyframe captured from the encoder |
| `BenchmarkSoakRecordDrift` | soak harness: long offline-paced record reporting heap drift, GC cycles, and total GC pause; run with a large `-benchtime` for an extended-stability artifact |

The `pipeline` package adds the executor-level fanout sweeps
(`BenchmarkDirectFanout`, `BenchmarkDirectFanoutParallel`,
`BenchmarkBufferedFanout` over 1/8/64/512 targets), and
`container/matroska` + `container/webm` add demux/remux throughput benches
(`BenchmarkReadWebRTCCorpus`, `BenchmarkReadWebMCorpus`, and write/read
variants) with optional external-tool comparisons (ffprobe/ffmpeg/mkvmerge,
skipped when not installed).

`scripts/bench/perf-lab.sh` runs the performance-lab subset and saves a
timestamped artifact under the checked layout in
[`bench-results/README.md`](../bench-results/README.md):
`bench-results/baseline/<timestamp>/<machine>.txt` for the full transcript,
`bench-results/latency/<scenario>-<timestamp>.json` for p50/p95/p99 summaries,
`bench-results/rss/<scenario>-<timestamp>.json` for heap/sys/RSS summaries,
`bench-results/pressure/<scenario>-<timestamp>.json` for drop/backpressure
smoke, `bench-results/control/<scenario>-<timestamp>.json` for attach/detach
under load, `bench-results/fanout/<scenario>-<timestamp>.json` for 1/8/64/512
fanout, `bench-results/live-sync/<scenario>-<timestamp>.json` for
live-room sync latency, drift, and drop smoke,
`bench-results/container/<scenario>-<timestamp>.json` for Matroska/WebM corpus smoke,
and `bench-results/pprof/<scenario>-<timestamp>/cpu.out` plus `mem.out` for
profiles. The Go benchmarks report latency quantiles, heap/sys metrics,
drop/backpressure costs, attach/detach under load, fanout sweep costs, real
Opus encode/decode throughput, live-room sync drift/drops
(`BenchmarkLiveRoomSync`), and container corpus smoke. Optional external
field-corpus benches stay skipped unless `GOAV_MATROSKA_FIELD_CORPUS` or
`GOAV_WEBM_FIELD_CORPUS` is set. The script wraps the memory benchmark with
`/usr/bin/time` on Linux and macOS so max RSS lands in the artifact when the
host exposes it. CI runs a `PERF_BENCHTIME=1x` smoke to catch bit rot and
uploads the generated benchmark artifacts. Serious performance claims still
need same-machine, longer-run artifacts attached to a release.

On pull requests, CI also runs `scripts/bench/ci-compare.sh` against the PR base
commit and uploads `bench-base.txt`, `bench-current.txt`, and
`benchstat-pr-vs-base.txt`. This is an advisory same-runner comparison for a
small benchmark subset; it catches obvious drift and gives reviewers a
benchstat table, but it is not a release-quality performance claim.

## Experimental

Performance characteristics that exist but are expected to change; treat the
current numbers as snapshots, not contracts:

- **Buffered copy-mode fanout**: borrowed payloads are copied into each
  target's slot (`BenchmarkBufferedFanout/copy` measures the per-target cost);
  a refcounted zero-copy fanout would remove it.
- **Runtime attach under load**: `BenchmarkAttachDetachUnderLoad` measures a
  cold-path control operation; its cost is dominated by planning and is not a
  data-plane figure.
- **Performance lab smoke**: the latency, memory, pressure, control, fanout,
  live-sync, container, and real-Opus rows prove the harness shape, not production
  percentiles, soak behavior, or reference-hardware throughput at realistic
  durations.
- **PR benchstat artifact**: `scripts/bench/ci-compare.sh` compares a small
  subset against the PR base on the same runner. It is useful review signal,
  not a pass/fail timing gate.

## Not proven

Stated plainly so the docs never imply otherwise:

- **Published tail-latency baselines** under representative long runs. The
  percentile harness exists, but the committed smoke numbers are not production
  evidence.
- **RSS under sustained load** (1h/6h). The perf-lab script captures bounded
  max RSS/heap smoke only.
- **Real-codec throughput on reference hardware**. The harnesses now exist
  (`BenchmarkReal{VP8,VP9,AV1}Encode`, `BenchmarkRealVP8Decode`,
  `BenchmarkRealOpus*`), so per-codec throughput is now *measurable and
  reproducible*; it becomes a *claim* only once same-machine artifacts are
  committed. H.264 encode is not measurable here — the bundle is decode-only.
- **Sustained-load soak** (hours-long stability, fragmentation, drift). The
  `BenchmarkSoakRecordDrift` harness exists and reports heap drift / GC cost;
  an extended `-benchtime` run produces the artifact, which is not yet committed.
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
throughput, soak drift) into `bench-results/`. Those files are machine-specific
and `.gitignored` by design — a number is only evidence on the machine that
produced it. To release an artifact, run the harness on the target hardware and
attach the output to the release, naming the host and Go version:

```sh
PERF_BENCHTIME=2000x scripts/bench/perf-lab.sh           # full smoke
go test -run '^$' -bench BenchmarkSoakRecordDrift \
  -benchtime=10m -benchmem .                             # extended soak artifact
```

A throughput, tail-latency, soak, or scaling claim is only valid with the
matching committed artifact; until then PERFORMANCE.md lists it under
"Not proven".
