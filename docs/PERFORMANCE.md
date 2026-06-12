# Performance

Rule: this repo does not claim performance it cannot prove. Every statement
here is either **proven** (a named test enforces it in CI), **measured** (a
named benchmark reports it, numbers vary by machine), or **not proven**
(explicitly listed). Anything else in the docs is design intent, worded as
intent.

## Design intent

`goav` treats construction and steady-state media flow differently. Cold paths
may allocate: runtime/graph construction, registry setup, recipe planning and
validation, codec open/configuration, format probing, runtime attach.

Hot paths must avoid hidden allocation:

- per RTP packet
- per depacketized packet
- per decoded/encoded frame
- per mux/demux packet
- per direct pipeline message

Once running, stages reuse caller-owned messages, result structs, frame
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
| `SourcePush.Packet` / `SourcePush.Frame` delivery | `goav.TestSourcePushDeliveryAllocs` | 0 |
| `goav.SinkFunc` (collector-free) sink delivery | `goav.TestSinkFuncDeliveryAllocs` | 0 |
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

Benchmarks never run under plain `go test`; run them with:

```sh
scripts/bench/run.sh                  # full suite, -benchmem, saved to bench-results/
scripts/bench/compare.sh old new      # benchstat old-vs-new (same machine!)
```

`compare.sh` needs `go install golang.org/x/perf/cmd/benchstat@latest`.
No baseline numbers are committed: they are machine-dependent, so only
same-machine old-vs-new comparison is meaningful.

The canonical-workload suite (root `bench_test.go`) runs the public recipe
grammar against the deterministic `goavtest` runtime: fake passthrough codecs
and fake byte-faithful containers (so numbers include the fake's serialization
cost, not a real codec's), plus the **real** std resize/resample filters.
Each benchmark builds its task untimed, then pushes exactly `b.N` messages:
ns/op and allocs/op are per-message steady state. ns/op is the latency proxy;
p50/p95 percentiles are future work needing a histogram harness and are not
faked.

| Benchmark | Workload |
|---|---|
| `BenchmarkRecordPackets` | RTP-style record: packet source -> Copy -> fake-container file (0 allocs/op measured) |
| `BenchmarkRemuxPackets` | file->file packet remux (demux -> Copy -> mux, 0 allocs/op measured) |
| `BenchmarkDecodeToFrameSink` | packets -> decode (fake) -> frame sink (0 allocs/op measured) |
| `BenchmarkDecodeEncode` | decode -> re-encode (fake) -> sink (0 allocs/op measured) |
| `BenchmarkResample` | real std filter, 44.1kHz stereo -> 48kHz mono (0 allocs/op measured) |
| `BenchmarkResize` | real std filter, 320x180 -> 160x90 I420 (0 allocs/op measured) |
| `BenchmarkBranchFanout/branches=2,8` | one decode, N planned branches to sinks (0 allocs/op measured) |
| `BenchmarkSharedMuxGroup` | audio+video chains sharing one mux destination (0 allocs/op measured) |
| `BenchmarkMix/arms=2,8` | N-arm audio mix on a blocking buffered graph (0 allocs/op measured) |
| `BenchmarkComposite` | 2-arm video composite (0 allocs/op measured) |
| `BenchmarkSelectPassthrough` | one-of-N selector forwarding the active arm (0 allocs/op measured) |
| `BenchmarkAttachDetachUnderLoad` | runtime branch attach+detach per op while live traffic flows (a cold-path control operation, measured against load) |
| `BenchmarkSourcePush/dropping,blocking` | the flow-control hot path: SourcePush into a DropOldest vs Blocking queue (0 allocs/op measured for both) |

The `pipeline` package adds the executor-level fanout sweeps
(`BenchmarkDirectFanout`, `BenchmarkDirectFanoutParallel`,
`BenchmarkBufferedFanout` over 1/8/64/512 targets), and
`container/matroska` + `container/webm` add demux/remux throughput benches
with optional external-tool comparisons (ffprobe/ffmpeg/mkvmerge, skipped when
not installed).

## Experimental

Performance characteristics that exist but are expected to change; treat the
current numbers as snapshots, not contracts:

- **Buffered copy-mode fanout**: borrowed payloads are copied into each
  target's slot (`BenchmarkBufferedFanout/copy` measures the per-target cost);
  a refcounted zero-copy fanout would remove it.
- **Runtime attach under load**: `BenchmarkAttachDetachUnderLoad` measures a
  cold-path control operation; its cost is dominated by planning and is not a
  data-plane figure.

## Not proven

Stated plainly so the docs never imply otherwise:

- **Tail latency** (p50/p95/p99): ns/op is an average; no percentile harness
  exists yet.
- **Memory footprint** (RSS) under sustained load.
- **Real-codec throughput** on reference hardware: the canonical suite uses
  passthrough fakes by design; only the container packages benchmark real
  parsing work against external tools.
- **Sustained-load soak** (hours-long stability, fragmentation, drift).
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
