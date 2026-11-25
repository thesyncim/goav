# Performance

Rule: this repo does not claim performance it cannot prove. Every statement here
is **proven** (enforced by a CI test), **measured** (reported by a named
benchmark), or **not proven**.

## Hot-Path Rules

Cold paths (graph planning, adapter `Open`, format probing, runtime attach) may
allocate. Steady-state media loops take no per-message mutex
(`TestDataPlaneTakesNoMutex`, `TestDataPlaneNoContendedMutex`) and keep
allocations explicit and bounded:

- Use `Into` methods with caller-owned result structs and `av.NewBuffer` pooled payloads.
- Fanout shares immutable `av.Buffer` references (`Ref`/`Retain`/`Release`), never copies. `-tags goavdebug` poisons released bytes with `0xDB`.
- Optional `runconfig.WithMediaPools(true)` installs per-runtime scratch pools for metadata maps and join frame-plane slices.

## Proven: Allocation Pins (`testing.AllocsPerRun`)

| Path | Test | Enforced |
| --- | --- | --- |
| Direct graph pass-through | `pipeline.TestGraphDirectRunAllocs` | 0 |
| Buffered fanout steady path (1→2, ordinary & pooled 1/8/64/512) | `pipeline.TestGraphBufferedSteadyEmitAllocs`, `pipeline.TestBufferedFanoutRefcountAllocs` | 0 |
| Direct contended & blocked producers | `pipeline.TestGraphDirectContendedEmitAllocs`, `pipeline.TestGraphBufferedParkedProducersAllocs` | 0 |
| Drop-policy & core resets | `pipeline.TestDropControllerDecideAllocs`, `av.TestCoreResetAllocs` | 0 |
| `source.Push` / `component.SinkFunc` delivery | `goav.TestSourcePushDeliveryAllocs`, `goav.TestSinkFuncDeliveryAllocs` | <=1 |
| Select, Mix (2/8 arms), Composite (2 I420 arms) | `goav.TestSelectorPassthroughAllocs`, `goav.TestAudioMixStepAllocs`, `goav.TestVideoCompositeStepAllocs` | 0 |
| Codec, format, and filter stages | `codec.TestDecoderStageAllocs`, `codec.TestEncoderStageAllocs`, `format.TestDemuxSourceAllocs`, `format.TestMuxStageAllocs`, `filter.TestStageAllocs`, `adapters/resize.TestFilterAllocs`, `adapters/resample.TestFilterAllocs` | 0 |
| RTP receive, depacketizers, jitter, feedback | `rtpav.TestSourceStartAllocs`, `rtpav.TestJitterRingAllocs` | 0 |
| Containers & adapters (Matroska, WebM, IVF, Annex B, codecs) | per-package `*Allocs` tests | 0 |

## SIMD & Measured Kernels

`GOEXPERIMENT=simd` (Go 1.27) is the CI and release default; core, codecs, and
filters carry no assembly (`TestNoRepositoryAssembly`; `cameraav` has two
exempted macOS C-ABI callback trampolines). `-tags purego` and default builds
without `GOEXPERIMENT=simd` select scalar fallbacks.

Local measurements on Apple M4 Max (`darwin/arm64`, not a cross-machine guarantee):

- **Codecs** (128×96 clips, `-benchtime=2s -count=5`): `GOEXPERIMENT=simd` cut codec time by ~45% geomean (`BenchmarkRealVP8Decode` 59.5→16.5 µs, `BenchmarkRealVP8Encode` 562→263 µs, `BenchmarkRealVP9Encode` 220→114 µs, `BenchmarkRealAV1Encode` 146→105 µs, `BenchmarkRealOpusEncode` 62.5→47.0 µs, `BenchmarkRealOpusDecode` 14.6→10.8 µs).
- **Audio mix** (`BenchmarkMixS16Kernel`, bit-exact via `TestMixS16KernelMatchesScalarRandom`): 56 ns (2 arms), 313 ns (3 arms), 675 ns (8 arms) SIMD vs 1.68 / 2.24 / 5.3 µs scalar, 0 allocs/op.
- **I420 resize** (`BenchmarkScaleI420Kernel`, bit-exact via `TestScaleFastPathsMatchScalarReference`): single-pass SIMD 2×2 box downscale + in-register SIMD vertical pass + unrolled 3/4-tap horizontal fast paths, all 0 allocs/op:
  - `320x180_to_half`: 11.0 µs SIMD vs 18.5 µs single-pass scalar (was 62–78 µs 2-pass scalar)
  - `640x360_to_half`: 43.0 µs SIMD vs 72.7 µs single-pass scalar (2.0 GB/s)
  - `1280x720_to_half`: 167 µs SIMD vs 295 µs single-pass scalar (2.06 GB/s)
  - `1920x1080_to_1280x720`: 3.12 ms SIMD vs 4.99 ms scalar
- **Composite & resample** (scalar, 0 allocs/op): `BenchmarkCompositeI420BlitKernel/two_arms/160x90` ~1.5 µs (skips full-canvas black fill when a frame covers the canvas); `BenchmarkResampleS16Kernel/stereo44100_to_mono48000/20ms` ~4.8–6.2 µs.

## Running Benchmarks & Perf Lab

```sh
scripts/bench/run.sh                  # full suite, -benchmem
scripts/bench/compare.sh old new      # benchstat old-vs-new (same machine)
scripts/bench/baseline.sh check       # CI alloc/B-op ratchet (bench-results/reference/*.json)
scripts/bench/pgo.sh                  # refresh default.pgo + default.pgo.meta
scripts/bench/perf-lab.sh             # latency quantiles, heap/sys/RSS, soak, manifest
scripts/bench/ci-compare.sh BASE_REF  # PR-vs-base benchstat artifact
```

`scripts/bench/perf-lab.sh` writes artifacts under `bench-results/` (see [`bench-results/README.md`](../bench-results/README.md)), including `bench-results/manifest/perf-lab-<timestamp>.json` and `bench-results/soak/<scenario>-<timestamp>.json` (covering `BenchmarkLiveRoomAttachDetachSoak` with `max_rss_B` and `churn_interval`). CI runs `PERF_BENCHTIME=1x` with `release_quality=false` to catch bit rot; release claims require `PERF_RELEASE_QUALITY=true` with duration-based `PERF_SOAK_BENCHTIME` and `PERF_LIVE_ROOM_CHURN_BENCHTIME` (`PERF_GO_TEST_TIMEOUT` defaults to `0` in release-quality mode).
