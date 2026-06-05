# Progress

Compact tracker for the current `goav` buildout.

## Mission

Build `goav` as a pure-Go media runtime: simple at the edge, explicit and
composable inside, realtime-first, zero steady-state hot-path allocations, and
ready for codec adapters over `gopus`, `govpx`, `goav1`, and `goh264`.

## Non-negotiables

- Pure Go only: no cgo and no FFmpeg/GStreamer runtime dependency.
- Core stays small, stable, and codec/container agnostic.
- Hot paths use caller-owned buffers, result structs, and preallocated slices.
- Per-packet/per-frame paths must avoid hidden heap allocation after warm-up.
- RTP metadata, timestamps, loss, discontinuity, codec epochs, backpressure, and
  keyframe requests are first-class data/events.
- Pion RTP/RTCP/WebRTC types stay at RTP/WebRTC package boundaries.
- Optional codec/container integrations stay out of the core import graph.
- Codec internals live in sibling modules; `goav` provides adapter boundaries.
- Tests must include allocation guards for implemented hot paths.

## Package Status

| Area | Status | Next |
| --- | --- | --- |
| `av` | reset helpers, ownership docs, RTP timebase helpers | richer timestamp conversion helpers |
| `codec` | Into-style contracts, capabilities, explicit registry | adapter descriptors and alloc tests |
| `format` | Into-style read/write contracts | probe registry and first demuxer boundary |
| `pipeline` | direct executor, fanout, stream/event routes, backpressure guard | bounded async edges and drop-policy tests |
| `rtpav` | Pion boundary, static payload map, sequence loss detector, jitter ring, Opus depacketizer | RTCP feedback and richer payload formats |
| `webrtcav` | Pion boundary sketch | TrackRemote adapter/source boundary |
| `filter` | Into-style resize/resample result contract | concrete allocation-safe filters later |
| `transcode` | ladder contracts | graph compiler boundary |
| adapters | not started | `gopus` first, then `govpx`, `goav1`, `goh264` |

## Implementation Order

1. Inspect and tighten current public contracts.
2. Convert hot-path contracts to caller-owned `Into` style.
3. Add `Reset` helpers and allocation tests for core hot-path structs.
4. Add explicit codec registry implementation.
5. Add minimal direct-call pipeline executor. Done.
6. Add RTP static payload map, sequence/loss detector, jitter ring, Opus
   depacketizer. Done.
7. Add `gopus` adapter and RTP Opus to PCM vertical slice.
8. Add compile-safe adapter skeletons for `govpx`, `goav1`, and `goh264`.
9. Add examples and docs for simple API, graph API, ownership, and adapters.
10. Keep `gofmt`, `go test ./...`, allocation guards, and no-cgo hygiene green.

## First Vertical Slice

```text
Pion TrackRemote or RTP packet source
  -> payload map
  -> sequence/loss detector
  -> Opus RTP depacketizer
  -> codec.Decoder using gopus
  -> caller-owned PCM av.Frame
```

Required proof:

- packet loss emits `EventPacketLoss`
- discontinuity emits `EventDiscontinuity`
- keyframe request emits `EventKeyframeRequired` where relevant
- hot-path allocation tests pass after warm-up
- core package imports stay lightweight

## Adapter Targets

- `adapters/gopus`: Opus decode first, PLC via loss events, encode if API is
  straightforward.
- `adapters/govpx`: VP8/VP9 decode/encode where stable; preserve I420 planes.
- `adapters/goav1`: AV1 decode path first; encode only if honestly ready.
- `adapters/goh264`: Annex B/AVCC, SPS/PPS, IDR, FU-A/STAP-A boundaries; keep
  default build safe if the module is unavailable.

## Validation Gates

- `go test ./...`
- allocation tests for reset/results/pipeline/RTP/depacketize/adapters
- benchmarks for passthrough, RTP Opus depacketize, fanout no-copy, and gopus
  decode adapter
- no core cgo imports
- lifecycle tests for start/close/flush/late-after-close behavior
