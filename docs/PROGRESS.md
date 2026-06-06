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
- Every new fluent workflow must pass through the recursive working loop in
  `docs/LOOP.md`: simple expression, explicit graph, narrow implementation,
  allocation/event proof, tracker update.

## Package Status

| Area | Status | Next |
| --- | --- | --- |
| `av` | reset helpers, ownership docs, RTP timebase helpers | richer timestamp conversion helpers |
| `codec` | Into-style contracts, capabilities, explicit registry, decoder and encoder pipeline stages | richer concrete adapter alloc tests |
| `format` | Into-style read/write contracts, registry, default static prober, demux source, mux stage | richer stream probing and more containers |
| `pipeline` | direct executor, fanout, simple node-to-node links, stream/event routes, backpressure guard, graph specs with text/DOT/Mermaid rendering | bounded async edges and drop-policy tests |
| `rtpav` | Pion boundary, static payload map, sequence loss detector, jitter ring, Opus/VP8/VP9/AV1/H264 depacketizers, RTCP feedback helpers, pipeline source, depacketizer event delivery, codec-change payload-map refresh | WebRTC renegotiation mapping |
| `webrtcav` | Pion PeerConnection session, track accept queue, TrackRemote reader, stream mapping, payload map boundary, RTCP feedback bridge | codec-change/session renegotiation events |
| `filter` | Into-style resize/resample result contract | concrete allocation-safe filters later |
| `transcode` | ladder contracts | graph compiler boundary |
| runtime | `goav.New` options, adapter registration hooks, private graph compiler loop, simple named graph connections, explicit Source/Stage/Sink builder graphs with links/routes, pre-build and task graph descriptions, high-level remux/fanout compiler, high-level selected-stream decode-to-sink compiler, RTP packet-reader record/fanout compiler | encode/filter/transcode graph compilers |
| adapters | `ivf` packet demux/mux active; `annexb` H264 packet mux active; `gopus` Opus decoder active; `govpx`, `goav1`, `goh264` descriptor boundaries | concrete video adapters |

## Implementation Order

1. Inspect and tighten current public contracts.
2. Convert hot-path contracts to caller-owned `Into` style.
3. Add `Reset` helpers and allocation tests for core hot-path structs.
4. Add explicit codec registry implementation.
5. Add minimal direct-call pipeline executor. Done.
6. Add RTP static payload map, sequence/loss detector, jitter ring, Opus
   depacketizer. Done.
7. Add `gopus` adapter and RTP Opus to PCM vertical slice. RTP packet to PCM
   frame is covered; WebRTC TrackRemote now maps into the RTP reader boundary.
8. Add compile-safe adapter descriptor boundaries for `govpx`, `goav1`, and
   `goh264`. Done.
9. Add examples and docs for simple API, graph API, ownership, and adapters.
   Runtime options and adapter docs are started.
10. Add a first concrete format adapter for packet recording. IVF demux/mux is
   active for VP8, VP9, and AV1.
11. Add the high-level RTP packet-reader record/fanout graph compiler. Done.
12. Add bounded VP8/VP9 RTP video depacketizers for packet-preserving recording.
   Done.
13. Add bounded AV1 RTP video depacketizer for packet-preserving recording.
   Done.
14. Add WebRTC PeerConnection session receive boundary with track acceptance and
   RTCP feedback routing into the existing RTP source. Done.
15. Add RTP codec-change payload-map refresh and depacketizer epoch reset
   proof. Done.
16. Simplify explicit graph references to node-to-node connections while
    keeping text/DOT/Mermaid rendering equivalent to runtime graphs. Done.
17. Add bounded H264 RTP depacketization and Annex B packet recording. Done.
18. Keep `gofmt`, `go test ./...`, allocation guards, and no-cgo hygiene green.

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
- RTP Opus depacketize to `gopus` decode is covered by a compile-time example
  and adapter test.
- WebRTC TrackRemote boundary exposes streams, payload map, RTP reads, metadata,
  and EOS events.
- WebRTC session boundary exposes Pion PeerConnection negotiation, bounded
  remote-track acceptance, stream-added/backpressure events, and RTCP feedback
  writes through the same Pion PeerConnection used by the session.
- RTCP NACK/PLI/FIR helpers use caller-owned feedback scratch.
- RTP packet readers can now feed a direct pipeline source that emits
  `av.Packet` and `av.Event` messages.
- `codec.DecoderStage` converts packet messages into frame messages while
  preserving realtime events, flushing before EOS, and driving PLC from packet
  loss events.
- `codec.EncoderStage` converts frame messages into packet messages while
  preserving realtime events and flushing delayed packets before EOS.
- `format.DemuxSource` and `format.MuxStage` adapt container boundaries into
  the event-aware pipeline without per-packet allocation.
- The runtime builder can plan and compile explicit source/stage/sink graphs
  with links/routes and expose generated graph specs with text, DOT, and
  Mermaid renderers before or after build.
- The runtime builder can also plan and compile simple remux/fanout jobs from
  `Input(...).Output(...).Build(ctx)` when registered format adapters can probe,
  demux, and mux the selected boundaries.
- The runtime builder can plan and compile selected-stream decode jobs from
  `Input(...).Decode(...).Sink(...).Build(ctx)` when format probing resolves
  one matching stream and the codec registry has a decoder factory. The graph
  includes an explicit stream-select stage so unrelated packets do not reach the
  decoder.
- `adapters/ivf` provides a narrow packet recording boundary for one VP8, VP9,
  or AV1 video stream with allocation-guarded demux/mux hot paths.
- The runtime builder can plan and compile RTP/WebRTC packet-reader record jobs
  from `RTP(...).Output(...).Build(ctx)`, including jitter/depacketizer options,
  multiple mux outputs, lifecycle closure, graph rendering, and event
  visibility.
- `rtpav.Source` now forwards realtime events into depacketizers before graph
  delivery, so loss-aware depacketizers can reset or drop partial payloads.
- `rtpav.Source` refreshes payload maps on `EventCodecChanged`, and
  depacketizers update matching stream epochs while dropping partial video until
  the next sync frame.
- `rtpav.NewVP8Depacketizer`, `rtpav.NewVP9Depacketizer`,
  `rtpav.NewAV1Depacketizer`, and `rtpav.NewH264Depacketizer` strip RTP payload
  headers, assemble fragmented frames in bounded scratch, emit
  `EventKeyframeRequired` after loss, keep dropping until sync, and feed the RTP
  record compiler into IVF or Annex B in end-to-end tests.

## Adapter Targets

- `adapters/ivf`: IVF packet demux/mux is active for single-stream VP8, VP9,
  and AV1 recording paths.
- `adapters/annexb`: H264 Annex B packet mux is active for single-stream H264
  recording paths.
- `adapters/gopus`: Opus decode first is active, PLC via loss events works,
  encode adapter remains unclaimed.
- `adapters/govpx`: descriptor boundary exists; concrete VP8/VP9 adapters need
  stable caller-owned frame paths.
- `adapters/goav1`: descriptor boundary exists; concrete AV1 decode path still
  needs capability validation.
- `adapters/goh264`: descriptor boundary exists with `goav_goh264` build-tag
  marker for future concrete integration.

## Done Criteria

| Gate | Evidence | State |
| --- | --- | --- |
| Clear minimal architecture | `README.md`, `docs/ARCHITECTURE.md`, package boundaries | active |
| Simple high-level API | runtime builder, named graph connections, remux/fanout compiler, decode-to-sink compiler, RTP record/fanout compiler | first slices active |
| Explicit low-level API | `pipeline`, `codec`, `format`, `rtpav`, `webrtcav` contracts | active |
| Realtime Opus vertical slice | RTP/WebRTC boundary, Opus depacketizer, `gopus` decoder | active |
| Allocation guarded hot paths | `testing.AllocsPerRun` guards across core/RTP/codec/format/adapters | active for implemented paths |
| Adapter boundaries | `adapters/ivf`, `adapters/annexb`, `adapters/gopus`, `adapters/govpx`, `adapters/goav1`, `adapters/goh264` | active |
| No cgo | `hygiene_test.go` | active |
| Lightweight core imports | codec modules isolated under `adapters/...` | active |
| Docs explain shape | README, architecture, adapters, performance, RTP/WebRTC docs | active |

## Next Slices

1. Choose one workflow and write its shortest fluent expression.
2. Add the private graph compiler and `Describe` output first.
3. Add the smallest runtime behavior behind existing stages/adapters.
4. Add allocation, event, lifecycle, and graph-equivalence tests for that slice.
5. Update this tracker with the new evidence and next pressure point.

Current pressure point: map WebRTC renegotiation or track replacement into
payload-map and codec-change events, then validate concrete H264 decode adapter
behavior without pulling codec internals into the core runtime.

## Validation Gates

- `go test ./...`
- allocation tests for reset/results/pipeline/RTP/depacketize/adapters
- benchmarks for passthrough, RTP Opus depacketize, fanout no-copy, and gopus
  decode adapter
- no core cgo imports
- lifecycle tests for start/close/flush/late-after-close behavior
