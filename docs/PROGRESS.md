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
| `av` | reset helpers, ownership docs, RTP timebase helpers, allocation-free timestamp and duration rescale/compare helpers | richer timestamp metadata helpers |
| `codec` | Into-style contracts, capabilities, explicit registry, decoder and encoder pipeline stages, decode bounds for realtime adapter scratch planning | richer concrete adapter alloc tests |
| `format` | Into-style read/write contracts, registry, default static prober, demux source, mux stage | richer stream probing and more containers |
| `pipeline` | direct executor, bounded buffered executor, fanout, first-class node-to-node and one-to-many connections, stream/event scoped routing, backpressure guard, allocation-free drop-policy decisions, preallocated copy slots for borrowed media buffers, buffered runtime transcode and live receive proofs, graph specs with detail-aware text/DOT/Mermaid rendering | richer realtime lifecycle proof |
| `rtpav` | Pion boundary, static payload map, sequence loss detector, jitter ring, timestamp discontinuity detection, Opus/VP8/VP9/AV1/H264 depacketizers, RTCP feedback helpers, pipeline source, depacketizer event delivery, codec-change payload-map refresh, stream-scoped EOS for single-stream readers | richer multi-stream receive |
| `webrtcav` | Pion PeerConnection session, TrackSet multi-track coordinator, replaceable TrackRemote readers, stream mapping, payload map boundary, track codec-update events, RTCP feedback bridge | live graph composition helpers |
| `filter` | Into-style resize/resample result contract, explicit registry, frame-transform pipeline stage | richer concrete filters later |
| `transcode` | ladder contracts, rendition-to-output selection model, resize/resample branch insertion through filter factories | richer branch planning |
| runtime | `goav.New` options, codec/format/filter adapter registration hooks, private graph compiler loop, simple named graph connections with multi-target fanout and stream/event scoped variants, explicit Source/Stage/Sink builder graphs, pre-build and task graph descriptions with node details, high-level remux/fanout compiler, selected-stream decode-to-sink compilers with optional filter stages for file/protocol and RTP/WebRTC receive, selected-stream decode/filter/encode-to-output compilers for file/protocol and RTP/WebRTC receive, shared-decode multi-rendition `Transcode(plan)` compiler with transform branches, buffered multi-output transcode proof, multi-RTP/WebRTC packet-reader record/fanout compiler with buffered borrowed-payload proof | next codec adapter validation |
| adapters | `ivf` packet demux/mux active; `annexb` H264 packet mux active; `resample` S16 audio filter active; `resize` I420/YUV420P video filter active; `gopus` Opus decoder active; `goh264` H264 decoder active behind `goav_goh264` with adapter-owned allocation and lifecycle guards; `govpx` VP8/VP9 decoders and encoders active behind `goav_govpx` with caller-owned I420/packet-buffer guards; `goav1` descriptor-only by default and active behind `goav_goav1` with caller-owned decoder state, low-overhead AV1 decode, borrowed gray8/I420 frame output, runner reuse, keyframe requests, allocation guards, and lifecycle proof; default-build optional video adapters report unavailable factories explicitly | richer AV1 RTP/WebRTC recovery and output formats |

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
18. Add WebRTC track codec-update events that refresh RTP payload maps and
    depacketizer epochs through the existing RTP source. Done.
19. Add WebRTC track replacement updates that swap the underlying RTP reader
    while reusing the same codec-change event path. Done.
20. Add WebRTC TrackSet orchestration that keeps one long-lived reader per
    logical stream while applying accepted replacement tracks through
    `UpdateTrack`. Done.
21. Add runtime-level multi-RTP/WebRTC input graph composition from repeated
    `RTP(...)` calls into shared mux outputs. Done.
22. Separate descriptor-only codec discovery from factory availability with
    `codec.ErrUnavailable`, including an H264 runtime decode build proof. Done.
23. Simplify explicit fluent graph routing to `ConnectStream(...)` and
    `ConnectEvent(...)`. Done.
24. Add build-tagged `goh264` decoder factory with real Annex B decode proof
    into borrowed video planes and keyframe request behavior after loss. Done.
25. Add runtime-level RTP/WebRTC selected-stream decode-to-sink compiler, with
    stream-scoped RTP source EOS so unrelated live inputs do not flush the
    selected decoder. Done.
26. Allow selected decode paths to insert ordered filter stages before the sink
    for both file/protocol and RTP/WebRTC receive. Done.
27. Add runtime-level selected decode/filter/encode-to-output compilers for
    file/protocol inputs and RTP/WebRTC receive, with target-codec validation,
    mux fanout, graph-equivalence tests, and encoder EOS flush proof. Done.
28. Add `Transcode(plan)` compiler for one selected decode feeding multiple
    named encoder branches and mux outputs that select renditions by name or
    label. Done.
29. Add filter registry and frame-transform stage, then let transcode renditions
    insert resize/resample branch stages when matching filter factories are
    registered. Done.
30. Add pure-Go S16 audio resample filter adapter with linear interpolation,
    channel conversion, caller-owned output buffers, and allocation guard. Done.
31. Simplify explicit graph fanout with multi-target `Connect(...)`,
    `ConnectStream(...)`, and `ConnectEvent(...)`. Done.
32. Add pure-Go I420/YUV420P video resize filter adapter with exact, fit, fill,
    and passthrough modes, caller-owned output planes, runtime branch scratch
    allocation, and allocation guard. Done.
33. Harden build-tagged H264 decode with allocation guards for adapter-owned
    borrowed-frame mapping and keyframe request emission, codec-change identity
    proof, and deterministic close behavior. Done.
34. Add build-tagged `govpx` VP8 decoder factory with caller-owned I420 output
    buffers, startup/loss drop-until-sync behavior, keyframe requests,
    codec-change identity proof, allocation guards, and deterministic close
    behavior. Done.
35. Extend build-tagged `govpx` decode to VP9 with the same caller-owned I420
    output contract, drop-until-keyframe receive behavior, codec-change
    identity proof, allocation guard, and deterministic close behavior. Done.
36. Add build-tagged `govpx` VP8 encoder factory with caller-owned packet
    output buffers, keyframe request handling, descriptor merge proof,
    allocation guard, and deterministic close behavior. Done.
37. Add detail-aware graph specs so runtime-created sources, select stages,
    codec stages, transcode transforms, RTP receive nodes, and mux outputs
    render useful text/DOT/Mermaid labels while preserving the simple
    node-to-node API. Done.
38. Add allocation-free `av.TimeBase`, `av.Timestamp`, and `av.Duration`
    helpers for RTP/media/std-duration rescaling, plus first adapter use in
    tagged VP8 encode FPS selection. Done.
39. Add timestamp delta/compare helpers and let `rtpav.Source` emit
    discontinuity events for backward timestamps or configured max-gap
    thresholds, with fluent `WithRTPMaxTimestampGap` graph detail. Done.
40. Promote `pipeline.Connection` as the simple direct graph connection API and
    thread runtime explicit graph helpers through it. Done.
41. Add build-tagged `govpx` VP9 encoder factory with caller-owned packet
    output buffers, keyframe request handling, descriptor merge proof,
    allocation guard, and deterministic close behavior. Done.
42. Add `codec.DecodeBounds` so realtime adapters can merge caller-provided
    stream limits with adapter defaults before binding large scratch. Done.
43. Add an allocation-free pipeline drop controller for `BufferPolicy` so
    backpressure, drop-oldest, drop-newest, wait-for-sync, and non-key-video
    decisions are tested before bounded async execution uses them. Done.
44. Add bounded buffered graph execution behind the existing `BufferPolicy`
    surface, with immutable-message pass-through, borrowed-payload rejection,
    drop-oldest, drop-newest, and backpressure tests. Done.
45. Add preallocated buffered media copy slots for borrowed packet/frame
    outputs, bounded by `BufferPolicy`, while preserving immutable pass-through,
    drop behavior, and unsafe-lifetime rejection when no bound is configured.
    Done.
46. Prove `WithBufferPolicy(...).Transcode(plan)` on a multi-output runtime
    graph: unsafe encoder-owned packet payloads fail without copy bounds and
    reach every mux output with copied bytes when `CopyPacketBytes` is set.
    Done.
47. Prove `WithBufferPolicy(...).RTP(...).Output(...)` on a live receive
    record/fanout graph: unsafe depacketizer-owned packet payloads fail without
    copy bounds and reach every mux output with copied bytes when
    `CopyPacketBytes` is set. Done.
48. Collapse the runnable graph edge surface to the `Connection` model by
    removing secondary graph methods while preserving stream/event routing.
    Done.
49. Reserve the `goav_goav1` optional adapter boundary and pin the sibling AV1
    RTP stream-runner API with a tagged compile proof, without registering a
    premature decoder factory. Done.
50. Add a tagged AV1 `DecoderState` binder that owns exact-format frame pools,
    retained RTP scratch, event/parser scratch, reference/output slots, and
    reusable backend runtime handles from `codec.DecodeConfig.Bounds` plus
    adapter-specific scratch sizing. Done.
51. Collapse the builder edge vocabulary to `Connect`, `ConnectStream`, and
    `ConnectEvent`, with fanout expressed as multiple targets instead of a
    separate public branch verb. Done.
52. Add tagged AV1 low-overhead payload planning and backend runner binding on
    `DecoderState`, with a tiny valid-stream run proof over caller-owned
    scratch, frame pool, and worker pool. Done.
53. Add a build-tagged AV1 decoder factory over caller-owned `DecoderState`,
    depacketized low-overhead OBU payloads, borrowed `gray8`/I420 frame planes,
    loss keyframe requests, runner reuse, result-capacity proof, allocation
    guards, and deterministic close behavior. Done.
54. Keep `gofmt`, `go test ./...`, allocation guards, and no-cgo hygiene green.

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
- `av` timebase helpers rescale RTP timestamps, media durations, and standard
  Go durations without allocation so adapters share the same clock-domain math.
- RTP receive can emit `EventDiscontinuity` for timestamp regressions or
  application-configured timestamp gaps before the affected packet reaches
  downstream stages.
- RTP packet readers can now feed a direct pipeline source that emits
  `av.Packet` and `av.Event` messages.
- `codec.DecoderStage` converts packet messages into frame messages while
  preserving realtime events, flushing before EOS, and driving PLC from packet
  loss events.
- `codec.EncoderStage` converts frame messages into packet messages while
  preserving realtime events and flushing delayed packets before EOS.
- `filter.Stage` converts frame messages into transformed frame messages while
  preserving realtime events and flushing before EOS.
- `format.DemuxSource` and `format.MuxStage` adapt container boundaries into
  the event-aware pipeline without per-packet allocation.
- The runtime builder can plan and compile explicit source/stage/sink graphs
  with direct connections plus stream/event route options, and expose generated
  graph specs with text, DOT, and Mermaid renderers before or after build.
  Runtime-created nodes and any explicit node that implements the optional
  node-describer contract can include short graph details without introducing
  lower-level executor vocabulary into the public builder.
- The runtime builder can also plan and compile simple remux/fanout jobs from
  `Input(...).Output(...).Build(ctx)` when registered format adapters can probe,
  demux, and mux the selected boundaries.
- The runtime builder can plan and compile selected-stream decode jobs from
  `Input(...).Decode(...).Sink(...).Build(ctx)` when format probing resolves
  one matching stream and the codec registry has a decoder factory. The graph
  includes an explicit stream-select stage so unrelated packets do not reach the
  decoder, and optional filter stages can run before the sink.
- `adapters/ivf` provides a narrow packet recording boundary for one VP8, VP9,
  or AV1 video stream with allocation-guarded demux/mux hot paths.
- The runtime builder can plan and compile RTP/WebRTC packet-reader record jobs
  from `RTP(...).Output(...).Build(ctx)`, including jitter/depacketizer options,
  repeated RTP/WebRTC inputs, aggregated stream lists for muxers, multiple mux
  outputs, lifecycle closure, graph rendering, and event visibility.
- The runtime builder can plan and compile selected-stream live decode jobs from
  `RTP(...).Decode(...).Sink(...).Build(ctx)`, including repeated RTP/WebRTC
  inputs, graph rendering, decoder lifecycle closure, and filtering of
  unrelated packets and stream-scoped EOS before they reach the decoder.
  Ordered filter stages can run between decode and the sink when their selector
  matches the decoded stream.
- The runtime builder can plan and compile selected-stream encode jobs from
  `Input(...).Decode(...).Filter(...).Encode(...).Output(...).Build(ctx)` and
  `RTP(...).Decode(...).Filter(...).Encode(...).Output(...).Build(ctx)`. The
  graph shares the selected decode/filter prefix, requires an explicit target
  codec, forwards EOS far enough to flush encoders, and can fan one encoded
  packet stream to multiple mux outputs.
- The runtime builder can plan and compile shared-decode transcode jobs from
  `Transcode(plan).Build(ctx)` when all renditions resolve to one selected
  stream. Each rendition becomes a named encoded stream, outputs can select
  renditions by name or label, and graph description is equivalent before and
  after build. Resize and resample branch configs insert filter stages when
  matching filter factories are registered, preallocate branch frame scratch
  when geometry is known, and fail explicitly when missing.
- `rtpav.Source` now forwards realtime events into depacketizers before graph
  delivery, so loss-aware depacketizers can reset or drop partial payloads.
- `rtpav.Source` refreshes payload maps on `EventCodecChanged`, and
  depacketizers update matching stream epochs while dropping partial video until
  the next sync frame.
- `webrtcav.TrackReader.UpdateCodec` accepts new Pion codec parameters or a
  custom payload map, bumps the stream epoch, emits `EventCodecChanged`, and is
  covered by an RTP-source test that depacketizes packets using the new payload
  type.
- `webrtcav.TrackReader.UpdateTrack` accepts a replacement `RemoteTrack`, swaps
  the underlying RTP reader for the same logical stream, emits
  `EventCodecChanged`, and is covered by an H264 RTP-source test that drops
  until the replacement sync frame.
- `webrtcav.TrackSet` accepts session tracks, adds one reader per new logical
  stream, applies later same-stream tracks through `UpdateTrack`, preserves
  reader order, and owns reader closure without closing the session.
- `rtpav.NewVP8Depacketizer`, `rtpav.NewVP9Depacketizer`,
  `rtpav.NewAV1Depacketizer`, and `rtpav.NewH264Depacketizer` strip RTP payload
  headers, assemble fragmented frames in bounded scratch, emit
  `EventKeyframeRequired` after loss, keep dropping until sync, and feed the RTP
  record compiler into IVF or Annex B in end-to-end tests.
- Descriptor-only codec adapters such as default-build `govpx`,
  default-build `goav1`, and default-build `goh264` remain visible in registry
  discovery, but decode build attempts fail with `codec.ErrUnavailable` until a
  concrete factory is registered.
- With `goav_goh264`, `adapters/goh264` registers a concrete decoder factory
  over `github.com/thesyncim/goh264`, decodes real Annex B samples into
  borrowed `av.Frame` video planes, resets on codec-change/discontinuity
  events, requests keyframes after packet loss, and has allocation guards for
  adapter-owned frame mapping and loss request emission plus closed-state
  lifecycle tests.
- With `goav_govpx`, `adapters/govpx` registers concrete VP8 and VP9 decoder
  factories over `github.com/thesyncim/govpx`, decodes real samples into
  caller-owned I420 `av.Frame` planes, drops inter frames until a keyframe after
  startup or loss, requests keyframes through `codec.ControlRequest`, resets on
  codec-change/discontinuity events, and has allocation guards for adapter
  output preparation and loss request emission plus closed-state lifecycle
  tests. The same build tag now registers VP8 and VP9 encode, mapping
  caller-owned I420 frames into caller-owned packet buffers, forcing keyframes
  on request or discontinuity, and preserving merged descriptors for decode and
  encode.
- With `goav_goav1`, `adapters/goav1` registers a concrete decoder factory over
  `github.com/thesyncim/goav1`, consumes depacketized low-overhead AV1 OBU
  payloads through caller-owned `DecoderState`, maps decoded 8-bit gray8/I420
  backend frames into borrowed `av.Frame` planes, resets and requests keyframes
  after loss/discontinuity/corrupt packets, updates identity on codec-change
  events, reuses the bound runner in steady state, and has allocation,
  result-capacity, and close-lifecycle tests.
- `codec.DecodeBounds` gives future realtime adapters a small common place to
  receive payload, retained-fragment, output-count, and geometry limits while
  keeping adapter-specific arenas behind documented `OpaqueState` types.
- `pipeline.BufferPolicy` now has one allocation-free decision point for
  direct backpressure, oldest/newest dropping, wait-for-sync recovery, and
  non-key-video dropping. This keeps the future bounded executor from spreading
  policy logic across connection code.
- `pipeline.BufferedGraph` is selected by the default factory whenever
  `GraphConfig.Buffer` is non-direct. It runs sources and downstream nodes with
  bounded per-node queues, copies message headers into queue slots, applies the
  shared drop controller, shares immutable media buffers, copies borrowed packet
  payloads and frame planes into policy-bounded preallocated slots, and rejects
  borrowed media when no copy bound is configured.
- Runtime `Transcode(plan)` uses the same graph policy: a shared-decode
  multi-rendition graph fails on unsafe encoder-owned packet payloads without a
  copy bound and delivers copied encoded payloads to multiple mux outputs when
  `CopyPacketBytes` is configured.
- Runtime `RTP(...).Output(...)` record/fanout uses that policy too:
  depacketizer-owned packet payloads fail without a copy bound and copied
  payloads reach every mux output when `CopyPacketBytes` is configured.

## Adapter Targets

- `adapters/ivf`: IVF packet demux/mux is active for single-stream VP8, VP9,
  and AV1 recording paths.
- `adapters/annexb`: H264 Annex B packet mux is active for single-stream H264
  recording paths.
- `adapters/resample`: S16 audio resample and channel conversion filter is
  active, using caller-owned output buffers and allocation-guarded hot paths.
- `adapters/resize`: I420/YUV420P video resize filter is active, using
  caller-owned output planes and allocation-guarded hot paths.
- `adapters/gopus`: Opus decode first is active, PLC via loss events works,
  encode adapter remains unclaimed.
- `adapters/govpx`: descriptor-only by default; `goav_govpx` activates VP8 and
  VP9 decode into caller-owned I420 frames plus VP8 and VP9 encode into
  caller-owned packet buffers.
- `adapters/goav1`: descriptor-only by default; `goav_goav1` activates first
  low-overhead AV1 decode over caller-owned state, borrowed gray8/I420 output,
  loss keyframe requests, runner reuse, and allocation/lifecycle guards.
- `adapters/goh264`: descriptor-only by default; `goav_goh264` activates a
  decode factory for 8-bit planar H264 frames, with adapter-owned allocation
  and lifecycle guards active.

## Done Criteria

| Gate | Evidence | State |
| --- | --- | --- |
| Clear minimal architecture | `README.md`, `docs/ARCHITECTURE.md`, package boundaries | active |
| Simple high-level API | runtime builder, named graph connections, remux/fanout compiler, decode-to-sink compiler, RTP record/fanout compiler, selected encode-to-output compiler, shared-decode transcode compiler with transform branches | first slices active |
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

Current pressure point: broaden tagged AV1 decode toward real RTP/WebRTC
receive, especially sync detection after loss, codec-switch recovery, and
additional output formats without expanding the core API.

## Validation Gates

- `go test ./...`
- allocation tests for reset/results/pipeline/RTP/depacketize/adapters
- benchmarks for passthrough, RTP Opus depacketize, fanout no-copy, and gopus
  decode adapter
- no core cgo imports
- lifecycle tests for start/close/flush/late-after-close behavior
