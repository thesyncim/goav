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
- Minimal runtimes can still use explicit adapter registration; the beginner
  recipe path uses a standard in-repo adapter bundle through `Default()`.
- Codec internals live in sibling modules; `goav` provides adapter boundaries.
- Tests must include allocation guards for implemented hot paths.
- Every new fluent workflow must pass through the recursive working loop in
  `docs/LOOP.md`: simple expression, explicit graph, narrow implementation,
  allocation/event proof, tracker update.

## Package Status

| Area | Status | Next |
| --- | --- | --- |
| `av` | reset helpers, ownership docs, RTP timebase helpers, allocation-free timestamp and duration rescale/compare helpers | richer timestamp metadata helpers |
| `codec` | Into-style contracts, descriptors with one owner for identity/modes/status and capability lists for concrete media compatibility, descriptor-driven backend discovery, explicit registry, optional decode-state provisioning, decoder pipeline stages, event-consuming encoder stages, decode bounds for realtime adapter scratch planning, explicit unsupported live codec-switch guard for bound decoder stages | richer concrete adapter alloc tests |
| `format` | Into-style read/write contracts, registry, default static prober, demux source with stream-added/EOS lifecycle events, mux stage | richer stream probing and more containers |
| `pipeline` | direct executor, bounded buffered executor, fanout, one route model with one-to-many targets, stream/event scoped routing labeled as `stream=...` or `event=...`, backpressure guard, allocation-free drop-policy decisions, preallocated copy slots for borrowed media buffers, buffered runtime transcode and live receive proofs, detail-aware graph specs as the core inspection object | richer realtime lifecycle proof |
| `rtpav` | Pion boundary, static payload map, sequence loss detector, jitter ring, timestamp discontinuity detection, Opus/VP8/VP9/AV1/H264 depacketizers, RTCP feedback helpers, pipeline source, depacketizer event delivery, codec-change payload-map refresh including new-codec depacketizer handoff when registered, replacement-stream identity adoption for single-stream readers, targeted old-ID replacement for multi-stream readers, stream-scoped EOS | richer multi-stream receive |
| `webrtcav` | single `NewSession` PeerConnection entry, TrackSet multi-track coordinator, replaceable TrackRemote readers, stream mapping, payload map boundary, track codec-update events, RTCP feedback bridge | live graph composition helpers |
| `filter` | Into-style resize/resample result contract, explicit factory registry, event-preserving frame-transform pipeline stage | richer concrete filters later |
| `transcode` | one explicit `Plan` contract, rendition-to-output selection model, resize/resample branch insertion through filter factories | richer branch planning |
| runtime | recipe front door with `Record`, `From`, `Decode`, `Transcode`, stream-scoped audio/video recipe builders, stream-local `Resize`/`Resample` transforms, actionable stream-selection and stream-mismatch diagnostics, first-stream `StreamIndex(0)` selection, `FileInput`, single `FileOutput` output constructor, `WebRTCTrack`, multi-input realtime `From(input).And(other...)` composition, RTP codec intent, codec/resize/resample specs, standard `Default()` adapter bundle, function stage/sink adapters, handle-based `Runtime.Graph()` advanced builder with `Source/Stage/Sink` handles and `Connect`, runtime-owned codec/format/filter registries extended by adapter hooks, private graph compiler loop, decoder state-provider hook, RTP decode-bound hints for high-level receive, compatibility `Routes(goav.Route(...))` builder path plus `.ByStream(...)`/`.ByEvent(...)` modifiers, pre-build and task graph descriptions with node details, high-level remux/fanout compiler, type-selected decode graphs that can follow codec-change replacement streams with old-ID or replacement-ID targets and fail explicitly on different-codec live switches, selected-stream decode-to-sink compilers with optional filter stages for file/protocol and RTP/WebRTC receive, selected-stream decode/filter/encode-to-output compilers for file/protocol and RTP/WebRTC receive, recipe encode guardrails for current Opus/VP8/VP9 readiness, shared-decode multi-rendition `Transcode(plan)` compiler with transform branches, buffered multi-output transcode proof, multi-RTP/WebRTC packet-reader record/fanout compiler with buffered borrowed-payload proof | intent compiler passes and graph subflows |
| adapters | `ivf` packet demux/mux active; `annexb` H264 packet mux active; `resample` S16 audio filter active; `resize` I420/YUV420P video filter active; `gopus` Opus decoder active; `goh264` H264 decoder active behind `goav_goh264` with adapter-owned allocation and lifecycle guards; `govpx` VP8/VP9 decoders and encoders active behind `goav_govpx` with caller-owned I420/packet-buffer guards; `goav1` descriptor-only by default and active behind `goav_goav1` with caller-owned decoder state, runtime state provisioning from RTP decode bounds, low-overhead AV1 decode, concrete raw RTP payload decode, high-level RTP receive and replacement-stream codec-change proof for old-ID and replacement-ID event targets, borrowed gray8/I420/I422/I444 frame mapping with yuv420p/yuv422p/yuv444p accepted as aliases, runner reuse, keyframe requests, drop-until-sync recovery from packet markers or parsed payloads, allocation guards, and lifecycle proof; default-build optional video adapters report unavailable factories explicitly | richer AV1 RTP/WebRTC recovery and output formats |

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
16. Simplify explicit graph references to node-to-node routes with
    stream/event route labels, while keeping graph descriptions equivalent to
    runtime graphs. Done.
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
23. Simplify explicit fluent graph routing with stream/event-scoped routes.
    Done.
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
31. Simplify explicit graph fanout with multi-target routes. Done.
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
    carry useful workflow labels while preserving the simple node-to-node API.
    Done.
38. Add allocation-free `av.TimeBase`, `av.Timestamp`, and `av.Duration`
    helpers for RTP/media/std-duration rescaling, plus first adapter use in
    tagged VP8 encode FPS selection. Done.
39. Add timestamp delta/compare helpers and let `rtpav.Source` emit
    discontinuity events for backward timestamps or configured max-gap
    thresholds, with fluent `WithRTPMaxTimestampGap` graph detail. Done.
40. Promote `pipeline.Route` as the simple direct graph edge API and thread
    runtime explicit graph helpers through it. Done.
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
48. Collapse the runnable graph edge surface to the `Route` model by removing
    secondary graph methods while preserving stream/event routing.
    Done.
49. Reserve the `goav_goav1` optional adapter boundary and pin the sibling AV1
    RTP stream-runner API with a tagged compile proof, without registering a
    premature decoder factory. Done.
50. Add a tagged AV1 `DecoderState` binder that owns exact-format frame pools,
    retained RTP scratch, event/parser scratch, reference/output slots, and
    reusable backend runtime handles from `codec.DecodeConfig.Bounds` plus
    adapter-specific scratch sizing. Done.
51. Collapse the builder edge vocabulary so fanout is expressed as multiple
    route targets instead of a separate public branch verb. Done.
52. Add tagged AV1 low-overhead payload planning and backend runner binding on
    `DecoderState`, with a tiny valid-stream run proof over caller-owned
    scratch, frame pool, and worker pool. Done.
53. Add a build-tagged AV1 decoder factory over caller-owned `DecoderState`,
    depacketized low-overhead OBU payloads, borrowed `gray8`/I420 frame planes,
    loss keyframe requests, runner reuse, result-capacity proof, allocation
    guards, and deterministic close behavior. Done.
54. Add tagged AV1 drop-until-sync recovery after packet loss, discontinuity,
    corrupt packet drops, and codec-change events, using either the
    `av.Packet.Keyframe` marker produced by realtime depacketizers or parsed
    low-overhead sequence/key-frame payload contents. Done.
55. Add optional `codec.DecodeStateFactory` provisioning so high-level decode
    builders can open adapters that need large caller-owned arenas, then prove
    tagged AV1 RTP receive through stream-scoped RTP decode recipes with
    runtime-provided decoder state. Done.
56. Prove same-stream tagged AV1 RTP codec-change recovery through the
    high-level receive builder: payload-map refresh, epoch update,
    drop-until-sync, keyframe request, and resumed decode on the next sync
    packet. Done.
57. Prove tagged AV1 RTP receive for planar 8-bit 4:2:0 through the high-level
    builder, accepting both `i420` and `yuv420p` declarations and emitting
    canonical `i420` frame planes. Done.
58. Simplify explicit graph authoring with route-map helpers and stream/event
    route constructors. Done.
59. Add concrete tagged AV1 raw RTP payload decode through
    `DecodeRTPPayloadInto`, with separate planning RTP scratch, retained
    fragment preservation across rebinds, and after-loss recovery that preserves
    known sequence state. Done.
60. Let single-stream RTP/WebRTC codec-change events carry replacement stream
    identity through source stream state, video depacketizers, EOS, and
    type-selected runtime decode graphs while keeping ID-pinned selectors
    strict. Prove tagged AV1 replacement-stream recovery through stream-scoped
    RTP decode recipes. Done.
61. Let targeted multi-stream RTP codec-change events use `Event.StreamID` for
    the old stream and `Event.Stream` for the replacement identity, canonicalize
    accepted events downstream, and prove AV1/H264 depacketizer plus tagged AV1
    runtime recovery for old-ID replacement targets. Done.
62. Let RTP sources switch payload maps across codecs when a matching
    depacketizer is present, and make selected runtime decode graphs fail
    explicitly with `codec.ErrUnsupportedCodecSwitch` when a live codec-change
    event would require a different decoder factory. Done.
63. Add `WithRTPDecodeBounds(...)` so high-level RTP decode builders can pass
    payload, retained-fragment, output-count, and geometry limits into decoder
    result scratch and adapter-provided decode state. Render tuned RTP nodes as
    carrying decode bounds. Done.
64. Simplify friendly explicit graph authoring to one public route constructor:
    `Routes(goav.Route(...))`, with stream/event matching expressed through
    `.ByStream(...)` and `.ByEvent(...)`; remove older duplicate high-level
    spellings. Done.
65. Add tagged AV1 8-bit planar 4:2:2 and 4:4:4 format contracts: descriptors
    advertise `i422`/`yuv422p` and `i444`/`yuv444p`, stream aliases bind exact
    backend frame pools, and decoded frame mapping emits canonical `i422` and
    `i444` borrowed planes. Done.
66. Collapse RTP depacketizer configuration to one variadic public option:
    `WithRTPDepacketizers(...)` handles one or many depacketizers. Done.
67. Collapse the remaining pipeline edge vocabulary to one `Route` value:
    remove the old exported edge alias and standalone constructor helper, keep
    `Graph.Connect(route)` as the graph action, and route planner/build internals
    through the same type. Done.
68. Prune unused public planning vocabulary: transcode keeps one explicit
    `Plan` path through `Runtime.Transcode(plan)`, and graph specs use
    `pipeline.NodeRef` directly instead of a duplicate node helper. Done.
69. Keep graph inspection core and diagram generation optional:
    `pipeline.Spec` is the structured runtime graph object, while text and
    diagram exporters live in the small `graphrender` utility package instead
    of the pipeline core. Done.
70. Prune unused receive factory vocabulary: WebRTC sessions use one
    `NewSession(ctx, config)` entry, and RTP receive keeps the direct
    `PacketReader`/`FeedbackWriter` contracts without a speculative receiver
    factory config. Done.
71. Collapse graph construction to one public path: `pipeline.NewGraph(config)`
    selects direct or bounded buffered execution from `GraphConfig.Buffer`; the
    concrete direct/buffered runners and the runtime graph factory hook stay
    private. Done.
72. Prune runtime registry getters: registry wiring is configured through
    `goav.New(...)` options and adapter hooks, while runtime users keep the
    forward path of `Probe`, `New`, `Describe`, `Build`, and `Run`. Done.
73. Collapse registry setup to one registration style: codec, format, and
    filter registries use explicit `Register...` methods instead of parallel
    constructor option helpers. Done.
74. Collapse runtime registry wiring to adapter hooks: the runtime owns concrete
    codec, format, and filter registries directly; custom replacement registry
    options and unused registry interface types are removed. Done.
75. Prune unused observability hooks: runtime logging and metrics placeholders
    are removed until concrete event or metrics semantics exist. Done.
76. Collapse codec descriptor capability duplication: codec identity, media
    type, modes, realtime, and experimental status live on `Descriptor`, while
    `Capabilities` carries only concrete media compatibility lists. Done.
77. Prune the central planned-backend list: adapter discovery now has one path
    through descriptors registered by adapter packages. Done.
78. Prune the bundled RTP receiver interface: receive keeps the direct
    `PacketReader` and `FeedbackWriter` contracts without a second combined
    name. Done.
79. Prune unused codec control request types: decoder stages expose the
    implemented keyframe request path and drop unproduced reset/drop request
    names. Done.
80. Prune the format prober getter: probing is configured through explicit
    prober registration and consumed through `Registry.Probe`, without exposing
    registry internals. Done.
81. Prune filter descriptor-list registration: transform planning keeps one
    registry path through `RegisterFactory` and `Factory`, while filters still
    describe themselves at the adapter boundary. Done.
82. Collapse demux lifecycle controls: `DemuxSource` always emits discovered
    streams and EOS, so container input follows the same first-class lifecycle
    event model as realtime receive. Done.
83. Prune the filter stage event-drop knob: filters observe events and preserve
    them, while graph routes remain the one way to decide where events flow.
    Done.
84. Prune mux packet passthrough: `MuxStage` writes packets and emits muxer
    events, while packet fanout remains one graph route with one or many
    targets. Done.
85. Prune the mux input-event drop switch: mux is an output boundary that
    consumes upstream events after the graph observes them and only emits
    muxer-produced events downstream. Done.
86. Prune the encoder input-event drop switch: encoders observe events and
    flush delayed packets, while event fanout stays in graph routes. Done.
87. Add the recipe/intent front door: `Record`, `From`, `Decode`,
    `Transcode`, file/URI/RTP specs, codec/transform presets, standard
    `Default()` adapters, `Describe`, and acceptance tests that keep the
    README-level API small. Done.
88. Add function adapters for custom packet, frame, event, and sink hooks so
    simple processing does not require full graph interface implementations.
    Done.
89. Add `Runtime.Graph()` as the named advanced builder entry while keeping
    `Runtime.New()` as a compatibility alias. Done.
90. Give runtime demux sources bounded packet payload storage so default
    file-record recipes can run through real packet demuxers such as IVF
    without per-packet growth. Done.
91. Promote the expert graph escape hatch to handle-based wiring:
    `Runtime.Graph()` names nodes once, then connects `Out`, `In`, `Stream`,
    and `Event` handles while still compiling to the existing route graph.
    Done.
92. Add stream-scoped recipe builders on `From(input)`: selected audio/video
    streams can `Decode`, `Do` custom stages, encode with the currently ready
    Opus/VP8/VP9 recipe targets, and route to outputs while lowering through
    the existing selected-stream runtime compilers. H264 and AV1 recipe encode
    paths return an explicit work-in-progress diagnostic. Done.
93. Add WebRTC recipe input constructors: `WebRTCTrack(track)` and
    `WebRTCRemote(remote)` adapt Pion tracks through `webrtcav` into the same
    RTP receive graph, derive codec/depacketizer intent from the track stream,
    and report invalid tracks through recipe build diagnostics. Done.
94. Add multi-input realtime recipe composition: `From(input).And(other...)`
    accepts repeated RTP/WebRTC inputs and lowers through the existing realtime
    receive compiler, while non-realtime multi-input attempts fail with an
    actionable diagnostic. Done.
95. Add actionable stream-selection diagnostics: ambiguous and missing stream
    selections report matching candidates, suggest stream-scoped selectors, and
    `StreamIndex(0)` now selects the first stream instead of meaning unset.
    Done.
96. Prune the duplicate file output alias so recipes use one constructor:
    `FileOutput(name, writer)`. Done.
97. Add stream-local recipe transforms: `Audio().Resample(...)` and
    `Video().Resize(...)` lower through the existing filter registry before
    encode or frame sinks, and Opus recipe defaults no longer override
    transformed audio geometry unless the user sets codec parameters. Done.
98. Add stream-mismatch diagnostics for filter and encode requests, plus
    missing encode-target diagnostics that preserve `ErrUnsupportedBuild`.
    Done.
99. Tighten transcode branch targets so `.To(...)` validates output labels
    before planning and empty labels fail with actionable output guidance.
    Done.
100. Add intent-layer transcode diagnostics for missing branches and branches
    without output routes, preserving the same unsupported-build compatibility
    sentinel. Done.
101. Add intent-layer transcode transform diagnostics so wrong-media transforms
    and transform chains fail before the single-transform plan silently drops
    intent. Done.
102. Tighten recipe encode target validation so unknown codec specs, automatic
    codec selection, copy requests, and H264/AV1 work-in-progress targets fail
    as actionable build diagnostics. Done.
103. Tighten RTP recipe codec intent so built-in depacketizer wiring is limited
    to Opus, VP8, VP9, H264, and AV1, while manual depacketizers remain the
    custom codec escape hatch. Done.
104. Add recipe output validation so nil frame sinks, empty output specs, and
    file outputs without writers fail before graph compilation. Done.
105. Add recipe RTP reader validation so nil packet readers fail before source
    construction while codec-intent diagnostics still cover valid readers.
    Done.
106. Add plain recipe input validation so zero-value input specs fail with
    constructor guidance before format probing. Done.
107. Remove text rendering from `pipeline.Spec` so core graph inspection stays
    structured-only and all string/diagram exporters live in `graphrender`.
    Done.
108. Add recipe custom-stage validation so `.Do(nil)` fails with stage guidance
    instead of looking like an empty resize/resample transform. Done.
109. Make function stage/sink adapters return nil for nil callbacks so bad
    hooks fail through normal recipe stage or sink diagnostics. Done.
110. Add recipe transform value validation so non-positive resize dimensions,
    sample rates, and channel counts fail before filter compilation. Done.
111. Add recipe stream selector validation so negative `StreamIndex(...)` values
    fail before probing with direct selector guidance. Done.
112. Add recipe encode value validation so negative bitrates and invalid
    explicit audio overrides fail before encoder construction. Done.
113. Add transcode output-label validation so duplicate named outputs fail
    before a later definition can silently replace an earlier one. Done.
114. Add shared recipe output-label validation so repeated file, URI, or frame
    sink output names fail before graph construction. Done.
115. Add ordinary stream-recipe validation so repeated `Audio()`/`Video()`
    selections fail instead of replacing the first selected stream. Done.
116. Add transcode branch-name validation so duplicate rendition handles fail
    before outputs can refer to an ambiguous branch. Done.
117. Add transcode branch-name validation so empty branch names fail before
    hidden fallback rendition handles are generated. Done.
118. Add per-branch transcode output validation so one branch cannot route the
    same rendition to the same output more than once. Done.
119. Add repeated realtime-input validation so explicit RTP/WebRTC recipe names
    are unique before graph source handles are planned. Done.
120. Add stream-recipe output-scope validation so selected `Audio()`/`Video()`
    chains use stream-local `.To(...)` instead of mixed generic job outputs.
    Done.
121. Add stream-recipe output-kind validation so one selected stream chain
    cannot mix decoded frame sinks with muxed file/URI outputs. Done.
122. Carry explicit recipe output formats into ordinary record/stream builders,
    and reject writer-only file outputs that provide no name, URI, MIME type, or
    format signal. Done.
123. Wrap missing format probe, demuxer, and muxer registry failures with
    actionable build diagnostics that name the input/output and adapter role.
    Done.
124. Add recipe-level `.MIME(...)` customization for inputs and outputs so
    unnamed readers and writer-backed outputs can still drive format probing.
    Done.
125. Add recipe RTP policy validation so negative buffer limits and invalid
    timestamp-gap durations fail before source construction. Done.
126. Add URI-target graph rendering in the optional `graphrender` package so
    `pipeline.Spec` stays structured-only while exports gain one simple target
    path.
    Done.
127. Add stream-chain order validation so ordinary stream recipes and transcode
    branches reject processing steps after the terminal encoder and reject
    duplicate encoders instead of silently reordering or replacing intent.
    Done.
128. Let the beginner `Record(input, output...)` recipe fan out to multiple
    outputs directly while preserving `UseRuntime(...)` on the same call.
    Done.
129. Lower recipe RTP codec intent after packet-reader stream discovery so
    unnamed single-stream readers keep their stream identity without manual
    depacketizer wiring.
    Done.
130. Make the top-level `Decode(input, output)` recipe use `FrameSink(...)`
    output specs, matching the rest of the recipe API and rejecting mux outputs
    with direct guidance.
    Done.
131. Prune transcode branch-local direct outputs so branches route through one
    label-only `.To(...)` path and outputs are defined once with `.Output(...)`.
    Done.
132. Promote recipe `.Run(ctx)` as the beginner execution path in the README and
    acceptance tests, while keeping `.Build(ctx)` for task events, graph specs,
    and explicit lifecycle control.
    Done.
133. Reject empty transcode output definition labels so `.Output(...)` uses one
    explicit stable label path instead of falling back to filenames or generated
    handles.
    Done.
134. Let stream-local frame sinks and frame-processing steps imply decode so
    selected stream recipes read as `Audio().To(FrameSink(...))`,
    `Audio().Resample(...).Opus(...).To(...)`, or
    `Video().Resize(...).VP9(...).To(...)` while muxed outputs still require an
    explicit encoder.
    Done.
135. Remove the redundant transcode branch `.Decode()` method because transcode
    branches decode by definition before transforms and the terminal encoder.
    Done.
136. Remove the redundant ordinary stream `.Decode()` method because frame
    sinks, custom stages, transforms, and encoders already imply decode intent.
    Done.
137. Align use-case RTP/WebRTC and generic recipe examples with the beginner
    `.Run(ctx)` path, and add a default `From(...).To(...).Run(ctx)` fanout
    acceptance test.
    Done.
138. Keep `gofmt`, `go test ./...`, allocation guards, and no-cgo hygiene green.

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
  with direct routes plus stream/event match options, and expose generated
  graph specs before or after build. Runtime-created nodes and any explicit
  node that implements the optional node-describer contract can include short
  graph details without introducing lower-level executor vocabulary into the
  public builder. Optional diagram/text generation lives in `graphrender`.
- The runtime builder can also plan and compile simple remux/fanout jobs from
  `Input(...).Output(...).Build(ctx)` when registered format adapters can probe,
  demux, and mux the selected boundaries.
- The recipe/runtime path can plan and compile selected-stream decode jobs from
  stream-scoped `.To(goav.FrameSink(...))` recipes when format probing
  resolves one matching stream and the codec registry has a decoder factory. The
  graph includes an explicit stream-select stage so unrelated packets do not
  reach the decoder, and optional filter stages can run before the sink.
- `adapters/ivf` provides a narrow packet recording boundary for one VP8, VP9,
  or AV1 video stream with allocation-guarded demux/mux hot paths.
- The runtime builder can plan and compile RTP/WebRTC packet-reader record jobs
  from `RTP(...).Output(...).Build(ctx)`, including jitter and the variadic
  depacketizer option, repeated RTP/WebRTC inputs, aggregated stream lists for
  muxers, multiple mux outputs, lifecycle closure, graph specs, and event
  visibility.
- The recipe/runtime path can plan and compile selected-stream live decode jobs
  from stream-scoped RTP/WebRTC `.To(goav.FrameSink(...))` recipes,
  including repeated RTP/WebRTC inputs, graph specs, decoder lifecycle closure,
  and filtering of unrelated packets and stream-scoped EOS before they reach the
  decoder.
  Ordered filter stages can run between decode and the sink when their selector
  matches the decoded stream. Decoder factories that implement
  `codec.DecodeStateFactory` can provision adapter-specific state for this
  high-level path before `NewDecoder`.
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
- `rtpav.Source` refreshes payload maps on `EventCodecChanged`, can adopt a
  replacement stream identity for single-stream readers, can update a targeted
  stream in multi-stream readers when the event names the old ID and carries a
  replacement stream, and keeps EOS and timestamp tracking aligned with the
  accepted identity. With multiple depacketizers registered, payload-map refresh
  can hand packets to a new codec depacketizer. Video depacketizers update
  matching stream epochs or replacement identities while dropping partial video
  until the next sync frame.
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
  payloads through caller-owned or runtime-provided `DecoderState`, and exposes
  a concrete `DecodeRTPPayloadInto` path for raw AV1 RTP aggregation payloads.
  It maps decoded 8-bit gray8/I420/I422/I444 backend frames into borrowed
  `av.Frame` planes, accepts yuv420p/yuv422p/yuv444p aliases as the same planar
  layouts, resets and requests keyframes after loss/discontinuity/corrupt
  packets, drops packets until a packet keyframe marker or parseable
  sequence/key-frame payload, updates identity on codec-change events, reuses
  the bound runner in steady state, and has allocation, result-capacity,
  sync-recovery, high-level RTP receive, raw RTP retained-fragment and
  after-loss recovery, same-stream and
  replacement-stream RTP codec-change recovery for old-ID and replacement-ID
  event targets, and close-lifecycle tests.
- `codec.DecodeBounds` gives future realtime adapters a small common place to
  receive payload, retained-fragment, output-count, and geometry limits while
  keeping adapter-specific arenas behind documented `OpaqueState` types.
  `WithRTPDecodeBounds(...)` now feeds those limits into high-level RTP decode
  builders and graph descriptions.
- `pipeline.BufferPolicy` now has one allocation-free decision point for
  direct backpressure, oldest/newest dropping, wait-for-sync recovery, and
  non-key-video dropping. This keeps the future bounded executor from spreading
  policy logic across connection code.
- `pipeline.NewGraph` selects bounded buffered execution whenever
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
  low-overhead AV1 decode over caller-owned or runtime-provided state, borrowed
  gray8/I420/I422/I444 output with yuv420p/yuv422p/yuv444p accepted as planar
  aliases, loss keyframe requests, drop-until-sync recovery from packet markers
  or parsed payloads, concrete raw RTP payload decode with retained-fragment
  recovery, high-level RTP receive and replacement-stream codec-change proofs
  for old-ID and replacement-ID event targets, runner reuse, and
  allocation/lifecycle guards.
- `adapters/goh264`: descriptor-only by default; `goav_goh264` activates a
  decode factory for 8-bit planar H264 frames, with adapter-owned allocation
  and lifecycle guards active.

## Done Criteria

| Gate | Evidence | State |
| --- | --- | --- |
| Clear minimal architecture | `README.md`, `docs/ARCHITECTURE.md`, package boundaries | active |
| Simple high-level API | runtime builder, named graph routes, remux/fanout compiler, decode-to-sink compiler, RTP record/fanout compiler, selected encode-to-output compiler, shared-decode transcode compiler with transform branches | first slices active |
| Explicit low-level API | `pipeline`, `codec`, `format`, `rtpav`, `webrtcav` contracts | active |
| Realtime Opus vertical slice | RTP/WebRTC boundary, Opus depacketizer, `gopus` decoder | active |
| Allocation guarded hot paths | `testing.AllocsPerRun` guards across core/RTP/codec/format/adapters | active for implemented paths |
| Adapter boundaries | `adapters/ivf`, `adapters/annexb`, `adapters/gopus`, `adapters/govpx`, `adapters/goav1`, `adapters/goh264` | active |
| No cgo | `hygiene_test.go` | active |
| Lightweight core imports | codec modules isolated under `adapters/...` | active |
| Docs explain shape | README, architecture, adapters, performance, RTP/WebRTC docs | active |

## Next Slices

1. Choose one workflow and write its shortest fluent expression.
2. Add the private graph compiler and structured graph spec first; keep
   rendering/export outside core.
3. Add the smallest runtime behavior behind existing stages/adapters.
4. Add allocation, event, lifecycle, and graph-equivalence tests for that slice.
5. Update this tracker with the new evidence and next pressure point.

Current pressure point: broaden tagged AV1 decode toward real RTP/WebRTC
receive, especially dynamic new-codec graph rebind policy, richer scratch
auto-sizing policy, runtime stream fixtures for broader planar formats, and
deciding whether the concrete raw RTP payload path should remain low-level or
grow a high-level builder policy.

## Validation Gates

- `go test ./...`
- allocation tests for reset/results/pipeline/RTP/depacketize/adapters
- benchmarks for passthrough, RTP Opus depacketize, fanout no-copy, and gopus
  decode adapter
- no core cgo imports
- lifecycle tests for start/close/flush/late-after-close behavior
