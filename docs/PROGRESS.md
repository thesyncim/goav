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

## Current Goal

Take `goav` to the next level by simplifying hard. The project is not released,
so breaking changes are preferred over compatibility sugar when they remove a
second way to express the same work. GStreamer is only a maturity checklist for
media-flow behavior, not vocabulary to copy.

The rule is:

```text
one way to express work
one planner
one operation model
one destination model
one branch model
one runtime extension model
```

The public grammar is:

```text
From(input) -> Chain -> operations -> Tap -> Branch -> Destination -> Task
```

The public nouns are `Input`, `Stream`, `Operation`, `Tap`, `Branch`,
`Destination`, `Flow`, and `Task`. Remove or demote beginner-facing `Target`,
`Record`, `Transcode`,
decode helpers, `Path`, `Output`/`Outputs`, `To("label")`, `Flow.To(...)`,
compatibility shims, and graph handles as the normal composer. `Destination` is
the routing handle: reusing the same `Destination` value groups branches into
one sink or mux destination. External destination behavior belongs behind
providers and constructors; goav owns destination identity so shared groups are
reliable.

The implementation target is explicit:

- direct streams are syntax sugar for an implicit `Branch("main")`;
- direct streams, planned branches, runtime attached branches, and flows all use
  one ordered `OperationSpec` list;
- every fluent operation appends exactly one operation: decode, copy, shape,
  transform, stage, encode, or tap;
- `Destination` is the target: file, URI, writer, object, frame sink, packet
  sink, event sink, and shared mux/sink group are all destination values with
  stable identity; external implementations use a provider contract instead of
  becoming routing identities themselves;
- one planner lowers normalized composition into a `WorkPlan` for initial build
  and a `WorkPatch` for runtime attach;
- `Explain`, `Build`, `Attach`, and `Snapshot` read the same planned work;
- normal composition does not import `goav/transcode`, carry labels, carry string
  destination names, or dispatch by workflow kind;
- root `Runtime` stays small; graph composition is internal or expert-only, not
  the normal public API;
- observation remains ordinary branch composition from typed taps with `Do(...)`
  and `Sink(...)` unless a separate API proves necessary;
- custom sources become symmetric with custom operations, sinks, writers, and
  object destinations;
- shape validation is central across inputs, operations, flows, taps, branches,
  sinks, byte destinations, shared destinations, and custom sources;
- branch buffering, detach policy, destination commit/abort/close, branch drops,
  lifecycle state, and safe media ownership are first-class planner/executor
  responsibilities;
- shape owns structural compatibility facts such as domain, media kind, codec,
  format, resolution, pixel/sample formats, sample rate, channels, stream
  identity, and realtime intent; `CodecSpec.Settings` owns bitrate, FPS,
  keyframe interval, profile/level, quality, speed/deadline, typed configs,
  named params, controls, and adapter knobs. `.Encode(codec)` is the only
  fluent encode step; tuning values live in codec options, not on stream or
  branch builders.

The attached direction is now part of the goal. The required convergence points
are:

- `File`, `URIOut`, `Writer`, `Object`, `Sink`, and `Custom` return stable
  destination handles; same handle means shared group, different handle with the
  same name means a planning error unless the planner can prove equivalence.
- `DestinationProvider` is the extension point for custom byte/object/sink
  behavior. `Destination` itself is the goav-owned handle and is not a public
  graph node, route label, or adapter registry key.
- Direct `.To(...)` streams are only ergonomic syntax. Internally they lower to
  the same branch model as `Branches(Branch("main").To(...))`.
- `BranchSpec`, chain state, runtime attached branches, and `FlowSpec` converge
  on one ordered operation list. Parallel fields such as `decode`, `steps`,
  `transforms`, `encode`, post-encode taps, labels, and destination-name bridge fields are
  migration debt until they disappear.
- Initial build emits `WorkPlan`; runtime attach emits `WorkPatch` from the same
  planner against existing typed taps. Attach validates destinations and opens
  them before graph mutation, then rolls back cleanly on failure.
- `branchComposePlan`, `runtimeBranch`, `destinationNames` bridge fields, string output refs,
  transcode imports in normal composition, and workflow-kind compiler switches
  are explicit removal targets, not extension points.
- `Runtime.Graph`, graph handles, source/stage/sink node wiring, and route APIs
  belong only in internal or expert-only packages. Normal advanced work is done
  with custom sources, `Do`, `Sink`, `Writer`, `Object`, `Flow`, `Branch`, and
  `Task.Attach`.
- `Flow` stays boring: reusable operations plus media kind, shape contract,
  taps, and explanation. It has no destination, branch source, route name,
  runtime state, or lifecycle policy.
- `Shape` validates every input, operation, flow, tap, branch, sink, byte
  destination, shared destination, codec, transform, and custom source with
  expected-vs-actual diagnostics.
- Observation and live diagnostics remain branch work from typed taps:
  `Branch(...).From(tap).Do(...).To(Sink(...))`. Add a separate observation API
  only if branch composition cannot express a real use case.
- `Snapshot` remains the inspection surface alongside `Events` and stats; do
  not add bus/caps/pad/bin vocabulary.
- `Source(name, shape, func(ctx, push) error, ...)` is now active for packet,
  frame, and event-domain custom input.
- `From(inputs...)` must support multi-input audio/video composition without
  graph handles, with explicit stream selectors and ambiguity diagnostics.
- Acceptance gates must forbid normal README examples from using `Record`,
  `Transcode`, `Decode(input, ...)`, `Path`, `Output`, `Outputs`, `Target`,
  `.To("label")`, `Runtime.Graph`, or graph handles.

## Package Status

| Area | Status | Next |
| --- | --- | --- |
| `av` | reset helpers, ownership docs, RTP timebase helpers, allocation-free timestamp and duration rescale/compare helpers | richer timestamp metadata helpers |
| `codec` | Into-style contracts, descriptors with one owner for identity/modes/status and capability lists for concrete media compatibility, descriptor-driven backend discovery with cloned descriptor metadata, explicit registry, optional decode-state provisioning, decoder pipeline stages, event-consuming encoder stages, decode bounds for realtime adapter scratch planning, explicit unsupported live codec-switch guard for bound decoder stages | richer concrete adapter alloc tests |
| `format` | Into-style read/write contracts, registry with container descriptors, default static prober, demux source with stream-added/EOS lifecycle events, mux stage | richer stream probing and more containers |
| `pipeline` | direct executor with running stage/sink attachment, stoppable dynamic branch removal, bounded buffered executor with running stage/sink attachment and dynamic branch removal, fanout, one route model with one-to-many targets, stream/event scoped routing labeled as `stream=...` or `event=...`, backpressure guard, allocation-free drop-policy decisions, preallocated copy slots for borrowed media buffers, buffered runtime transcode and live receive proofs, detail-aware graph specs as the core inspection object | richer realtime lifecycle proof |
| `graphrender` | optional graph-spec renderer with one URI-based public entry point, kept outside `pipeline` core | later hosted/shareable graph views |
| `rtpav` | Pion boundary, static payload map, sequence loss detector, jitter ring, timestamp discontinuity detection, Opus/VP8/VP9/AV1/H264 depacketizers, RTCP feedback helpers, pipeline source, depacketizer event delivery, codec-change payload-map refresh including new-codec depacketizer handoff when registered, replacement-stream identity adoption for single-stream readers, targeted old-ID replacement for multi-stream readers, stream-scoped EOS | richer multi-stream receive |
| `webrtcav` | single `NewSession` PeerConnection entry, TrackSet multi-track coordinator, replaceable TrackRemote readers, stream mapping, payload map boundary, track codec-update events, RTCP feedback bridge | live graph composition helpers |
| `filter` | Into-style resize/resample result contract, explicit factory registry with descriptor capability metadata for pixel formats, sample formats, and resize modes, event-preserving frame-transform pipeline stage | richer concrete filters later |
| `transcode` | internal migration `Plan` contract, branch-to-output selection model, mixed audio/video output grouping, resize/resample branch insertion through filter factories | retire as a special runtime path in favor of generic `MediaPlan` branches |
| runtime | composition-first `From(input)` recipe front door with packet-preserving `Copy().To(...)`, explicit chain `.Decode()`, audio/video chains, typed `Branch`, typed `Tap`, `Destination`, and `Chain` composition, `Branches(...)` planned splits from one selected stream, canonical `Flow(name).Audio()/Video()` operation sequences applied to chains, planned branches, and runtime branches, chain-local `Resize`/`Resample` transforms, ordered branch operation intent with custom stage, transform, tap, and encode steps, actionable stream-selection and stream-mismatch diagnostics, first-stream `StreamIndex(0)` selection, `FileInput`, direct `File`/`URIOut`/`Sink` destinations, custom `Writer` destinations with `DestinationInfo`, stable destination handles for shared mux/sink groups, `WebRTCTrack`, multi-input realtime `From(input).And(other...)` composition, RTP codec intent, built-in and generic `Codec` specs, standard `Default()` adapter bundle, function stage/sink adapters, probe-only `Runtime` interface plus explicit `goav.Expert(runtime).Graph()` advanced builder quarantined outside the README front door, runtime-owned codec/format/filter registries extended by adapter hooks, custom codec registration through `WithDecoder` and `WithEncoder`, private recipe intent compiler state with validation, `MediaPlan` branch IR and `Explain(ctx)` branch/decision/tap reports with probed/live stream shape, shared-operation markers, and operation output shapes, recipe-owned branch-compose plan for declared branches, advanced `transcode.Plan` boundary adaptation, graph-plan recognition required for normal recipe build/describe, planned `pipeline.Spec` emission for `Job`, decoder state-provider hook, RTP decode-bound hints for high-level receive, executable graph-plan boundary for normal recipe build/describe, pre-build and task graph descriptions with node details, high-level remux/fanout compiler, type-selected decode graphs that can follow codec-change replacement streams with old-ID or replacement-ID targets and fail explicitly on different-codec live switches, selected-stream decode-to-sink compilers with optional filter stages and high-level custom sinks for file/protocol and RTP/WebRTC receive, selected-stream decode/filter/encode-to-output compilers for file/protocol and RTP/WebRTC receive, recipe encode guardrails for H264/AV1 work-in-progress while generic custom codecs flow to adapter validation, grouped audio/video branch compiler with ordered custom stage and transform branches plus shared mux destinations, descriptor-backed codec/filter/container capability reporting with config-specific decode, transform, and encode validation, buffered multi-output proof, live RTP/WebRTC branch compiler, direct and bounded-buffered runtime `Task.Attach` branch attachments from typed taps with custom stages, resize/resample plus decode/encode descriptor preflight, nested frame and packet taps, dependent branches after runtime resize taps, post-encode packet taps feeding dependent packet-copy branches, flow-applied Opus encode-to-destination branches, Opus/VP8/VP9 late encode-to-destination, packet-copy late destination and recording branches, Opus encoded late recording from frame taps, `Attachment.Close(ctx)`/`Task.Detach(ctx, h)`, `Task.Snapshot()` with task stats, stable taps, and active runtime branch states, and multi-RTP/WebRTC packet-reader record/fanout compiler with buffered borrowed-payload proof | finish generic branch lowering behind the branch composer and deepen capability planning |
| adapters | `ivf` packet demux/mux active; `annexb` H264 packet mux active; `resample` S16 audio filter active; `resize` I420/YUV420P video filter active; `gopus` Opus decoder and encoder active; `goh264` H264 decoder active behind `goav_goh264` with adapter-owned allocation and lifecycle guards; `govpx` VP8/VP9 decoders and encoders active by default with caller-owned I420/packet-buffer guards; `goav1` AV1 decoder active by default with caller-owned decoder state, runtime state provisioning from RTP decode bounds, low-overhead AV1 decode, concrete raw RTP payload decode, high-level RTP receive and replacement-stream codec-change proof for old-ID and replacement-ID event targets, borrowed gray8/I420/I422/I444 frame mapping with yuv420p/yuv422p/yuv444p accepted as aliases, runner reuse, keyframe requests, drop-until-sync recovery from packet markers or parsed payloads, allocation guards, and lifecycle proof | richer AV1 RTP/WebRTC recovery and output formats |

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
    named encoder branches and mux outputs that select branches by name or
    label. Done.
29. Add filter registry and frame-transform stage, then let transcode branches
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
47. Prove live RTP receive record/fanout under `WithBufferPolicy(...)`: unsafe
    depacketizer-owned packet payloads fail without copy bounds and reach every
    mux destination with copied bytes when
    `CopyPacketBytes` is set. Done.
48. Collapse the runnable graph edge surface to the `Route` model by removing
    secondary graph methods while preserving stream/event routing.
    Done.
49. Pin the sibling AV1 RTP stream-runner API and register the concrete default
    decoder factory once the runtime-owned state boundary is ready. Done.
50. Add an AV1 `DecoderState` binder that owns exact-format frame pools,
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
64. Simplify explicit graph route planning to one route model with stream/event
    matching, before the later handle-based `Runtime.Graph()` API became the
    public expert path. Done.
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
68. Prune unused public planning vocabulary: transcode keeps one internal
    `Plan` path for compiler tests, and graph specs use `pipeline.NodeRef`
    directly instead of a duplicate node helper. Done.
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
    forward path of `Probe`, graph specs, `Build`, and `Run`. Done.
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
89. Add `Runtime.Graph()` as the named advanced builder entry. Done.
90. Give runtime demux sources bounded packet payload storage so default
    file-record recipes can run through real packet demuxers such as IVF
    without per-packet growth. Done.
91. Promote the expert graph escape hatch to handle-based wiring:
    `Runtime.Graph()` names nodes once, then connects `Out`, `In`, `Stream`,
    and `Event` handles while still compiling to the existing route graph.
    Done.
92. Add stream-scoped recipe builders on `From(input)`: selected audio/video
    streams can `Decode`, `Do` custom stages, encode with the currently ready
    Opus/VP8/VP9 recipe targets, and route to targets while lowering through
    the existing selected-stream runtime compilers. H264 and AV1 recipe encode
    paths return an explicit work-in-progress diagnostic. Done.
93. Add the WebRTC recipe input constructor: `WebRTCTrack(track)` adapts Pion
    tracks through `webrtcav` into the same RTP receive graph, derives
    codec/depacketizer intent from the track stream, and reports invalid tracks
    through recipe build diagnostics. Done.
94. Add multi-input realtime recipe composition: `From(input).And(other...)`
    accepts repeated RTP/WebRTC inputs and lowers through the existing realtime
    receive compiler, while non-realtime multi-input attempts fail with an
    actionable diagnostic. Done.
95. Add actionable stream-selection diagnostics: ambiguous and missing stream
    selections report matching candidates, suggest stream-scoped selectors, and
    `StreamIndex(0)` now selects the first stream instead of meaning unset.
    Done.
96. Prune the duplicate file output alias so recipes use one constructor:
    `File(name, writer)`. Done.
97. Add stream-local recipe transforms: `Audio().Resample(...)` and
	`Video().Resize(...)` lower through the existing filter registry before
	encode or sink destinations, and Opus recipe defaults no longer override
	transformed audio geometry unless the user sets codec parameters. Done.
98. Add stream-mismatch diagnostics for filter and encode requests, plus
    missing encode-target diagnostics that preserve `ErrUnsupportedBuild`.
    Done.
99. Earlier experiment: tighten transcode string destinations so `.To(...)`
    validates destination names before planning. Superseded before release by
    `Destination` handles carried by branches.
    Done.
100. Add intent-layer transcode diagnostics for missing branches and branches
    without output routes, preserving the same unsupported-build compatibility
    sentinel. Done.
101. Add intent-layer transcode transform diagnostics so wrong-media transforms
    and transform chains fail before the single-transform plan silently drops
    intent. Done.
102. Tighten recipe encode target validation so automatic codec selection, copy
    requests, and H264/AV1 work-in-progress targets fail as actionable build
    diagnostics while generic custom codec specs continue to adapter
    capability validation. Done.
103. Tighten RTP recipe codec intent so built-in receive wiring is limited to
    Opus, VP8, VP9, H264, and AV1, while custom payload handling stays in the
    advanced runtime layer. Done.
104. Add recipe output validation so nil sink destinations, empty destination refs, and
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
116. Add transcode branch-name validation so duplicate branch handles fail
    before outputs can refer to an ambiguous branch. Done.
117. Add transcode branch-name validation so empty branch names fail before
    hidden fallback branch handles are generated. Done.
118. Add per-branch transcode output validation so one branch cannot route the
    same branch to the same output more than once. Done.
119. Add repeated realtime-input validation so explicit RTP/WebRTC recipe names
    are unique before graph source handles are planned. Done.
120. Add stream-recipe output-scope validation so selected `Audio()`/`Video()`
    chains use stream-local `.To(...)` instead of mixed generic job outputs.
    Done.
121. Add stream-recipe output-kind validation so one selected stream chain
	cannot mix decoded sink destinations with muxed file/URI outputs. Done.
122. Carry explicit recipe output formats into ordinary record/stream builders,
    and reject writer-only file outputs that provide no name, URI, MIME type, or
    format signal. Done.
123. Wrap missing format probe, demuxer, and muxer registry failures with
    actionable build diagnostics that name the input/output and adapter role.
    Done.
124. Add recipe-level MIME customization for inputs and destination MIME options
    for outputs so unnamed readers and writer-backed outputs can still drive
    format probing.
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
128. Earlier experiment: let a `Record(input, output...)` helper fan out to
    multiple outputs directly. Superseded before release by
    `From(input).Copy().To(outputs...)`. Done.
129. Lower recipe RTP codec intent after packet-reader stream discovery so
    unnamed single-stream readers keep their stream identity without manual
    depacketizer wiring.
    Done.
130. Earlier experiment: make a top-level `Decode(input, output)` helper use
	`Sink(...)` destination refs. Superseded before release by
	`From(input).Stream()/Audio()/Video().Decode().To(Sink(...))`. Done.
131. Earlier experiment: prune transcode branch-local direct outputs so branches
    route through labels and outputs are defined separately. Superseded before
    release by `Destination` handles carried directly by branches.
    Done.
132. Promote recipe `.Run(ctx)` as the beginner execution path in the README and
    acceptance tests, while keeping `.Build(ctx)` for task events, graph specs,
    and explicit lifecycle control.
    Done.
133. Earlier experiment: reject empty transcode output definition labels.
	Superseded before release by `Destination` handles and branch-owned
	destinations.
	Done.
134. Earlier experiment: let stream-local sink destinations and frame-processing steps
	imply decode. Superseded by the clearer explicit `.Decode()` composition
	style used by `From(...).Audio()/Video().Decode()...`. Done.
135. Earlier experiment: remove branch `.Decode()` because branch work decoded
    by definition. Superseded by explicit tap/branch composition where the
    upstream stream names its decoded outlet. Done.
136. Earlier experiment: remove ordinary stream `.Decode()` because sink destinations
	and transforms implied decode. Superseded by the current explicit
	`.Decode()` public story. Done.
137. Align use-case RTP/WebRTC and generic recipe examples with the beginner
    `.Run(ctx)` path, and add a default `From(...).To(...).Run(ctx)` fanout
    acceptance test.
    Done.
138. Collapse optional graph rendering to a single URI-based public entry point,
    keeping text, DOT, and Mermaid choices out of the core and out of the public
    renderer surface.
    Done.
139. Demote the legacy fluent builder from the public `Runtime` interface so
    expert users enter through `Runtime.Graph()` while recipes keep using the
    compiler target internally.
    Done.
140. Remove exported `SelectAudio`, `SelectVideo`, and `Route` helpers so
    stream selection stays recipe-scoped and expert graph wiring stays
    handle-based.
    Done.
141. Make the legacy fluent builder interface package-internal so the public
    top-level API exposes recipes, graph handles, and tasks, not compiler
    plumbing.
    Done.
142. Remove top-level `Input` and `Output` aliases so low-level container
    structs stay in `format`, while recipes keep `FileInput`, `URI`, `RTP`,
    `WebRTCTrack`, `File`, `URIOut`, and `Sink` as the front door.
    Done.
143. Remove top-level `ProbeRequest` and `ProbeResult` aliases so format
    probing details stay in `format`, while `Runtime.Probe` remains available
    for applications that need it.
    Done.
144. Remove top-level `Source`, `Stage`, and `Sink` aliases so graph-kernel
    primitives live in `pipeline`, while recipe helpers still expose
    `Sink`, `PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc`.
    Done.
145. Hide the internal branch-plan compiler method so users stay on
    composition recipes and diagnostics speak in recipe terms instead of
    `transcode.Plan`.
    Done.
146. Remove top-level `Metadata` and `CodecParameters` aliases so low-level
    media structs stay in `av`, while recipes continue to expose codec and
    stream intent through `CodecSpec`, options, and input/target specs.
    Done.
147. Demote legacy `WithRTP...` builder options and `RTPOption` to
    package-private compiler plumbing so recipe users configure RTP through
    `goav.RTP(reader).Name(...).Codec(...).RTPBuffer(...)`.
    Done.
148. Hide `WebRTCRemote` and `TrackOption` so the root WebRTC recipe has one
    constructor, `WebRTCTrack(track)`, while advanced session orchestration
    stays in `webrtcav`.
    Done.
149. Remove `RTPInputOption` and the `RTP(reader, options...)` constructor path
    so RTP recipe configuration has one shape: `RTP(reader).Name(...).Codec(...)`
    plus stream-local methods for buffering, feedback, jitter, and receive
    policy.
    Done.
150. Demote the `RecordOption` marker interface to package-private plumbing so
    the public record recipe keeps the same call shape without exposing an
    implementation detail.
    Done.
151. Remove the package-level job-option surface so custom runtimes compose
    through `.UseRuntime(...)` on recipe jobs instead of constructor options.
    Done.
152. Demote the `StreamOption` marker type to package-private plumbing so
    `Audio(goav.StreamIndex(0))` and branch selection stay fluent without
    making stream option types part of the beginner surface.
    Done.
153. Earlier experiment: demote the `CodecOption` marker type to package-private
    plumbing. Superseded before release because custom/external composition
    needs to mirror codec option forwarding without positional bitrate helpers;
    `CodecOption` is now the single public codec tuning option type.
    Done.
154. Demote the `ResizeOption` and `AudioOption` marker types to
    package-private plumbing so transform recipes stay on `.Resize(...)` and
    `.Resample(...)` instead of teaching extra option nouns.
    Done.
155. Make RTP codec-intent diagnostics recipe-first: unsupported or automatic
    RTP codecs now suggest explicit built-in codec specs before pointing custom
    payload work at advanced receive adapters.
    Done.
156. Replace recipe RTP graph details like `depacketizers=1` with codec-intent
    details such as `codec=opus` or `codec=vp8`, keeping implementation counts
    out of beginner graph inspection.
    Done.
157. Remove recipe-level `.Depacketize(...)` so RTP/WebRTC recipes stay on
    codec intent, while advanced runtime compilers can still use explicit
    depacketizers internally.
    Done.
158. Trim README front-door guidance so exact adapter registration and RTP
    buffering/gap knobs stay in advanced docs instead of the getting-started
    path.
    Done.
159. Demote WebRTC track option helpers to package-private test/runtime
    plumbing so the public WebRTC recipe shape is only `WebRTCTrack(track)`.
    Done.
160. Move WebM and tagged-encoder examples out of the README 30-second path so
    first-page examples only use formats available in `goav.Default()` today.
    Done.
161. Extend the roadmap around executable examples, planner-first internals,
    mixed audio/video branch composition, explicit adapter requirements,
    optional graph/report tooling outside core, reusable flows, live
    codec-change policy, observability, beginner signature cleanup, and v0.1
    readiness. Done.
162. Keep `gofmt`, `go test ./...`, allocation guards, and no-cgo hygiene green.
163. Earlier experiment: make `Record(input, outputs...)` the real public
    signature and move runtime selection onto `.UseRuntime(...)`. Superseded
    before release by `From(input).Copy().To(outputs...)`. Done.
164. Let transcode compile audio and video branches as separate decoded stream
    groups that can feed one shared muxed output, so one output is a media
    composer instead of an implicit same-stream ladder.
    Done.
165. Add stream-level `OnCodecChange(goav.RealtimeCodecChangePolicy())` so live
    receive recipes can name the supported compatible-rebind/keyframe/sync/drop
    policy while unsupported custom policies fail during build.
    Done.
166. Add first task observability slice: `Task.Stats()` returns graph
    packet/frame/event counters, event counts by type, buffered drop counts by
    policy, and the last observed event.
    Done.
167. Route `Job` and the internal branch-plan migration adapter through a
    private recipe intent compiler state that carries the public `Intent`,
    concrete IO attachments, validation passes, transcode planning, and builder
    lowering before handing off to the migration graph compilers. Done.
168. Move migration graph-compiler selection into the recipe intent pass chain,
    so `Describe` and `Build` use the resolved recipe compiler result instead
    of calling back into builder-level compiler matching. Done.
169. Emit the planned `pipeline.Spec` during recipe compiler resolution and let
    recipe `Describe` return that stored spec, while `Build` uses the same
    resolved migration compiler for the runnable graph. Done.
170. Earlier experiment: reject sink destinations on packet-preserving `Record` and
    generic `From(input).To(...)` recipes during intent validation. Superseded
    before release by packet-domain `Sink` support for `.Copy()` and
    stream-scoped packet sinks. Done.
171. Earlier experiment: reject sink destinations as planned branch destinations because
    planned branch destinations were treated as mux-only groups. Superseded before
    release by domain-honest branch destinations where sinks can receive raw frames
    or encoded packets. Done.
172. Preserve `ErrUnsupportedBuild` through more recipe `BuildError`
    diagnostics for unsupported shapes such as multiple file inputs, selected
    streams with no operation, missing encoders for mux outputs, and RTP inputs
    on `Transcode` recipes. Done.
173. Require explicit codec intent for raw `goav.RTP(reader)` recipes, while
    keeping `WebRTCTrack(track)` as the Pion metadata-driven path, so realtime
    receive does not silently build opaque RTP graphs. Done.
174. Give `WebRTCTrack(track)` its own unknown-codec diagnostic when Pion track
    metadata does not map to Opus, VP8, VP9, H264, or AV1, instead of reusing
    raw-RTP missing-codec guidance. Done.
175. Add public recipe acceptance coverage proving `Describe()` and `Build(ctx)`
    resolve the same `pipeline.Spec` for default record and fanout recipes.
    Done.
176. Extend recipe Describe/Build equivalence coverage to stream-scoped decoded
	sink destinations and transcode branches with muxed outputs, using test adapters
	so the public workflow contract stays true beyond packet-preserving recipes.
    Done.
177. Extend recipe Describe/Build equivalence coverage to realtime receive:
    WebRTC track recording and default RTP VP8 packet recording now prove the
    planned `pipeline.Spec` matches the built task graph. Done.
178. Prove public recipe task observability on the default RTP VP8 record path:
    after `Run(ctx)`, `Task.Stats()` reports packet totals, EOS event counts,
    delivery totals, last-event state, and no buffered drops without touching
    pipeline internals. Done.
179. Extend public recipe task observability proof to stream-scoped decode:
    `From(input).Audio(StreamIndex(0)).To(Sink(...))` now proves
    `Task.Stats()` reports decoded frame totals, stream-added and EOS event
    counts, delivery totals, last-event state, and no drops. Done.
180. Detach recipe compiler state from raw recipe builder pointers: compile
    entrypoints now capture inputs, outputs, stream selections, branches, and
    recipe errors into pass state, with a guard test preventing
    `*Job` or internal branch-plan adapters from returning to
    `recipeCompileState`. Done.
181. Move transcode branch planning onto `Intent.Streams`: the transcode plan
    pass now derives branch selectors, transforms, encode targets, and output
    routes from public intent data while concrete outputs remain captured
    attachments, with a guard preventing `[]streamBuild` from entering
    `recipeCompileState`. Done.
182. Move ordinary stream recipe lowering onto `Intent.Streams`: selected stream
    decode, transforms, encode target, and codec-change policy now lower from
    public intent data while `.Do(...)` stages and stream-local outputs remain
    captured attachments, with a guard preventing `*jobStreamBuild` from
    entering `recipeCompileState`. Done.
183. Collapse ordinary job output attachments in recipe compiler state: job and
    stream outputs are now captured once as the concrete output attachment set,
    while a small output count preserves the stream/global scope diagnostic;
    guard coverage prevents `jobOutputs` and `streamOutputs` fields from
    returning to `recipeCompileState`. Done.
184. Make recipe compiler attachments explicit and checked: input, output, and
    transcode IO attachments now use attachment-shaped state names, and a
    consistency pass rejects mismatches between public `Intent` counts and the
    captured concrete attachments before lowering. Done.
185. Split ordinary recipe validation into intent shape and concrete attachment
    passes: input/output presence, one selected stream, stream-local output
    scope, operation spec, selector, encode, and codec-change policy now fail
    from public `Intent` before readers, writers, sinks, and stage attachments
    are validated. Done.
186. Split transcode recipe validation the same way: one input, branch presence,
    branch names, selectors, encode targets, branch destination refs, duplicate
    branch routes, duplicate branch names, and transform shape now fail from
    public `Intent`, while RTP rejection and mux-output attachment validation
    stay in a concrete attachment pass before plan lowering. Done.
187. Earlier experiment: bind transcode branch string routes before plan
    lowering. Superseded before release by `Destination` handles carried by
    branch `.To(...)`.
    Done.
188. Bind ordinary stream recipe routes before lowering: stream-scoped
    `.Audio()`/`.Video()` `Targets` labels now check against concrete
    stream-local output attachments in their own compiler pass, matching the
    transcode route-binding shape. Done.
189. Validate ordinary stream recipe transforms from intent: stream-scoped
    `Resize`/`Resample` value, empty-transform, and media-mismatch errors now
    fail during public `StreamIntent` validation instead of first surfacing
    during builder lowering. Done.
190. Validate ordinary stream step attachments before lowering: nil
    `.Do(...)` stages and mismatched transform attachments now fail in a
    dedicated compiler pass before the stream lowerer mutates the runtime
    builder. Done.
191. Validate ordinary stream output kinds before lowering: mixed sink/mux
	outputs and mux outputs without an encoder now fail in a dedicated compiler
	pass before runtime builder mutation. Encoded packet sinks were later
	promoted to supported packet-domain targets. Done.
192. Validate ordinary stream runtime capabilities before lowering: custom
    recipe runtimes that lack live codec-change or transform support now fail
    in a dedicated compiler pass before inputs, streams, or outputs mutate the
    builder. Done.
193. Make recipe graph compiler selection diagnostic: standard-runtime recipes
    that fail to match the migration compiler set now return an actionable
    `recipe_graph_unsupported` build error with recipe counts and front-door
    suggestions instead of leaking a bare unsupported graph sentinel. Done.
194. Preflight recipe output format adapters: ordinary and transcode muxed
    outputs now resolve probed or explicit output formats and missing muxers in
    compiler passes before runtime builder lowering opens inputs or creates
    graph nodes. Done.
195. Preflight recipe encode adapters at build time: ordinary and transcode
    encode intents now fail on missing or descriptor-only encoder factories
    before inputs are opened, while `Describe()` remains adapter-agnostic graph
    inspection. Done.
196. Preflight known live recipe decode adapters at build time: ordinary
    RTP/WebRTC decode intents now fail on missing or descriptor-only decoder
    factories before the runtime builder opens live inputs, while ambiguous
    receive selection still defers to stream resolution. Done.
197. Preflight recipe input format adapters at build time: ordinary and
    transcode file/protocol inputs now resolve metadata-detected containers and
    missing demuxers before runtime builder lowering, while RTP/WebRTC receive
    stays on the live input path. Done.
198. Preflight recipe transform adapters at build time: ordinary and transcode
    `Resize`/`Resample` intents now fail on missing filter factories before
    inputs are opened, while custom `.Do(...)` stages remain caller-provided
    graph components. Done.
199. Preflight live recipe stream selection at build time: ordinary RTP/WebRTC
    stream recipes now report obvious ambiguous or missing `Audio()`/`Video()`
    selections from intent before opening live receivers or checking decoder
    adapter availability. Done.
200. Reuse build-time input probe results for recipe stream selection:
    ordinary and transcode file/protocol recipes now report obvious ambiguous
    or missing `Audio()`/`Video()` branch selections before demuxers are opened
    whenever the registered prober already exposes streams. Done.
201. Carry resolved output formats through build lowering: ordinary and
    transcode recipe output preflight now stores inferred mux formats as
    runtime open hints while keeping graph details reserved for explicit
    destination format options, preserving `Describe`/`Build` equivalence. Done.
202. Reuse build-time input probe stream codecs for decode-adapter checks:
    ordinary and transcode file/protocol recipes now report missing or
    descriptor-only decoders before demuxers are opened whenever the registered
    prober already exposes an unambiguous selected stream codec. Done.
203. Validate unresolved transcode encode intents through the shared recipe
    encode rules: `Auto()` and `Copy()` branch destinations now fail with explicit
    intent diagnostics instead of falling through as missing encoders. Done.
204. Make unmatched transcode output groups actionable: advanced transcode
    plans whose output branch lists select no branch now return
    `transcode_output_unmatched` with branch-name and label guidance instead of
    a bare unsupported sentinel. Done.
205. Make invalid advanced transcode plan shapes actionable: empty
    branch/output lists and duplicate branch names now return structured
    build errors with concrete plan-shape guidance instead of bare unsupported
    sentinels. Done.
206. Make advanced transcode transform failures actionable: mixed
    resize/resample branches and transform/media mismatches now return
    structured build errors with stream-scoped guidance instead of bare
    unsupported sentinels. Done.
207. Make advanced transcode resize planning failures actionable: impossible
    fit/fill geometry and unknown resize modes now return structured build
    errors with mode, input geometry, target geometry, and resize-mode guidance
    instead of bare unsupported sentinels. Done.
208. Make the reusable component-kit promise explicit: add
    `docs/COMPONENTS.md` as the concise catalog for core media, pipeline, RTP,
    codec, format, filter, WebRTC, and adapter components, link it from the
    README, and guard the recipe/component/expert-graph contract with a doc
    test. Done.
209. Prove a reusable component pipeline without recipes: manually compose
    `format.DemuxSource`, two `format.MuxStage` instances, and `pipeline.Graph`
    fanout in `TestComponentFileRemuxFanout`, including graph spec details,
    packet delivery, stats, and lifecycle closure. Done.
210. Prove reusable custom stage composition: add
    `TestComponentCustomStageForwardsEvents`, which wires a handwritten
    `pipeline.Stage` with reusable event/message scratch into a direct graph
    and verifies event forwarding, stats, and lifecycle closure without recipe
    builders. Done.
211. Prove codec and mux stages as reusable graph components: add
    `TestComponentCodecStageFlushesOnEOS` and
    `TestComponentMuxStageEmitsWriteEvents`, wiring exported
    `codec.DecoderStage` and `format.MuxStage` directly into `pipeline.Graph`
    without recipe builders and verifying EOS flush, mux write events, stats,
    and lifecycle closure. Done.
212. Prove the direct RTP Opus decode component graph: add
    `TestComponentRTPOpusDecodeGraph`, wiring a Pion RTP `PacketReader`
    boundary through `rtpav.Source`, `rtpav.NewOpusDepacketizer`, a concrete
    `gopus` decoder stage, and a sink without recipe builders, verifying graph
    spec details, decoded PCM frame facts, stats, and lifecycle closure. Done.
213. Add reusable flow intent fragments without a second DSL: introduce
    `AudioFlow` and `VideoFlow`, `.Apply(flow)` for stream and branch builders,
    and branch-owned targets on `From(input).Audio()/Video().Branches(...)` as a
    `Job` expression that expands to branch stream intents, preserves snapshots,
    emits actionable flow diagnostics, and describes branch graphs through the
    existing branch composer. Done.
214. Prove WebRTC TrackSet receive as reusable components: add
    `TestComponentWebRTCTrackSetFeedsRTPSource`, accepting an initial and
    replacement Pion-typed remote track through `webrtcav.TrackSet`, reusing
    one long-lived reader, feeding that reader into `rtpav.Source`, and
    verifying replacement epoch packet output, graph spec details, stats,
    lifecycle closure, and caller-owned session lifetime. Done.
215. Prove component graph specs stay stable across execution: all direct
    component graph tests now capture `pipeline.Spec` before `Run` and compare
    it afterward for file remux fanout, custom stages, RTP Opus decode, WebRTC
    TrackSet receive, codec EOS flush, and mux write events. Done.
216. Keep flow ordering consistent with stream recipes: `AudioFlow` and
    `VideoFlow` now reject operations declared after a terminal encoder instead
    of silently normalizing call order, preserving the one-chain rule that
    frame operations come first, one encoder comes last, then outputs are attached;
    the sealed `Flow` implementation now uses a private snapshot hook and
    typed-nil flows or branches become structured build errors instead of
    panics. Done.
217. Make reusable component allocation proof auditable: add an Allocation
    Proofs section to `docs/COMPONENTS.md` naming the package-local
    `testing.AllocsPerRun` guards for core media, pipeline, RTP, codec,
    format, filter, and default adapters, and lock the catalog with a docs
    acceptance test. Done.
218. Make live reusable branches a real receive compiler: `Branches` lowering
    now marks the internal transcode job it creates, RTP/WebRTC inputs lower
    into the builder with their Pion-backed packet reader, and
    `rtpTranscodeGraphCompiler` composes receive, shared decode, per-flow
    transforms, per-flow encoders, and mux outputs. Public recipe coverage
    proves live branch descriptions, and runtime coverage proves RTP packets
    decode once then feed two encoded mux outputs. Done.
219. Add the first runtime attachment control-plane slice:
    `Task.Attach(ctx, goav.Branch(name).From(node).Do(stage).To(sink))` now
    attaches a downstream branch to a built direct graph while it is running,
    using node names from `Task.Describe()`, and returns a handle with
    `Close(ctx)`. Direct graph mutation snapshots route targets per message so an
    attachment added after one packet receives later packets and can be removed
    before later packets. This first slice still guarded buffered graphs until
    queue and worker extension landed later. Done.
220. Add recipe-shaped runtime branch anchors through named taps:
    recipe `.Tap(name)` creates stable outlets, `Task.Taps()` exposes them on
    built tasks, and `goav.Branch(name).FromTap(name)` attaches late analysis or
    sink branches without exposing compiler node naming. `.From(node)` remains
    the expert graph escape hatch. Done.
221. Add task-level runtime attachment cleanup:
    `Task.Detach(ctx, h)` removes a live runtime branch under the same attach
    control plane, individual `Attachment.Close(ctx)` handles remain idempotent,
    and `Task.Close()` stops runtime attachments before closing the graph. Done.
222. Add renderer-free recipe explanations:
    `Job.Explain(ctx)` now compiles through the
    same build-preflight path, return `PlanReport` with structured inputs,
    streams, outputs, adapter requirements, warnings, and `pipeline.Spec`, and
    keep text/diagram rendering outside core. Done.
223. Add the first `MediaPlan` planner IR:
    recipe compilation now emits inputs, stream selectors, branch operations,
    outputs, and planner decisions before runtime lowering.
    `Explain(ctx)` reports branch operation chains and decisions, and transcode
    explains as ordinary demux/select/decode/transform/encode branches instead
    of a special transcode operation. Done.
224. Move the public recipe direction to composition-first `From`:
    top-level workflow helper shortcuts are removed before release, packet-preserving
    work is expressed as `From(input).Copy().To(...)`, stream work is expressed
    with `.Audio()/Video().Decode()...To(...)`, planned output composition uses
    `.Branches(goav.Branch(...).To(destination))`, reusable flows apply to branches,
    and runtime branches attach through
    `Task.Taps()` plus `goav.Branch(name).FromTap(name)`. Built tasks now carry
    planned tap metadata for stable runtime attachment points, including taps
    after decode, transforms, custom stages, and encode in the media plan. Done.
225. Start moving graph description onto the media-plan compiler path:
    packet-preserving `From(input).Copy().To(...)` file/remux and RTP/WebRTC
    record/fanout recipes now emit exact `pipeline.Spec` values from the
    recipe compiler's media-plan pass while recipes move to
    `MediaPlan -> pipeline.Graph`.
    Done.
226. Harden runtime branch attachment at operation boundaries:
    stream recipes now have coverage for `Task.Attach(...FromTap(...))` after a
    custom frame stage and after encode, proving named taps resolve to the
    correct frame-domain or packet-domain runtime node and can be detached
    cleanly. Done.
227. Move packet-preserving recipe build off the migration compiler list:
    file/remux and RTP/WebRTC record/fanout recipes now carry concrete IO
    attachments into the resolved media-plan build path, construct the same
    demux/RTP source, mux stages, routes, and task graph directly, and skip
    migration compiler selection for both `Describe` and `Build`. Done.
228. Move decoded sink-destination recipes off the migration compiler list:
	`From(input).Audio()/Video().Decode().To(Sink(...))` for file/protocol
	and RTP/WebRTC inputs now emits and builds through the media-plan sink-destination
	path, preserving ordered custom stages before the sink while skipping
    migration compiler selection. Done.
229. Move stream encode-to-output recipes off the migration compiler list:
    `From(input).Audio()/Video().Decode().Do/Resize/Resample().Opus/VP8/VP9().To(File(...))`
    now emits and builds through the media-plan encode path for file/protocol
    and RTP/WebRTC inputs, preserving ordered custom processing before encode
    and mux fanout without migration compiler selection. Done.
230. Move branch/composer recipes off the migration compiler list:
    declared `Branches(goav.Branch(...).To(destination))` compositions and
    live branch `Branches(...)` recipes now emit and build through the media-plan
    branch-composer path, preserving shared decode, per-branch transforms,
    encoders, shared mux outputs, and RTP/WebRTC receive without migration
    compiler selection. Done.
231. Require media-plan recognition for recipe compilation and keep custom
    codecs orthogonal:
    normal `Job` and flow/transcode recipe compile chains now end with a
    media-plan requirement instead of falling back to the fixed runtime compiler
    list. Unsupported recipe shapes fail with `recipe_graph_unsupported` after
    the media-plan pass. Custom codec intent uses the same `Codec` spec as
    built-ins, with concrete factories registered through `WithDecoder` and
    `WithEncoder`; descriptor-only registration still reports unavailable
    factories instead of pretending encode/decode is implemented. Done.
232. Add grouped stream branches and broaden the custom-component goal:
    `Branches(goav.Branch(name)...)` now groups encoded alternatives from one
    selected stream so complex multi-output examples stay readable without
    repeating the selected stream. README and use-case docs name custom stages,
	custom sink destinations, adapter hooks, and late runtime branch sinks as one
	optional composition surface. Generic `Codec` specs now pass recipe encode
    validation and fail at adapter capability/preflight when no encoder exists.
    Custom filter adapters and late muxed outputs were intentionally tracked as
    follow-on operation-model slices instead of shortcuts. Done.
233. Make branch operations ordered and component-capable:
    `StreamIntent` now carries ordered operations, `Branch(...)` accepts
    `.Apply(flow)`, `.Do(stage)`, `Resize`, `Resample`, `Tap`, and encode in
    one chain, and transcode branches carry ordered `Step` values through
    `Describe`, `Explain`, and runtime graph construction. Branch-local custom
    stages are inserted and closed by the running graph, transform names stay
    stable regardless of custom stage position, and recipe branches can chain
    multiple transforms instead of failing on the old single-transform shape.
    Done.
234. Unify planned splits around branches:
    stream-level legacy split methods were removed before release. Reusable
    `AudioFlow`/`VideoFlow` values apply to `BranchSpec`, streams split with
    `.Branches(...)`, and mux groups are `Destination` handles carried by
    branch `.To(...)`.
    Branch splits preserve upstream operation specs, so branches can begin after
    decode, resize/resample, custom stages, and taps; runtime attachment remains
    the late control plane through `Task.Attach` and named taps. Done.
235. Make `Explain(ctx)` useful when adapter preflight fails:
    build preflight errors now return the original `BuildError` together with a
    partial `PlanReport` when the recipe shape can still be described. Missing
    muxers, demuxers, decoders, encoders, and filters become structured
    `RequiredAdapters` entries with `missing`, `unavailable`, or `unknown`
    status, plus `Missing` and `Warnings` diagnostics so applications can show
    actionable plan reports without pretending the build can run. Done.
236. Move `Explain(ctx)` adapter requirements onto operation planning:
    `RequiredAdapters` now follows planned branch operation chains instead of
    the older flattened stream fields. Decode, transform, and encode
    requirements are attached to the branch that owns the operation, demuxers
    and muxers stay attached to inputs and outputs, and standard runtime
    registries mark requirements as `available`, `missing`, `unavailable`,
    `built-in`, `unknown`, or `required` before any adapter is opened. Done.
237. Add first mux compatibility planning:
    known constrained output formats now participate in recipe preflight.
    IVF mux groups must resolve to exactly one VP8, VP9, or AV1 video branch,
    and Annex B mux groups must resolve to exactly one H264 video branch.
    `Build` returns `destination_mux_incompatible` before opening the muxer when a
    mismatch is provable, while `Explain(ctx)` carries the same structured
    diagnostic without treating it as a missing adapter. Done.
238. Clean the composition vocabulary around branches and targets:
    public planned splits use `Branch`/`Branches`, logical destinations use
    `Target`, destination refs remain `File`, `URIOut`, and `Sink`, and
    flows are reusable operation sequences without `.To(...)`. `Task.Attach`
    now consumes the same `BranchSpec` shape for runtime sink attachment from
    named taps, and the old public `Output`/`Outputs` recipe binding surface is
    removed. Done.
239. Let runtime branches publish downstream taps:
    `Task.Attach(ctx, goav.Branch(...).FromTap(...).Do(stage).Tap(name).To(Sink(...)))`
    now exposes the branch tap through `Task.Taps()`, preserves the anchor shape
    where known, lets later runtime branches attach from that new tap, rejects
    duplicate runtime tap names, and detaches dependent runtime branch subtrees
    when the parent attachment is removed. Done.
240. Add runtime frame transforms from taps:
    `Task.Attach(ctx, goav.Branch(name).FromTap(tap).Resize(...).Tap(...).To(Sink(...)))`
    and the audio `Resample(...)` equivalent now materialize through the same
    filter stage path used by planned branches, require frame-domain taps,
    carry transformed shape onto nested runtime taps, and keep the branch grammar
    identical to planned branches. Done.
241. Make flows ordered operation sequences:
    `AudioFlow` and `VideoFlow` now carry custom `.Do(stage)` steps,
    `.Tap(name)` outlets, transforms, and an optional terminal encoder through
    one private ordered step snapshot. Stream builders, planned branches, and
    non-encoding runtime branch attachments all apply that same step sequence,
    while flow operations after encode fail with the existing terminal-encoder
    diagnostic. Duplicate branch tap bookkeeping was removed so branch intent
    derives taps from ordered operations. Done.
242. Add runtime destination branches from taps:
    `Task.Attach(ctx, goav.Branch(name).From(tap).Encode(goav.Opus(...)).To(goav.File(...)))`
    now opens a late encoder and mux destination from frame-domain taps, while
    `.Copy().To(goav.File(...))` records packet-domain taps without decoding.
	Runtime attach validates copy/encode domains before graph mutation, keeps
	H264/AV1 recipe encode guarded as work in progress, and supports destination
	detach through the same attachment subtree lifecycle as sink branches. Done.
243. Make sink destinations domain-honest:
    `Sink` works as a frame sink before encode, an encoded packet sink after
    `.Encode(...)`, and a packet sink after `.Copy()`.
    Stream-level `Copy()` now lowers to selected packet fanout instead of
    forcing decoder preflight, direct encode-to-sink reuses the encoder stage
    path, and planned/built graph equivalence plus runtime packet delivery are
    covered by tests. Done.
244. Support sink destinations as planned branch destinations:
	`Branches(goav.Branch(name).To(goav.Sink(...)))`
	now stays in frame domain when no encoder is declared, can attach after
	ordered branch operations such as `Resize(...)`, and sends encoded packets to
	the same sink destination shape after Opus/VP8/VP9 encode. Mux destinations still
	require explicit encode, planned branch `.Copy()` remains rejected because
	planned branches begin after decode, and the low-level transcode plan now
	carries either a mux destination or a sink destination per destination. Done.
245. Keep recipe branch composition off the internal transcode request list:
    the transcode/branch intent pass still produces the shared resolved plan,
    but recipe lowering no longer calls `builder.Transcode(plan)`. Media-plan
    branch describe/build now pass the resolved plan directly into shared graph
    planning and compile helpers, while the advanced internal `Transcode(plan)`
    builder path remains available for low-level tests and migration work. Done.
246. Give declared branch composition its own internal plan:
	recipes now compile into a private branch-compose plan with branches,
	ordered steps, typed destinations, sink destinations, and resolved mux-open formats.
	The advanced `Transcode(plan)` path remains available but adapts into that
    branch-compose plan at the boundary. Recipe compile state no longer stores a
    `transcode.Plan`, and branch-composer describe/build consume the new plan
    directly. Done.
247. Rename branch-composer runtime helpers around the new plan:
    prepared routes, destination routes, selector/stream groups, ordered
    media-transform stages, branch step validation, and destination diagnostics now
    use branch/media vocabulary internally. The explicit advanced
    `Transcode(plan)` compiler wrappers remain as boundary adapters, but graph
    planning/build helpers consume branch-compose routes and targets. Done.
248. Rename branch-composition recipe compiler state:
    recipe compilation now uses branch-composition job, branch input attachment,
    branch destination attachment, branch probe, branch destination validation, and branch
    transform validation names. `Explain(ctx)`, media-plan recognition, mux
    compatibility checks, and tests use the renamed state while the advanced
    `Transcode(plan)` boundary remains isolated. Done.
249. Keep branch-composition inputs on the resolved media plan:
    RTP/WebRTC branch compositions no longer lower their input into the recipe
    compile builder. The resolved recipe carries the branch input through the
    media-plan boundary instead of relying on an empty-builder shape gate for
    branch composition. Done.
250. Keep direct stream recipes on resolved media-plan attachments:
	selected stream decode-to-sink, encode-to-sink, and encode-to-output
	recipes no longer lower inputs, operation specs, or destination refs into the
	recipe compile builder before media-plan recognition. The resolved recipe
	carries concrete inputs, destination refs, ordered stream attachments, codec-change
	policy, custom stages, transforms, and taps; describe/build construct the
    small runtime helper from those resolved attachments at the media-plan
    boundary. The old builder-shape gates and lower passes for these direct
    stream jobs are removed. Done.
251. Move direct stream media-plan specs/builds onto resolved graph plans:
	direct selected-stream decode/encode recipes now create a small resolved
	single-stream graph plan from intent plus concrete input, destination, stage,
	transform, tap, and codec attachments. `Describe` and `Build` for those
    recipes call parameterized decode/filter/encode/source/destination helpers
    instead of first populating a runtime builder and matching its internal
    fields. Existing expert builder compilers reuse the same helpers so planned
    graph equivalence stays covered while the recipe path moves closer to
    `Intent -> MediaPlan -> pipeline.Spec -> pipeline.Graph`. Done.
252. Move branch composition onto a resolved media-plan graph:
    declared branch recipes now create a resolved branch-compose graph plan from
    the concrete input, resolved targets, and private branch-compose plan.
    `Describe` and `Build` use that plan directly; runtime adapter services are
    still borrowed for opening demux/RTP sources, transform stages, encoders,
    and muxers, but recipe branch composition no longer creates a temporary
    graph builder to own the media-plan boundary. The advanced `Transcode(plan)`
    compiler path remains as an explicit boundary adapter over the same
    stateless branch route planning and compile helpers. Done.
253. Move packet-copy recipes onto a resolved media-plan graph:
    packet-preserving record/remux/fanout recipes now create a resolved
    packet-copy graph plan from concrete inputs, destination refs, and optional selected
    stream intent. `Describe` and `Build` share that plan for file/protocol and
    RTP/WebRTC packet copy, including selected-stream fanout, instead of keeping
    packet copy as recipe-resolved graph mutation helpers. Done.
254. Add the first buffered runtime attachment slice:
    bounded buffered graphs now accept running `AddStage`, `AddSink`, `Connect`,
    `Disconnect`, and non-source `Remove` before drain starts. Late nodes get
    queues immediately, workers start under the active run context, route
    delivery is protected against concurrent detach, and removing an attachment
    closes its queue and waits for the worker before closing the stage or sink.
    Pipeline and runtime tests prove a late buffered sink branch receives future
    messages from a running graph. Done.
255. Prove buffered late packet-copy recording:
    `Task.Attach(ctx, goav.Branch(...).From(goav.PacketTap(...)).Copy().To(goav.File(...)))`
    now has runtime coverage on a running bounded buffered graph. The test
    pauses before a packet, attaches an Ogg file destination from the packet tap,
    resumes, verifies the late mux receives the future packet, then detaches and
    closes the runtime mux stage. Done.
256. Prove buffered late Opus encode recording:
    `Task.Attach(ctx, goav.Branch(...).From(goav.FrameTap(...)).Encode(goav.Opus(...)).To(goav.File(...)))`
    now has runtime coverage on a running bounded buffered graph. The test
    pauses before a frame, attaches an Ogg recording destination from the frame tap,
    applies a branch-local bounded packet-copy policy for the encoder-to-mux
    queue, resumes, verifies the late encoder and mux receive future media, then
    detaches and closes both runtime-owned stages. Done.
257. Prove buffered late branching after runtime resize:
    bounded buffered graphs now cover a parent runtime branch that attaches from
    a frame tap, resizes, publishes a new `Tap(...)`, then accepts a dependent
    child branch from that transformed tap before future frames arrive. The test
    verifies both parent and child sinks receive the future resized frame path,
    transformed shape are exposed through `Task.Taps()`, and detaching the parent
    closes the dependent child branch and runtime-owned resize stage. Done.
258. Prove buffered runtime flows can encode to destinations:
    `Task.Attach(ctx, goav.Branch(...).Apply(flow).To(goav.File(...)))`,
    where the flow encodes tuned Opus through `goav.Opus(...)`,
    now has runtime coverage on a running bounded buffered graph. The test
    proves that `Flow` remains a reusable operation sequence, not a destination:
    the branch owns the destination, the flow expands to a custom stage plus terminal
    Opus encode, the late mux destination receives the future packet, and detaching
    closes the flow-owned stage, encoder, and mux stage. Done.
259. Make post-encode branch taps real packet taps:
    branch-level `.Tap(...)` calls after `.Encode(...)` or `.Copy()` now become
    packet-domain taps in planned branch intent and in
    runtime attachment. A planned-branch test verifies the operation order and
    `After: OpEncode` tap metadata; branch-composition build coverage verifies
    the built task exposes codec-bearing packet shape so a later recording destination
    can attach from the planned encoded tap; bounded-buffered runtime tests
    attach parent branches that publish encoded or copied packet taps, then
    attach child packet-copy recording branches from those taps before future
    media arrives. Done.
260. Prove direct stream copy taps can become runtime mux destinations:
    `From(RTP(...).Codec(Opus())).Audio().Copy().Tap("audio.copied")`
    now exposes a packet-domain, codec-bearing tap on the selected stream, and
    a later `Task.Attach(ctx, goav.Branch(...).From(goav.PacketTap(...)).Copy().To(goav.File(...)))`
    recording branch receives future RTP packets through the same typed
    `Branch`/`Destination` grammar as planned composition. Done.
261. Share planned branch operation prefixes before tapped split points:
    `Video().Decode().Resize(...).Tap(...).Branches(...)` now lowers the resize
    as one shared branch-composer node feeding downstream encode, thumbnail, or
    analysis branches instead of duplicating the parent resize inside every
    branch. Describe/build graph equivalence coverage pins the shared-prefix
    shape so planned branches and runtime `FromTap` branches keep the same
    media-point model. Done.
262. Make planned branches split from the current stream point without requiring
    an explicit tap:
    `Video().Decode().Resize(...).Branches(...)` now shares the resize as the
    current branch point too; `.Tap(...)` remains the way to publish a stable
    runtime attachment outlet, not a requirement for planned graph
    correctness. Intent coverage verifies these unnamed current-point splits do
    not pretend to reference `video.decoded`, and describe/build equivalence
    coverage pins the shared-prefix graph. Done.
263. Broaden current-point branch proofs beyond video resize:
    planned branch composition now has coverage for custom `.Do(stage)` current
    points and an audio `Resample(...)` current point. The audio proof runs the
    graph and verifies one shared resample feeds both an Opus mux branch and a
    raw sink branch, preserving the same `Branch`/`Destination` grammar across
    custom stages, audio transforms, and video transforms. Done.
264. Keep generic codec composition and resolved destinations orthogonal:
    pre-lowered branch-composer plans now refresh concrete input/destination
    attachments after adapter probing while preserving their shared operation
    prefixes, so `Describe`, `Explain`, and `Build` see the same resolved destination
    formats. Custom `goav.Codec(...)` encode specs are now covered through
    direct stream encode, planned `Branch(...).Encode(...).To(destination)`, and
    runtime `Task.Attach(ctx, Branch(...).FromTap(...).Encode(...))` mux
    branches without adding a built-in-only pathway. Done.
265. Let runtime branches fan out to multiple typed destinations:
    `Task.Attach(ctx, goav.Branch(...).From(goav.FrameTap(...)).Encode(goav.Opus(...)).To(archive, monitor))`
    now encodes once, keeps terminal destinations separate from the operation chain,
    and connects the running branch to multiple mux/sink destinations under one
    attachment. Runtime branches now match planned branch `.To(destination, ...)`
    grammar for destination fanout while preserving detach and nested tap ownership.
    Done.
266. Prove planned raw sink destination fanout:
    a single decoded planned branch can now be tested as
    `Branch("frames").To(Sink(...), Sink(...))`,
	with no encoder inserted and both sink destinations receiving the same decoded
	frame. This keeps planned and runtime branch fanout aligned across mux and
    sink destinations. Done.
267. Keep mux diagnostics in the Branch/Destination vocabulary:
    recipe preflight now reports missing destination muxers as
    `destination_muxer_missing` and constrained mux groups as
    `destination_mux_incompatible`, including `destination=...` details for the branches
    feeding the mux group. The expert graph builder still reports
    `output_muxer_missing` at the low-level `format.Output` boundary, keeping
    public recipe diagnostics aligned with `Destination` without blurring runtime
    internals. Done.
268. Remove explicit destination-registration sugar from the public Job API:
	branch composition now relies on typed destinations collected from
	`Branch(...).To(destination)` and direct destination `.To(...)` calls; the
    exported `Job.Targets(...)` registration escape hatch was removed. The API
    guard now rejects `Targets` alongside `Path`, `Paths`, `Output`, and
    `Outputs`, keeping the normal grammar to one way to attach a destination. Done.
269. Prove runtime observer branches are ordinary branches:
    a late branch can now be tested as
    `Branch("analysis").From(FrameTap("audio.decoded")).Do(FrameFunc(...)).Tap("audio.observed").To(Sink(...))`,
    with a dependent runtime branch attaching from `audio.observed`. Running
    the task proves the base sink, observer sink, and dependent sink all receive
    the decoded frame, and detaching the parent closes the dependent subtree.
    This pins sink/observation-boundary branching without adding a new public
    concept. Done.
270. Guard README flow examples against branch duplication:
    the README now has test coverage that the reusable flow example contains
    exactly one `voice` branch and one `archive` branch, keeping the public docs
    aligned with the one-branch-name/one-destination grammar. Done.
271. Allow direct packet-domain fanout to mux and sink destinations:
	stream recipes now permit `.Copy().To(file, Sink(...))` and
	`.Encode(goav.Opus(...)).To(file, Sink(...))` because both destinations receive
	encoded packets. The media-plan encode path now plans and builds sink
	destinations beside mux destinations after one encoder, while decoded frame-domain
	sink+mux mixes still fail with the existing guidance. Runtime tests prove
    both packet-copy and Opus-encode fanout write the mux and packet sink from
    the same upstream packet path. Done.
272. Allow planned packet-copy branches:
    `.Copy().Branches(...)` now lowers as packet-domain branch composition
    instead of forcing the shared branch composer through decode. The planner
    selects the stream once, routes copy branches directly from that selector,
    opens mux destinations with the original stream metadata, reports packet taps on
    the selector node, and still rejects frame transforms or encoding on packet
    branches. Runtime tests prove mux and sink destinations receive packets without
    any decoder adapter. Done.
273. Add branch-local runtime observability:
    pipeline stats now include per-node input/output packet, frame, event, and
    drop counters for both direct and bounded buffered graphs. `Task.Stats()`
    exposes the whole graph, while `Attachment.Stats()` filters those counters
    down to nodes owned by that runtime branch, so a late sink, transform, mux,
    or observer branch can be measured without confusing it with upstream
    traffic. Direct, buffered, and runtime attachment tests pin the behavior.
    Done.
274. Prove buffered detach stops future branch media:
    a bounded-buffered runtime branch now has regression coverage for attach,
    one future packet delivery, detach while the source is paused, and a later
    packet that continues to the base graph without reaching the detached
    branch. The same test verifies the attachment keeps branch-local node stats
    after detach and does not leak base graph counters. Done.
275. Make filter capability metadata first-class in explanations:
    `filter.SimpleRegistry` now retains `filter.Descriptor` metadata beside the
    factory, and `PlanReport.RequiredAdapters` includes transform input/output
    media kind, realtime/stateless flags, and adapter metadata when available.
    Tests prove descriptor cloning, ordered branch requirements with filter
    media details, and custom resample metadata in `Explain(ctx)`. Done.
276. Validate transform adapter descriptor compatibility:
    recipe preflight now rejects `resize` adapters that advertise non-video
    input/output and `resample` adapters that advertise non-audio input/output
    before opening inputs or mutating graphs. `Explain(ctx)` reports the
    incompatible filter requirement with status `incompatible` and the declared
    media kinds. Compile-pass, build-path, and explanation tests pin the
    behavior. Done.
277. Make container capability metadata first-class:
    `format.SimpleRegistry` now retains `format.Descriptor` metadata for
    demuxers and muxers, with cloned media/codecs/metadata slices and maps.
    IVF and Annex B adapters register descriptors for their concrete
    single-stream container constraints. `PlanReport.RequiredAdapters` now
    reports muxer/demuxer media kind, codec, stream-count, realtime, and
    metadata details when available. Mux compatibility preflight can reject
    descriptor-backed custom containers before graph mutation, while existing
    IVF/Annex B target diagnostics remain actionable. Focused registry,
    adapter, explanation, and descriptor-backed mux tests pin the behavior.
    Done.
278. Validate transform descriptor config capabilities:
    `filter.Descriptor` now carries supported pixel formats, sample formats,
    and resize modes. Planned recipe and branch preflight reject unsupported
    resize modes, resize pixel formats, and resample sample formats before
    opening graph components. Runtime `Task.Attach` applies the same descriptor
    validation for late branches from taps before mutating the running graph.
    `Explain(ctx)` reports those typed filter capability fields and incompatible
    requirements preserve the supported values in diagnostics. Registry,
    adapter, compile-pass, explanation, and runtime-attach tests pin the
    behavior. Done.
279. Preflight runtime mux descriptor compatibility:
    late `Task.Attach` branches that end in file or URI targets now resolve the
    destination container format and run the same descriptor-backed mux
    compatibility checks used by planned branches before constructing a muxer or
    mutating the graph. Descriptor media, codec, and stream-count conflicts
    return `destination_mux_incompatible` with destination and branch details. Runtime
    tests prove an incompatible late Opus-to-custom-container branch opens no
    muxer and leaves the graph unchanged. Done.
280. Validate encode descriptor capabilities:
    `codec.SimpleRegistry` now clones descriptor slices so planner capability
    metadata cannot be mutated by callers. Planned recipes and branch
    composition preflight encoder descriptors for declared media kind,
    sample-format, and pixel-format constraints after confirming a concrete
    encoder factory exists. Runtime `Task.Attach` applies the same descriptor
    preflight after resolving encode config and before opening the encoder
    stage, so incompatible late branches fail without graph mutation.
    `Explain(ctx)` reports codec descriptor media/sample/pixel capability
    details and marks incompatible encoder requirements with
    `encode_adapter_incompatible`. Registry, compile-pass, explanation, and
    runtime-attach tests pin the behavior. Done.
281. Validate decode descriptor capabilities:
    live RTP/WebRTC recipes and probed file/branch recipes now preflight
    decoder descriptor media kind, sample-format, and pixel-format constraints
    after confirming a concrete decoder factory exists. Runtime decoder stage
    construction applies the same descriptor guard before decoder state or the
    decoder itself is opened. Unknown stream frame formats stay unconstrained,
    but known incompatible metadata returns `decode_adapter_incompatible`.
    `Explain(ctx)` reports incompatible decoder requirements with descriptor
    media/sample/pixel details. Compile-pass, explanation, and runtime builder
    tests pin the behavior. Done.
282. Carry branch stream shape through planning:
    `MediaPlan` branches now retain `MediaShape` resolved from probed file
    streams or live RTP/WebRTC codec intent. `BranchReport` exposes those shapes
    through `Explain(ctx)`, and planned taps inherit the same shapes while updating
    domain and transform output details such as resize width/height or resample
    sample rate/channels. Built tasks install those richer tap shapes, so later
    `Task.Attach(...FromTap(...))` branches start with useful stream context
    instead of only media kind and codec. Probed-file, live-RTP, and runtime
    resize-tap tests pin the behavior. Done.
283. Carry operation output shapes through planning:
    planner operations now carry the stream shape after each operation. Decode,
    transform, custom-stage, copy, and encode steps update domain, codec,
    dimensions, pixel format, sample rate, channels, and sample format as far as
    the recipe/probe metadata can prove. `OperationReport` exposes those shapes
    through `Explain(ctx)`, and post-encode planned taps now inherit packet shape
    with the encoded codec and transformed geometry. A public branch report test
    pins resize-to-VP9 operation shapes and matching post-encode tap shape. Done.
284. Prove live buffered subtree detach from nested packet taps:
    `TestTaskDetachBufferedPostEncodeTapSubtreeStopsFutureMessages` now runs a
    bounded buffered task while a late parent branch encodes from a frame tap,
    publishes a post-encode packet tap, and a dependent child copies from that
    nested tap. Detaching the parent closes both branches, removes the nested
    tap, and prevents later source frames from reaching the detached subtree
    while the base graph keeps running. Done.
285. Prove live buffered subtree detach from custom-stage taps:
    `TestTaskDetachBufferedCustomStageTapSubtreeStopsFutureMessages` now runs a
    bounded buffered task while a late parent branch executes a caller-provided
    `.Do(...)` stage, publishes a frame-domain tap from that stage, and a
    dependent child attaches to the nested tap. Detaching the parent closes the
    custom stage plus both sinks, removes the nested tap, and keeps later source
    frames out of the detached subtree while the base graph continues. Done.
286. Prove live buffered subtree detach from runtime transform taps:
    `TestTaskDetachBufferedRuntimeResizeTapSubtreeStopsFutureMessages` now runs
    a bounded buffered task while a late parent branch resizes video from a
    frame tap, publishes a transformed frame tap, and a dependent child attaches
    to that nested tap. Detaching the parent closes the resize filter plus both
    sinks, removes the nested tap, and prevents later frames from re-entering
    the detached transform subtree while the base graph continues. Done.
287. Prove live buffered subtree detach from runtime resample taps:
    `TestTaskDetachBufferedRuntimeResampleTapSubtreeStopsFutureMessages` now
    runs a bounded buffered task while a late parent branch resamples audio from
    a frame tap, publishes a 16 kHz mono frame tap with retained sample-format
    shapes, and a dependent child attaches to that nested tap. Detaching the
    parent closes the resample filter plus both sinks, removes the nested tap,
    and prevents later frames from re-entering the detached audio transform
    subtree while the base graph continues. Done.
288. Prove runtime filter cleanup after late validation failure:
    `TestTaskAttachRejectsDuplicateTapAfterRuntimeFilterOpenAndClosesFilter`
    now opens a runtime resample filter, then rejects the branch because it
    tries to publish a tap name already present on the task. The opened filter
    is closed, no branch nodes or taps are registered, and the graph spec stays
    unchanged. Done.
289. Prove runtime graph rollback after filter-backed mutation failure:
    `TestTaskAttachRollsBackRuntimeFilterWhenGraphConnectFails` now uses a
    package-local graph that accepts the runtime resample stage, then rejects
    the first branch connection. Runtime attach removes the partially added
    stage, closes the opened filter, registers no branch taps, and leaves the
    graph spec unchanged. Done.
290. Prove runtime terminal-stage rollback after graph mutation failure:
    `TestTaskAttachRollsBackRuntimeTerminalStageWhenGraphConnectFails` now
    attaches from an audio frame tap, opens a resample filter, Opus encoder, and
    Ogg mux destination, then rejects the terminal target connection after two
    successful branch-stage connects. Runtime attach removes the mux, encoder,
    and transform nodes in reverse order, drops the partial edges, closes every
    owned component, registers no branch taps, and leaves the graph spec
    unchanged. Done.
291. Prove runtime sink-destination rollback after graph mutation failure:
    runtime rollback coverage now
    attaches from an audio frame tap, opens a resample filter, adds a terminal
    `Sink`, then rejects the terminal sink connection after the
    transform connect succeeds. Runtime attach removes the sink and transform
    nodes in reverse order, drops the partial edge, closes the filter and sink,
    registers no branch taps, and leaves the graph spec unchanged. Done.
292. Reject dynamic graph additions after close and clean prepared runtime
    components:
    direct and buffered graphs now return `pipeline.ErrClosed` from
    `AddSource`, `AddStage`, and `AddSink` once closed, with
    `TestGraphDirectRejectsAddAfterClose` and
    `TestGraphBufferedRejectsAddAfterClose` pinning the low-level invariant.
    `TestTaskAttachAfterCloseClosesPreparedRuntimeComponents` proves a late
    runtime branch can prepare a resample filter, Opus encoder, and Ogg mux
    target, then fail on the closed graph without leaking any prepared
    component, branch node, edge, or tap. Done.
293. Prove duplicate runtime node-name cleanup after component preparation:
    `TestTaskAttachClosesPreparedComponentsWhenRuntimeNodeNameExists` now
    prepares a late branch from an audio frame tap through resample, Opus
    encode, and an Ogg target, then rejects it because the computed terminal
    node already exists in the task graph. The opened filter, encoder, and
    muxer are closed, no branch node, edge, or tap is added, and the duplicate
    error stays actionable as `runtime_branch_node_duplicate`. Done.
294. Prove runtime flow plus custom codec composition:
    `TestTaskAttachRuntimeFlowCustomEncodeMuxBranch` now applies an
    `Flow(...).Encode(goav.Codec(...))` operation sequence to a live
    `Branch(...).FromTap(...)`, writes through a `Destination`, and verifies
    custom encoder config, muxed packet delivery, and detach cleanup. This keeps
    Flow as reusable operations rather than a destination and proves custom
    codecs compose through the same runtime Branch/Target grammar as built-ins.
    Done.
295. Add decode as a first-class branch operation:
    `Branch(...).Decode()` now lets packet-domain planned branches and runtime
    branches cross into frame-domain processing without a new workflow concept.
    `TestBranchCompositionPacketBranchDecodeSinkRuns` proves
    `Audio().Copy().Branches(Branch(...).Decode().To(Sink(...)))`
    describes, builds, and runs as select/decode/sink with graph equivalence.
    `TestTaskAttachRuntimeDecodeBranchFromPacketTap` proves a live packet tap
    can attach a branch-owned decoder, publish a decoded frame tap, feed a
    dependent branch from that tap, and detach the whole subtree cleanly.
    `TestBranchCompositionRejectsDecodeAfterBranchOperation` and
    `TestBranchCompositionRejectsDecodeThenCopy` pin the public operation order.
    `TestBranchCompositionRejectsDecodeFromFrameBranchPoint` keeps decode as the
    packet-to-frame boundary instead of compatibility sugar. Done.
296. Prove decoded packet branches compose into full operation chains:
    `TestBranchCompositionPacketBranchDecodeResampleEncodeMuxRuns` proves a
    planned packet-copy split can decode, resample, encode Opus, and mux through
    the same Branch/Target grammar.
    `TestTaskAttachRuntimeDecodeResampleEncodeMuxBranchFromPacketTap` proves a
    running packet tap can attach that same decode/resample/encode/mux chain,
    publish a post-encode packet tap, and detach all owned components. Done.
297. Let reusable flows own decode when that is the reusable operation boundary:
    `AudioFlow` and `VideoFlow` now expose `.Decode()` as a first operation.
    Applying a decode flow to a packet branch or runtime packet tap feeds the
    same BranchSpec decode path as direct `Branch.Decode()`, while applying it
    after an already-decoded stream fails with actionable guidance.
    `TestFlowDecodeAppliesToPacketBranchIntent`,
    `TestFlowDecodeRejectsAfterStreamDecode`,
    `TestFlowDecodeMustBeFirstOperation`, and
    `TestTaskAttachRuntimeFlowDecodeBranchFromPacketTap` pin the contract.
    Done.
298. Preserve flow media kind on branch application:
    `Branch.Apply(flow)` now carries the flow's audio/video kind into
    `BranchSpec`, so branch validation reports `flow_media_mismatch` directly
    when an `AudioFlow` is applied to a video branch or a branch mixes
    incompatible flow kinds. `TestFlowBranchMediaMismatchIsActionable` and
    `TestBranchRejectsConflictingFlowMedia` pin planned diagnostics, while
    `TestTaskAttachRuntimeFlowMediaMismatchBeforeMutation` proves runtime
    attach rejects the same mismatch before graph mutation. Done.
299. Prove flow-owned decode on direct chains:
    `Audio().Apply(Flow(name).Decode()...)` now has explicit intent and
    runtime coverage, completing the stream, planned branch, and runtime branch
    triangle for reusable decode-owning flows.
    `TestFlowDecodeAppliesToStreamRecipeIntent` pins the public intent shape,
    while `TestStreamRecipeFlowDecodeSinkRuns` proves Describe, Build, Run,
    tap metadata, and cleanup for direct stream flow decode. Done.
300. Carry operation-boundary metadata through public taps:
    `TapIntent`, planned `planTap`, runtime `TapInfo`, and `TapReport` now
    preserve the operation boundary a tap follows. Decode, transform, custom
    stage, encode, copy, and select taps stay self-describing for planned and
    runtime branch attachment.
    `TestFlowAppliesToTranscodeBranch`,
    `TestStreamRecipeFlowDecodeSinkRuns`,
    `TestBranchCompositionTaskExposesAndAttachesAfterResizeTap`,
    `TestStreamRecipeTaskAttachesAfterCustomStageAndEncodeTaps`, and
    `TestTaskAttachRuntimeFlowDecodeBranchFromPacketTap` pin the boundary
    metadata. Done.
301. Unify direct stream destinations with branch destinations:
	stream `.To(...)` now accepts the same `Destination` handles as branch
	`.To(...)`. Direct stream destination names travel beside destination
	attachments, so the planner validates logical destinations without renaming
	file or URI refs used for adapter probing.
    `TestStreamRecipeCanWriteToTypedDestination` and
    `TestStreamRecipeEncodeToTypedDestinationRuns` pin intent, Describe/Build
    equivalence, mux execution, and cleanup. Done.
302. Unify packet-preserving job destinations with the same destination grammar:
	root `From(input).Copy().To(...)` now accepts the same `Destination` handles
	as stream and branch `.To(...)` without a separate registration step.
	Job-level destination names travel beside destination
	attachments, preserving physical file/URI names for format probing while
    keeping stable logical destination names in intent and diagnostics.
    `TestRecordRecipeCanWriteToTypedDestination` and
    `TestRecordRecipeCopyToTypedDestinationRuns` pin intent, graph descriptions,
    build equivalence, mux execution, and cleanup. Done.
303. Validate duplicate runtime branch destinations before graph mutation:
    `Task.Attach(ctx, goav.Branch(...).To(target, target))` now fails with the
    same `Branch`/`Target` vocabulary used by planned branches, before opening
    encoders, opening muxers, registering taps, or changing graph edges.
    `TestTaskAttachRejectsDuplicateRuntimeBranchTargetsBeforeMutation` pins the
    diagnostic, zero resource opens, unchanged graph spec, and unchanged tap
    list. Done.
304. Make planned branch tap anchors honest:
    `Branches(...)` now honors `Branch(name).FromTap(name)` against the parent
    stream instead of silently treating every branch as current-point work.
    Branches can start from implicit decoded/packet taps or declared taps after
    decode, resize/resample, or custom stages; omitted `FromTap` still means the
    current stream point. Planned branches reject graph-node `.From(...)` and
    missing taps with actionable diagnostics, and stream-level encoders before
    `Branches(...)` are terminal for planned composition while post-encode taps
    remain the runtime `Task.Attach(...FromTap(...))` control-plane shape.
    `TestBranchCompositionCanSplitFromEarlierTap`,
    `TestBranchCompositionRejectsMissingPlannedTap`,
    `TestBranchCompositionRejectsGraphNodeSource`,
    `TestBranchCompositionRejectsStreamEncodeBeforeBranches`, and the
    packet-copy branch intent assertion pin the behavior. Done.
305. Attach runtime branch groups without adding a second concept:
    `Task.Attach(ctx, Branch(...), Branch(...))` now treats several runtime
    branches as one atomic attachment. Later branches in the same call can
    anchor from taps published by earlier branches, duplicate destination names with
    different destination handles fail before graph mutation, and any later
    prepare/connect failure rolls the whole group back. `Attachment.Spec()`,
    `Attachment.Stats()`,
    `Attachment.Close(ctx)`, and `Task.Detach(ctx, h)` operate over the grouped
    branch-owned nodes and taps, while parent-detach cleanup tracks every anchor
    used by a grouped attachment.
    `TestTaskAttachRuntimeBranchGroup`,
    `TestTaskAttachRuntimeBranchGroupCanUsePendingTap`,
    `TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`, and
    `TestTaskAttachRuntimeBranchGroupRejectsDuplicateMuxTargets` pin the behavior.
    Done.
306. Share runtime sink destinations inside branch groups:
    grouped runtime branches can now reuse the same `goav.Sink(sink)`
    destination handle and attach one shared sink node with multiple incoming
    branch routes. Different destination handles with the same name still fail
    before graph mutation, so sharing is explicit instead of stringly inferred.
    This makes `Destination` the shared sink group handle for runtime attach as
    well as planned composition, while file/URI mux destination sharing remains
    guarded for planned `Branches(...)` until live mux groups are implemented.
    `TestTaskAttachRuntimeBranchGroupSharesSinkTarget` pins the shared node,
    delivery, stats, and detach cleanup, and
    `TestTaskAttachRuntimeBranchGroupRejectsDuplicateSinkTargetNames` pins the
    duplicate-name diagnostic.
    Done.
307. Share runtime mux destinations inside branch groups:
    grouped runtime branches can now reuse the same `goav.File(...)`
    destination handle and attach one shared mux node opened with every branch
    output stream. Shared mux destinations are prepared
    from the full group before graph mutation, route every matching branch into
    one destination node, report branch-owned mux stats through `Attachment.Stats()`,
    and close on detach. Different destination handles with the same name still fail
    before graph mutation. `TestTaskAttachRuntimeBranchGroupSharesMuxTarget`
    pins the shared mux node, stream set, writes, stats, and detach cleanup.
    Done.
308. Rename the private runtime mux builder verb:
    the internal builder interface now uses `Mux(format.Output)`, and runtime
    compiler tests use `Mux(...)` so the older output-group vocabulary does not
    remain in private mux plumbing.
	Public recipes stay on the cleaner `Destination` handle model, while the
    advanced graph escape hatch remains handle-based. `TestRuntimeBuilderUsesMuxVerbNotOutput`
    pins the private builder contract.
    Done.
309. Let flows publish post-encode packet taps:
    `AudioFlow` and `VideoFlow` now carry taps declared after an encoder as
    packet-domain attach points, matching stream and branch tap semantics
    instead of making flows a weaker operation language. Flow-applied taps flow
    through direct chains, planned branches, and runtime branches.
    `TestFlowTapAfterEncodeIsPacketTap` pins the intent shape, and
    `TestTaskAttachRuntimeFlowCustomEncodeMuxBranch` proves a live flow-encoded
    runtime branch can publish a packet tap that another runtime branch attaches
    from.
    Done.
310. Rename the sealed `To(...)` argument:
    `.To(...)` accepts destination refs, while the concrete user concepts are
    destination handles returned by `File`, `URIOut`, `Sink`, `Writer`,
    `Object`, or `Custom`. Internal compiler bindings keep the same destination
    shape, so the old routing-union vocabulary does not remain in the public
    grammar. Slice coverage proves dynamic destination slices still work, and
    `TestPackageKeepsLegacyHelpersOutOfFrontDoor` rejects obsolete exported
    helper names.
    Done.
311. Make packet copy reusable as a flow operation:
    `Flow(name).Audio()` and `Flow(name).Video()` now expose `Copy()` as a
    packet-domain terminal operation. Copy flows can publish post-copy packet taps, apply to direct
    stream chains, planned packet branches, and runtime branches, and reject
    attempts to copy after decode, resample, resize, custom stages, or frame
    taps. `TestFlowCopyAppliesToStreamRecipeIntent`,
    `TestFlowCopyRequiresPacketDomain`, and
    `TestTaskAttachRuntimeFlowCopyBranchFromPacketTap` pin the intent,
    diagnostics, and live attach behavior.
    Done.
312. Add typed tap refs and the smaller first-page grammar:
    `FrameTap(name)` and `PacketTap(name)` now create typed tap refs for
    `.Tap(ref)` and runtime `.From(ref)`. Runtime attach
    validates typed tap domains against `Task.Taps()` before graph mutation, and
    stream/flow tap declarations reject frame-vs-packet mismatches at build
    time. `Sink(...)` is the preferred sink destination spelling, and
    `Flow(name).Audio()` / `Flow(name).Video()` are the canonical reusable-chain
    constructors. The README first page now teaches Input, Stream, Operation,
    Tap, Branch, Destination, Flow, and Task, and its preferred examples use
    typed taps, `Sink`, and canonical `Flow`. `TestRuntimeBranchTapAnchorIsPublicAPI`,
    `TestTypedTapRefsDriveStreamIntent`, `TestTypedTapDomainMismatchIsActionable`,
    and `TestReadmeUsesBranchTargetVocabulary` pin the public grammar.
    Done.
313. Remove unreleased compatibility sugar and build-kind dispatch:
    the old tap, flow, and sink helper spells no longer
    exist as public helper spells; typed taps, `Flow(name).Audio()/Video()`, and
    `Sink(...)` are the only front-door grammar. Tap steps now carry typed domain
    metadata into stream, branch, flow, and runtime-attach planning, so packet
    graph branches can publish packet taps after custom packet stages while
    decoded chains still reject packet taps before encode. Normal recipe build
    now carries an executable media-plan graph object instead of switching on a
    `mediaBuildKind` string. `TestPackageKeepsLegacyHelpersOutOfFrontDoor`,
    `TestReadmeUsesBranchTargetVocabulary`, `TestTaskAttachRuntimeBranchGroupCanUsePendingTap`,
    and media-plan executable-shape tests pin the cleanup.
    Done.
314. Carry typed branch anchors through public intent:
    `StreamIntent` now exposes `From TapRef` instead of a stringly
    `FromTap` field, so planned branch origins preserve both stable tap name and
    frame/packet domain all the way through the composition boundary. Current
    point branches keep an empty tap ref, explicit frame branches report
    `DomainFrame`, packet-copy branches report `DomainPacket`, and a structural
    API test rejects the old field name from returning. This keeps planned
    branches, runtime attach, and reusable flows on the same typed tap grammar.
    Done.
315. Collapse high-level implementation naming onto destinations:
    private target bindings, branch destinations, runtime branch terminal
    preparation, mux format helpers, and encode-to-destination graph helpers now
    use destination naming throughout. Internal target records
    store destinations, direct `.To(...)` bindings are direct destinations,
    runtime attach prepares branch destinations, and media-plan encode paths
    plan and compile destination paths. The old terminal vocabulary remains only
    in regression guards, not in active composition code.
    Done.
316. Hide concrete chain builder type names:
    `From(...).Audio()/Video()/Stream()`, `Branch(name)`, and
    `Flow(name).Audio()/Video()` still expose the same fluent chain grammar, but
    their concrete builder structs are package-private implementation details.
    The public named concept is `Chain`, with `BranchSpec` as the value passed
    into planned branches and runtime attach. Guard coverage now rejects
    `BranchBuilder`, `JobStreamBuilder`, `FlowBuilder`, `AudioFlowBuilder`, and
    `VideoFlowBuilder` as exported front-door types while preserving
    `Branches(...)` as the single planned split verb.
    Done.
317. Hide destination implementation values:
    destination constructors return the public `Destination` abstraction instead
    of exporting concrete implementation values. Shared destination name,
    concrete destination binding, and identity are still preserved internally for
    planned branch composition and runtime attach grouping, but users only pass
    the destination value to `.To(...)`. Guard coverage rejects concrete
    destination implementation types from the front door.
    Done.
318. Hide concrete destination specs:
    `File`, `URIOut`, `Sink`, and custom constructors now return public
    destination values
    instead of concrete output binding records. The concrete output binding
    record is package-private, compiler-state tests use private constructors
    where they need exact records, and guard coverage rejects concrete
    destination records as exported front-door types.
    Done.
319. Unify single-stream media-plan execution:
    decoded frame sinks, encode-to-file/URI destinations, encode-to-sink
    destinations, and mixed encoded fanout now all compile through
    `mediaPlanSingleStreamGraph` instead of separate sink and encode executable
    wrappers. The media-plan selector has one single-stream branch, tests assert
    that common single-stream recipes share that executable path, and flow
    diagnostics are covered by the destination-vocabulary guard.
    Done.
320. Share media-plan task execution:
    `recipeResolved.Build` now uses one media-plan executor that opens the
    runtime graph, asks the selected media plan to compile into it, closes the
    graph on compile failure, and returns the task. Packet copy, single-stream,
    and branch-composition plans expose `runtimeRef` plus `compile(...)` instead
    of each owning a duplicate `build` loop, and guard coverage keeps
    `mediaPlanExecutable` from reintroducing per-plan build methods.
    Done.
321. Collapse packet-copy and direct stream graphs:
    packet-preserving record/fanout jobs and decoded/encoded direct stream jobs
    now share `mediaPlanStreamGraph`. Packet copy is a stream-graph mode rather
    than its own executable graph family, the stream graph reuses one source
    opener for file and RTP inputs, and `mediaPlanGraph` now selects between
    stream execution and branch composition instead of packet-copy,
    single-stream, and branch families. Guard coverage rejects separate
    packet-copy and single-stream graph types.
    Done.
322. Share media-plan source lowering:
    stream and branch-composition graphs now use the same source-spec and
    source-compile helpers for file and RTP/WebRTC inputs. Branch composition no
    longer carries its own file/RTP source planning or opening methods, and
    guard coverage keeps the branch-only source helpers and old packet-copy
    source helper name from returning.
    Done.
323. Collapse operation-step vocabulary onto chain steps:
    stream chains, reusable flows, planned branches, branch-compose lowering,
    media-plan stream attachments, and runtime attach conversion now share the
    same internal `chainStep` representation for ordered stage, transform, and
    tap operations. Runtime branch attach lowers `BranchSpec` through
    `runtimeBranchStepsFromChain`, planned branch composition lowers through
    branch chain-step helpers, and guard coverage rejects the old
    job-stream step vocabulary from production chain internals.
    Done.
324. Remove the branch-compose step mirror:
    branch composition no longer owns a second stage/resize/resample step
    record beside `chainStep`. Advanced transcode boundary steps, stream-chain
    branches, ordered operation specs, shared decode anchors, and branch
    route lowering all carry `chainStep` directly, with tap-only steps filtered
    at the branch-composition boundary. Guard coverage now rejects
    the deleted mirror type from production chain internals.
    Done.
325. Keep diagnostics on the current front door:
    production build diagnostics no longer suggest old recipe families or
    stale terminal vocabulary when the user should choose `From(...)`,
    `Runtime.Graph()`, `Target`, `Sink`, `File`, or `URIOut`.
    Branch target validation now talks about destinations and targets, and guard
    coverage rejects the stale diagnostic vocabulary from production files.
    Done.
326. Name reusable fragments as chains internally:
    `Flow(name).Audio()/Video()` remains the public constructor for reusable
    operation fragments, but the implementation now lowers them through
    `chainSpec`, `chainBuilder`, and `chainSpecFrom` instead of a separate
    flow-shaped spec/builder vocabulary. The sealed `Chain` marker is now
    `isChain`, package comments list inputs/chains/taps/branches/targets/tasks,
    and guard coverage rejects the old reusable-flow implementation names.
    Done.
327. Keep architecture docs on the smaller grammar:
    architecture and roadmap wording now describe recipes as `From` plus
    chains, taps, branches, and targets, and call the resolved runtime boundary
    graph-plan lowerers instead of media-plan build kinds. Documentation
    guard coverage rejects stale phrases that would re-teach stream-chain or
    build-kind concepts as public design language.
    Done.
328. Surface shared branch work in Explain reports:
    `OperationSpec`, `planOperation`, and `OperationReport` now carry a
    `Shared` flag. Planned branches mark parent decode and shared chain
    operations when they reuse the current chain point or a typed frame tap,
    while branch-private encodes, transforms, and sink work remain unshared.
    `TestExplainMarksSharedBranchOperations` proves the report matches the
    graph's shared decode/resize/tap shape without adding another front-door
    branching concept.
    Done.
329. Keep current status docs on chain vocabulary:
    the package status table, done criteria, roadmap, architecture notes, and
    current pressure point now talk about chains instead of stream chains where
    they describe the public model. Guard coverage checks the living progress
    slices plus architecture and roadmap docs, while older numbered entries can
    remain as historical evidence.
    Done.
330. Make file and URI destinations canonical:
    writer-backed destinations now use `File(name, writer)`, URI-backed
    destinations use `URIOut(uri)`, and sink destinations continue to use
    `Sink(sink)`. README examples, use cases, diagnostics, tests, and guards
    now reject the old `FileOutput`/`URIOutput` spelling from the public
    surface, keeping destination construction aligned with `Target` and `To`.
    Done.
331. Stop teaching destinations as a public noun:
    living status and pressure-point docs now list `Target`, `File`, `URIOut`,
    and `Sink` under the small composition grammar instead of presenting
    destination machinery beside `Input`, `Chain`, `Tap`, `Branch`, and `Task`.
    Guard coverage rejects that regression in the current tracker slices while
    preserving older numbered entries as historical evidence.
    Done.
332. Remove the resolved-recipe builder fallback:
    `recipeResolved` no longer carries the old runtime builder, and
    `Describe()` now only returns the stored media-plan spec or an actionable
    unsupported media-plan diagnostic. Recipe compiler state now consists of
    intent, concrete attachments, media-plan reports, and the executable
    media-plan graph.
    Done.
333. Type runtime branch sources:
    `Branch.From(...)` now accepts only sealed branch-source values: typed taps
    for media outlets and `GraphNode`/`GraphOutlet` handles from
    `Runtime.Graph()` for expert graph attachments. Raw string node names no
    longer form the public branch-origin path, and guards keep README/current
    architecture docs plus attach tests on typed source values.
    Done.
334. Remove builder services from recipe compilation:
    normal `From(...)` recipes no longer open or carry the old runtime builder
    during compiler passes. Runtime validation now checks directly for the
    standard `Default()`/`New(...)` runtime before media-plan emission, and
    unsupported custom runtimes fail with recipe/runtime guidance instead of
    builder-specific diagnostics. Guard coverage rejects `builderAPI` fields in
    recipe compiler state and old runtime-builder wording in production
    diagnostics.
    Done.
335. Rename the public destination plumbing to destination refs:
    `File`, `URIOut`, and `Sink` moved onto the public destination-ref
    vocabulary accepted by `.To(...)`, replacing the old exported routing union.
    README and current architecture docs teach concrete destination values,
    while guard coverage rejects the old exported type from the front door. A
    later slice split direct destination refs from named refs so one destination
    handle cannot wrap another.
    Done.
336. Promote debugging and diagnostics as normal composition:
    README and use-case docs now show the intended debug loop: call
    `Explain(ctx)` before opening the graph, drain `Task.Events()` while the
    graph runs, attach a live diagnostic branch from a typed tap with
    `Task.Attach`, and sample both `Attachment.Stats()` and whole-task
    `Task.Stats()`. This keeps diagnostics on the same Input, Stream,
    Operation, Tap, Branch, Destination, Flow, and Task grammar instead of
    adding a separate debug API.
    Done.
337. Carry codec-specific configs and controls through recipe codecs:
    `CodecSpec.Settings` now carries one typed `Config`, named `Param` values,
    and typed `Control` values. Stream, branch, flow, branch-composition, and
    runtime attach paths clone and propagate those values into
    `codec.DecodeConfig.Settings` and `codec.EncodeConfig.Settings`, while
    Opus/VP8/VP9 remain the simple full-codec helpers and generic `Codec(...)`
    keeps custom adapters orthogonal.
    Runtime coverage proves declarative decode and encode options reach custom
    factories through the branch composer.
    Done.
338. Make performance and declarative-first explicit goal constraints:
    `docs/PERFORMANCE.md`, the roadmap, and this tracker now say recipe
    planning may allocate, but running packet/frame/event paths must reuse
    messages, result structs, buffers, and adapter scratch. The expert graph
    builder remains available for adapter work and advanced embedding, while
    normal workflows should be expressible through the declarative Input,
    Stream, Operation, Tap, Branch, Destination, Flow, and Task grammar.
    Done.
339. Set the next planner target to one executable graph plan:
    Direct streams become implicit branches, normal recipes lower toward
    `GraphPlan -> pipeline.Graph -> Task`, and runtime attach lowers toward
    `GraphPatch` from typed taps. This makes copy, decode-to-sink,
    encode-to-destination, branch composition, mixed target groups, and late
    attachments one branch-planning problem instead of separate workflow graph
    families.
    Done.
340. Split direct destinations from named routing:
    the temporary ref split was replaced by the public `Destination` model.
    Custom providers are wrapped by `Custom(name, provider)`, while destination
    handles stay opaque and cannot wrap other handles.
    Done.
341. Promote custom destinations as the extension surface:
    `Destination`, `DestinationContract`, `DestinationInfo`, `DestinationWriter`,
    `Writer`, `WriteCloser`, `Format`, and `MIME` make byte/object destinations
    implementable outside package `goav`. `File`, `URIOut`, `Sink`, `Writer`,
    and `Target` now feed `.To(...)` through the public `Destination` model,
    custom writers receive resolved destination info before mux open, transactional
    writers commit on successful run or detach and abort on build, runtime, or
    attach rollback failure, and README now teaches custom destinations instead
    of the explicit graph composer.
    Done.
342. Quarantine the explicit graph composer behind the expert API:
    `Runtime` is now probe-only, and manual graph wiring starts with
    `goav.Expert(runtime).Graph()` instead of `runtime.Graph()`. This keeps
    the normal application model to declarative recipes while preserving the
    handle-based graph layer for tests, adapter work, and intentional expert
    graph attachments.
    Done.
343. Introduce the executable graph-plan boundary:
    recipe compilation now emits `graphPlan` as the build/describe boundary.
    `MediaPlan` remains the planner/report IR, while `graphPlan` owns the
    planned `pipeline.Spec` and wraps the transition executable used to build
    the runtime graph. `recipeResolved` no longer carries the old media graph
    executable directly, and tests pin `GraphPlan -> pipeline.Graph -> Task` as
    the normal recipe path.
    Done.
344. Expand graph-plan cold-path metadata:
    `graphPlan` now owns cloned nodes, edges, inputs, streams, taps, branches,
    targets, decisions, and diagnostics instead of wrapping a stored
    `pipeline.Spec` plus leaving report data on `recipeResolved`. `Describe`,
    `Explain`, mux diagnostics, and task tap installation now read cloned views
    from the graph plan, so the remaining transition executable is only the
    runtime lowering bridge.
    Done.
345. Add ordered graph-plan operations:
    `graphPlan` now carries a cloned operation sequence derived from branch
    operations and target groups. The sequence includes source boundary,
    select, decode, tap, transform, encode, mux, and sink/write target
    operations with branch, node, shapes, sharing, and destination refs, giving the
    next build-lowering slice a concrete plan to execute instead of matching
    workflow-specific compiler shapes.
    Done.
346. Move build through graph-plan lowerers:
    the old executable interface is now a `graphPlanLowerer`, and
    `buildGraphPlanTask` calls `graphPlan.lower(...)` instead of invoking a
    workflow compile method directly. Graph-plan lowering validates readiness,
    edge sources and targets, operation kinds, branch ownership, and destination refs
    before a lowerer can mutate the runtime graph. Tests pin invalid ordered
    operations failing with `graph_plan_invalid` before lowerer mutation.
    Done.
347. Lower packet-copy build from graph-plan operations:
    packet-preserving copy now prepares runtime select and target lowering from
    `graphPlan.operations`. Missing selected-stream `OpSelect`, unexpected
    select operations, missing target operations, unbound destination refs, and
    sink/mux destination-kind mismatches fail as graph-plan errors before source
    opening or runtime graph mutation. The stream lowerer still owns concrete
    source and destination adapters while packet-copy sequencing comes from the
    plan.
    Done.
348. Validate direct frame stream build from graph-plan operations:
    direct decode/filter/encode stream build now prepares select, decode,
    transform/stage count, encode, and target operations from
    `graphPlan.operations` before source opening. Encoded stream targets lower
    from prepared graph-plan destination refs, so mux/sink ordering and binding come
    from the plan instead of raw output loops. Tests pin missing decode, encode,
    and target operations failing as `graph_plan_invalid` before adapter/source
    work can mask the planner error.
    Done.
349. Validate branch composition build from graph-plan operations:
    grouped branch composition now prepares every branch and typed destination from
    `graphPlan.operations` before source opening. Branches must have select,
    decode/copy, shared transform/stage, private transform/stage, encode, and
    target operations that match the concrete branch routes. Target operations
    must bind to typed destinations, match sink versus byte-target kind, and name the
    same branch set as the target route. Tests pin missing branch, decode, and
    target operations failing as `graph_plan_invalid` before adapter/source
    work can mask planner defects.
    Done.
350. Remove label wording from normal branch destination plumbing:
    `BranchSpec` and branch `streamBuild` now carry `destinationNames` bridge
    fields instead of internal `labels`, and normal branch duplicate/missing
    destination diagnostics talk about typed destination names. This keeps the
    front-door composition model centered on direct `Destination` values instead
    of leaking historical string-label routing into new planner work.
    Done.
351. Prove runtime attach to custom writer destinations:
    runtime `Task.Attach` now has a positive acceptance test for attaching a
    packet branch from a typed tap to a `Writer(...)` destination. The test
    verifies destination open happens during attach with `DestinationInfo` carrying
    name, format, MIME type, and streams, then confirms muxed bytes reach the
    writer and successful task close commits and closes exactly once without an
    abort.
    Done.
352. Lower branch-compose inputs from graph-plan operation refs:
    branch-compose graph plans now name shared select/decode/transform
    operations with the same selector-scoped node refs used by the described
    graph. Build preparation groups select and decode operation refs by
    selector, validates they are shared consistently, and passes those refs into
    runtime input lowering. The input lowerer uses the plan refs for select and
    decode stages while old advanced callers keep fallback naming. Tests mutate
    planned select/decode refs and prove the built graph still equals the
    described graph, so branch-compose input lowering is now executing from the
    ordered operation record instead of recomputing node identity.
    Done.
353. Lower branch-compose shared steps from graph-plan operation refs:
    branch-compose build preparation now collects shared transform/stage refs
    from ordered operation records, validates that branches sharing one selector
    group also share the same planned step refs, and passes those refs into
    route lowering. Runtime shared-step lowering uses the graph-plan node name
    for filters and wraps custom stages when a planned name is different, while
    older advanced compiler paths keep fallback names. Tests mutate a planned
    shared resample node and prove the built graph still equals the described
    graph.
    Done.
354. Lower branch-compose private steps and encoders from graph-plan refs:
    branch-compose build preparation now records each branch's private
    transform/stage refs and encode ref from ordered operations. Runtime branch
    route lowering names private filters/custom stages and encoders from those
    refs, with fallback names preserved for older advanced compiler paths.
    Tests mutate planned private resample and encode node refs and prove the
    built graph still equals the described graph.
    Done.
355. Add explicit transactional object destinations:
    `Object(name, open, opts...)` now exposes object-store upload style
    destinations as the same first-class `Destination` model as `File`, `URIOut`,
    `Sink`, and `Writer`. `Metadata(...)` destination options flow into
    `DestinationInfo`, and tests prove object destinations open with resolved format,
    MIME, streams, metadata, write through the muxer, commit on success, abort on
    failure through the existing transactional writer path, and close once.
    Done.
356. Lower branch-compose targets from graph-plan refs:
    graph-plan emission now stores concrete mux/sink node refs on target
    operations instead of logical target labels. Branch-compose target lowering
    validates target operation nodes, derives branch matches from target operation
    records, names mux and sink runtime nodes from those refs, and keeps fallback
    names for older advanced compiler paths. Tests mutate planned mux and sink
    destination nodes and prove the built graph still equals the described graph.
    Done.
357. Lower direct stream targets from graph-plan refs:
    packet-copy, decoded-frame sink, and encoded mux/sink direct stream lowerers
    now validate target operation nodes and name runtime mux/sink destination nodes
    from graph-plan refs. Tests mutate planned destination refs for packet-copy,
    decoded-frame, and encoded direct chains and prove the built graph still
    equals the described graph.
    Done.
358. Lower direct operation specs from graph-plan refs:
    decoded direct stream lowering now records select, decode, transform/stage,
    and encode operation node refs from the graph plan before source opening.
    Runtime decode/filter helpers accept planned node refs, built-in transforms
    and custom stages are named from those refs, and direct encode lowering uses
    the planned encode node ref. Tests mutate planned select, decode, resample,
    and encode refs and prove the built graph still equals the described graph.
    Done.
359. Scope selected direct lowering to one branch operation set:
    selected packet-copy and direct frame-stream graph-plan lowerers now isolate
    exactly one branch operation set before reading select, decode, filter,
    encode, and destination refs. Whole-input packet copy still supports multiple
    input branches, but selected direct chains can no longer accidentally lower
    against unrelated branch operations in the same plan. Tests inject a second
    planned branch into selected packet-copy and decoded frame-stream plans and
    prove graph-plan validation rejects them before source opening.
    Done.
360. Emit direct frame-stream specs through the branch route planner:
    decoded-to-sink, decode/filter/encode-to-destination, and encode-to-sink direct
    stream specs now convert their resolved single chain into one branch route
    plus target routes and call `planBranchComposeRoutes`. The old direct spec
    helpers remain only for older advanced builders. Custom stage details now
    use the stage's own node description so planned specs match built graphs,
    and branch route planning carries codec-change policy into shared decoder
    details and runtime decode construction. Tests pin the branch-planner path,
    preserve Describe/Build parity for direct frame-stream jobs, keep codec
    change policy visible, and keep graph-plan ref mutation coverage green.
    Done.
361. Lower direct frame-stream builds through branch routes:
    direct frame-stream runtime builds now convert the resolved chain into one
    branch route, map graph-plan select/decode/filter/encode/destination refs into
    branch-compose lowering structs, then call `compileBranchComposeInputs` and
    `compileBranchComposeRoutes`. The old direct decode/filter/encode/target
    lowerer helpers are gone from `mediaPlanStreamGraph`. Direct decoded sink
    chains preserve their decoder input-event drop behavior through an explicit
    route flag, while normal branch composition keeps events visible downstream.
    Guard tests pin both spec and runtime planner reuse, and existing
    Describe/Build parity plus graph-plan ref mutation tests stay green.
    Done.
362. Lower selected packet-copy through branch routes:
    selected packet-copy specs and runtime builds now convert the resolved
    selected stream into one branch route, map the graph-plan select ref and
    destination refs into branch-compose inputs/targets, then call the shared
    branch-compose input and route lowerers. Whole-input packet copy still keeps
    its multi-input fanout path. Guard tests pin selected packet-copy branch
    route reuse, and ref-mutation tests prove the selected packet-copy build
    consumes the planned select node instead of recreating a workflow-specific
    selector.
    Done.
363. Make destination configuration option-only:
    public destination constructors now all return `Destination` directly.
    `File`, `URIOut`, `Writer`, `WriteCloser`, and `Object` use the same
    `Format`, `MIME`, and `Metadata` option path, and the old
    `ConfigurableDestination` method-chain surface is gone. Tests enforce the
    constructor return types and keep `ConfigurableDestination` out of the
    public front door, while docs and diagnostics point users at constructor
    options instead of `.Format(...)`/`.MIME(...)` destination chaining.
    Done.
364. Introduce the runtime attach graph-patch boundary:
    `Task.Attach` now collects late-branch anchors, planned taps, applied nodes,
    routes, and published taps in a private `runtimeGraphPatch` before creating
    the attachment handle. Rollback removes patch nodes through that boundary,
    and attachment handles clone the patch state instead of aliasing local
    slices. Tests pin the graph-patch lowering shape and prove grouped runtime
    branches publish a pending tap once internally while still allowing a later
    branch in the same attach call to consume it.
    Done.
365. Share runtime attach mux destination preparation:
    individual late mux destinations and grouped shared mux destinations now use one
    runtime mux-format resolver and one mux compatibility helper, and
    single-target mux open/preflight lives behind
    `prepareRuntimeBranchMuxTerminal`. Sink terminals and shared-mux terminals
    are built by small helpers, so the runtime branch destination loop expresses
    domain decisions while target preparation stays concentrated behind the
    patch-era helpers. Guard tests reject split mux format or compatibility
    helpers from returning.
    Done.
366. Validate packet-copy operation and target records during lowering:
    whole-input and selected packet-copy lowerers now require the executable
    graph plan to contain the planned `copy` operation before any source is
    opened. Whole-input packet copy now also requires target operations to agree
    on the same node/kind across every branch feeding a named target, validates
    the target operation branch refs against the media-plan output branch refs,
    and connects only the matched source branches while preserving all streams
    for single-source remux. Regression tests remove `OpCopy`, remove one branch
    target binding from a multi-input copy plan, and mutate duplicate target
    nodes to prove build fails from graph-plan validation before runtime graph
    construction.
    Done.
367. Tighten the flow versus branch rule in docs:
    README and use-case reuse sections now state the direct-chain versus branch
    split rule explicitly: use a direct chain when one reusable flow feeds one
    target, and use branches when the same media point needs several downstream
    chains. Documentation guard tests reject the old repeated-direct-flow
    anti-pattern and require the rule text beside the distinct branch examples.
    Done.
368. Carry source stream groups through packet-copy lowering:
    `compileMediaPlanSources` now records the streams that belong to each opened
    source. Whole-input packet-copy destination lowering uses those groups with the
    graph-plan branch matches, so a matched source branch contributes all of its
    streams and no streams from unmatched branches. Regression coverage proves
    branch stream-group selection directly and preserves all streams for
    single-source remux into a mux destination.
    Done.
369. Start formal media shape contracts:
    `MediaShape`, `ShapeSet`, `ShapeContract`, shape constructors, compatibility
    checks, and operation/flow shape reporting are public. Planner state, plan
    reports, tap reports, reusable flows, and compile diagnostics now use the
    `Shape` vocabulary directly; pre-release compatibility aliases were removed
    instead of preserved.
    Done.
370. Add GoAV-native branch buffer API:
    `BranchBuffer`, `BranchBufferMode`, `Blocking`, `DropOldest`,
    `DropNewest`, `Latest`, `CopyMode`, and copy-bound options are the normal
    branch-facing buffering API. `Branch.Buffer(...)` now accepts
    `BranchBuffer` and lowers to `pipeline.BufferPolicy` internally, keeping
    graph buffer structs out of normal branch composition. Tests cover
    constructor lowering, invalid policies, unsupported unbounded buffers, and
    the public method signature.
    Done.
371. Add media shape annotations:
    `Shape(MediaShape)` is now an ordered operation on stream chains, branches,
    audio/video flows, and runtime attachments. It updates the current media
    shape without creating a graph node, so structural compatibility facts can
    be attached after decode, resize/resample, custom stages, or runtime taps
    without adding one-off branch verbs. Encoder tuning is deliberately not part
    of shape: bitrate, FPS, keyframe cadence, profile/level, configs, params,
    and controls live only on `CodecSpec`. Planned reports show the shape
    operation and post-encode taps inherit the resulting structural shape. The
    WebRTC runtime ladder demo now uses `Branch` vocabulary end to end.
    Done.
372. Enforce ordered operation shape contracts:
    Normal jobs and branch-composition jobs now run a compiler pass that walks
    each stream's ordered operations from the selected packet stream shape.
    Decode, resize/resample, encode, and copy consume declared `ShapeSet`
    contracts; `Shape(...)` annotations update the current shape but cannot make
    a later operation consume the wrong domain or media kind. Diagnostics report
    operation index, expected shape, actual shape, and targeted suggestions.
    Decode shape contracts now require packet domain/media/codec identity
    without treating preset sample-rate or channel defaults as proof required
    from packet inputs.
    Done.
373. Validate terminal destination shapes and runtime attach shapes:
    Planned recipes now compute each stream's final ordered-operation shape and
    reject file, URI, writer, object, and mux destinations unless the terminal media
    is packet-domain. Sink targets remain the raw observation endpoint for frame
    or packet shapes. Runtime `Task.Attach` now runs the same ordered operation
    shape validation before allocating branch decoders, filters, encoders, muxers,
    or sinks, and runtime byte destinations use the same packet-domain terminal rule.
    Rejected runtime branches keep the graph unmodified.
    Done.
374. Add task snapshots for runtime diagnostics:
    `Task.Snapshot()` now returns the current graph spec, graph stats, stable
    taps, and active runtime branch snapshots. Each branch snapshot reports its ID,
    name, lifecycle string, tap/node anchors, branch-owned nodes, nested taps,
    scoped spec, and scoped stats, so live diagnostics can inspect a running task
    without graph handles or manual attachment bookkeeping.
    Done.
375. Start collapsing targets into destination handles:
    Built-in `File`, `URIOut`, `Writer`, `Object`, `WriteCloser`, and `Sink`
    destinations now carry stable internal identity. Planned branches and runtime
    attach can share one mux or sink group by reusing the same destination value
    directly, while two different destination handles with the same name fail
    during planning. README and use-case examples now teach shared destinations
    without `Target(...)`, and user-facing duplicate/missing destination
    diagnostics point at destination values.
    Done.
376. Remove the public `Target` compatibility wrapper:
    `Target(...)` is no longer exported from the root API, and `Destination` is
    now a concrete opaque handle rather than an externally implemented
    interface. Public tests and runtime examples pass `Destination` values
    directly, and planner assertions expect destination names such as
    `archive.ogg` or sink names instead of separate logical destination names.
    `DestinationProvider` plus `Custom(name, provider, opts...)` is the
    custom-provider entry point, so external provider implementations must be
    wrapped by a goav-owned destination handle before they can participate in
    routing identity. Diagnostics that previously suggested `Target(...)` now
    point at `File`, `URIOut`, `Sink`, `Custom`, or `.To(output)`.
    Done.
377. Start converging direct streams, branches, and flows on one ordered
    operation list:
    `chainSpec`, `BranchSpec`, direct stream builds, and branch stream builds
    now carry `[]OperationSpec` as the migration base for the one-operation
    model. Flow shape/tap reporting reads the ordered operation list, fluent
    flow/branch/direct-stream methods append operation records as they mutate
    the bridge fields, and direct stream methods that imply frames now record
    their implicit decode operation before transform/stage/encode work. Direct
    stream intent now reads the stored operation list when present. Planned
    branch splits preserve the combined parent-plus-branch operation list, and
    tests pin flow, direct stream, and branch operation order. Legacy parallel
    fields remain only as lowerer bridge debt until `WorkPlan`/`WorkPatch`
    consumes operation lists directly.
    Done.
378. Split planned branch operations into shared parent work and private branch
    work:
    Planned branch stream builds now preserve both a flattened operation view
    and explicit `sharedOps`/`privateOps` metadata. The split keeps parent
    decode/transform/tap operations only through the selected branch anchor,
    treats parent `.Copy()` as a packet-domain anchor instead of branch work
    when a downstream branch decodes, and keeps unchanged packet-copy fanout as
    copy-plus-packet-tap work. Normalized branch operation reporting now inserts
    implicit decode when a branch-local flow starts with frame operations, drops
    parent copy before decode/transform/encode branches, and marks parent
    operations shared while branch-local resize/encode work stays private.
    Tests pin implicit decode, parent-copy packet anchoring, earlier frame-tap
    slicing, and shared/private operation flags.
    Done.
379. Project branch-compose bridge steps from split operations:
    When planned branch stream builds carry split operation metadata, the branch
    composition planner now derives `SharedSteps` and branch-local `Steps` from
    `sharedOps` and `privateOps` instead of from the older `sharedSteps`/`steps`
    bridge fields. This keeps the existing `branchComposePlan` lowerers stable
    while moving their input source toward the single operation model. Tests
    clear the bridge step fields before planning and prove shared parent resize
    and private thumbnail resize still survive through operation projection.
    Done.
380. Carry operation records through the branch-compose plan:
    `branchComposeBranch` now carries the flattened operation list plus
    `SharedOperations` and `PrivateOperations` beside the legacy step fields.
    Recipe-created branch plans populate those operation records from
    `streamBuildOperationSpecs`, while intent-only paths split operations by their
    `Shared` flag. Route preparation now prefers operation records when
    creating shared and branch-local transform routes, using the projected step
    fields only as a compatibility fallback. Tests prove packet-copy branches
    retain copy/tap operations in the plan and that routes still prepare from
    operation records after the bridge step fields are cleared.
    Done.
381. Emit branch-compose graph-plan operations from the branch-compose plan:
    `buildMediaPlan` now detects branch-compose plans with operation records
    and builds plan streams, branches, decisions, taps, and graph-plan
    operations from `state.plan.Branches[*].Operations` instead of relying on
    `state.intent.Streams` as the only operation source. Intent-only branch
    paths still fall back to the existing stream intent planner, and legacy
    branch-compose plans without operations keep their previous path. Tests
    clear intent operation specs after the plan is built, then prove the
    media plan and `graphPlanOperationsFromMediaPlan` still emit decode, taps,
    shared resize, private encode, and target operations from the plan records.
    Done.

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
- The internal runtime compiler can also plan and compile simple remux/fanout
  jobs from low-level `format.Input` and `format.Output` values when registered
  format adapters can probe, demux, and mux the selected boundaries.
- The recipe/runtime path can plan and compile selected-stream decode jobs from
  stream-scoped `.To(goav.Sink(...))` recipes when format probing
  resolves one matching stream and the codec registry has a decoder factory. The
  graph includes an explicit stream-select stage so unrelated packets do not
  reach the decoder, and optional filter stages can run before the sink.
- `adapters/ivf` provides a narrow packet recording boundary for one VP8, VP9,
  or AV1 video stream with allocation-guarded demux/mux hot paths.
- The internal runtime compiler can plan and compile RTP/WebRTC packet-reader
  record jobs, including jitter and depacketizer selection, repeated RTP/WebRTC
  inputs, aggregated stream lists for muxers, multiple mux outputs, lifecycle
  closure, graph specs, and event visibility.
- The recipe/runtime path can plan and compile selected-stream live decode jobs
  from stream-scoped RTP/WebRTC `.To(goav.Sink(...))` recipes,
  including repeated RTP/WebRTC inputs, graph specs, decoder lifecycle closure,
  and filtering of unrelated packets and stream-scoped EOS before they reach the
  decoder.
  Ordered filter stages can run between decode and the sink when their selector
  matches the decoded stream. Decoder factories that implement
  `codec.DecodeStateFactory` can provision adapter-specific state for this
  high-level path before `NewDecoder`.
- The internal runtime compiler can plan and compile selected-stream encode
  jobs. The graph shares the selected decode/filter prefix, requires an
  explicit target codec, forwards EOS far enough to flush encoders, and can fan
  one encoded packet stream to multiple mux outputs.
- The runtime builder can plan and compile transcode recipe jobs grouped by
  selected stream. Video branches share a video decode, audio branches share an
  audio decode, each branch becomes a named encoded stream, and one mux output
  can receive coordinated audio and video branches. Graph description is
  equivalent before and after build. Resize and resample branch configs insert
  filter stages when matching filter factories are registered, preallocate
  branch frame scratch when geometry is known, and fail explicitly when missing.
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
- `adapters/govpx` registers concrete VP8 and VP9 decoder
  factories over `github.com/thesyncim/govpx`, decodes real samples into
  caller-owned I420 `av.Frame` planes, drops inter frames until a keyframe after
  startup or loss, requests keyframes through `codec.ControlRequest`, resets on
  codec-change/discontinuity events, and has allocation guards for adapter
  output preparation and loss request emission plus closed-state lifecycle
  tests. The same adapter registers VP8 and VP9 encode, mapping
  caller-owned I420 frames into caller-owned packet buffers, forcing keyframes
  on request or discontinuity, and preserving merged descriptors for decode and
  encode.
- `adapters/goav1` registers a concrete decoder factory over
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
- The shared-decode transcode compiler uses the same graph policy: a
  multi-branch graph fails on unsafe encoder-owned packet payloads without a
  copy bound and delivers copied encoded payloads to multiple mux outputs when
  `CopyPacketBytes` is configured.
- Internal RTP/WebRTC record/fanout compilers use that policy too:
  depacketizer-owned packet payloads fail without a copy bound and copied
  payloads reach every mux output when `CopyPacketBytes` is configured.

## Adapter Focus

- `adapters/ivf`: IVF packet demux/mux is active for single-stream VP8, VP9,
  and AV1 recording paths.
- `adapters/annexb`: H264 Annex B packet mux is active for single-stream H264
  recording paths.
- `adapters/resample`: S16 audio resample and channel conversion filter is
  active, using caller-owned output buffers and allocation-guarded hot paths.
- `adapters/resize`: I420/YUV420P video resize filter is active, using
  caller-owned output planes and allocation-guarded hot paths.
- `adapters/gopus`: Opus decode and encode are active, with PLC via loss
  events and caller-owned packet/frame buffers.
- `adapters/govpx`: VP8 and VP9 decode into caller-owned I420 frames plus VP8
  and VP9 encode into caller-owned packet buffers.
- `adapters/goav1`: low-overhead AV1 decode is active over caller-owned or
  runtime-provided state, borrowed
  gray8/I420/I422/I444 output with yuv420p/yuv422p/yuv444p accepted as planar
  aliases, loss keyframe requests, drop-until-sync recovery from packet markers
  or parsed payloads, concrete raw RTP payload decode with retained-fragment
  recovery, high-level RTP receive and replacement-stream codec-change proofs
  for old-ID and replacement-ID event routing, runner reuse, and
  allocation/lifecycle guards.
- `adapters/goh264`: descriptor-only by default; `goav_goh264` activates a
  decode factory for 8-bit planar H264 frames, with adapter-owned allocation
  and lifecycle guards active.

## Done Criteria

| Gate | Evidence | State |
| --- | --- | --- |
| Clear minimal architecture | `README.md`, `docs/ARCHITECTURE.md`, package boundaries | active |
| Simple high-level API | `From`, stream selection, ordered operations, typed taps, branches, destinations, flows, runtime attach, structured `Explain(ctx)`, executable work-plan boundary, and custom codec hooks | first slices active |
| Explicit low-level API | `pipeline`, `codec`, `format`, `rtpav`, `webrtcav` contracts for advanced embedding and adapter work, not the normal composer | active |
| Full Opus/VP8/VP9 codec verticals | Opus, VP8, and VP9 are the first full encode/decode recipe targets; H264 and AV1 stay receive/decode-first until encode is equally solid | active |
| Formal media shapes | `MediaShape`/shape-contract planning owns structural media attributes; operation and flow contracts now expose inferred input/output shape, and ordered job/branch operation validation is active, with destination validation next | active |
| Branch flow control | branch-local `BranchBuffer` policy is public branch API; drop accounting, branch stats, and safe packet/frame ownership remain active work | active |
| Observation/control | `Branch + Do + Sink`, `Events`, `Snapshot`, branch snapshots, destination snapshots, and scoped stats expose diagnostics without graph handles | first snapshot slice active |
| One grammar, one engine | normal workflows lower from `input -> stream -> operations -> tap -> branch -> destination` into `WorkPlan -> pipeline.Graph -> Task`; runtime attach lowers the same branch model into `WorkPatch` | planned |
| Custom sources | custom frame/packet/event sources mirror custom stages, sinks, writers, and object destinations | planned |
| Allocation guarded hot paths | `testing.AllocsPerRun` guards across core/RTP/codec/format/adapters | active for implemented paths |
| Adapter boundaries | `adapters/ivf`, `adapters/annexb`, `adapters/gopus`, `adapters/govpx`, `adapters/goav1`, `adapters/goh264` | active |
| No cgo | `hygiene_test.go` | active |
| Lightweight core imports | codec modules isolated under `adapters/...` | active |
| Docs explain shape | README, architecture, adapters, performance, RTP/WebRTC docs | active |

## Next Slices

1. Finish collapsing destination internals onto `Destination`. `File`,
   `URIOut`, `Writer`, `Object`, `Sink`, and `Custom` return stable destination
   handles. Reusing one destination handle groups branches; different
   destinations with the same name but incompatible providers/config fail during
   planning. README and normal API examples must keep avoiding `Target(...)` and
   `.To("label")`.
2. Replace parallel stream/branch fields with one ordered operation list.
   `Decode`, `Copy`, `Shape`, `Resize`, `Resample`, `Do`, `Encode`, codec
   helpers, and `Tap` all append `OperationSpec` entries. Direct streams,
   planned branches, runtime attach, and flows share validation and lowering.
3. Treat direct streams as implicit branches. Copy-to-file, decode-to-sink,
   encode-to-destination, branch composition, and mixed audio/video shared
   destinations become branch plans over ordered operations, not workflow modes.
4. Build one planner for build and attach. Initial jobs emit `WorkPlan`; runtime
   attach emits `WorkPatch` from the same branch planner against existing taps.
   `Explain`, `Build`, `Attach`, and `Snapshot` must read the same planned work.
5. Remove old workflow residue from normal composition: `branchComposePlan`,
   `runtimeBranch`, labels, string destination names, `destinationNames` bridge fields, `transcode`
   imports, workflow-kind compiler switches, route-policy leaks, and pre-release
   compatibility aliases.
6. Keep `Flow` boring. A flow is reusable operations plus media kind, shape
   contract, taps, and explanation; it has no destination, source, runtime state,
   label, destination name, or branch lifecycle.
7. Keep `Shape` as the central validation system. Inputs, operations, flows,
   taps, branches, sinks, byte destinations, shared destinations, codecs,
   transforms, and custom sources must participate in expected-vs-actual shape
   diagnostics.
8. Complete custom source symmetry. The active source foundation uses
   `Source(name, PacketShape(...), func(ctx, push) error)` and
   `Source(name, FrameShape(...), func(ctx, push) error)` with `SourcePush` for
   packets, frames, events, and EOS through normal `From(input)` recipes, and
   `Source(name, EventShape(), func(ctx, push) error)` for event-only source to
   sink workflows.
9. Support multi-input composition without graph handles. `From(inputs...)`
   selects streams across inputs, supports input/stream/codec selectors, reports
   ambiguity with candidates, and lets branches from different inputs feed one
   shared destination when timebase/format compatibility is valid.
10. Keep branch buffering and lifecycle first-class. Branch buffers, detach
    policy, drain/abort behavior, transactional destination commit/abort, close
    exactly once, branch-local drops, and sibling isolation are planner/executor
    responsibilities.
11. Keep observation as branch composition unless proven insufficient. A
    diagnostic branch is `Branch(...).From(tap).Do(...).To(Sink(...))`; temporary
    blocking or replacement behavior should be branch options, not a separate
    graph/probe system.
12. Extend snapshots instead of adding a bus/query subsystem. `Task.Snapshot()`
    and `Attachment.Snapshot()` should cover task state, taps, branch snapshots,
    destination snapshots, lifecycle, scoped stats, and attach/detach/drop/error
    events while `Events()` remains the event stream.
13. Preserve zero-allocation/zero-cost hot paths. Planning may allocate; running
    packet/frame/event paths must reuse messages, result structs, buffers, and
    adapter scratch with allocation guards for every new planner slice.
14. Keep Opus, VP8, and VP9 as the complete encode/decode verticals while H264
    and AV1 stay receive/decode-first until encode adapters are equally solid.
15. Add acceptance tests that forbid old front-door vocabulary, prove direct
    chain equals implicit branch, prove shared destinations replace named
    targets, prove runtime attach and planned branches share the planner, and
    prove adding an operation requires only constructor, shape contract, and
    component builder.

Current pressure point: collapse typed destination routing into stable `Destination`
handles, then normalize direct streams and branches onto one ordered operation
list so shape validation, destination compatibility, runtime attach, and branch
buffers move into the single `WorkPlan`/`WorkPatch` planner.
Packet-copy, direct frame-stream, and grouped branch-compose builds now consume
graph-plan operation records for validation, operation node construction,
destination node construction, and destination routing; direct frame-stream builds consume
one branch operation set plus select/decode/filter/encode refs, direct
frame-stream specs and runtime builds use branch route helpers, selected
packet-copy specs and runtime builds consume one branch route plus planned
select/destination refs through the same helpers, and grouped branch-compose consumes
select/decode input refs, shared/private step refs, encode refs, and destination
refs. Whole-input packet copy still preserves multi-input fanout while it waits
for the broader branch/patch planner shape, but its lowerer now validates
planned copy operations, consistent duplicate destination operations, and target
branch bindings before source opening, then routes destinations by planned branch
matches with source-owned stream groups. The intended public recipe surface is
small: `From`, stream selection, ordered operations, `Tap`, `Branch`,
`Branches`, `File`, `URIOut`, `Writer`,
`Object`, `Sink`, `Flow`, `Codec`, and runtime `Attach`; `Destination` is the
stable routing handle and externally implementable extension surface for custom
byte writers, object uploads, URI-backed outputs, sink groups, and shared mux
groups without graph-node plumbing. Flows expand
optional first decode plus ordered stage/tap/transform/encode operations into
branch intent instead of a parallel graph language, and
codec-specific config stays in `CodecSpec.Settings` through the `Bitrate`,
`FPS`, `KeyframeInterval`, `Profile`, `Level`, `Config`, `Param`, and
`Control` option constructors instead of growing per-codec public APIs. Opus,
VP8, and VP9 are the full encode/decode verticals; H264 and AV1 recipe encode
remains guarded until the adapters are complete.
`Task.Attach` remains the late branch control plane for running graphs, now with
a private runtime graph-patch boundary that owns anchors, planned taps, applied
nodes, routes, and published taps during prepare/apply/rollback,
shared mux-format resolution and single mux-terminal preflight are behind
runtime attach destination helpers,
including custom-stage, resize/resample, branch-local node stats, dependent
branches after runtime resize and resample taps, post-encode packet taps
feeding dependent packet-copy branches, live buffered parent detach that removes
nested transform frame-tap, custom-stage frame-tap, and post-encode packet-tap
subtrees before future media reaches them, runtime filter cleanup after
post-open duplicate-tap rejection, graph rollback after post-open connect
failure, terminal-stage rollback after post-open transform/encode/mux destination
mutation failure, sink-destination rollback after post-open transform/sink graph
mutation failure, flow-applied Opus encode-to-destination branches, late Opus/VP8/VP9
encode-to-destination, packet-copy destination, packet-copy recording, Opus encoded
late recording, and sink branches that can publish nested runtime taps for later
attachments. Direct and buffered graphs now reject dynamic node additions after
close so runtime attach fails before mutating a closed graph while still closing
prepared branch components, and duplicate runtime node-name validation closes
already-prepared branch components before returning.
Runtime attachments also now prove flow-applied custom `Codec(...)` encode
branches to typed destinations, matching the same operation grammar used by planned
branches and direct chains.
`Branch.Decode()` is active for packet-domain planned branches and runtime
packet taps, so packet-copy fanout and late receive tasks can branch into raw
frame processing, transform, re-encode, publish new packet taps, and write new
destinations without rebuilding the task.
Runtime branch origins are typed: stable media outlets use `FrameTap` or
`PacketTap`, while expert explicit-graph attachments use the `GraphNode` or
`GraphOutlet` handles returned by `goav.Expert(runtime).Graph()`.
Front-door examples now include the diagnostics loop explicitly:
`Explain(ctx)` before build, `Task.Events()` while running, `Task.Attach` for a
live debug branch, and scoped `Attachment.Stats()` beside whole-task
`Task.Stats()`.
Recipe compilation no longer opens a builder as a validation side channel; the
normal path validates runtime support, emits a graph plan, and builds from that
graph plan. The next shape is stricter: every normal expression lowers to
`GraphPlan`, and every late branch lowers to the runtime graph-patch boundary. Direct streams are
implicit branches, runtime attach is downstream patch application from typed
taps, planned and runtime branches must share shape/destination/buffer/lifecycle
validation, and no normal composition path should dispatch by copy/sink/encode/
branch workflow shape.
The expert graph builder is not the target user-facing composer; it remains the
advanced escape hatch and runtime substrate while normal use cases move through
declarative recipes. Flexible composition must still compile into direct
runtime objects: hot packet/frame/event paths should stay allocation-guarded and
avoid dispatching through recipe abstractions.
Destination configuration is deliberately singular: pass `Format`, `MIME`, and
`Metadata` options to `File`, `URIOut`, `Writer`, `WriteCloser`, or `Object`;
do not grow destination method-chain sugar back onto the front door.
The public intent/report surface now uses `DestinationIntent`,
`Intent.Destinations`, stream `Destinations`, and `DestinationReport`; exported
`TargetIntent`, `TargetReport`, and `.Targets` fields are removed instead of
kept as compatibility aliases.
`MediaPlan` currently expresses record, stream decode, encode, and branch
composition as input refs, stream selectors, ordered operations, destination
refs, taps, and planner decisions. `GraphPlan` now carries a derived internal
`workPlan` with inputs, streams, operations, taps, branches, destinations,
edges, decisions, and diagnostics. Runtime attach now also emits an internal
`workPatch` from prepared branch specs before graph mutation, carrying branch
operations, typed taps, destination groups, planned edges, and rollback
metadata on the resulting attachment. `Task.Snapshot()` and
`Attachment.Snapshot()` now report runtime branch destinations from that
`workPatch`. These are the new executable convergence boundaries: `Describe`,
`Build`, `Explain(ctx)`, `Attach`, and `Snapshot` must move toward the same
planned work instead of parallel workflow compilers. The next implementation
work is to make `workPlan`/`workPatch` the actual lowerer input, delete
`branchComposePlan` and `runtimeBranch` as separate models, broaden
descriptor-backed destination/container capability data as WebM/Ogg arrive, and
keep broadening runtime attachment stress around generic lifecycle boundaries
without weakening the direct branch grammar.
The latest direction sharpens the active goal further: normal composition must
use only `Input`, `Stream`, `Operation`, `Tap`, `Branch`, `Destination`,
`Flow`, and `Task`;
`Branch` is the real unit of work; `Destination` is the routing handle; `Flow`
is only reusable operations; runtime inspection is attached branch work; and
`WorkPlan`/`WorkPatch` are the executable boundaries for build and attach.
Current code now names the shared ordered operation record `OperationSpec`
instead of `StreamOperation`, so chains, flows, planned branches, runtime branch
shape validation, explanations, and media plans no longer describe that record
as stream-only. The next cleanup remains deleting the projection bridge around
`chainStep`, `branchComposePlan`, and `runtimeBranch` so `OperationSpec` feeds
the planner directly.
Codec tuning is also part of the active simplification: there is one data owner
for bitrate, FPS, keyframe cadence, profile/level, typed configs, named params,
and controls: `codec.CodecSettings`, carried by `CodecSpec.Settings`,
`codec.DecodeConfig.Settings`, and `codec.EncodeConfig.Settings`. The public
address remains the codec option list passed to `Opus`, `VP8`, `VP9`, `H264`,
`AV1`, or generic `Codec`. Chain, branch, and flow builders expose
`.Encode(codec)` as the only encode operation, keeping the main builder from
accumulating one-off tuning methods. `OpusVoice` and `OpusMusic` are removed
before release for the same reason; voice/music choices are ordinary
`Opus(Bitrate(...), Channels(...))` codec values.
Reusable flows and ordinary chains now store their ordered work
only as `OperationSpec`. Legacy `chainStep` is derived at the remaining
branch/runtime lowerer boundary, which keeps this pass scoped while making the
next deletion obvious: branch composition and runtime attach should consume the
same operation sequence directly instead of projecting back through a parallel
step list.
Planned and runtime branch specs now follow the same rule: `BranchSpec` stores
ordered work only as `OperationSpec`, and the temporary `chainStep` view is
derived while the older branch-compose and runtime attach lowerers still need
it. Planned branch stream builds now also keep their work as `OperationSpec`
plus split `sharedOps`/`privateOps`; they no longer store `sharedSteps` or
branch `steps`. `branchComposePlan` now follows that shape too: branches carry
flattened, shared, and private operation records instead of legacy
`SharedSteps`/`Steps` fields. The remaining step bridge is now in lowerer route
execution state and `runtimeBranch`, while the larger `branchComposePlan` model
still needs to collapse into `WorkPlan`.
Runtime attach now starts from the same canonical sequence too:
`runtimeBranch` carries `[]OperationSpec` from `BranchSpec`, shape validation
uses those operations directly, and mutable `runtimeBranchStep` values are
derived only as preparation/execution state for opened decoders, filters,
encoders, taps, and rollback.
`WorkPatch` now reports runtime branch work by walking the canonical
`OperationSpec` list and pairing each operation with its prepared runtime step
only when a node or prepared shape exists. Packet copy is therefore visible as
a real operation even though it has no stage, and mutable steps are no longer
the reporting source of truth.
Operation-backed transforms now drive validation, explanation, media planning,
and filter lowering before the legacy `StreamIntent.Transforms` mirror is read.
Private `chainSpec`, `BranchSpec`, and planned `streamBuild` state no longer
store a parallel transform slice; transforms live in `OperationSpec` there.
`StreamIntent.Transforms` remains only a temporary report/migration mirror while
the older lowerers are deleted.

## Validation Gates

- `go test ./...`
- allocation tests for reset/results/pipeline/RTP/depacketize/adapters
- benchmarks for passthrough, RTP Opus depacketize, fanout no-copy, and gopus
  decode adapter
- no core cgo imports
- lifecycle tests for start/close/flush/late-after-close behavior
