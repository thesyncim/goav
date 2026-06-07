# Roadmap

`PROGRESS.md` is the compact tracker for current implementation status and
validation gates. This roadmap keeps the broader phase view.

## Next-Level Priorities

The recipe surface is now pointed in the right direction; the next work is to
make the implementation match the composable planner promise.

1. Make `Intent -> MediaPlan -> pipeline.Spec -> pipeline.Graph` the normal
   recipe branch. Normal recipes should require media-plan recognition instead of
   adding workflow-specific matchers.
2. Treat branches as generic ordered stream operations and mux groups.
   `From(input).Audio()/Video().Branches(...)`, `Branch(...)`, and
   `AudioFlow`/`VideoFlow` values should produce equivalent `MediaPlan` shapes
   where possible. Branches must be orthogonal at operation boundaries: after
   decode, after resize/resample, after custom stages, after taps, and later
   after sink/output attachment where runtime support makes sense. Custom stage
   and transform steps are active, reusable flows carry ordered stage, tap,
   transform, terminal encode, and sink endpoint steps. Planned branches can now
   end in frame-domain sink endpoints after decode/resize/resample/custom stages
   or in packet-domain sink endpoints after Opus/VP8/VP9 encode. Runtime
   branches can apply flows from frame taps, publish nested taps, encode
   Opus/VP8/VP9 into endpoints, and copy packet taps into endpoints. Buffered
   runtime mutation and deeper capability metadata remain next slices.
3. Move `Describe()` onto `MediaPlan.Spec()` equivalence, then move `Build(ctx)`
   for `From`, packet copy, stream decode, branch composition, and reusable flows
   onto direct media-plan graph construction. Branch composition now carries a
   recipe-owned branch-compose plan; the advanced `transcode.Plan` path adapts
   into that plan instead of being the recipe IR. Runtime branch-composer helpers
   now use branch/media naming, and branch inputs plus resolved targets stay on
   the resolved plan until a media-plan branch graph emits specs or builds the
   runtime graph. Packet copy/fanout recipes now do the same through a resolved
   packet-copy graph plan. Direct stream decode/encode recipes now keep resolved
   inputs, endpoints, ordered stream attachments, codec-change policy, custom
   stages, transforms, and taps until the media-plan boundary instead of
   compiling from pre-lowered builder state; they now describe/build through a
   resolved single-stream graph plan and shared parameterized graph helpers. The
   remaining work is deeper direct graph construction and capability planning
   behind the remaining media-plan build kinds.
4. Add a capability model for streams, codecs, filters, and containers so the
   planner can explain copy/decode/encode choices, missing adapters, transform
   incompatibilities, and mux-output conflicts before runtime execution. Codec
   descriptors now preflight known decoder and encoder media/sample/pixel
   compatibility for planned and runtime branches. Branch planning now carries
   stream caps from probed inputs and live codec intent into `Explain(ctx)` and
   planned taps. Filter descriptor metadata now feeds `Explain(ctx)` and
   descriptor media mismatches plus config-specific
   pixel/sample-format and resize-mode mismatches fail during planned and
   runtime branch preflight. Format descriptors now report container
   media/codecs/stream-count capabilities and descriptor-backed mux target
   conflicts fail before planned or runtime graph mutation.
5. Keep custom composition orthogonal: application-local codecs use
   `goav.Codec`, `WithDecoder`, and `WithEncoder`; custom stages, filters,
   sinks, and targets use the same stream and runtime-attachment concepts as
   built-ins instead of workflow-specific helpers.
6. Keep first-page examples executable with `goav.Default()`, or keep examples
   that require unavailable containers in clearly labeled adapter sections.
7. Treat adapter coverage as product surface after the planner can absorb it.
   WebM and Ogg remain the next high-value containers because they unlock
   expected RTP/WebRTC record and muxed audio/video examples.
8. Generalize flows as reusable operation sequences over stream chains, not as a
   second graph DSL. Branches own targets through `.To(goav.Target(...))`;
   runtime `Task.Attach(ctx, goav.Branch(...))` remains the late control-plane
   branch for running direct graphs.
9. Promote live codec-change behavior into explicit policy: compatible rebind,
   keyframe request, drop-until-sync, and different-codec failure/rebuild
   choices should be visible to realtime users.
10. Add runtime observability through task stats, traces, drop reasons, and
   latency counters. Task stats now include graph and per-node counters, and
   runtime attachments expose branch-owned node stats; traces and latency
   counters remain future slices.
11. Prepare v0.1 only after README examples compile/run or clearly name their
    adapter requirements, default and tagged tests pass, core stays cgo-free,
    hot-path allocation guards remain green, and one public RTP/WebRTC record
    branch plus one public file transcode branch work end to end.
12. Confirm `go 1.26` in `go.mod` is intentional before tagging; it sets the
    installation floor for users and CI.

## Phase 0: API sketch

- Core media types.
- Codec registry contracts.
- Format probe/demux/mux contracts.
- Event-aware pipeline contracts.
- Pion-based RTP/WebRTC contracts.

## Phase 1: First realtime receive slice

- Pion WebRTC remote track adapter.
- RTP sequence/loss event surface.
- Opus depacketizer branch.
- `gopus` decoder adapter.
- Raw PCM sink for validation.

## Phase 1b: Generic source shape

- Explicit source/stage/sink graph builder.
- Pre-build graph description and optional diagram exporters.
- Protocol source contracts outside the core runtime.
- Demux boundaries for protocol/file adapters.
- Live input events for connect, disconnect, and timestamp discontinuity.
  Timestamp discontinuity first slice active for RTP receive.
- Shared packet flow into the same pipeline graph used by WebRTC.
- Core timestamp/timebase rescale helpers. First slice active.

## Phase 2: Video receive

- VP8/VP9 depacketization.
- `govpx` VP8/VP9 decode adapters behind `goav_govpx`. First slices active.
- `govpx` VP8/VP9 encode adapters behind `goav_govpx`. First slices active.
- Keyframe request events.
- Loss recovery and drop-until-sync behavior.

## Phase 3: Recording and remux

- High-level one-input/many-output remux recipe branch.
- High-level one-or-more RTP packet-readers to output recipe branch.
- IVF output for VP8/VP9/AV1 packet recording.
- WebRTC session receive to file.
- Probe output and stream inspection.
- Multiple target branches from one receive graph.

## Phase 3b: Transcode ladders

- Resize filter contract implementation. I420/YUV420P adapter active.
- Resample filter contract implementation. S16 adapter active.
- Decode sharing across branches. First planner branch active.
- Per-branch encoder configs. First planner branch active.
- Multiple mux/output targets from one plan. First planner branch active.
- Resize/resample branch execution. First concrete adapters active.

## Phase 4: H264 and concrete AV1 decode

- H264 RTP depacketization and Annex B bridge. Done.
- Descriptor-only H264 adapter availability checks. Done.
- Build-tagged `goh264` decoder factory. Done.
- Allocation and lifecycle hardening for concrete video decode paths. H264 and
  VP8/VP9 tagged adapter guards active.
- Allocation and lifecycle hardening for concrete video encode paths. VP8/VP9
  tagged adapter guards active.
- AV1 decode adapter validation. First low-overhead factory slice is active
  behind `goav_goav1`; recovery can use packet keyframe markers or parsed
  low-overhead sequence/key-frame payloads. The runtime can provision
  conservative decoder state for high-level AV1 receive, and
  stream-scoped RTP decode recipes have tagged receive, same-stream
  codec-change, and replacement-stream codec-change proofs for old-ID and
  replacement-ID event targets. The concrete decoder also has raw RTP payload
  runner integration with retained-fragment and after-loss tests. RTP sources
  can hand off packets to a different registered depacketizer after payload-map
  refresh, while selected decode graphs fail explicitly until dynamic decoder
  graph rebind policy exists. High-level RTP decode builders can pass explicit
  decode bounds into adapter-provided state; richer automatic scratch sizing
  and high-level raw-RTP policy remain. 8-bit 4:2:0 receive is active through
  both `i420` and `yuv420p` declarations, and tagged decoder frame contracts
  now accept 8-bit 4:2:2/4:4:4 aliases with canonical `i422`/`i444` output
  mapping; runtime stream fixtures for those broader layouts remain.
- `goav1` adapter as it matures.

## Phase 5: High-level API And Planner

- Fluent receive/decode/filter/encode/output recipes for selected streams.
  First file/protocol and RTP/WebRTC planner slices are active.
- `MediaPlan` as the shared branch-operation IR for record, decode, reusable
  branches, and transcode recipes. `Explain(ctx)`, media-plan `Describe`, and
  resolved-attachment `Build` slices are active for record, direct streams, and
  branch composition; deeper direct graph construction remains planned.
- Reusable `AudioFlow`/`VideoFlow` values that apply to stream chains,
  branches, and runtime attachments. Build-time file/protocol and RTP/WebRTC
  branch slices are active; runtime stage/sink attachments are active for direct
  task graphs and bounded buffered task graphs, with packet-copy late recording
  targets, Opus encoded late recording from frame taps, flow-applied Opus
  encode-to-target branches, post-encode packet taps, and dependent late
  branches after runtime resize taps covered; deeper buffered lifecycle stress
  proof remains planned.
- Detail-aware graph introspection is active; richer stats and tracing remain
  future work.
