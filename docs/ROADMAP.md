# Roadmap

`PROGRESS.md` is the compact tracker for current implementation status and
validation gates. This roadmap keeps the broader phase view.

## Next-Level Priorities

The recipe surface is now pointed in the right direction; the next work is to
make the implementation match the composable planner promise.

1. Make `Intent -> MediaPlan -> pipeline.Spec -> pipeline.Graph` the normal
   recipe path. Normal recipes should require media-plan recognition instead of
   adding workflow-specific matchers.
2. Treat declared branches as generic branch operations and mux groups.
   `From(input).Audio()/Video().Tap(...).Branch(...)` and `Tee(...)` flows
   should produce equivalent `MediaPlan` shapes where possible.
3. Move `Describe()` onto `MediaPlan.Spec()` equivalence, then move `Build(ctx)`
   for `From`, packet copy, stream decode, branch composition, and flow tee onto
   direct media-plan graph construction.
4. Add a capability model for streams, codecs, filters, and containers so the
   planner can explain copy/decode/encode choices, missing adapters, transform
   incompatibilities, and mux-output conflicts before runtime execution.
5. Keep custom codecs orthogonal: application-local codecs use `goav.Codec`,
   `WithDecoder`, and `WithEncoder`; built-in specs are presets over the same
   compiler path.
6. Keep first-page examples executable with `goav.Default()`, or keep examples
   that require unavailable containers in clearly labeled adapter sections.
7. Treat adapter coverage as product surface after the planner can absorb it.
   WebM and Ogg remain the next high-value containers because they unlock
   expected RTP/WebRTC record and muxed audio/video examples.
8. Generalize flows as reusable intent fragments over stream chains, not as a
   second graph DSL. `Tee` remains planned fanout; runtime `Task.Attach(ctx,
   goav.Branch(...))` remains the late control-plane tap for running direct
   graphs.
9. Promote live codec-change behavior into explicit policy: compatible rebind,
   keyframe request, drop-until-sync, and different-codec failure/rebuild
   choices should be visible to realtime users.
10. Add runtime observability through task stats, traces, drop reasons, and
   latency counters. First task stats slice active for graph message/event/drop
   counters.
11. Prepare v0.1 only after README examples compile/run or clearly name their
    adapter requirements, default and tagged tests pass, core stays cgo-free,
    hot-path allocation guards remain green, and one public RTP/WebRTC record
    path plus one public file transcode path work end to end.
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
- Opus depacketizer path.
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

- High-level one-input/many-output remux recipe path.
- High-level one-or-more RTP packet-readers to output recipe path.
- IVF output for VP8/VP9/AV1 packet recording.
- WebRTC session receive to file.
- Probe output and stream inspection.
- Multiple output branches from one receive graph.

## Phase 3b: Transcode ladders

- Resize filter contract implementation. I420/YUV420P adapter active.
- Resample filter contract implementation. S16 adapter active.
- Decode sharing across renditions. First planner path active.
- Per-rendition encoder configs. First planner path active.
- Multiple mux/output targets from one plan. First planner path active.
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
- `MediaPlan` as the shared branch-operation IR for record, decode, flow tee,
  and transcode recipes. First `Explain(ctx)` report slice is active; direct
  `Describe`/`Build` lowering remains planned.
- Reusable `AudioFlow`/`VideoFlow` branches with `.Tee(...)`. Build-time
  file/protocol and RTP/WebRTC slices are active; runtime stage/sink attachments
  are active for direct task graphs; buffered attachments and late recording outputs
  remain planned.
- Detail-aware graph introspection is active; richer stats and tracing remain
  future work.
