# Roadmap

`PROGRESS.md` is the compact tracker for current implementation status and
validation gates. This roadmap keeps the broader phase view.

## Next-Level Priorities

The recipe surface is now pointed in the right direction; the next work is to
make the implementation and adapter coverage match the promise.

1. Keep first-page examples executable with `goav.Default()`, or move examples
   that require unavailable containers into clearly labeled adapter sections.
2. Add product-facing container adapters, starting with WebM and Ogg, then WAV
   and Y4M where they unlock common record/transcode workflows.
3. Make `Intent` the single compiler input: recipes should lower through
   validation, probing, stream resolution, format/codec resolution,
   demux/depacketize, decode, transform, encode, mux, route, buffer-policy, and
   graph-emission passes instead of growing one matcher per workflow.
4. Expand transcode from same-stream ladders into a media output composer where
   one muxed output can receive coordinated audio and video branches. First
   compiler slice active.
5. Add an `Explain` report above `Describe` so users can inspect selected
   streams, required adapters, missing adapters, warnings, and the graph without
   reading raw node/edge details first.
6. Add reusable flow/subflow builders so common audio or video processing blocks
   can be named, reused, and teed without manual graph wiring.
7. Promote live codec-change behavior into explicit policy: compatible rebind,
   keyframe request, drop-until-sync, and different-codec failure/rebuild
   choices should be visible to realtime users. First recipe policy slice
   active for today's supported behavior.
8. Add runtime observability through task stats, traces, drop reasons, and
   latency counters. First task stats slice active for graph message/event/drop
   counters.
9. Make beginner signatures describe exactly what callers may pass, especially
   `Record(input, outputs...)`, before freezing a v0.1 API.
10. Prepare v0.1 only after README examples compile/run or clearly name their
    adapter requirements, default and tagged tests pass, core stays cgo-free,
    hot-path allocation guards remain green, and one public RTP/WebRTC record
    path plus one public file transcode path work end to end. Confirm the
    `go.mod` Go version is an intentional user installation floor before
    tagging.

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

- High-level one-input/many-output remux compiler.
- High-level one-or-more RTP packet-readers to output compiler.
- IVF output for VP8/VP9/AV1 packet recording.
- WebRTC session receive to file.
- Probe output and stream inspection.
- Multiple output branches from one receive graph.

## Phase 3b: Transcode ladders

- Resize filter contract implementation. I420/YUV420P adapter active.
- Resample filter contract implementation. S16 adapter active.
- Decode sharing across renditions. First compiler active.
- Per-rendition encoder configs. First compiler active.
- Multiple mux/output targets from one plan. First compiler active.
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

## Phase 5: High-level API

- Fluent receive/decode/filter/encode/output builder compilers for selected
  streams. First file/protocol and RTP/WebRTC slices are active.
- Branchable multi-rendition transcode builder compilers and fluent explicit
  graph fanout through multi-target routes. First shared-decode slice is active.
- Detail-aware graph introspection is active; richer stats and tracing remain
  future work.
