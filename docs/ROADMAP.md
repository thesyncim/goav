# Roadmap

`PROGRESS.md` is the compact tracker for current implementation status and
validation gates. This roadmap keeps the broader phase view.

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
- Pre-build graph description and text/DOT/Mermaid rendering.
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
- AV1 decode adapter validation.
- `goav1` adapter as it matures.

## Phase 5: High-level API

- Fluent receive/decode/filter/encode/output builder compilers for selected
  streams. First file/protocol and RTP/WebRTC slices are active.
- Branchable multi-rendition transcode builder compilers and fluent explicit
  graph branch helpers. First shared-decode slice is active.
- Detail-aware graph introspection is active; richer stats and tracing remain
  future work.
