# goav

`goav` is a pure-Go realtime media runtime in progress. The goal is an API that
keeps common jobs as one natural expression, while compiling into explicit,
inspectable pipeline graphs for serious media systems.

It is not a wrapper around FFmpeg or GStreamer. Codecs and containers live behind
small Go interfaces and optional adapters, so WebRTC/RTP receive, recording,
remuxing, analysis, and transcoding can share the same packet/frame/event flow.

## Design Rules

- Pure Go core, no cgo runtime dependency.
- Simple fluent API for natural workflows.
- Explicit graph API for custom realtime systems.
- Graphs are named sources, stages, sinks, and routes; fanout is one route with
  multiple targets.
- A route may match all media, one stream, or one event type.
- The explicit graph API reads as `goav.Route("source", "sink")` or
  `goav.StreamRoute("source", "video", "sink")`.
- Rendered graph nodes can carry short workflow details without changing the
  simple node-to-node API; routed edges render as `stream=video` or
  `event=packet_loss`.
- Caller-owned buffers and result structs on hot paths.
- RTP metadata, loss, discontinuity, codec epochs, keyframe requests, EOS, and
  backpressure are first-class events.
- RTP receive can surface backward timestamps and configured timestamp gaps as
  discontinuity events.
- Pion RTP/RTCP/WebRTC types stay at package boundaries.
- Realtime decoder adapters can receive explicit payload, retained-fragment,
  output-count, and geometry bounds before binding scratch.
- RTP receive builders can pass those bounds with `WithRTPDecodeBounds` so
  high-level decode graphs still keep caller-owned scratch explicit.
- Decoder factories may optionally provision adapter-specific state for
  high-level builders, while low-level callers can still own exact state.
- Codec implementations stay in adapter packages for `gopus`, `govpx`,
  `goav1`, and `goh264`.

## Examples

Remux or fan out one input to many outputs when matching format adapters are
registered:

```go
rt := goav.New(goav.WithFormatAdapter(ivf.Register))

task, err := rt.New().
    Input(goav.Input{Name: "input.ivf", Reader: in}).
    Output(goav.Output{Name: "archive.ivf", Writer: archive}).
    Output(goav.Output{Name: "preview.ivf", Writer: preview}).
    Build(ctx)
if err != nil {
    return err
}

_ = task.Describe().Mermaid()

if err := task.Run(ctx); err != nil {
    return err
}
return task.Close()
```

Decode a selected stream into a frame sink when a decoder factory is registered:

```go
task, err := rt.New().
    Input(goav.Input{Name: "input.ogg"}).
    Decode(goav.SelectAudio()).
    Sink(frames).
    Build(ctx)
```

Record or fan out one or more RTP/WebRTC packet readers when depacketizers and
muxers are available:

```go
task, err := rt.New().
    RTP(audio,
        goav.WithRTPName("audio"),
        goav.WithRTPJitter(jitter),
        goav.WithRTPDepacketizers(opus),
    ).
    RTP(video,
        goav.WithRTPName("video"),
        goav.WithRTPDepacketizers(vp8),
    ).
    Output(goav.Output{Name: "recording.webm", Writer: file}).
    Build(ctx)
```

Decode a selected live RTP/WebRTC stream directly into a frame sink:

```go
task, err := rt.New().
    RTP(audio,
        goav.WithRTPName("audio"),
        goav.WithRTPDepacketizers(opus),
    ).
    Decode(goav.SelectAudio()).
    Sink(frames).
    Build(ctx)
```

Transcode a selected stream into one or more outputs when decoder, encoder,
filter, and mux adapters are registered:

```go
task, err := rt.New().
    RTP(audio,
        goav.WithRTPName("audio"),
        goav.WithRTPDepacketizers(opus),
    ).
    Decode(goav.SelectAudio()).
    Filter(goav.SelectAudio(), resample).
    Encode(goav.SelectAudio(), opusEncode).
    Output(goav.Output{Name: "archive.ogg", Writer: archive}).
    Output(goav.Output{Name: "preview.ogg", Writer: preview}).
    Build(ctx)
```

Describe a small rendition plan when one decode should feed multiple encoders:

```go
plan := transcode.Plan{
    Input: goav.Input{Name: "input.ogg", Reader: in},
    Renditions: []transcode.Rendition{
        {Name: "main", Selector: goav.SelectAudio(), Encode: opusMain, Labels: []string{"archive"}},
        {Name: "low", Selector: goav.SelectAudio(), Encode: opusLow, Labels: []string{"archive", "preview"}},
    },
    Outputs: []transcode.Output{
        {Name: "archive.ogg", Target: goav.Output{Writer: archive}, Renditions: []string{"archive"}},
        {Name: "preview.ogg", Target: goav.Output{Writer: preview}, Renditions: []string{"low"}},
    },
}
task, err := rt.New().Transcode(plan).Build(ctx)
```

Build an explicit graph when the application owns the stages:

```go
builder := rt.New().
    Source(source).
    Stage(decode).
    Sink(record).
    Sink(preview).
    Sink(stats).
    Routes(
        goav.StreamRoute("source", "audio", "decode"),
        goav.Route("decode", "record", "preview", "stats"),
    )

spec, err := builder.Describe()
if err != nil {
    return err
}
_ = spec.DOT()
```

Unsupported fluent combinations fail early. New high-level workflows are added
as private graph compilers that must support both `Describe` and `Build`.

## Current Surface

- `av`: media identifiers, streams, packets, frames, timestamps, events,
  timebase conversion helpers, reset helpers, and ownership markers.
- `pipeline`: direct-call graph executor, bounded buffered graph executor,
  fanout, simple route helpers, stream/event routes,
  backpressure surface, drop-policy decisions, bounded copy slots for borrowed
  media buffers, simple route graph surface, detail-aware text/DOT/Mermaid graph
  specs.
- `format`: probe/demux/mux contracts plus demux source and mux stage adapters.
- `codec`: decoder/encoder contracts, realtime decode bounds, registry,
  optional decode-state provisioning, decoder and encoder pipeline stages.
- `rtpav`: Pion RTP/RTCP boundary, payload map, loss detection, jitter ring,
  timestamp discontinuity detection, Opus/VP8/VP9/AV1/H264 depacketizers,
  feedback helpers, RTP source, replacement-stream codec-change handling.
- `webrtcav`: Pion PeerConnection session, TrackSet multi-track coordinator,
  replaceable TrackRemote readers, RTCP feedback, and codec-update event
  boundaries.
- `filter`: resize/resample contracts, registry, and frame-transform pipeline
  stage.
- `transcode`: rendition and ladder planning contracts, with a first
  shared-decode multi-rendition compiler.
- `adapters/ivf`: IVF demux/mux for VP8, VP9, and AV1 packet recording.
- `adapters/annexb`: H264 Annex B packet mux for `.h264` recording.
- `adapters/resample`: pure-Go `s16` audio resample/channel conversion filter.
- `adapters/resize`: pure-Go planar 8-bit 4:2:0 video resize filter.
- `adapters/gopus`: active Opus decoder adapter.
- `adapters/govpx`: descriptor-only by default; `goav_govpx` enables real VP8
  and VP9 decoder factories over `github.com/thesyncim/govpx` into
  caller-owned I420 frames, plus VP8 and VP9 encode into caller-owned packet
  buffers.
- `adapters/goav1`: descriptor-only by default; `goav_goav1` enables a first
  AV1 decoder factory over caller-owned `DecoderState`, depacketized
  low-overhead OBU payloads, concrete raw RTP payload decode, borrowed 8-bit
  `gray8`/4:2:0/4:2:2/4:4:4 frame-plane mapping, and drop-until-sync recovery
  after loss.
- `adapters/goh264`: descriptor-only by default; `goav_goh264` enables a real
  H264 decoder factory over `github.com/thesyncim/goh264` for 8-bit planar
  video frames.

## Status

Implemented slices:

- RTP/WebRTC Opus receive primitives.
- Loss/discontinuity events from RTP sequence gaps.
- Allocation-free timestamp/timebase rescale helpers for RTP, codec, and
  transcode boundaries.
- RTP source timestamp discontinuity detection for backward timestamps and
  configured max-gap thresholds.
- Opus RTP depacketization.
- Opus decode through `gopus` into caller-owned PCM frames.
- Event-aware decoder and encoder stages.
- Demux source and mux stage graph adapters.
- Fluent remux/fanout compiler.
- Fluent selected-stream decode-to-sink compiler, with optional filter stages.
- Fluent selected-stream decode/filter/encode-to-output compiler.
- Fluent `Transcode(plan)` compiler for one selected decode feeding multiple
  named encode/output branches, including resize/resample branch stages when
  filter factories are registered.
- Fluent `Routes(goav.Route(...))`, `StreamRoute(...)`, and `EventRoute(...)`
  helpers for explicit application graphs.
- First-class route helpers for direct graph composition without extra graph
  concepts.
- Fluent RTP/WebRTC packet-reader record/fanout compiler, including repeated
  `RTP(...)` inputs.
- Fluent RTP/WebRTC selected-stream decode-to-sink compiler for live receive,
  with optional filter stages.
- Pre-build and runtime graph rendering as text, DOT, and Mermaid.
- Detail-aware graph descriptions for runtime-created sources, select stages,
  codecs, filters, RTP receive nodes, and mux outputs.
- IVF packet demux/mux adapter with allocation-guarded read/write paths.
- Annex B packet mux adapter for H264 recording.
- S16 audio resample filter adapter with allocation-guarded hot path.
- I420/YUV420P video resize filter adapter with allocation-guarded hot path.
- VP8/VP9/AV1/H264 RTP depacketizers for packet-preserving video recording.
- WebRTC session track accept loop with RTCP feedback routed through Pion.
- WebRTC TrackSet keeps one long-lived reader per logical stream.
- WebRTC track codec updates and replacement tracks emit codec-change events
  consumed by RTP sources.
- RTP codec-change events refresh payload maps and depacketizer epochs.
- RTP codec-change events can also carry a replacement stream identity for
  single-stream receivers; sources, depacketizers, EOS, and type-selected
  decode graphs follow that replacement while ID-pinned selectors stay strict.
- Multi-stream RTP sources can use `Event.StreamID` as the old stream target
  while `Event.Stream` carries the replacement identity; accepted events emit
  the canonical replacement stream downstream.
- RTP sources can refresh payload maps across codec changes when matching
  depacketizers are present; selected runtime decode graphs now fail explicitly
  with `codec.ErrUnsupportedCodecSwitch` when that change would require a
  different decoder factory.
- `WithRTPDecodeBounds(...)` lets high-level RTP decode builders size decoder
  result capacity and adapter-provided state for payload, retained-fragment,
  output-count, and geometry limits.
- Descriptor-only codec adapters are discoverable while unavailable factories
  fail explicitly with `codec.ErrUnavailable`.
- Build-tagged H264 decode maps real `goh264` output into borrowed `av.Frame`
  planes, updates identity on codec changes, requests keyframes after loss, and
  has adapter-owned allocation and close-lifecycle tests.
- Build-tagged VP8 and VP9 decode map real `govpx` output into caller-owned
  I420 `av.Frame` planes, drop until sync after loss or startup, request
  keyframes, update identity on codec changes, and have allocation and
  lifecycle tests.
- Build-tagged VP8 encode maps caller-owned I420 `av.Frame` input into
  caller-owned `av.Packet` payload buffers, honors keyframe request events, and
  has allocation and lifecycle tests.
- Build-tagged VP9 encode maps caller-owned I420 `av.Frame` input into
  caller-owned `av.Packet` payload buffers through `govpx` profile 0 encode,
  honors keyframe request events, and has allocation and lifecycle tests.
- Decode bounds give realtime video adapters a common way to prebind bounded
  scratch without importing codec internals into the core.
- Bounded buffered graph execution is available through the existing
  `BufferPolicy` surface for immutable media, events, and policy-bounded
  copies of borrowed packet payloads or frame planes, with drop-oldest,
  drop-newest, and backpressure behavior covered.
- Runtime multi-output `Transcode(plan)` builds are covered through buffered
  execution with policy-bounded copies of encoder-owned packet payloads.
- Runtime RTP/WebRTC packet-reader record/fanout builds are covered through
  buffered execution with policy-bounded copies of depacketizer-owned packet
  payloads.
- The friendly explicit graph surface now has one route shape: `Routes(...)`
  with `Route`, `StreamRoute`, or `EventRoute`.
- Tagged AV1 decode now registers a factory behind `goav_goav1`, consumes
  depacketized low-overhead OBU payloads through caller-owned `DecoderState`,
  can provision conservative runtime state for high-level builders, maps
  decoded backend frames into borrowed `av.Frame` planes, reuses the bound
  runner in steady state, and sync-gates post-loss packets until a packet
  keyframe marker or parseable low-overhead sequence-header/key-frame payload.
- Tagged AV1 RTP receive is covered through the high-level
  `RTP(...).Decode(...).Sink(...)` builder path: Pion RTP packet, AV1
  depacketizer, runtime-provided decoder state, and borrowed frame output.
- Tagged AV1 RTP receive now covers both `gray8` and planar 8-bit 4:2:0:
  `i420` and `yuv420p` stream declarations bind the same backend format and
  emit canonical `i420` frame planes.
- Tagged AV1 decoder format contracts now also accept planar 8-bit 4:2:2 and
  4:4:4 declarations: `yuv422p` maps to canonical `i422`, and `yuv444p` maps
  to canonical `i444` at state and frame boundaries.
- The same AV1 RTP builder path now has same-stream and replacement-stream
  codec-change proofs: payload-map refresh, epoch/identity update,
  old-ID or replacement-ID event targeting, drop-until-sync, keyframe request,
  and resumed decode on the next sync packet.
- The concrete tagged AV1 decoder can also consume raw AV1 RTP aggregation
  payload bytes through `DecodeRTPPayloadInto`, including retained fragments
  and after-loss recovery that preserves known sequence state.

Next pressure points:

- Broaden the tagged AV1 factory from the tiny low-overhead proof toward real
  RTP/WebRTC AV1 streams: dynamic new-codec graph rebind policy, runtime stream
  proofs for broader output formats, richer automatic scratch sizing policy,
  and deciding how much of the raw RTP runner path should surface in high-level
  builders.

## Working Loop

Each slice follows the same loop:

1. Express the workflow with the smallest natural API.
2. Render the explicit graph, with useful node details, first.
3. Add one compiler, stage, adapter, or format boundary.
4. Prove lifecycle, event behavior, and hot-path allocation expectations.
5. Update the progress tracker.

See:

- `docs/LOOP.md`
- `docs/PROGRESS.md`
- `docs/ARCHITECTURE.md`
- `docs/RTP_WEBRTC.md`
- `docs/ADAPTERS.md`
- `docs/PERFORMANCE.md`
