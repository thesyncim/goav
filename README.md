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
- Graphs are named sources, stages, sinks, and direct links; stream/event
  routing is a connection option.
- Caller-owned buffers and result structs on hot paths.
- RTP metadata, loss, discontinuity, codec epochs, keyframe requests, EOS, and
  backpressure are first-class events.
- Pion RTP/RTCP/WebRTC types stay at package boundaries.
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
        goav.WithRTPDepacketizer(opus),
    ).
    RTP(video,
        goav.WithRTPName("video"),
        goav.WithRTPDepacketizer(vp8),
    ).
    Output(goav.Output{Name: "recording.webm", Writer: file}).
    Build(ctx)
```

Decode a selected live RTP/WebRTC stream directly into a frame sink:

```go
task, err := rt.New().
    RTP(audio,
        goav.WithRTPName("audio"),
        goav.WithRTPDepacketizer(opus),
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
        goav.WithRTPDepacketizer(opus),
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
    Connect("source", "decode", goav.ForStream("audio")).
    Connect("decode", "record").
    Sink(record)

spec, err := builder.Describe()
if err != nil {
    return err
}
_ = spec.DOT()
```

Unsupported fluent combinations fail early. New high-level workflows are added
as private graph compilers that must support both `Describe` and `Build`.

## Current Surface

- `av`: media identifiers, streams, packets, frames, timestamps, events, reset
  helpers, and ownership markers.
- `pipeline`: direct-call graph executor, fanout, stream/event routes,
  backpressure surface, simple node-to-node connections, text/DOT/Mermaid graph
  specs.
- `format`: probe/demux/mux contracts plus demux source and mux stage adapters.
- `codec`: decoder/encoder contracts, registry, decoder and encoder pipeline
  stages.
- `rtpav`: Pion RTP/RTCP boundary, payload map, loss detection, jitter ring,
  Opus/VP8/VP9/AV1/H264 depacketizers, feedback helpers, RTP source.
- `webrtcav`: Pion PeerConnection session, TrackSet multi-track coordinator,
  replaceable TrackRemote readers, RTCP feedback, and codec-update event
  boundaries.
- `filter`: resize/resample contracts, registry, and frame-transform pipeline
  stage.
- `transcode`: rendition and ladder planning contracts, with a first
  shared-decode multi-rendition compiler.
- `adapters/ivf`: IVF demux/mux for VP8, VP9, and AV1 packet recording.
- `adapters/annexb`: H264 Annex B packet mux for `.h264` recording.
- `adapters/gopus`: active Opus decoder adapter.
- `adapters/govpx`, `adapters/goav1`: descriptor boundaries for future
  concrete adapters; factory lookups return `codec.ErrUnavailable` until a
  real adapter is registered.
- `adapters/goh264`: descriptor-only by default; `goav_goh264` enables a real
  H264 decoder factory over `github.com/thesyncim/goh264` for 8-bit planar
  video frames.

## Status

Implemented slices:

- RTP/WebRTC Opus receive primitives.
- Loss/discontinuity events from RTP sequence gaps.
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
- Fluent RTP/WebRTC packet-reader record/fanout compiler, including repeated
  `RTP(...)` inputs.
- Fluent RTP/WebRTC selected-stream decode-to-sink compiler for live receive,
  with optional filter stages.
- Pre-build and runtime graph rendering as text, DOT, and Mermaid.
- IVF packet demux/mux adapter with allocation-guarded read/write paths.
- Annex B packet mux adapter for H264 recording.
- VP8/VP9/AV1/H264 RTP depacketizers for packet-preserving video recording.
- WebRTC session track accept loop with RTCP feedback routed through Pion.
- WebRTC TrackSet keeps one long-lived reader per logical stream.
- WebRTC track codec updates and replacement tracks emit codec-change events
  consumed by RTP sources.
- RTP codec-change events refresh payload maps and depacketizer epochs.
- Descriptor-only codec adapters are discoverable while unavailable factories
  fail explicitly with `codec.ErrUnavailable`.
- Build-tagged H264 decode maps real `goh264` output into borrowed `av.Frame`
  planes and requests keyframes after loss.

Next pressure points:

- Concrete allocation-safe resize/resample filter adapters.
- Allocation and lifecycle hardening for concrete video decode paths.

## Working Loop

Each slice follows the same loop:

1. Express the workflow with the smallest natural API.
2. Render the explicit graph first.
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
