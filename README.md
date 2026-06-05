# goav

`goav` is a pure-Go realtime media runtime in progress. The goal is an API that
feels small for common jobs, while compiling into explicit, inspectable pipeline
graphs for serious media systems.

It is not a wrapper around FFmpeg or GStreamer. Codecs and containers live behind
small Go interfaces and optional adapters, so WebRTC/RTP receive, recording,
remuxing, analysis, and transcoding can share the same packet/frame/event flow.

## Design Rules

- Pure Go core, no cgo runtime dependency.
- Simple fluent API for natural workflows.
- Explicit graph API for custom realtime systems.
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
rt := goav.New(goav.WithFormatRegistry(formats))

task, err := rt.New().
    Input(goav.Input{Name: "input.ogg"}).
    Output(goav.Output{Name: "archive.ogg"}).
    Output(goav.Output{Name: "preview.ogg"}).
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

Decode one unambiguous stream into a frame sink when a decoder factory is
registered:

```go
task, err := rt.New().
    Input(goav.Input{Name: "input.ogg"}).
    Decode(goav.SelectAudio()).
    Sink(frames).
    Build(ctx)
```

Build an explicit graph when the application owns the stages:

```go
builder := rt.New().
    Source(source).
    Stage(decode).
    Route(pipeline.Route{
        From:   pipeline.PadRef{Node: "source", Pad: "out"},
        To:     []pipeline.PadRef{{Node: "decode", Pad: "inout"}},
        Policy: pipeline.RouteByStream,
        Label:  "audio",
    }).
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
  backpressure surface, text/DOT/Mermaid graph specs.
- `format`: probe/demux/mux contracts plus demux source and mux stage adapters.
- `codec`: decoder/encoder contracts, registry, decoder and encoder pipeline
  stages.
- `rtpav`: Pion RTP/RTCP boundary, payload map, loss detection, jitter ring,
  Opus depacketizer, feedback helpers, RTP source.
- `webrtcav`: Pion TrackRemote reader boundary.
- `filter`: resize/resample contracts.
- `transcode`: rendition and ladder planning contracts.
- `adapters/gopus`: active Opus decoder adapter.
- `adapters/govpx`, `adapters/goav1`, `adapters/goh264`: descriptor
  boundaries for future concrete adapters.

## Status

Implemented slices:

- RTP/WebRTC Opus receive primitives.
- Loss/discontinuity events from RTP sequence gaps.
- Opus RTP depacketization.
- Opus decode through `gopus` into caller-owned PCM frames.
- Event-aware decoder and encoder stages.
- Demux source and mux stage graph adapters.
- Fluent remux/fanout compiler.
- Fluent single-stream decode-to-sink compiler.
- Pre-build and runtime graph rendering as text, DOT, and Mermaid.

Next pressure points:

- Multi-stream decode selection without routing unrelated packets into a
  decoder.
- Concrete demuxer/muxer adapters for first recording paths.
- WebRTC session receive loop and RTCP feedback wiring.
- VP8/VP9, H264, and AV1 RTP/codec adapter validation.
- Allocation-safe resize and resample implementations.

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
