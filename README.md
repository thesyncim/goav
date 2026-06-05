# goav

`goav` is a pure-Go media runtime sketch for realtime receive, decode, encode,
remux, transcode, and recording.

The target is an API that feels simple at the edge like FFmpeg, but remains
composable internally like GStreamer: sources, demuxers, depacketizers, codecs,
filters, muxers, and sinks all meet through explicit contracts.

This repository starts from interfaces and type contracts, then adds narrow
vertical slices behind those boundaries. Current implemented pieces include the
direct pipeline executor, RTP receive primitives, WebRTC track reading, Opus RTP
depacketization, format graph adapters, and an Opus decode adapter over `gopus`.

## First-class goals

- Realtime WebRTC/RTP receive.
- Generic realtime and file sources through explicit source and format
  boundaries.
- Loss-aware media flow: gaps, late packets, discontinuities, keyframe requests.
- Codec switches through explicit codec epochs and stream events.
- Multi-rendition transcoding: one input into many resized/resampled outputs.
- Pure Go integration with Pion RTP, RTCP, and WebRTC types.
- Pluggable codec adapters for:
  - `github.com/thesyncim/gopus`
  - `github.com/thesyncim/govpx`
  - `github.com/thesyncim/goh264`
  - `github.com/thesyncim/goav1`
- Transport-neutral packet/frame/event model.
- Registry-driven codecs, formats, payload maps, and pipeline stages.
- Default static format probing for common file/live boundaries.

## Package map

```text
av          Core media identifiers, packets, frames, streams, and events.
codec       Decoder/encoder contracts and backend descriptors.
format      Probe, demux, and mux contracts for files and containers.
pipeline    Event-aware graph/stage contracts for realtime composition.
rtpav       RTP receive contracts using Pion RTP/RTCP types directly.
webrtcav    WebRTC receive/session contracts using Pion WebRTC types directly.
filter      Resize/resample/frame-transform contracts.
transcode   Multi-output ladder and rendition planning contracts.
adapters    Optional codec/container integrations outside the core import graph.
```

## Shape

The high-level API should make natural media jobs small. The first compiler
slice supports one input remuxed or fanned out to one or more outputs when the
runtime has matching prober, demuxer, and muxer factories registered:

```go
runtime := goav.New(goav.WithFormatRegistry(formats))

task, err := runtime.New().
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

Decode, encode, filter, and full transcode fluent graphs keep the same surface,
but still fail explicitly until their graph compilers can select the needed
sources, codec stages, filters, muxers, and sinks.

The lower-level contracts stay explicit enough to build SFU receivers, recorders,
transcoders, analyzers, and custom realtime graphs without hiding timestamps,
loss, codec changes, or backpressure.

Explicit graphs can be built and inspected directly:

```go
builder := runtime.New().
    Source(source).
    Stage(decode).
    Link(pipeline.Link{
        From: pipeline.PadRef{Node: "source", Pad: "out"},
        To: pipeline.PadRef{Node: "decode", Pad: "inout"},
    }).
    Route(pipeline.Route{
        From: pipeline.PadRef{Node: "decode", Pad: "inout"},
        To: []pipeline.PadRef{{Node: "record", Pad: "in"}},
        Policy: pipeline.RouteByStream,
        Label: "audio",
    }).
    Sink(record)

spec, err := builder.Describe()
if err != nil {
    return err
}
_ = spec.Mermaid()

task, err := builder.Build(ctx)
if err != nil {
    return err
}
```

## Current status

This is still an API-first project, but the first receive/decode path is taking
shape.

The current narrow vertical slice is:

1. RTP/WebRTC Opus receive using Pion track types.
2. Loss/discontinuity events from RTP sequence gaps.
3. Depacketized Opus packets into a reusable decoder stage.
4. A `gopus` adapter behind the codec registry producing caller-owned PCM
   frames.
5. A reusable encoder stage for upcoming transcode and multi-output branches.
6. Reusable demux and mux graph adapters for recording, remuxing, and generic
   protocol/file ingest-output work.
7. High-level one-input/many-output remux graph compilation through the format
   registry.
8. Pre-build graph planning plus text, DOT, and Mermaid renderers.

Parallel use cases should shape the same contracts:

- Generic live/file ingest to several outputs.
- One live input fanning out into multiple encoded layers.
- Resize video renditions for ABR ladders.
- Resample audio for output compatibility.
- Record one branch while forwarding or transcoding another.

See `docs/USE_CASES.md` for the current design scenarios.

See `docs/PROGRESS.md` for the compact implementation tracker.

See `docs/ADAPTERS.md` and `docs/PERFORMANCE.md` before adding hot-path code.
