# goav

`goav` is a pure-Go media runtime sketch for realtime receive, decode, encode,
remux, transcode, and recording.

The target is an API that feels simple at the edge like FFmpeg, but remains
composable internally like GStreamer: sources, demuxers, depacketizers, codecs,
filters, muxers, and sinks all meet through explicit contracts.

This repository intentionally starts with interfaces and type contracts only.
Codec, container, jitter-buffer, and WebRTC implementations will arrive behind
these boundaries as the design settles.

## First-class goals

- Realtime WebRTC/RTP receive.
- Realtime RTMP ingest and FLV-style container boundaries.
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

The high-level API is intended to make simple jobs small:

```go
var runtime goav.Runtime

task, err := runtime.New().
    Input(goav.Input{Name: "call", Realtime: true}).
    Decode(goav.SelectVideo()).
    Output(goav.Output{Name: "recording.ivf"}).
    Build(ctx)
if err != nil {
    return err
}
return task.Run(ctx)
```

The builder surface exists, but real media execution is still gated on source,
format, and sink implementations. Unsupported graphs fail explicitly.

The lower-level contracts stay explicit enough to build SFU receivers, recorders,
transcoders, analyzers, and custom realtime graphs without hiding timestamps,
loss, codec changes, or backpressure.

## Current status

This is an API sketch. There are no media implementations yet.

The next iteration should choose the first narrow vertical slice, likely:

1. RTP/WebRTC Opus receive using Pion track types.
2. Loss/discontinuity events from RTP sequence gaps.
3. Depacketized Opus packets into the `codec.Decoder` interface.
4. A `gopus` adapter behind the codec registry.

Parallel use cases should shape the same contracts:

- RTMP ingest to several outputs.
- One live input fanning out into multiple encoded layers.
- Resize video renditions for ABR ladders.
- Resample audio for output compatibility.
- Record one branch while forwarding or transcoding another.

See `docs/USE_CASES.md` for the current design scenarios.

See `docs/PROGRESS.md` for the compact implementation tracker.

See `docs/ADAPTERS.md` and `docs/PERFORMANCE.md` before adding hot-path code.
