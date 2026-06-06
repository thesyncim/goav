# Architecture

`goav` separates stable media contracts from implementations.

The central rule is:

```text
every edge carries media data or media events
```

That keeps live WebRTC realities visible to the application: packet loss,
renegotiation, codec changes, backpressure, discontinuities, track lifecycle,
and keyframe recovery are represented as events instead of hidden side effects.

## Layers

```text
Application
  |
Runtime / Builder
  |
Pipeline graph
  |
Sources -> packet/frame/event stages -> sinks
  |
Format, RTP, WebRTC, codec, and filter adapters
```

`goav.New` is the composition root. It owns explicit codec, format, and
pipeline registries, with small adapter registration hooks for optional codec
and container integrations. The builder compiles through private graph
compilers. Each compiler owns one workflow shape and must implement both
pre-build description and runnable graph construction, so rendered graphs and
execution graphs stay equivalent.

The current compilers cover:

- empty graphs for lifecycle tests
- explicit `Source -> Stage -> Sink` graphs with direct named connections and
  stream/event route options
- one-input/many-output remux and fanout through
  `format.DemuxSource -> format.MuxStage...` when the format registry can
  probe, demux, and mux the requested boundaries
- one-input selected-stream decode to a frame sink through
  `format.DemuxSource -> stream select -> codec.DecoderStage -> optional filter
  stages -> Sink` when the selector resolves to one stream and the codec
  registry has a decoder factory
- one or more RTP/WebRTC packet readers to one or more outputs through
  `rtpav.Source -> format.MuxStage...` when the application provides
  depacketizers and the format registry can mux the output boundaries
- one or more RTP/WebRTC packet readers to a selected frame sink through
  `rtpav.Source... -> stream select -> codec.DecoderStage -> optional filter
  stages -> Sink` when one stream matches the selector and the codec registry
  has a decoder factory

Encode and transcode discovery still return a clear unsupported error until
source, codec, filter, mux, and sink selection is ready.

## Core media model

The `av` package owns transport-neutral vocabulary:

- `Stream`
- `CodecParameters`
- `Packet`
- `Frame`
- `Event`
- `Timestamp`
- `TimeBase`
- `Epoch`

RTP and WebRTC details do not leak into `av`. Dedicated packages use Pion types
directly and translate their output into `av.Packet` and `av.Event` values.

## Format Probing

The default runtime includes cold-path static probing for common boundaries:
Ogg/Opus, IVF, FLV, Matroska/WebM, MP4, Annex B, RTP, and WebRTC. Probe scores
prefer magic bytes and protocol declarations over extension fallback.

## Codec epochs

Each stream carries an `Epoch`.

When codec parameters change, the receiver emits `av.EventCodecChanged` with a
new epoch. Downstream decoders can then drain, reset, or drop until sync without
guessing whether a packet belongs to old state.

WebRTC track readers expose that boundary through `UpdateCodec` and
`UpdateTrack`, using Pion codec parameters, payload maps, and replacement tracks
directly. `webrtcav.TrackSet` coordinates accepted tracks into one long-lived
reader per logical stream, so same-stream replacements update existing graph
inputs instead of forcing the application to rebuild around a new reader. The
RTP source consumes the event by refreshing its payload map before depacketizing
subsequent packets.

## Planned codec backends

The first adapters should wrap existing pure-Go codec projects:

```text
gopus   -> Opus decode/encode
govpx   -> VP8/VP9 decode/encode
goh264  -> H264 decode adapter
goav1   -> AV1 decode/development adapter
```

These adapters should live behind `codec.DecoderFactory` and
`codec.EncoderFactory`. The core runtime should not depend on codec internals.
Descriptor-only adapters are allowed for planned or optional backends; they are
discoverable through registry descriptors, while factory lookup returns
`codec.ErrUnavailable` until an active factory is registered. `goh264` follows
that pattern in normal builds and registers a decoder factory only when the
`goav_goh264` build tag is enabled.

## Realtime pipeline

The pipeline API is deliberately event-aware. A stage receives a `Message` and
can emit zero or more messages.

The default executor is a synchronous direct-call graph. It does not create
goroutines or channels per packet, and fanout delivers the same message and
payload references unless a future explicit policy asks for copying. Buffered
edges are intentionally separate from the direct executor so backpressure and
drop behavior remain visible.

Builders and graphs can produce a `pipeline.Spec`: structured nodes and edges
plus human-readable text, DOT, and Mermaid rendering. This makes generated
pipelines easy to validate, log, inspect, or visualize before running media
through them. Specs render simple node-to-node connections; executor-specific
details stay behind the graph implementation.

The codec package includes generic decoder and encoder stages. They adapt
`codec.Decoder` and `codec.Encoder` implementations to pipeline messages using
caller-owned result scratch. The decoder stage turns packet messages into frame
messages, keeps upstream events visible by default, flushes before EOS, and uses
packet-loss events to trigger audio PLC paths such as Opus concealment. The
encoder stage turns frame messages into packet messages and flushes delayed
packets before EOS.

The format package follows the same pattern for containers. `DemuxSource`
adapts a `format.Demuxer` into packet and event messages, including stream and
EOS events. `MuxStage` writes packet messages through a `format.Muxer` and emits
write-result events through the graph, so output-side state remains observable
instead of disappearing inside a terminal sink.

The RTP package provides the live receive source. `rtpav.Source` keeps Pion RTP
packets at the boundary, applies optional jitter and depacketizers, forwards
realtime events into those depacketizers, and emits normal packet/event messages
for the same mux, decode, and analysis stages used by file or protocol inputs.
When a packet reader represents one stream, its EOS event carries that stream ID
and epoch so selected live-decode graphs do not flush unrelated decoders.

`adapters/ivf` is the first concrete format adapter. It keeps the scope small:
one VP8, VP9, or AV1 video stream, packet demux/mux only, and no container
features beyond what the first recording path needs.

`adapters/annexb` adds the equivalent narrow packet recording target for H264:
one H264 video stream, mux-only, writing Annex B access-unit bytes exactly as
the depacketizer produced them.

That shape supports:

- demux -> decode -> filter -> encode -> mux
- WebRTC track -> jitter buffer -> depacketizer -> decoder -> recorder
- protocol source -> demux -> decode -> resize/resample -> encode ladder -> many outputs
- RTP input -> loss monitor -> stats sink
- WebRTC session -> TrackSet -> one RTP source per active stream
- codec switch events -> decoder reset
- packet loss events -> RTCP feedback and keyframe requests

## Multi-output transcoding

The `transcode` package describes ladders and renditions without deciding how
they are executed. A compiler can later turn that plan into a pipeline graph with
decode sharing, filter branches, multiple encoders, and multiple muxers.

Typical use cases:

- Generic live receive to several outputs.
- WebRTC receive to recording plus preview plus analysis.
- One video decode feeding several resize branches.
- One audio decode feeding several resample branches.
- Per-output codec, bitrate, container, and protocol decisions.
