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
pipeline registries. The current builder is intentionally conservative: it can
compile explicit `Source -> Stage -> Sink` graphs and expose their generated
graph description. Higher-level `Input/Decode/Output` graph discovery still
returns a clear unsupported error until source, demux, codec, mux, and sink
selection is ready.

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

## Planned codec backends

The first adapters should wrap existing pure-Go codec projects:

```text
gopus   -> Opus decode/encode
govpx   -> VP8/VP9 decode/encode
goh264  -> H264 decode/development adapter
goav1   -> AV1 decode/development adapter
```

These adapters should live behind `codec.DecoderFactory` and
`codec.EncoderFactory`. The core runtime should not depend on codec internals.

## Realtime pipeline

The pipeline API is deliberately event-aware. A stage receives a `Message` and
can emit zero or more messages.

The default executor is a synchronous direct-call graph. It does not create
goroutines or channels per packet, and fanout delivers the same message and
payload references unless a future explicit policy asks for copying. Buffered
edges are intentionally separate from the direct executor so backpressure and
drop behavior remain visible.

Every graph can produce a `pipeline.Spec`: structured nodes and edges plus
human-readable text and DOT rendering. This makes generated pipelines easy to
log, inspect, or visualize before running media through them.

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

That shape supports:

- demux -> decode -> filter -> encode -> mux
- WebRTC track -> jitter buffer -> depacketizer -> decoder -> recorder
- protocol source -> demux -> decode -> resize/resample -> encode ladder -> many outputs
- RTP input -> loss monitor -> stats sink
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
