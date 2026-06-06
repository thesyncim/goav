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
Recipes: Record, Decode, Transcode, From/To
  |
Intent graph: inputs, streams, transforms, outputs, policies
  |
Runtime builder
  |
Pipeline graph
  |
Sources -> packet/frame/event stages -> sinks
  |
Format, RTP, WebRTC, codec, and filter adapters
```

`goav.New` is the composition root. It owns codec, format, and filter
registries, with small adapter registration hooks for optional codec,
container, and filter integrations. `goav.Default()` registers the standard
in-repo adapters for the beginner path, while `goav.New(...)` keeps minimal and
embedded runtimes explicit. Package-level recipes such as `Record(...)`,
`Decode(...)`, and `Transcode(...)` are now the beginner-facing front door. They
produce a small intent model, then lower into the existing runtime builder and
graph compilers.

The handle-based graph builder remains available as the advanced layer through
`Runtime.Graph()`. It names sources, stages, and sinks once, then connects typed
handles such as `source.Stream("audio")` and `decode.Out()` to node inputs.
`Runtime.New()` remains as a compatibility builder and as the recipe compiler
target. Runtime builders compile through private graph compilers. Each compiler
owns one workflow shape and must implement both pre-build description and
runnable graph construction, so described graphs and execution graphs stay
equivalent. The graph layer stays available for inspection and custom stages;
optional diagram output lives outside the runtime core. A route carries all
media by default, or matches one stream or event type.

The current compilers cover:

- empty graphs for lifecycle tests
- explicit `Source -> Stage -> Sink` graphs with handle-based connects,
  multi-target fanout, and stream/event match options
- one-input/many-output remux and fanout through
  `format.DemuxSource -> format.MuxStage...` when the format registry can
  probe, demux, and mux the requested boundaries
- one-input selected-stream decode to a frame sink through
  `format.DemuxSource -> stream select -> codec.DecoderStage -> optional filter
  stages -> Sink` when the selector resolves to one stream and the codec
  registry has a decoder factory
- one-input selected-stream decode/filter/encode to one or more outputs through
  `format.DemuxSource -> stream select -> codec.DecoderStage -> optional filter
  stages -> codec.EncoderStage -> format.MuxStage...` when the selected stream,
  target encoder, and mux boundaries are explicit
- one or more RTP/WebRTC packet readers to one or more outputs through
  `rtpav.Source -> format.MuxStage...` when recipe codec intent or the
  application provides depacketizers and the format registry can mux the output
  boundaries
- one or more RTP/WebRTC packet readers to a selected frame sink through
  `rtpav.Source... -> stream select -> codec.DecoderStage -> optional filter
  stages -> Sink` when one stream matches the selector and the codec registry
  has a decoder factory
- one or more RTP/WebRTC packet readers to selected-stream
  decode/filter/encode outputs through the same decoder, filter, encoder, and
  mux stages used by file or protocol inputs
- `Transcode(plan)` for one input where all renditions resolve to the same
  selected stream, sharing one decode and fanning frames into multiple named
  encoder branches; resize/resample configs insert filter stages through the
  filter registry, and outputs can receive all renditions or select branches by
  rendition name or label

Resize and resample branch configs fail explicitly at build time when no matching
filter factory is registered.
When output geometry is known, branch filter stages receive preallocated frame
scratch so concrete resize filters can keep plane ownership with the caller.

Recipe helpers also expose `PacketFunc`, `FrameFunc`, `EventFunc`, and
`SinkFunc` so small custom processing hooks can participate in the graph without
implementing full source/stage/sink types.

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
Timestamp helpers in `av` keep RTP clock domains, media durations, and standard
Go durations convertible without allocating or making each adapter repeat its
own timebase math.

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
Decoder factories may also implement `codec.DecodeStateFactory` when a
high-level runtime needs adapter-specific reusable state before opening the
decoder; exact low-level applications can still pass their own state through
`codec.DecodeConfig.OpaqueState`.
Descriptor-only adapters are allowed for planned or optional backends; they are
discoverable through registry descriptors and planned media compatibility lists,
while factory lookup returns
`codec.ErrUnavailable` until an active factory is registered. `goh264`,
`govpx`, and `goav1` follow that pattern in normal builds and register concrete
factories only when their build tags are enabled.

## Realtime pipeline

The pipeline API is deliberately event-aware. A stage receives a `Message` and
can emit zero or more messages.

The default executor is a synchronous direct-call graph. It does not create
goroutines or channels per packet, and fanout delivers the same message and
payload references unless a future explicit policy asks for copying. When a
non-direct `BufferPolicy` is configured, the default factory builds a bounded
buffered graph instead. That graph copies message headers into per-node queues,
uses the shared drop controller for backpressure and drop behavior, shares
immutable media buffers, and copies borrowed packet payloads or frame planes
into preallocated per-node slots when `BufferPolicy` provides explicit byte
bounds. Borrowed media without a configured copy bound fails early instead of
extending unsafe lifetimes. Buffered handlers must copy any message header or
buffer data they need after `Handle` returns; slot-owned pointers are reused.
High-level runtime compilers use the same policy surface, so multi-output
transcode and RTP/WebRTC packet-reader record/fanout graphs can switch from
direct calls to bounded buffered execution without a separate graph shape.

Builders and graphs produce a `pipeline.Spec`: structured nodes and edges that
are the core inspection contract. Nodes may include short workflow details such
as `rtp receive`, `packets -> frames`, `frames -> packets`, `resize`, or `mux`.
This makes generated pipelines easy to validate, log, inspect, or visualize
before running media through them. Optional exporters live outside the runtime
core in `graphrender`, so diagram formats can evolve without changing graph
composition. Specs keep routed edges labeled as media concepts such as
`stream=video` or `event=packet_loss`; executor-specific details stay behind
the graph implementation.

The codec package includes generic decoder and encoder stages. They adapt
`codec.Decoder` and `codec.Encoder` implementations to pipeline messages using
caller-owned result scratch. The decoder stage turns packet messages into frame
messages, keeps upstream events visible by default, flushes before EOS, and uses
packet-loss events to trigger audio PLC paths such as Opus concealment. The
runtime builder asks decoder factories for optional adapter-owned state before
opening stages, so heavyweight backends can stay hidden behind the same fluent
`Decode(...).Sink(...)` path. The encoder stage turns frame messages into packet
messages, observes upstream events for encoder state, flushes delayed packets
before EOS, and consumes input events after the graph observes them.

The format package follows the same pattern for containers. `DemuxSource`
adapts a `format.Demuxer` into packet and event messages, including stream and
EOS events. Runtime-created demux sources own bounded packet payload storage
for demuxers that fill caller-provided packets. `MuxStage` writes packet
messages through a `format.Muxer` and emits write-result events through the
graph, so output-side state remains observable instead of disappearing inside a
terminal sink. Packet fanout happens before mux stages through graph routes, and
input events stay observable through the graph event stream rather than being
relayed by the mux boundary.

The filter package follows the codec stage model for frame transforms.
`filter.Stage` adapts a `filter.FrameFilter` to frame and event messages,
flushes before EOS, and uses caller-owned result scratch. Runtime transcode
branches resolve resize and resample configs through the filter registry before
attaching the stage ahead of each encoder.
The first concrete filters are `adapters/resample` for interleaved S16 audio and
`adapters/resize` for planar 8-bit 4:2:0 video.

The RTP package provides the live receive source. `rtpav.Source` keeps Pion RTP
packets at the boundary, applies optional jitter and depacketizers, forwards
realtime events into those depacketizers, and emits normal packet/event messages
for the same mux, decode, and analysis stages used by file or protocol inputs.
It also tracks configured stream timestamps and emits discontinuity events when
timestamps move backward or exceed an application-provided maximum gap.
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

The `transcode` package exposes one explicit plan shape for renditions and
output selection. The runtime compiler turns that plan into a graph with one
shared selected decode, multiple encoder branches, and mux outputs that select
renditions by name or label. Resize and resample branch configs become filter
stages when matching factories are registered.

Typical use cases:

- Generic live receive to several outputs.
- WebRTC receive to recording plus preview plus analysis.
- One video decode feeding several resize branches.
- One audio decode feeding several resample branches.
- Per-output codec, bitrate, container, and protocol decisions.
