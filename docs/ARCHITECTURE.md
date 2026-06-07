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
Recipes: From, stream chains, taps, branches, targets
  |
Intent graph: inputs, streams, transforms, targets, policies
  |
MediaPlan planner passes
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
embedded runtimes explicit. `From(input)` is the beginner-facing front door. It
produces a small intent model for packet copy, stream decode, transform, encode,
declared branch composition, and runtime tap naming. The target architecture is one media planner that
validates, probes, resolves streams, resolves formats/codecs, chooses
packet-copy or decode branches, inserts demux or depacketize boundaries, inserts
select/decode/transform/stage/tap/encode operations, groups branches by targets,
assigns routes and buffer policy, then emits the `pipeline.Spec` used to build
the runnable graph. `MediaPlan` is the planner IR for that work: declared
branches, reusable flow branches, decode recipes, and packet-preserving copy/remux
all become ordinary branch operations over the same model. Recipe compilation
must recognize a media-plan shape before it can describe or build a normal
workflow.

The active recipe compiler state carries public `Intent` plus concrete readers,
writers, sinks, and stages through validation, media-plan creation, planner
lowering, and planned-spec emission. Branches carry ordered stage, transform, tap,
and encode operations and can start after earlier stream operations such as
decode, resize, resample, custom stages, and taps. `Job.Explain(ctx)` reports the
`MediaPlan` branch operations, taps, and decisions. The next architectural
pressure is to shrink the remaining internal builder lowering behind each
media-plan build kind until graph construction is directly
`MediaPlan -> pipeline.Spec -> pipeline.Graph`.

The handle-based graph builder remains available as the advanced layer through
`Runtime.Graph()`. It names sources, stages, and sinks once, then connects typed
handles such as `source.Stream("audio")` and `decode.Out()` to node inputs.
The internal builder is no longer a method on the public `Runtime` interface or
an exported top-level type. Described graphs and execution graphs must stay
equivalent for every media-plan build kind. The graph layer stays available for
inspection and custom stages. Recipe `Explain(ctx)` returns structured
workflow-report data, branch operations, planner decisions, and the same
`pipeline.Spec`; optional diagram or prose rendering lives outside runtime
composition. A route carries all media by default, or matches one stream or
event type.

`Task.Attach` is the first runtime control-plane operation. It attaches a named
stage/sink branch to a built direct graph and returns an attachment handle with
`Close(ctx)`. `Task.Detach(ctx, h)` removes one live attachment, and
`Task.Close()` stops attachments before closing the graph. Stable recipe outlets
come from `.Tap(name)` and are listed by `Task.Taps()`; runtime branches attach
with `goav.Branch("name").FromTap(name)`. A late branch can run custom
`.Do(...)` stages, resize or resample from frame taps, and expose its own
`.Tap(name)` outlets, so another late branch can attach downstream without
rebuilding the task. Detaching a parent runtime branch also removes dependent
runtime branches anchored from that parent's taps. Expert graph nodes can still
be addressed with `From(node)` and `Task.Describe`. This is for late analysis
taps, meters, and screenshot collectors that should observe future messages
without rebuilding the task. Buffered runtime attachments, runtime encoders, and
late muxed target branches remain separate slices because they need queue,
worker, codec, and mux lifecycle management.

Current graph execution covers:

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
- live RTP/WebRTC reusable branches that receive through `rtpav.Source`, share the
  selected stream decode, then route each flow-derived branch through its own
  transforms, encoder, and mux output
- transcode recipes for one input grouped by selected stream: video branches can
  share a video decode, audio branches can share an audio decode, and one output
  label is a mux group that can receive coordinated encoded audio and video
  branches. Resize/resample configs insert filter stages through the filter
  registry, and outputs select branches by branch name.

Resize and resample branch configs fail explicitly at build time when no matching
filter factory is registered.
When output geometry is known, branch filter stages receive preallocated frame
scratch so concrete resize filters can keep plane ownership with the caller.

Recipe helpers also expose `PacketFunc`, `FrameFunc`, `EventFunc`, and
`SinkFunc` so small custom processing hooks can participate in the graph without
implementing full source/stage/sink types.
Stream recipe transforms such as `Audio().Resample(...)` and
`Video().Resize(...)` lower through the same filter registry as transcode
branches, so common processing does not require manually building filter stages.

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
core in `graphrender` behind URI targets, so generated graph output can evolve
without changing graph composition. Specs keep routed edges labeled as media
concepts such as `stream=video` or `event=packet_loss`; executor-specific
details stay behind the graph implementation.

The codec package includes generic decoder and encoder stages. They adapt
`codec.Decoder` and `codec.Encoder` implementations to pipeline messages using
caller-owned result scratch. The decoder stage turns packet messages into frame
messages, keeps upstream events visible by default, flushes before EOS, and uses
packet-loss events to trigger audio PLC paths such as Opus concealment. The
runtime builder asks decoder factories for optional adapter-owned state before
opening stages, so heavyweight backends can stay hidden behind the same fluent
stream-scoped decode recipe. The encoder stage turns frame messages into
packet messages, observes upstream events for encoder state, flushes delayed
packets before EOS, and consumes input events after the graph observes them.

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

## Multi-output media planning

`Transcode` is user-facing syntax, not a runtime engine. It lowers into the
same `MediaPlan` branch shape as `From(input).Audio()/Video().Branches(...)` and
flow-derived branches: input ref, stream selector, operation chain, target refs,
and mux groups. Mixed audio/video outputs are modeled as mux groups receiving
ordinary encoded branches.

Multiple branches that select the same input stream should share upstream demux,
selection, and decode nodes unless a future isolation policy asks otherwise.
One target can be a mux group that receives coordinated encoded branches from
different media streams. Resize, resample, and custom stage steps become
ordinary branch operations; transform steps use matching filter factories when
registered.

Typical use cases:

- Generic live receive to several outputs.
- WebRTC receive to recording plus preview plus analysis.
- One video decode feeding several resize branches.
- One audio decode feeding several resample branches.
- Per-output codec, bitrate, container, and protocol decisions.
