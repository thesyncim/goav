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
Recipes: From, chains, taps, branches, destinations
  |
Intent graph: inputs, selected media, chain operations, destinations, policies
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
embedded runtimes explicit. `From(input)` is the beginner-facing front door. The
surface is small: `From`, chains, taps, branches, destinations, flows, and tasks.
`Branch`, `Destination`, and `Chain` composition is the normal user-facing model.
produces a small intent model for packet copy, stream decode, transform, encode,
declared branch composition, and runtime tap naming. The target architecture is
one media work planner that validates, probes, resolves streams, resolves
formats/codecs, chooses
packet-copy or decode branches, inserts demux or depacketize boundaries, inserts
select/decode/transform/stage/tap/encode operations, groups branches by
destinations, assigns routes and buffer policy, then emits the `pipeline.Spec` used to build
the runnable graph. `MediaPlan` is the planner/report IR for that work:
declared branches, reusable flow branches, decode recipes, and
packet-preserving copy/remux all become ordinary branch operations over the same
model. `WorkPlan` is the target executable cold-path boundary: recipe
compilation must emit planned work before it can explain, describe, or build a
normal workflow, and that work plan owns planned nodes, edges, ordered
operations, report inputs, streams, taps, branches, destinations, decisions,
diagnostics, plus the work-plan lowerers used to build the runtime graph.

The active recipe compiler state carries public `Intent` plus concrete readers,
writers, sinks, and stages through validation, media-plan creation, graph-plan
emission, and planned-spec emission. Branches carry ordered stage, transform,
tap, and encode operations and can start after earlier operation specs such as
decode, resize, resample, custom stages, and taps. `Job.Explain(ctx)` reports the
`MediaPlan` branch operations, each operation's output shape, resolved branch
stream shape, taps, decisions, and adapter capability details. Planned taps
inherit stream shape from probe/live metadata and update it through transforms
and encoders, so runtime branches attached from those taps start with useful
codec, width/height, sample-rate, channel, and sample/pixel-format context.
Codec descriptors describe encode/decode media and frame-format constraints,
filter descriptors describe transform media constraints, and format descriptors
describe target container media, codec, and stream-count constraints so adapter
conflicts can fail before planned or runtime graph mutation. Declared branch
composition still carries private branch-compose migration state owned by the
recipe compiler; the advanced `transcode.Plan` path must keep shrinking until it
is outside normal composition. Runtime branch-composer graph helpers are being
folded into the same branch planner used for initial builds and attach patches.
Branch-composition inputs and resolved destinations are carried by the resolved
recipe into planned work; `Describe` and `Build` use that work plan directly and
only borrow runtime services for adapter-backed sources,
filters, encoders, and muxers. Packet-preserving copy/fanout recipes lower
their optional select operation and destination operations from the work plan's
ordered operation sequence while still borrowing concrete input and destination
openers from the stream lowerer. Direct selected-stream decode/encode recipes
validate their select, decode, transform/stage, encode, and destination
operations from the same sequence before source opening, and
select/decode/filter/encode nodes plus mux/sink destination nodes lower from
work-plan refs. Those direct stream
recipes still keep concrete inputs, destinations, ordered stream attachments,
codec-change policy, custom stages, transforms, and taps on the resolved recipe
until work-plan emission. They describe through a resolved single-stream work
plan using the branch route planner, and build by lowering that single branch
through the shared branch-compose input/route helpers instead of a pre-populated
runtime builder. Selected packet-copy recipes use the same single-branch route
path for both planned specs and runtime builds, while whole-input packet copy
preserves multi-input fanout. Branch route planning carries codec-change policy
and decoded sink event-drop policy into planned decoder details and runtime
decoder construction. `recipeResolved` no longer carries a parallel media-plan report
copy:
`Explain`, mux diagnostics, and task tap installation read cloned views from the
work plan. The work plan also carries an ordered operation sequence derived
from branch operations and destination groups. Packet-copy, direct stream
decode/filter/encode, and grouped branch-compose builds now consume that
sequence for pre-mutation validation, destination binding, and destination node
construction. Selected packet-copy and direct frame-stream lowering isolate one
branch operation set before reading select/decode/filter/encode and destination
refs; selected packet-copy then lowers through branch-compose input and destination routes,
while whole-input packet copy can still preserve multiple input branches.
Grouped branch-compose input lowering also consumes work-plan select/decode node refs, so described
and built graphs stay equivalent even when operation refs are changed by later
planning passes. Shared branch-compose transform/stage lowering consumes the
same operation refs and validates that branches sharing one selector group also
share the same planned step refs. Private branch transform/stage and encoder
lowering now consume branch-local operation refs too. Branch-compose mux/sink
destination construction and branch-to-destination routing now consume
destination operation records, including planned destination node refs.
Whole-input packet copy carries source-owned stream groups so destination stream lists follow work-plan branch
matches instead of ad hoc output fanout.

The next architectural pressure is to introduce the explicit GoAV-native
planning layer:

```text
BranchSpec -> WorkPlan   // initial build
BranchSpec -> WorkPatch  // runtime attach
```

`MediaShape` is the target public contract for branch, flow, tap, sink, byte
destination, shared destination, and custom source compatibility; current
stream-cap metadata is the migration base. `WorkPlan` should own ordered
operations, shape transitions, taps, destinations, branch buffer policy, detach
policy, and lifecycle expectations. `WorkPatch` should use the same branch plan as initial build, but anchor downstream of
existing typed taps. This removes special workflow compilers from normal
composition and keeps runtime attach from becoming a separate graph language.

`Destination` is the public routing handle and extension surface for files, byte
writers, object-store uploads, URI-backed outputs, frame/packet/event sinks, and
shared mux/sink groups. Reusing one destination value groups branches. The work
plan keeps concrete destination openers cold until stream list, format, MIME,
metadata, and realtime policy are known.

The handle-based graph builder remains available only as the explicit advanced
layer through `goav.Expert(runtime).Graph()`. It names sources, stages, and
sinks once, then connects typed handles such as `source.Stream("audio")` and
`decode.Out()` to node inputs. The graph builder is no longer on the public
`Runtime` interface or an exported top-level constructor. Described graphs and
execution graphs must stay equivalent across work-plan lowerers. The graph
layer stays available for inspection and custom stages. Recipe `Explain(ctx)`
returns structured workflow-report data, branch operations, planner decisions,
and the same
`pipeline.Spec`; optional diagram or prose rendering lives outside runtime
composition. Branch operation reports mark shared upstream work, so the planner
can explain when branches reuse decode, transform, stage, or tap boundaries
before diverging into private downstream chains. A route carries all media by
default, or matches one stream or event type.

`Task.Attach` is the first runtime control-plane operation. It plans a private
runtime graph patch from one or more named downstream branches, prepares
destinations and branch components before graph mutation, applies the patch
atomically enough to roll back opened nodes on failure, and returns an
attachment handle with `Close(ctx)`. Direct graphs and bounded buffered graphs
both support late stage/sink branches for future messages. `Task.Detach(ctx, h)`
removes one live attachment, and `Task.Close()` stops attachments before closing
the graph.
Stable recipe outlets come from typed `.Tap(goav.FrameTap(name))` or
`.Tap(goav.PacketTap(name))` calls and are listed by `Task.Taps()`; runtime
branches attach with `goav.Branch("name").From(tap)`. A late branch can run
custom `.Do(...)` stages, apply reusable flows, resize or resample from frame
taps, encode Opus/VP8/VP9 from frame taps into a destination, copy packet taps
into a destination, decode packet taps into frame-domain work, apply
flows that own the packet-to-frame boundary, and expose its own typed tap
outlets, so another late branch can attach downstream without rebuilding the
task. Taps declared after encode or copy operations are packet-domain outlets.
H264 and AV1 recipe encoding remain work in progress. Detaching a parent runtime
branch also removes dependent runtime branches anchored from that parent's taps.
Expert graph attachments can still start from the `GraphNode` or `GraphOutlet`
handles returned by `goav.Expert(runtime).Graph()`. This is for late analysis,
meters, screenshot collectors, and late recording branches that should observe
future messages without rebuilding the task. Buffered runtime attachment owns
queue and worker lifecycle for late nodes; packet-copy recording destinations are
covered, Opus encode-to-recording from frame taps is covered with bounded
packet copy into the late mux destination, flow-applied Opus encode-to-destination
branches are covered, flow-owned decode from packet taps is covered,
post-encode runtime branch taps can feed dependent packet copy branches, and
bounded buffered graphs can attach a dependent branch after a runtime resize tap
before future frames arrive. Broader encoded mux capability and teardown stress
coverage remain active slices.

Current graph execution covers:

- empty graphs for lifecycle tests
- explicit `Source -> Stage -> Sink` graphs with handle-based connects,
  multi-target fanout, and stream/event match options
- one-input/many-output remux and fanout through
  `format.DemuxSource -> format.MuxStage...` when the format registry can
  probe, demux, and mux the requested boundaries
- one-input selected-stream decode to a sink through
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
- one or more RTP/WebRTC packet readers to a selected sink through
  `rtpav.Source... -> stream select -> codec.DecoderStage -> optional filter
  stages -> Sink` when one stream matches the selector and the codec registry
  has a decoder factory
- one or more RTP/WebRTC packet readers to selected-stream
  decode/filter/encode outputs through the same decoder, filter, encoder, and
  mux stages used by file or protocol inputs
- live RTP/WebRTC reusable branches that receive through `rtpav.Source`, share the
  selected stream decode, then route each flow-derived branch through its own
  transforms, encoder, and mux output
- transcode-shaped recipes for one input grouped by selected stream: video branches can
  share a video decode, audio branches can share an audio decode, and one output
  destination can be a mux group that receives coordinated encoded audio and video
  branches or a sink that receives frames before encode and packets
  after encode. Resize/resample configs insert filter stages through the filter
  registry, and destination groups select branches by planned branch ownership.

Resize and resample branch configs fail explicitly at build time when no matching
filter factory is registered or when the registered descriptor advertises an
incompatible media kind, pixel/sample format, or resize mode. Runtime branch
attachment applies the same descriptor preflight before mutating a running
graph. Filter registrations retain their descriptors, so `Explain(ctx)` can
report transform input/output media kind, supported config values,
realtime/stateless flags, and adapter metadata alongside
missing/available/incompatible status.
When output geometry is known, branch filter stages receive preallocated frame
scratch so concrete resize filters can keep plane ownership with the caller.
Mux targets also preflight format descriptor media, codec, and stream-count
constraints for both planned branches and runtime attachments before a muxer is
opened.
Decoder inputs preflight codec descriptor media, sample-format, and pixel-format
constraints for live/probed recipe streams and runtime graph construction before
a decoder is opened. Unknown stream frame formats remain unconstrained until an
adapter can inspect real input.
Encoder targets preflight codec descriptor media, sample-format, and
pixel-format constraints for planned branches and runtime attachments before an
encoder is opened.

Recipe helpers also expose `PacketFunc`, `FrameFunc`, `EventFunc`, and
`SinkFunc` so small custom processing hooks can participate in the graph without
implementing full source/stage/sink types.
Chain transforms such as `Audio().Resample(...)` and
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
chain decode recipe. The encoder stage turns frame messages into
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
attaching the stage ahead of each encoder or runtime branch sink.
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
flow-derived branches: input ref, stream selector, operation chain, destinations,
and mux groups. Mixed audio/video outputs are modeled as mux groups receiving
ordinary encoded branches.

Multiple branches that select the same input stream should share upstream demux,
selection, and decode nodes unless a future isolation policy asks otherwise.
When a stream chain declares operations before `.Branches(...)`, the planner
treats the current stream point as a shared prefix: one resize/resample/stage
can feed several downstream branches. Naming that point with `.Tap(...)` is
only required when a stable runtime attachment handle should be exposed through
`Task.Taps()`. One target can be a mux group that receives coordinated encoded
branches from different media streams. Resize, resample, and custom stage steps
become ordinary branch operations; transform steps use matching filter
factories when registered.

Typical use cases:

- Generic live receive to several outputs.
- WebRTC receive to recording plus preview plus analysis.
- One video decode feeding several resize branches.
- One resized video point feeding several encoded, thumbnail, or analysis branches.
- One audio decode feeding several resample branches.
- Per-output codec, bitrate, container, and protocol decisions.
