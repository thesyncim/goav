# Architecture

`goav` separates stable media contracts from implementations. Read this as the
map behind the recipe API: what the public grammar describes, where validation
happens, and which runtime pieces stay hidden unless you deliberately opt into
the expert layer.

The central rule is:

```text
every edge carries media data or media events
```

That keeps live WebRTC realities visible to the application. Packet loss,
renegotiation, codec changes, backpressure, discontinuities, track lifecycle,
and keyframe recovery are represented as events instead of hidden side effects.

## Layers

```text
Application
  |
Recipes: From, stream selection, operations, taps, branches, destinations
  |
Intent graph: inputs, selected streams, ordered operations, destinations, policies
  |
Work-plan planner passes
  |
Pipeline graph
  |
Sources -> packet/frame/event stages -> sinks
  |
Format, RTP, WebRTC, codec, and filter adapters
```

`goav.New` is the composition root. It owns the per-runtime codec, format, and
filter registries; there are no global registries. `bundle.MustNew(opts...)`
registers the bundled in-repo adapters and then applies caller options
last-wins, so one call can add or override implementations.

Stream values are the beginner-facing front door, and the surface is small:
input stream selection (`input.Video()`), ordered operations, taps, forks by
reuse, `rt.Run`/`rt.Build`, tasks, and destinations — direct `File`/`URI`/`Sink` destinations, custom `Writer`
destinations with `provider.Info`, and shared mux/sink outputs by reusing one
destination value.

One media work planner owns the cold path. It:

- validates recipe shape and extension contracts;
- probes inputs and resolves streams, formats, and codecs;
- chooses packet-copy or decode paths;
- inserts demux, depacketize, select, decode, transform, stage, tap, and encode
  operations;
- groups branches by destination;
- assigns routes and buffer policy;
- emits the `pipeline.Spec` used to build the runnable graph.

`WorkPlan` is the executable cold-path boundary: the one planner/report IR
owning planned nodes, edges, ordered operations, report inputs, streams, taps,
branches, destinations, decisions, and diagnostics. Described graphs and built
graphs are the same graph: both come from the one lowering.

The compiler is one pipeline:

```text
Outputs -> recipe DAG -> branches -> one lowering -> WorkPlan + pipeline.Spec  // build
Outputs -> recipe DAG -> branches -> one lowering -> graph patch               // task.Attach
```

Every Stream value is one node of the recipe DAG (`internal/recipeir.Graph`):
a Stream used twice is one node with several children, so the work before a
fork is planned once, and a join is one node with several parents. A branch is
the path from a root (an input's select or a join) to the Outputs it ends in.
One validator walks each branch's operations, joins plan as ordinary nodes,
the shape solver keys each conversion by the node it feeds, and the lowering
emits each node once. Any number of joins and streams plan in one recipe, and
a join arm may read a stream other work reads.

`shape.Spec` is the public compatibility contract for branch, tap, sink,
byte destination, shared destination, and custom source shapes. `WorkPlan`
owns ordered operations, shape transitions, taps, destinations, branch buffer
policy, detach policy, and lifecycle expectations.

Runtime attach is not a separate graph language or planner: `task.Attach`
compiles its Outputs through the same DAG, validator, and lowering, rooted at
the task's taps, tracks, or graph routes, and opens each planned node through
the build's opener. Work and destinations its Outputs share are one node, a
branch's own nodes are named under the branch (`voice/encode-voice`), and the
planned patch is the applied one (`TestAttachPatchDescribeEqualsBuild`).

Inputs and destinations open through one extension point per side: every input
kind (file, URI, RTP, WebRTC, custom source) resolves through one source
opener, and destination kinds resolve through destination providers. The build
does not branch on input/output kind.

`Destination` is the public routing handle and extension surface for files,
byte writers, object-store uploads, URI-backed outputs, frame/packet/event
sinks, and shared mux/sink outputs. The handle is the identity: one value reused
across chains or branches feeds one mux or sink, and separately constructed
same-name handles are rejected. The work plan keeps
concrete destination openers cold until stream list, format, MIME, metadata,
and realtime policy are known.

The handle-based graph builder remains available only as the explicit advanced
layer through `expert.Graph(runtime)`; it is not on the public `Runtime`
interface. Recipe `Explain(ctx)` returns structured workflow-report data,
branch operations, planner decisions, and the same `pipeline.Spec`; rendering
lives outside core in `graphrender`.

Branch operation reports mark shared upstream work, so the planner can explain
when branches reuse decode, transform, stage, or tap boundaries before
diverging. A route carries all media by default, or matches one stream or event
type.

`task.Attach` is the runtime control-plane operation for late branches. It
plans a private graph patch from one or more Outputs (named downstream branches),
prepares destinations and components before graph mutation, applies the patch
with rollback on failure, and returns an attachment handle with `Close(ctx)`.

Stable outlets come from `.Tap(name)` calls, listed by `task.Taps()`, whose
frame or packet domain follows their position; attached Outputs start from
`task.Tap(name)` or `task.Track(input, stream)`. Late branches can run custom
stages, resize/resample from frame taps, encode Opus/VP8/VP9/AV1
from frame taps, copy or decode packet taps, and expose their own typed taps
for later attachments. Detaching a parent removes dependent branches anchored
from its taps. H.264 recipe encoding is intentionally descriptor-only in the
default bundle until an encoder backend is registered explicitly.

Design direction: a tap is not a second data path. It is a named stream point
that later consumers can bind to. Planned forks, attached Outputs, join arms,
and `Control.AtTap` should keep sharing that one internal anchor
model: `.Tap(name)` declares the point, `task.Tap(name)` consumes it, and the
planner owns the node/domain/shape facts. That gives the useful part of "tap
at any point" while preserving the small public grammar and avoiding a second
branch API.

Typed task control rides the same graph: untargeted controls (keyframe
requests) enter at the source boundary and follow the data path to capable
nodes; `.AtTap(name)` narrows to a tap's point; node-targeted `.At(...)` is
expert-only. `Select` joins consume `SelectActive` controls to switch the live
arm. Convergence (`Mix`/`Composite`/`Select`) lowers each arm to a source
chain and joins N edges into one stage node; the join output is an ordinary
stream point for taps, branches, encode, and destinations.

Current graph execution covers: lifecycle-only graphs; explicit
source/stage/sink graphs with fanout and stream/event match options;
remux/fanout through `format.DemuxSource -> format.MuxStage...`; selected-stream
decode to sinks; decode/filter/encode to one or more outputs; one or more
RTP/WebRTC packet readers into the same mux, decode, and analysis stages used
by file inputs; reusable flow branches sharing one selected decode; and
transcode-shaped grouped branches where one mux-group destination receives
coordinated encoded audio and video branches.

Resize and resample configs fail explicitly at build time when no matching
filter factory is registered or the registered descriptor is incompatible.
Runtime attachment applies the same descriptor preflight before mutating a
running graph. Mux, decoder, and encoder boundaries preflight format and codec
descriptor constraints (media kinds, codecs, stream counts, sample/pixel
formats) before anything is opened, for both planned branches and runtime
attachments. Filter registrations retain descriptors so `Explain(ctx)` can
report capability details alongside missing/available/incompatible status.

Recipe helpers expose `PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc` so
small custom hooks can participate without implementing full graph types.
Operation transforms such as `Audio().Decode().Resample(...)` and
`Video().Decode().Resize(...)` lower through the same filter registry as branch
transforms.

## Package layering

What the compiler enforces today: the sibling packages (`av`, `errcode`,
`plan`, `shape`, `flow`, `provider`, `pipeline`, `codec`, `format`, `filter`,
`container`, `lifecycle`, `snapshot`, `rtpav`, `webrtcav`, `playoutav`,
`adapters`, `graphrender`) are leaves; none of them imports the root `goav`
package. Root depends on leaves, never the reverse.

The one exception is `expert`: it sits above root because it imports `goav` to
return the `LiveTask` its `Build` produces.
Root reaches back only through structural interfaces
(`ExpertGraph() any`, the branch-anchor `Route` capability), never an import.

What is convention only: inside the root package, the grammar -> plan -> build
boundaries (the value grammar, the `compile*.go` passes and `WorkPlan`,
graph/attach lowering) are file-naming conventions, not import-checked. The
root package is deliberately one compilation unit.

The data boundary is the recipe DAG: the value grammar (`stream.go`,
`source.go`, `stream_rule.go`) builds Stream values, `recipe_graph.go`
compiles Outputs into the DAG, and the compiler passes read the DAG plus the
concrete input and destination handles it indexes.
`TestGrammarFilesDoNotReadPlannerState` pins that the grammar never reads
compiler state, and `TestRecipeIRImportsOnlyLeafPackages` that
`internal/recipeir` imports only leaf vocabulary packages, never the root.

Why the compiler still cannot move to `internal/` packages: a type-checked
cross-file reference graph (measured 2026-07-02) closed the planner seed over
99.5% of the root package's non-test code, so moving it would move the root
package. The DAG narrows the waist — passes read plain DAG data — but the
lowering still builds stages from root runtime types (`Runtime`, `InputSpec`,
`Destination`), and relocating exported types is public-API churn (type
identity and `reflect.Type.PkgPath` change even under aliases). The split
stays an honest NO-GO.

## Core media model

The `av` package owns transport-neutral vocabulary: `Stream`,
`CodecParameters`, `Packet`, `Frame`, `Event`, `Timestamp`, `TimeBase`, and
`Epoch`. RTP and WebRTC details do not leak into `av`; dedicated packages use
Pion types directly and translate into `av.Packet`/`av.Event`. Timestamp
helpers keep RTP clock domains, media durations, and Go durations convertible
without allocating.

## Format probing

The default runtime includes cold-path static probing for common boundaries:
Ogg/Opus, IVF, FLV, Matroska/WebM, MP4, Annex B, RTP, and WebRTC. Probe scores
prefer magic bytes and protocol declarations over extension fallback.

## Codec epochs

Each stream carries an `Epoch`. When codec parameters change, the receiver
emits `av.EventCodecChanged` with a new epoch, so downstream decoders can
drain, reset, or drop until sync without guessing. WebRTC track readers expose
that boundary through `UpdateCodec` and `UpdateTrack`; `webrtcav.TrackSet`
coordinates accepted tracks into one long-lived reader per logical stream, so
same-stream replacements update existing graph inputs. The RTP source consumes
the event by refreshing its payload map.

## Codec backends

Adapters wrap existing pure-Go codec projects (`gopus`, `goaac`, `govpx`,
`goav1`, `goh264`) behind `codec.DecoderFactory` and `codec.EncoderFactory`; the core
runtime does not depend on codec internals. Decoder factories may implement
`codec.DecodeStateFactory` when a high-level runtime needs adapter-specific
reusable state; exact low-level applications pass their own state through
`codec.DecodeConfig.OpaqueState`. Descriptor-only adapters are discoverable
through registry descriptors while factory lookup returns
`codec.ErrUnavailable` until a concrete factory is registered.

## Realtime pipeline

The pipeline API is event-aware: a stage receives a `Message` and emits zero or
more messages. The default executor is a synchronous direct-call graph with no
goroutines or channels per packet; fanout shares message and payload
references. With a buffered policy, the factory builds a bounded buffered graph
instead: per-node queues with a single serial worker each, the shared drop
controller for backpressure/drop behavior. A queue slot is a header copy that
holds references to the payload: published bytes are immutable and shared by
reference (`av.Buffer`, pooled by `av.NewBuffer`), so every branch reads the
producer's bytes and none is copied per edge (`TestFanoutSharesOnePayload`).
Codec, filter, and demux stages publish their adapters' output once into
pooled buffers (`internal/payload`), so a sink may keep a frame with `Ref`
(`TestRetainedFramesStayStableAcrossPushes`). A removed node gives up its
queue and its index (`TestRemovedNodesAreReclaimed`).
Both executors are delivery strategies over one shared topology
(`pipeline/topology.go`: nodes, routes, the routing snapshot, spec, stats,
readiness, observer events, the run's error contract, and shutdown).
Routing is lock-free: a message finds its consumers through an atomically
swapped snapshot and counts itself in per-node atomics, so no graph-wide lock
sits on the media path. Each node is serialized, handling one message at a
time: the direct runner takes the node's handle lock and the buffered producer
its queue lock, uncontended unless several producers feed one node. Every other
lock is on a cold path.
Runtime-owned scratch pools are likewise per-runtime and opt-in through
`runconfig.WithMediaPools(true)`, so attach/detach churn can reuse internal
metadata maps and join plane slices without creating a process-global recycler.
The allocation side of that contract is test-enforced
(`TestGraphDirectRunAllocs`, `TestGraphBufferedSteadyEmitAllocs`,
`TestBufferedFanoutRefcountAllocs`) and measured by the fanout benchmarks; see
`docs/PERFORMANCE.md` for what is proven versus not proven.

Builders and graphs produce a `pipeline.Spec`: structured nodes and edges with
short workflow details (`rtp receive`, `packets -> frames`, `resize`, `mux`)
and media-concept edge labels (`stream=video`, `event=packet_loss`). The codec
package provides generic decoder/encoder stages over caller-owned result
scratch (decode flushes before EOS and drives PLC from loss events; encode
observes control events and flushes delayed packets). The format package
mirrors that for containers (`DemuxSource`, `MuxStage` with write-result
events), and the filter package for frame transforms (`filter.Stage`).
`rtpav.Source` is the live receive source: payload maps, optional jitter,
depacketizers, loss/timestamp tracking, and stream-scoped EOS.

## Multi-output media planning

Fork composition lowers into one work-plan branch shape: input ref, stream
selector, operation sequence, destinations, and mux groups. Forks of the same
stream share upstream demux, selection, and decode; operations before the first
fork form a shared prefix (work shared below the first fork is duplicated per
fork today). Naming a point with
`.Tap(...)` is only required when a stable runtime attach handle should appear
in `task.Taps()`. Typical uses: live receive to several outputs, WebRTC receive
to recording plus preview plus analysis, one decode feeding several
resize/resample branches, and per-output codec/bitrate/container decisions.

## Joins

How `Mix`, `Composite`, `Select`, and `Join` plan and build (the user-facing
side is [Multi-input](MULTI_INPUT.md)).

All three plan through the same recipe compile. The joinSpec normalizes into
the compile state, `joinPlan` plans N arm sub-chains converging into an
`OpJoin` node (workPlan edges carry the N-to-1), and `Describe()` == `Build()`
is guard-tested per kind, nested case included. Per-kind behavior lives only in
the joinProfiles table.

Arm shape-solving goes through the central solver (`armExpected`/`armPolicy`).
The joined output runs through that same solver before terminal `.Encode(...)`
and before planned forks, seeded with the joined stream's shape: fork-local
`.Auto(...)` can insert conversions on that fork, and `.Auto(...)` on the
Mix/Composite output applies to the terminal encode path or to every planned
fork.

Taps converge mid-graph: an arm keeps its declared `.Decode()`/`.Tap(name)`.
The tap installs on the task anchored at the arm's decode (or source) node,
so one decode feeds the join and any other consumer (an Output attached from
`task.Tap(name)`). Join stages identify arms by stream id. Other arm
operations (transform/encode/stages) are rejected with the supported
alternatives instead of being silently dropped.

Joins are an extension point: `goav.Join(name, stage, streams...)` lowers a
caller-supplied convergence stage through the same joinSpec/joinProfile
machinery, so a third party can ship `Crossfade(streams...)` without core changes.
The per-kind behaviors the profile table carries are derived from the stage's
`shape.Contract` (frame-domain inputs -> explicit decoded arms like Mix; packet/any ->
passthrough like Select; one fact-carrying input shape -> solver-planned arm
conversions; declared output -> the joined stream, else first-arm facts) and
from the join's snake-safe name (node name, joined output stream id,
`<name>_*` error-code family). A custom join is a full citizen: `.Tap`,
forks, `.To`, nesting as a join arm, `Describe() == Build()`; proven from
outside the core in `adapterproof/join_proof_test.go`, including a
re-expression of Select's passthrough semantics. The stage contract is
documented on `goav.Join`; `audioMixStage` (audio_mix.go) is the reference
implementation.

