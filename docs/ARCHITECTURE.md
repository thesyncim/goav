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
filter registries; there are no global registries. `goav.Default(opts...)`
registers the standard in-repo adapters and then applies caller options
last-wins, so one call can add or override implementations. `From(input)` is
the beginner-facing front door. The
surface is small: `From`, stream selection, ordered operations, taps, branches,
destinations, flows, and tasks.
`Branch`, `Destination`, and operation composition is the normal user-facing model.

| Layer | Vocabulary |
| --- | --- |
| Simple high-level API | `From`, stream selection, ordered operations, direct `File`/`URI`/`Sink` destinations, custom `Writer` destinations with `provider.Info`, stable destination handles for shared mux/sink groups |

One media work planner validates, probes, resolves streams and formats/codecs,
chooses packet-copy or decode branches, inserts demux or depacketize
boundaries, inserts select/decode/transform/stage/tap/encode operations, groups
branches by destinations, assigns routes and buffer policy, then emits the
`pipeline.Spec` used to build the runnable graph. `WorkPlan` is the executable
cold-path boundary: the one planner/report IR owning planned nodes, edges,
ordered operations, report inputs, streams, taps, branches, destinations,
decisions, diagnostics, plus the work-plan lowerers used to build the runtime
graph. Described graphs and execution graphs must stay equivalent across
work-plan lowerers.

The native planning layer is:

```text
BranchSpec -> WorkPlan   // initial build
BranchSpec -> WorkPatch  // runtime attach
```

`shape.Spec` is the public compatibility contract for branch, flow, tap, sink,
byte destination, shared destination, and custom source shapes. `WorkPlan`
owns ordered operations, shape transitions, taps, destinations, branch buffer
policy, detach policy, and lifecycle expectations. `WorkPatch` uses the same
branch plan as initial build, anchored downstream of existing typed taps. This
keeps special workflow compilers out of normal composition and keeps runtime
attach from becoming a separate graph language. The `runtimeBranch` and
`mediaPlan` parallel IRs are deleted and collapsed onto the work-plan model,
along with the per-workflow builder compilers. The remaining internal debt
(the `streamIntent` normalization layer) is tracked in `docs/NORTH_STAR.md`.

Inputs and destinations open through one extension point per side: every input kind
(file, URI, RTP, WebRTC, custom source) resolves through one source opener, and
destination kinds resolve through destination providers, so the build does not
branch on input/output kind.

`Destination` is the public routing handle and extension surface for files, byte
writers, object-store uploads, URI-backed outputs, frame/packet/event sinks, and
shared mux/sink groups. Reusing one destination value groups branches. The work
plan keeps concrete destination openers cold until stream list, format, MIME,
metadata, and realtime policy are known.

The handle-based graph builder remains available only as the explicit advanced
layer through `expert.Graph(runtime)`; it is not on the public `Runtime`
interface. Recipe `Explain(ctx)` returns structured workflow-report data,
branch operations, planner decisions, and the same `pipeline.Spec`; rendering
lives outside core in `graphrender`. Branch operation reports mark shared
upstream work, so the planner can explain when branches reuse decode,
transform, stage, or tap boundaries before diverging. A route carries all media
by default, or matches one stream or event type.

`Task.Attach` is the runtime control-plane operation for late branches. It
plans a private graph patch from one or more named downstream branches,
prepares destinations and components before graph mutation, applies the patch
with rollback on failure, and returns an attachment handle with `Close(ctx)`.
Stable outlets come from typed `.Tap(goav.FrameTap(name))` or
`.Tap(goav.PacketTap(name))` calls listed by `Task.Taps()`; runtime branches
anchor with `goav.Branch("name").From(tap)`. Late branches can run custom
stages, apply flows, resize/resample from frame taps, encode Opus/VP8/VP9/AV1 from
frame taps, copy or decode packet taps, and expose their own typed taps for
later attachments. Detaching a parent removes dependent branches anchored from
its taps. H264 recipe encoding remains work in progress.

Design direction: a tap is not a second data path. It is a named stream point
that later consumers can bind to. Planned branches, runtime branches, join-arm
`TapRef`s, and `Control.AtTap` should keep sharing that one internal anchor
model: `.Tap(...)` declares the point, `.From(tap)` consumes it, and the
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
Operation transforms such as `Audio().Resample(...)` and `Video().Resize(...)`
lower through the same filter registry as branch transforms.

## Package layering

What the compiler enforces today: the sibling packages (`av`, `errcode`, `plan`,
`shape`, `flow`, `provider`, `pipeline`, `codec`, `format`, `filter`,
`container`, `lifecycle`, `snapshot`, `rtpav`, `webrtcav`, `adapters`,
`graphrender`) are leaves; none of them imports the root `goav` package. Root
depends on leaves, never the reverse. The one exception is `expert`: it sits
above root (it imports `goav` to return `Task`), and root reaches back only
through structural interfaces (`ExpertGraph() any`, the branch-anchor `Route`
capability), never an import.

What is convention only: inside the root package, the grammar -> plan -> build
boundaries (recipe/branch grammar, `mediaPlan`/`WorkPlan` planning,
graph/attach lowering) are file-naming conventions, not import-checked. The
root package is deliberately one compilation unit.

Why the planner internals cannot move to `internal/` packages (measured on the
type-checked cross-file reference graph, 2026-06): the ~20 root files with no
exported API (`media_plan*`, `recipe_compile`, `branch_compose_*`, `work_*`,
`shape_solver`/`shape_glue`, `join_build`, `runtime_attach`/`_decode`/
`_encode`/`_demux`, `mux_destination`, `multi_input`, ~14.8k lines) reference
identifiers defined in 31 grammar files, including unexported fields of the
public types (`Branch.operations`, `Branch.dest`, `Runtime` and `Recipe`
internals). The coupling is bidirectional at field granularity: grammar files
equally read unexported fields of planner structs (`recipe.go`, `source.go`,
`stream_rule.go` into `recipe_compile.go` state; `audio_mix.go`,
`video_composite_build.go`, `select_build.go` into 46 `join_build.go`
identifiers). Computing the largest file set closed under intra-package
dependencies leaves only `join_sync.go` (+`tap.go` at best) movable, which is
not a useful package boundary.

What it would take to enforce the layering: either relocate the grammar records
(`Branch`/`BranchSpec`, `Recipe`, `Runtime`, `Destination`, `InputSpec`,
`operationSpec`, `joinSpec`) into a shared package and alias the exported ones
back through root, which would be public-API churn (type identity and
`reflect.Type.PkgPath` change even under aliases), while the planner records
stay unexported; or introduce a data-transfer boundary so the planner consumes
and returns plain plan data instead of reading and mutating grammar object
fields. Both are larger
restructurings than the boundary is currently worth; the intended cut, if ever,
is the second one, with the shared data types living in `plan`/`shape`.

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
controller for backpressure/drop behavior, shared immutable media buffers, and
preallocated copy slots for borrowed payloads under explicit byte bounds.
Borrowed media without a copy bound fails early. The data plane is lock-free by
design: per-node atomic stats and atomically-swapped routing snapshots, with
mutexes only on cold paths. The allocation side of that contract is
test-enforced (`TestGraphDirectRunAllocs`, `TestGraphBufferedSteadyEmitAllocs`)
and measured by the fanout benchmarks; see `docs/PERFORMANCE.md` for what is
proven versus not proven.

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

Branch composition lowers into one work-plan branch shape: input ref, stream
selector, operation sequence, destinations, and mux groups. Branches selecting
the same stream share upstream demux, selection, and decode; operations
declared before `.Branches(...)` form a shared prefix. Naming a point with
`.Tap(...)` is only required when a stable runtime attach handle should appear
in `Task.Taps()`. Typical uses: live receive to several outputs, WebRTC receive
to recording plus preview plus analysis, one decode feeding several
resize/resample branches, and per-output codec/bitrate/container decisions.
