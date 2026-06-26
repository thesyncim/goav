# API surface

This page answers two practical questions:

- "What should an application author learn first?"
- "Where should an extension author plug in without reaching through the
  runtime?"

The public surface is governed, but not for bureaucracy's sake. Every exported
identifier belongs to one tier, the root and vocabulary surfaces are pinned
against silent growth (`api_surface_pin_test.go` + `testdata/api_surface.txt`),
and every exported symbol must be documented (`doc_pin_test.go`). If a new
export appears, the diff has to say so out loud.

The tiers are a reader map:

- **A. Grammar**: the first API an application should learn.
- **B. Extension points**: interfaces and options for sources, destinations,
  codecs, formats, filters, joins, controls, and tests.
- **C. Expert tier**: graph handles and lower-level building blocks for
  unusual compositions.
- **D. Leakage**: public things that should not be public. This section should
  stay empty.

## A. The Grammar

Most users should be able to stop here. A recipe starts with media, narrows to
the streams that matter, applies ordered operations, names any attach points,
branches when work diverges, and ends in destinations.

One-screen shape:

```
goav.From(input)                          inputs: FileInput, URIInput, Input(provider), Source(fn)
  .Audio() / .Video() / .Stream()         select a stream (InputName/StreamID/StreamIndex/StreamName)
  .Decode() .Copy() .Resize() .Resample() operations are chain methods
  .Do(stage) .Shape() .Auto() .Require() .Prefer()
  .Sync(flow.Sync("room", ...))            shared packet/frame timeline gates
  .Encode(codec.VP9(codec.Bitrate(...)))  codec specs from the codec package
  .Tap(goav.Tap|FrameTap|PacketTap)       named attach points
  .Branches(goav.Branch("x")...To(dst))   fan out; BranchSpec also drives Mutable.Attach
  input.Stream(av.Stream{ID: ...})        attach anchor for app-owned dynamic tracks
  .To(Write|URI|Writer|Custom|Sink|Mux)   destinations; Mux(name, destination) = explicit mux/sink group
  .OnStream(source.MatchMedia|source.MatchCodec|...)    dynamic-stream rules; OnRemove controls detach outcome
goav.Mix/Composite/Select(arms) / Join(name, stage, arms)   N arms -> one stream (JoinArm)
goav.Flow("name")                         reusable operation list (Chain)
job.Describe(); adapter-backed Explain/Build/Run use job.UseRuntime(rt), bundle.Describe/Build/Run
Task: Run, Close; ContextCloser: CloseContext(ctx)
Explainer: Explain
Inspectable: Describe, Taps, Snapshot -> snapshot.*, Stats
Mutable: Attach/Detach(lifecycle.DrainBranch|AbortBranch); Attachment.Rebranch
         (lifecycle.SwitchAt(lifecycle.NextFrame|lifecycle.NextKeyframe|lifecycle.AtMediaTime),
          lifecycle.DrainOldBranch|lifecycle.AbortOldBranch)
Controllable: Control(control.Control values from typed constructors, .AtTap)
Observable: Watch(inspect.EventFilter), Events; inspect.Subscribe/Snapshot/Stats/Render bridge task capabilities
goav.New(goavruntime.Option...) -> (*Runtime, error); goav.MustNew(...) -> bare Runtime; bundle.MustNew(...) -> bundled Runtime; job.UseRuntime(rt)
errors: *goav.BuildError{Family: errcode.FamilyX, Code: errcode.X, Fields: []goav.Detail, Fixes: []goav.Fix, ...} matched with errors.As/Is; branch on Family first; Detail(key) for typed facts
```

The checked operation reference is
[`docs/OPERATIONS.md`](OPERATIONS.md). It is the human-readable contract for
each operation: input shape, output shape, allowed domain, inserted
conversions, primary refusals, and runtime attach behavior.

Applications also read these vocabulary packages:

- `control`: live task control vocabulary (`Control`, `Keyframe`, `Rate`,
  `Seek`, `Segment`, `SetBitrate`, `SelectActive`, `Deliver`, `Must`).
- `inspect`: event watch filters (`EventFilter`, `WatchTypes`,
  `WatchStream`) plus structural helpers (`Subscribe`, `Snapshot`, `Stats`,
  `Render`) that avoid importing the root package.
- `errcode`: the error-code catalog (stable `Family` categories plus one
  detailed `Code` per refusal class).
- `plan`: everything `Explain` reports back.
- `snapshot`: point-in-time task/branch/destination/tap views.
- `lifecycle`: task/branch/destination states.
- `shape`: shape specs and `.Auto` policies (`AllowResample`, ...);
  `shape.Format` pins open-ended container or transport ids for custom
  adapters.
- `flow`: branch buffer policies (`Blocking`, `DropOldest`, ...), shared
  timeline sync policies (`Sync`, `SyncTolerance`, `SyncDropLate`), and the
  `DropReason*` keys for reading drop counters.
- `runtime`: per-runtime construction options (`WithEncoder`, `WithClock`,
  `WithBufferPolicy`, ...).
- `component`: custom `.Do(...)` and direct sink adapters
  (`PacketFunc`, `FrameFunc`, `EventFunc`, `SinkFunc`, `Emit`, `Message`).
- `source`: custom source callbacks and dynamic stream matchers
  (`Func`, `Push`, `Result`, `MatchMedia`, `MatchCodec`, ...).
- `av` identifiers: media/codec/format/protocol ids, event types, metadata.

## B. Extension Points

If you own media I/O, a codec, a transform, a join, or a control host, you
should not need private packages. The pattern is deliberately boring: implement
the exported interface, register it on a runtime with a value-typed `With*`
option, then keep using the normal recipe grammar.

For recipes and copyable examples, start with
[`docs/EXTENSION_COOKBOOK.md`](EXTENSION_COOKBOOK.md). For reference detail,
use [`docs/ADAPTERS.md`](ADAPTERS.md) and [`docs/COMPONENTS.md`](COMPONENTS.md).

- **Sources**: use these when your application or transport owns incoming
  media. `provider.Source` (`OpenSource`), `goav.Source(fn)` with
  `source.Func`/`source.Push`/`source.Result`; transports build on it:
  `rtpav.Receive` (PacketReader, Depacketizer, JitterBuffer, FeedbackWriter,
  PayloadMap extension points), `webrtcav.Track`/`Session` (TrackReader,
  TrackAdapter). `goav.WrapSource(spec, wrap)` is the decoration point: every
  input kind opens through one internal source boundary into a running
  `pipeline.Source`, and wrap intercepts it there, so externals decorate
  built-in inputs (count, mirror,
  transform the message stream) without reimplementing them. Node identity is
  pinned after wrapping (Describe == Build); a `provider.Source`-level wrap
  was rejected because file/URI inputs have no provider view before the
  runtime opens them. Destinations need no analog: every destination
  constructor takes a caller-held value (io.Writer for `Write`,
  `provider.Destination` for `Custom`/`Writer`, `pipeline.Sink` for `Sink`)
  that callers wrap before passing.
- **Joins**: use these when several streams become one. `goav.Join(name, stage,
  arms...)` is N->1 convergence with a
  caller-supplied `pipeline.Stage` as the convergence node. Mix, Composite,
  and Select are profiles over this same machinery; the per-kind behaviors
  the internal profile table carries are derived for externals from the
  stage's `shape.Contract` (frame-domain inputs -> decode arms like Mix,
  packet/any -> passthrough like Select; a single fact-carrying input shape ->
  solver-planned per-arm conversions; the contract's output -> the joined
  stream, falling back to first-arm facts) and from the join's snake-safe
  name (node name, output stream id, `<name>_*` error-code family). The
  result is a full citizen: `.Tap/.Branches/.To`, itself a `JoinArm`
  (nesting), `Describe() == Build()`.
- **Destinations**: use these when your application owns the output boundary.
  `provider.Destination` + `provider.Contract`/
  `provider.Info`, `goav.Writer` (`provider.OpenFunc`), transactional uploads
  via `provider.TransactionalWriter`, frame/packet sinks via `goav.Sink` +
  `component.SinkFunc`, and `goav.Mux(name, destination)` when independently
  built destinations should share one mux/sink group.
- **Custom stages**: use these for in-process inspection or transformation.
  `component.EventFunc`/`component.FrameFunc`/`component.PacketFunc`
  (+`component.Emit`) for
  `.Do(...)`; the node contracts live in `pipeline` (Source/Stage/Sink,
  Emitter, Message, Scratch, capability interfaces).
- **Codecs**: `codec` Descriptor/Decoder/Encoder/factories, caller-owned
  results, `goavruntime.WithDecoder`/`WithEncoder`/`WithCodecAdapter`/
  `WithCodecDescriptor`.
- **Control hosts**: `ctl` is the supported package for applications that
  run a task and expose it to `goav ctl --control unix://...`. It reuses the
  same allowlisted command framework as the bundled command: external hosts
  pass `CommandSpec` for app-specific control verbs, `PipelineRegistry` for
  custom branch-pipeline steps and encoder names, `ValidateCapabilities` to
  preflight host-owned names/aliases/settings metadata, and
  `ServeUnixWithOptions` to put those hooks behind a socket. Generic branch
  pipelines can also encode
  runtime-registered custom codecs with `encode codec=<id> media=<kind> ...`.
  The same socket renders live graph diagnostics through `goav ctl graph`
  (`format=mermaid|dot|text`).
- **Containers**: `format` Prober/Demuxer/Muxer/factories, Seeker for
  seekable inputs, `goavruntime.WithDemuxer`/`WithMuxer`/`WithFormatAdapter`/
  `WithProber`.
- **Filters**: `filter` FrameFilter/Factory/Descriptor,
  `goavruntime.WithFilter`/`WithFilterAdapter`.
- **Runtime config**: `goav.New`, `bundle.MustNew`, `goavruntime.WithClock`,
  `WithRealtime`, `WithBufferPolicy`, `WithEventCapacity`.
- **Media vocabulary**: `av` frames/packets/buffers (`Buffer`,
  `BufferOwnership`, `Plane`), timing (`TimeBase`, `Timestamp`, `Duration`,
  rescaling), `Clock`.
- **Testing**: `goavtest`: deterministic sources (`Audio`, `Video`,
  `Packets`, `LiveAudio`, `NewTestSource`), `Collector`, `Clock`, passthrough
  `Codec`/`Format`, one-liner `Runtime()`. Helpers return real grammar
  values. The nested `goavtest/expect` helper module is the `testing.TB`
  assertion layer for those fixtures: it uses `github.com/google/go-cmp/cmp`
  for structural diffs and adds goav-specific checks for collector samples,
  golden output, and structured `*goav.BuildError` fields.

### Extension closure

The precise statement of what the grammar accepts versus what externals can
implement, with the executable evidence:

- **Fully external.** Everything the grammar composes is implementable
  outside the repo, with built-ins as unprivileged users of the same extension
  points:
  codecs, containers, probers, filters, source providers, transactional
  destinations (`adapterproof/adapter_compat_test.go` - the five-extension-point proof),
  custom `.Do` stages, custom push sources, and custom joins
  (`adapterproof/join_proof_test.go` - an external interleaver runs with
  planner-inserted decode arms, taps, branches, and nested inside Mix, and
  Select's passthrough semantics are re-expressed through `goav.Join`,
  proving the built-ins hold no private power). Input decoration is closed by
  `goav.WrapSource`; destination decoration was already closed by value
  passing.
- **The solver's conversion-class boundary.** External filters are selectable
  by the shape solver within the declared delta classes: sample-rate/channel
  (resample), width/height (resize), pixel/sample-format (convert), the
  vocabulary of `shape.Conversions`, by registering under the bundled factory
  names (`filter.FactoryResample`, ...; adapterproof's toy upsampler is the
  bundled resample slot). A new delta class (say colorspace or channel-layout as
  a distinct class) is core work: `shape.Conversions` and `shape.Policy`
  enumerate the classes, and externals cannot extend that enumeration.
- **Controls.** The typed verbs (`Keyframe`, `Seek`, `Rate`, `SetBitrate`,
  `SelectActive`, ...) are core vocabulary, but the control value is opaque:
  payload-bearing constructors validate immediately, and callers target copies
  with `.AtTap(...)` or `.At(...)`. In-process stages still use
  `Deliver(event).AtTap(name)` to receive arbitrary custom events. Untargeted
  controls only infer a destination when exactly one valid target exists;
  otherwise callers use `.AtTap(...)`, `.At(...)`, `source=...`, or `node=...`.
  Hosts that need CLI access import `ctl`, add explicit `CommandSpec` rows for
  new verbs, and add `PipelineRegistry` rows for custom runtime branch
  components. There is no global registry and no arbitrary method invocation.

## C. Expert tier

Handle-based graph work, deliberately off the grammar:

- `expert.Graph(runtime)` -> `expert.GraphBuilder`/`GraphNode`/`GraphInlet`/
  `GraphOutlet`; `expert.ErrRuntimeRequired`. The package reaches the runtime
  through a structural bridge, and graph handles anchor runtime branches via
  their `Route` capability; the root imports nothing from `expert`.
- `Control.At(pipeline.NodeRef)`: node-targeted controls (grammar callers use
  `.AtTap`).
- `pipeline` graph machinery: `NewGraph`, `Graph`, `Spec`/`NodeSpec`/
  `EdgeSpec`/`Route`, `GraphStats`/`NodeStats`, node/route kind enums.
- Prebuilt graph components: `codec.DecoderStage`/`EncoderStage`,
  `format.DemuxSource`/`MuxStage`, `filter.Stage` (what the compiler itself
  assembles; usable directly under `expert.Graph`).
- `graphrender`: diagnostics over `pipeline.Spec` and live task snapshots:
  `RenderURI` renders any described graph as text, DOT, or Mermaid via a
  `goav:graph` URI; `RenderTaskFlowchart(task)` renders a running task-like
  value's current snapshot as a Mermaid flowchart, with runtime branch-owned
  nodes annotated by branch name/state; `RenderTaskURI` keeps the same URI
  format selection for live tasks. A leaf outside the core import graph,
  surface-pinned like the vocabulary packages.

## D. Leakage

No current root-package tier-D leakage is approved. The internal `intent` and
`operationSpec` records are unexported; external callers use `Explain`/`plan.Report`
for inspection, and tests reach normalization through test-only helpers.

Not leakage, by decision: builder funcs returning unexported types
(`Branch`, `Flow`, `Mix`, `Composite`, `Select`, stream/transform options):
the methods on those builders are the grammar; the types stay unexported so
the pinned package-level surface is the contract.

## Naming vocabulary

Near-miss names are deliberate distinctions, one line each (each verified
against the constructors in `input.go`/`provider.go`/`source.go`,
`destination.go`, `tap_ref.go`, `flow.go`/`branch.go`, `chain.go`,
`watch.go`, `task_control.go`, `expert/expert.go`):

- **Input vs Source vs provider.Source**: `FileInput`/`URIInput` are value
  inputs over media you already hold; `Source(name, shape, fn)` is the
  custom-push input where the application produces media through
  `source.Push`; `provider.Source` is the transport extension point
  (`OpenSource`) that `Input(p)` turns into a recipe input. Value inputs,
  custom push sources, and transport providers are three doors into one
  `InputSpec`. `InputSpec.Stream(av.Stream)` returns an `InputStream` attach
  anchor for app-owned dynamic tracks; it deliberately reuses
  `Branch(...).From(...)` and `Mutable.Attach` instead of adding a room/session
  workflow API.
- **Destination vs Sink vs Writer vs Write**: `Destination` is the routing
  handle every constructor returns; `Mux(name, destination)` is the explicit
  shared mux/sink group. `Write` wraps an `io.Writer` you already opened;
  `Writer` lets goav open the writer on demand with final `provider.Info`
  (and transactional commit/abort); `Sink` ends the branch in frames/packets
  instead of muxed bytes.
- **Tap vs FrameTap vs PacketTap**: `Tap` infers its domain from the chain
  point; `FrameTap`/`PacketTap` assert it, and a mismatch is a build error
  naming the typed constructor to use.
- **Flow vs Branch**: a `Flow` is a reusable operation list and owns no
  destination (`TestNorthStarFlowExposesNoDestinations`); a `Branch` routes
  fanout and owns its destinations.
- **Attach vs Detach vs Rebranch**: `Mutable.Attach` adds ordinary branch specs
  to a running task; `Mutable.Detach(ctx, h)` removes that attached branch, with
  `lifecycle.DrainBranch()` and `lifecycle.AbortBranch()` selecting whether branch destinations
  commit or abort; `Attachment.Rebranch` is attach-new-then-detach-old, with
  lifecycle boundary options (`NextFrame`, `NextKeyframe`, `AtMediaTime`) and
  old-branch outcome options.
- **Sync**: `flow.Sync(name, flow.SyncTolerance(...), flow.SyncDropLate())`
  returns a shared timeline policy. Reuse one `flow.SyncPolicy` across audio/video chains or
  branches; `.Sync(policy)` inserts a packet/frame gate that can hold early
  media or drop late media and reports sync drops through normal branch stats.
- **Shape vs Require vs Auto vs Prefer**: `Shape` states a fact about the
  current media point; `Require` asserts a contract that fails the build
  when unmet; `Auto` grants the solver permission to insert conversions;
  `Prefer` hints an open solver choice and never fails.
- **Copy**: `.Copy()` is the recipe spelling for packet-preserving passthrough.
  `codec.Copy()` is the internal `CodecSpec` value used by lowerers and helper
  code; user-facing recipes should write the verb.
- **Watch vs Events**: `Watch(filters...)` gives each consumer an independent
  filtered subscription with an explicit close handle. `Events()` is the
  unfiltered watch convenience. Each subscription sheds for itself only.
  Runtime branch lifecycle events
  (`av.EventBranchAttached`, `av.EventBranchDetached`) are published through
  `Watch`, with attachment id/name metadata and a detach disposition on
  detach.
- **Control vs Deliver**: `Control` verbs are typed intents (`Keyframe`,
  `SetBitrate`, `Seek`, `Rate`, `Segment`, `SelectActive`) built through
  constructors, not public struct fields;
  `Deliver(event)` is the escape hatch handing a verbatim event to a stage
  that interprets it itself.
- **recipe vs expert**: the recipe grammar (tier A) is the normal surface;
  `expert.Graph(runtime)` opens the handle-based graph layer (tier C) for
  compositions the grammar cannot express.

## Enforcement

Package discovery is dynamic: the pins walk the module (skipping hidden
directories, `testdata`, `internal`, nested modules, and test-only
directories) instead of trusting a hardcoded list, so a package added
tomorrow is governed the day it lands.

- `api_surface_pin_test.go` / `testdata/api_surface.txt`: exported
  package-level identifiers of root + `errcode`/`graphrender`/`lifecycle`/
  `plan`/`snapshot` must match the approved list exactly (both directions,
  sorted, no dups).
- `TestEveryPublicPackageIsGoverned`: every discovered package must be
  classified: frozen surface, doc-pin-governed extension point, or
  implementation subtree. An unclassified package fails the build.
- `doc_pin_test.go`: every exported symbol in every discovered public
  package carries a doc comment.
- `errors_pin_test.go`: every `BuildError` uses a catalog-derived
  `errcode.Family`, a catalog `errcode.Code`, and carries typed `Fields` or
  `Fixes`; legacy rendered details/fixes remain compatibility only.
- README front door: opening examples stay on the grammar
  (`TestReadmeFirstScreenAvoidsGraphInternals`); advanced knobs stay out of
  the guide (`TestReadmeKeepsAdvancedRuntimeKnobsOutOfFrontDoor`).
- Composability laws: `docs/COMPOSABILITY_LAWS.md` maps the front-door
  invariants to executable tests.

`adapters/*` and `container/*` are implementations behind the `codec`/`format`
extension points (registered by `bundle.MustNew`), outside the core import
graph and not part of the governed surface: an explicit, asserted exclusion
(`docPinImplementationSubtrees`), not a forgotten one.

## Module boundaries

The repository is split into a root module, transport modules, and runnable
example modules. The module boundary is the dependency boundary:

- **root package (`github.com/thesyncim/goav`)**: the grammar and extension
  seams. Importing this package does not import bundled adapter packages.
- **root module**: contains the root package, `goav/bundle`, bundled adapter
  packages, and pure-Go implementations. `goav/bundle` is a package, not a
  nested module; the root module still lists bundled backend requirements
  until/unless `goav/bundle` becomes a nested module. `TestRootModuleDependencyPurity`
  pins that module requirements stay inside `github.com/thesyncim/*` with no
  third-party requires and no `replace` directives.
- **`rtpav`, `webrtcav`**: nested modules carrying the Pion ecosystem. They
  require the root module (never the reverse), so importing goav pulls in no
  transport dependencies. Import paths are unchanged
  (`github.com/thesyncim/goav/rtpav`, `.../webrtcav`). Each runs its own doc
  pin (`doc_pin_test.go` per module) and its RTP/WebRTC-flavored integration
  tests (`rtpav/integration`, `webrtcav/integration`); the root module's
  dynamic package walk correctly stops at their `go.mod`.
- **runnable examples**: `examples/webrtc-runtime-ladder`,
  `examples/custom-source`, `examples/provider-source`,
  `examples/custom-destination`, `examples/custom-filter`,
  `examples/transactional-writer`, `examples/custom-codec`,
  `examples/custom-join`, `examples/dynamic-audio-room`, and
  `examples/control-plane-host` are nested modules. They are copyable
  adoption examples, free to carry their own dependencies, and CI builds/tests
  each module through the `examples/*/go.mod` loop. Root surface governance
  stops at their `go.mod`; each example owns its README and tests.

## Compatibility

Pre-v1, breaking renames land without aliases. The surface-hygiene wave moved
five clusters off the root: the live control vocabulary is `control`
(`Control`, `Keyframe`, `Rate`, `Seek`, `Segment`, `SetBitrate`,
`SelectActive`, `Deliver`, `Must`), event subscription and structural helpers
are `inspect` (`EventFilter`, `WatchTypes`, `WatchStream`, `Subscribe`,
`Snapshot`, `Stats`, `Render`), the error catalog is `errcode` (renamed from
`codes`), the source/destination extension contracts are `provider`
(`Source`, `Destination`, `Writer`, `TransactionalWriter`, `Contract`,
`Info`, `OpenFunc`), and the expert graph layer is `expert` (`Graph`,
`GraphBuilder`, handles, `ErrRuntimeRequired`) bridged structurally so the
root imports neither. New exported symbols in governed packages require a
matching `testdata/api_surface.txt` entry in the same change.
