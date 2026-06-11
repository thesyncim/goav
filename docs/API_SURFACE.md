# API surface

The public surface is a governed contract: every exported identifier belongs to
exactly one tier, the root and vocabulary surfaces are pinned against silent
growth (`api_surface_pin_test.go` + `testdata/api_surface.txt`), and every
exported symbol must be documented (`doc_pin_test.go`). Adding an exported
symbol is a reviewed act, not a side effect of layout.

Tiers: **A** — application grammar (the first-learn API), **B** — extension
seams (implement these to plug things in), **C** — expert (graph handles),
**D** — leakage (should not be public; tracked below).

## A. The grammar (one screen)

```
goav.From(input)                          inputs: FileInput, URIInput, Input(provider), Source(fn)
  .Audio() / .Video() / .Stream()         select a stream (InputName/StreamID/StreamIndex/StreamName)
  .Decode() .Copy() .Resize() .Resample() operations are chain methods
  .Do(stage) .Shape() .Auto() .Require() .Prefer()
  .Encode(codec.VP9(codec.Bitrate(...)))  codec specs from the codec package
  .Tap(goav.Tap|FrameTap|PacketTap)       named attach points
  .Branches(goav.Branch("x")...To(dst))   fan out; BranchSpec also drives Task.Attach
  .To(File|URI|Writer|Custom|Sink)        destinations; reuse one value = mux/sink group
  .OnStream(MatchMedia|MatchCodec|...)    dynamic-stream rules
goav.Mix(arms) / Composite(arms) / Select(arms)   N arms -> one stream (JoinArm)
goav.Flow("name")                         reusable operation list (Chain)
job.Describe() / Explain() -> plan.Report; job.Build(ctx) -> Task; job.Run(ctx)
Task: Run, Events, Watch(EventFilter), Snapshot -> snapshot.*, Stats,
      Attach/Detach/Rebranch (SwitchAt, Drain/AbortOldBranch, KeepOldOnFailure),
      Control(Keyframe|Seek|Segment|Rate|SetBitrate|SelectActive|Deliver, .AtTap)
goav.Default(opts...) / goav.New(opts...) -> Runtime; job.UseRuntime(rt)
errors: *goav.BuildError{Code: codes.X, ...} matched with errors.As/Is
```

Vocabulary read by applications, all tier A:

- `codes` — the error-code catalog (one `Code` per refusal class).
- `plan` — everything `Explain` reports back.
- `snapshot` — point-in-time task/branch/destination/tap views.
- `lifecycle` — task/branch/destination states.
- `shape` — shape specs and `.Auto` policies (`AllowResample`, ...).
- `flow` — branch buffer policies (`Blocking`, `DropOldest`, ...) and the
  `DropReason*` keys for reading drop counters.
- `av` identifiers — media/codec/format/protocol ids, event types, metadata.

## B. Extension seams (by domain)

Everything pluggable goes through an exported interface plus a value-typed
`With*` option on a per-runtime registry — external implementations are
first-class. See docs/ADAPTERS.md and docs/COMPONENTS.md.

- **Sources** — `goav.SourceProvider` (`OpenSource`), `goav.Source(fn)` with
  `SourceFunc`/`SourcePush`/`PushResult`; transports build on it:
  `rtpav.Receive` (PacketReader, Depacketizer, JitterBuffer, FeedbackWriter,
  PayloadMap seams), `webrtcav.Track`/`Session` (TrackReader, TrackAdapter).
- **Destinations** — `goav.DestinationProvider` + `DestinationContract`/
  `DestinationInfo`, `goav.Writer` (`WriterOpenFunc`), transactional uploads
  via `TransactionalDestinationWriter`, frame/packet sinks via `goav.Sink` +
  `SinkFunc`.
- **Custom stages** — `EventFunc`/`FrameFunc`/`PacketFunc` (+`Emit`) for
  `.Do(...)`; the node contracts live in `pipeline` (Source/Stage/Sink,
  Emitter, Message, Scratch, capability interfaces).
- **Codecs** — `codec` Descriptor/Decoder/Encoder/factories, caller-owned
  results, `WithDecoder`/`WithEncoder`/`WithCodecAdapter`/`WithCodecDescriptor`.
- **Containers** — `format` Prober/Demuxer/Muxer/factories, Seeker for
  seekable inputs, `WithDemuxer`/`WithMuxer`/`WithFormatAdapter`/`WithProber`.
- **Filters** — `filter` FrameFilter/Factory/Descriptor,
  `WithFilter`/`WithFilterAdapter`.
- **Runtime config** — `WithDefaults`/`WithStd*`, `WithClock`, `WithRealtime`,
  `WithBufferPolicy`, `WithEventCapacity`.
- **Media vocabulary** — `av` frames/packets/buffers (`Buffer`,
  `BufferOwnership`, `Plane`), timing (`TimeBase`, `Timestamp`, `Duration`,
  rescaling), `Clock`.
- **Testing** — `goavtest`: deterministic sources (`Audio`, `Video`,
  `Packets`, `LiveAudio`), `Collector`, `Clock`, passthrough `Codec`/`Format`,
  one-liner `Runtime()`. Helpers return real grammar values.

## C. Expert tier

Handle-based graph work, deliberately off the grammar:

- `goav.Expert(runtime).Graph()` → `GraphBuilder`/`GraphNode`/`GraphInlet`/
  `GraphOutlet`; `ErrExpertRuntimeRequired`.
- `Control.At(pipeline.NodeRef)` — node-targeted controls (grammar callers use
  `.AtTap`).
- `pipeline` graph machinery — `NewGraph`, `Graph`, `Spec`/`NodeSpec`/
  `EdgeSpec`/`Route`, `GraphStats`/`NodeStats`, node/route kind enums.
- Prebuilt graph components — `codec.DecoderStage`/`EncoderStage`,
  `format.DemuxSource`/`MuxStage`, `filter.Stage` (what the compiler itself
  assembles; usable directly under `Expert`).

## D. Leakage

Open items; the surface pin freezes them so the list only shrinks:

- `goav.Intent` — the normalized pre-compilation projection. No exported
  producer exists (`Job.Plan` is test-only via `export_test.go`) and its
  fields are unexported types. Migration: unexport once the recipe tests
  assert normalization through `plan.Report`/test-only aliases, or export a
  producer if it should be a real read surface. Needs a design call, not
  mechanical — deferred.
- `goav.OperationSpec` — the normalized chain operation; reachable only as a
  field type inside `Intent`'s unexported stream entries. Same migration and
  deferral as `Intent`.

Not leakage, by decision: builder funcs returning unexported types
(`Branch`, `Flow`, `Mix`, `Composite`, `Select`, stream/transform options) —
the methods on those builders are the grammar; the types stay unexported so
the pinned package-level surface is the contract.

## Naming vocabulary

Near-miss names are deliberate distinctions, one line each (each verified
against the constructors in `input.go`/`provider.go`/`source.go`,
`destination.go`, `tap_ref.go`, `flow.go`/`branch.go`, `chain.go`,
`watch.go`, `task_control.go`, `graph.go`):

- **Input vs Source vs SourceProvider** — `FileInput`/`URIInput` are value
  inputs over media you already hold; `Source(name, shape, fn)` is the
  custom-push input where the application produces media through
  `SourcePush`; `SourceProvider` is the transport seam (`OpenSource`) that
  `Input(provider)` turns into a recipe input — value, custom push, and
  transport are three doors into one `InputSpec`.
- **Destination vs Sink vs Writer vs File** — `Destination` is the routing
  handle every constructor returns (reuse = mux/sink group); `File` wraps an
  `io.Writer` you already opened; `Writer` lets goav open the writer on
  demand with final `DestinationInfo` (and transactional commit/abort);
  `Sink` ends the branch in frames/packets instead of muxed bytes.
- **Tap vs FrameTap vs PacketTap** — `Tap` infers its domain from the chain
  point; `FrameTap`/`PacketTap` assert it, and a mismatch is a build error
  naming the typed constructor to use.
- **Flow vs Branch** — a `Flow` is a reusable operation list and owns no
  destination (`TestNorthStarFlowExposesNoDestinations`); a `Branch` routes
  fanout and owns its destinations.
- **Shape vs Require vs Auto vs Prefer** — `Shape` states a fact about the
  current media point; `Require` asserts a contract that fails the build
  when unmet; `Auto` grants the solver permission to insert conversions;
  `Prefer` hints an open solver choice and never fails.
- **Copy vs Encode(codec.Copy())** — `.Copy()` is the grammar verb for
  packet-preserving passthrough; `codec.Copy()` is the `CodecSpec` value it
  lowers to (branch and flow `.Copy()` literally delegate to
  `.Encode(codec.Copy())`). Same operation, two spellings: write the verb;
  the spec value exists for code that builds `CodecSpec` values
  programmatically.
- **Events vs Watch** — `Events()` is the single raw firehose channel;
  `Watch(filters...)` gives each consumer an independent filtered
  subscription that sheds for itself only.
- **Control vs Deliver** — `Control` verbs are typed intents (`Keyframe`,
  `SetBitrate`, `Seek`, `Rate`, `Segment`, `SelectActive`);
  `Deliver(event)` is the escape hatch handing a verbatim event to a stage
  that interprets it itself.
- **recipe vs Expert** — the recipe grammar (tier A) is the normal surface;
  `Expert(runtime)` opens the handle-based graph layer (tier C) for
  compositions the grammar cannot express.

## Enforcement

- `api_surface_pin_test.go` / `testdata/api_surface.txt` — exported
  package-level identifiers of root + `codes`/`lifecycle`/`plan`/`snapshot`
  must match the approved list exactly (both directions, sorted, no dups).
- `doc_pin_test.go` — every exported symbol in every public package carries a
  doc comment.
- `errors_pin_test.go` — every `BuildError` uses a catalog `codes.Code`.
- README front door — first five examples stay on the grammar
  (`TestReadmeFirstScreenAvoidsGraphInternals`); advanced knobs stay out of
  the guide (`TestReadmeKeepsAdvancedRuntimeKnobsOutOfFrontDoor`).

`adapters/*` and `container/*` are implementations behind the `codec`/`format`
seams (registered by `Default()`/`WithStd*`), outside the core import graph
and not part of the governed surface.

## Compatibility

This audit changed no exported symbol; it added the approved-list pin, this
contract document, and the README first-screen guard. New exported symbols in
governed packages now require a matching `testdata/api_surface.txt` entry in
the same change.
