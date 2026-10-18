# Operations

The checked reference for the front-door grammar. Use it when you know the
operation you want and need to know what shape it consumes and produces, what
the shape solver may insert, which errors to expect, and whether the same
spelling works in an Output attached to a running task. It also owns the
encoder settings model and the composition laws that keep all of this one
grammar.

```text
input.Video/Audio/Stream() -> operations -> taps -> forks (reused Streams) -> .To(destinations) -> rt.Run
```

Operations are cold-path declarations, not per-packet calls. `rt.Describe`,
`rt.Explain`, `rt.Build`, planned forks, `task.Attach`, and
`Attachment.Rebranch` lower them through the same shape validation and
registration checks before resources open or the live graph changes. Start
with the scope table, then jump to your operation's row; the "Runtime attach"
column says whether the spelling also works for a late attached Output.

## Scopes

| Scope | Owns source? | Owns destination? | Runtime attach? | Notes |
|---|---|---|---|---|
| Stream | Yes, through `input.Audio()/Video()/Stream(...)` or a join | Yes, `.To(...)` makes an `Output` | No, the Outputs passed to `rt.Run`/`rt.Build` build the base `WorkPlan` | Packet-domain inputs must call `.Decode()` before frame-consuming methods; a stream routed without `.Decode()` stays packet-domain and copies. |
| Fork | A reused Stream value; `.As(name)` names it | Yes, through `.To(...)` | Yes, an Output from `task.Tap(name)` or `task.Track(input, stream)` is the `task.Attach` input | A fork starts from its parent's shape; packet taps need `.Decode()` before frame operations. |
| Function | No | No | Yes, on any Stream | A reusable sequence is a plain `func(goav.Stream) goav.Stream`. It cannot open sources, destinations, runtimes, or lifecycle. |

## Operation Matrix

Each row describes the public spelling, not a private planner step. "Inserted
conversions" means work the planner may add before build or attach; "Primary
refusals" lists the error families a caller should expect to handle.

| Operation | Input shape | Output shape | Allowed domain | Inserted conversions | Primary refusals | Runtime attach |
|---|---|---|---|---|---|---|
| `.Decode(options...)` | Packet-domain audio/video. The packet codec comes from stream facts, input provider metadata, or decode options. | Frame-domain audio/video. | Packet only. Frame-domain custom sources are already decoded. | None. Decode opens a decoder adapter; it does not use the shape solver. | `source_shape_mismatch`, `stream_codec_missing`, `decode_adapter_missing`, `decode_adapter_unavailable`, `decode_adapter_incompatible`, `flow_decode_duplicate`, `branch_decode_duplicate`, `branch_decode_order_invalid`, `stream_step_after_encode`. | Valid from a packet tap or packet stream anchor. Refused from a frame tap because the branch is already frame-domain. |
| Copy (no `.Decode()`) | Packet-domain audio/video routed `.To(...)` without `.Decode()`; `input.To(...)` copies every stream. | Packet-domain audio/video with the same encoded payload. | Packet only. | None. Leaving out `.Decode()` is the recipe spelling for packet passthrough. | `source_shape_mismatch`, `operation_shape_mismatch`, `flow_copy_domain_mismatch`, `branch_decode_copy_invalid`, `copy_unsupported`, `copy_branch_source_invalid`, `stream_step_after_encode`. | Valid from a packet tap or `task.Track` stream. Use it for packet-preserving runtime recording or fanout. |
| `.Shape(spec)` | Any shape; it records facts about the current media point. | The same media point with the stated facts merged into shape validation. | Packet, frame, or event when the spec describes that domain. | None. It is a fact annotation, not a runtime node. | `stream_step_after_encode`, `source_shape_invalid`, `source_shape_unsupported`, shape mismatch codes if later operations contradict the stated facts. | Applies to the branch operation list before attach mutation. It is validated cold-path and lowers to no runtime node. |
| `.Auto(policies...)` | Any shape. | No immediate shape change. | Packet or frame, but it only matters when a later operation has a shape delta the solver can satisfy. | Allows solver-inserted transforms such as resample, resize, and convert classes covered by `shape.Policy`. Insertions appear in `Describe` and `Explain`. | `shape_conversion_refused`, `shape_adapter_missing`, `shape_adapter_ambiguous`, `shape_requirement_unmet` when no active policy covers a needed conversion. | Same as build: conversions are planned before graph mutation; failed attach rolls back. |
| `.Require(spec)` | Current shape must already satisfy `spec`, or be convertible under an active `.Auto(...)` policy. | Same shape after any allowed inserted conversion. | Packet or frame according to `spec`. | May trigger a solver insertion when `.Auto(...)` allows the conversion needed to satisfy the requirement. | `shape_requirement_unmet`, `shape_conversion_refused`, `shape_adapter_missing`, `shape_adapter_ambiguous`, `operation_shape_mismatch`. | Checked during attach planning. It lowers to no runtime node after any required conversion is inserted. |
| `.Prefer(spec)` | Any shape. | No immediate shape change. | Packet or frame according to the preferred facts. | Can bias solver target facts or adapter choice when an open choice exists. | Preferences do not fail builds. Unused preferences appear as `shape_preference_ignored` diagnostics. | Same diagnostics and soft behavior during attach planning. |
| `.Resize(width, height, options...)` | Video frames. Packet-domain streams must call `.Decode()` before resize. | Video frames with the requested geometry (stretched by default; `filter.Fit` keeps the aspect ratio inside it, `filter.Fill` crops to cover it) and optional pixel-format facts. | Frame-domain video. | Explicit resize is the declared transform. `.Auto(shape.AllowResize())` can insert resize before a later operation that needs different geometry. | `transform_invalid`, `transform_media_mismatch`, `transform_adapter_missing`, `transform_adapter_incompatible`, `operation_shape_mismatch`, `packet_branch_transform_unsupported`, `stream_step_after_encode`. | Valid from a frame tap or after `.Decode()` from a packet tap. Requires a registered resize filter before mutation. |
| `.Resample(sampleRate, channels, options...)` | Audio frames. Packet-domain streams must call `.Decode()` before resample. | Audio frames with the requested sample rate, channel count, and optional sample format. | Frame-domain audio. | Explicit resample is the declared transform. `.Auto(shape.AllowResample())` can insert resample before a later operation such as an encoder requirement. | `transform_invalid`, `transform_media_mismatch`, `transform_adapter_missing`, `transform_adapter_incompatible`, `operation_shape_mismatch`, `packet_branch_transform_unsupported`, `stream_step_after_encode`. | Valid from a frame tap or after `.Decode()` from a packet tap. Requires a registered resample filter before mutation. |
| `.Do(stage...)` | Whatever each custom stage contract declares. `FrameFunc` consumes frames, `PacketFunc` consumes packets, and `EventFunc` consumes events. Packet-domain streams must call `.Decode()` before frame stages. | Whatever the stage emits through `Emit`. | Stage-defined. | Stage contracts can participate in shape solving when they expose shape facts. | `stage_missing`, `operation_shape_mismatch`, `stream_step_after_encode`, stage-open/runtime errors wrapped at the task boundary. | Valid when the branch anchor satisfies the stage contract. Custom diagnostic Outputs are usually `task.Tap(name).Do(...).To(goav.Sink(...))`. |
| `.Tap(name)` | Current media point; the tap carries frames or packets as its position does. | Same shape; the tap names this point for later branches, attach, controls, and diagnostics. | Packet or frame. | None. | `tap_invalid`, `tap_domain_mismatch`, runtime tap lookup errors such as `runtime_branch_tap_missing` or `runtime_branch_tap_duplicate`. | Taps are the normal runtime attachment anchors. A packet tap supports packet copy or decode; a frame tap supports transforms, stages, sinks, and encode. |
| `.Encode(codecSpec)` | Frame-domain audio/video for real codecs. `codec.Copy()` is packet-domain passthrough, normally written by leaving out `.Decode()`. Packet-domain streams must call `.Decode()` before real encode. | Packet-domain audio/video with codec facts from the encoder. | Frame for real codecs, packet for `codec.Copy()`. | `.Auto(...)` can insert needed frame conversions before opening the encoder. Encode itself opens a codec adapter. | `encode_missing`, `encode_duplicate`, `encode_parameter_invalid`, `encode_auto_unresolved`, `encode_adapter_missing`, `encode_adapter_unavailable`, `encode_adapter_incompatible`, `packet_branch_encode_unsupported`, `stream_step_after_encode`. | Valid from a frame tap, or after `.Decode()` from a packet tap. Encoder open and shape checks happen before live graph mutation. |
| Reuse (fork) | The reused Stream's point. Every Output built from it forks there; the work before the fork runs once. | No single output shape; each fork owns its operations and destinations. | Packet or frame depending on parent and fork operations. | Each fork has its own `.Auto(...)` policy and insertions. Fork-local buffer policy cannot corrupt siblings. | `branch_missing`, `output_scope_mixed`, `branch_duplicate`, `branch_tap_missing`, `branch_destination_invalid`, `destination_mux_incompatible`, `decode_config_conflict`. | Planned forks are build-time only. Runtime equivalents attach Outputs from `task.Tap(name)` with the same operations. |
| `.To(destinations...)` | Final stream shape. Mux/file/URI/writer destinations consume packets; sink destinations can consume frames, packets, or events depending on the sink. | An `Output`: terminal delivery. | Packet for muxed byte destinations; frame/packet/event for sinks. | None. Use `.Decode().To(goav.Sink(...))` for decoded frames, or `.To(goav.Sink(...))` without `.Decode()` for packet sinks. | `output_missing`, `destination_missing`, `destination_invalid`, `destination_duplicate`, `destination_format_unknown`, `destination_muxer_missing`, `destination_mux_incompatible`, `destination_shape_mismatch`, `operation_shape_mismatch`, `encode_missing`. | Runtime attach opens destinations before graph mutation. Outputs given the same destination value share one mux/sink output; same-name copies are rejected; attach failure rolls back opens and graph changes. |

## Encoder settings

`.Encode(spec)` takes a `codec.Spec` built by `codec.Opus`, `VP8`, `VP9`,
`H264`, `AV1`, or `Codec(id, media, ...)`, each of type
`func(...codec.Option) codec.Spec`. Options and constructors live in `codec`,
not the root (there is no `goav.Bitrate`), and come in two tiers:

- **Common typed settings.** `codec.Bitrate`, `FPS`, `KeyframeInterval`,
  `Profile`, `Level`, and the audio output overrides `Channels`, `SampleRate`,
  and `ClockRate` fill `codec.CodecSettings` (`Spec.Settings`);
  `codec.Setting(key, value)` carries adapter-owned string knobs. The pipeline
  language's `encode` binds these and passes any other key through as a
  `codec.Setting`; `goav elements` prints the option list, so do not copy it
  into docs.
- **`codec.Control(func(e *gopus.Encoder) error { ... })`.** The adapter calls it
  at open with its concrete native encoder, decoder, or construction config,
  typed as the callback asks: anything the backend exposes is one call away,
  and a callback for another type, or an error from it, fails the open. Each
  adapter documents the type it passes.

Structure is shape, not settings: `shape.Spec` owns codec, resolution,
pixel/sample format, rate, and channels. Constructors set structural defaults
(`codec.Opus()` is 48 kHz stereo), and the audio overrides above are the only
settings that change them. The adapter side of this contract is in
[`ADAPTER_AUTHORING.md`](ADAPTER_AUTHORING.md).

## Fork And Attach Methods

| Operation | Input shape | Output shape | Allowed domain | Inserted conversions | Primary refusals | Runtime attach |
|---|---|---|---|---|---|---|
| `task.Tap(name)`, `task.Track(input, stream)` | A declared tap, or a stream one of the task's inputs produces or will produce. | Sets the attached Output's start point. | Packet, frame, or event depending on the anchor. | None. | `stream_invalid`, `branch_source_invalid`, `runtime_branch_anchor_missing`, `runtime_branch_tap_missing`, `runtime_branch_invalid`. | This is the normal attach anchor. The runtime validates the anchor and tap/source-stream domain before mutation. |
| `input.Stream(goav.StreamID(id))` | Packet or frame stream selected by stream id (or `StreamIndex`). | Same media point, narrowed to one stream. | Packet or frame. | None. | Ambiguous-selection and stream-mismatch errors when no single stream matches. | `task.Track(input, stream)` narrows an attached Output to one source stream the same way. |
| `.As(name)` | Any stream. | Same media point, named from here on. | Packet or frame. | None. | None; repeated names get a numeric suffix. | Names the attached branch and the encoded stream id live controls address (`control.SetBitrate("hd", ...)`). |
| `.Buffer(policy)` | The fork's input shape. | Same shape; the rest of the fork runs behind its own `flow.BranchBuffer` queue. | Packet, frame, or event. | None. | `branch_buffer_invalid`, `branch_buffer_unsupported`, `buffer_payload_unsafe`, `buffer_payload_too_large`. | Buffer policy is fork-local. Blocking, dropping, and latest policies affect only that fork or attached Output. |

## Join Constructors

> **Governed pre-v1.** `Mix`, `Composite`, `Select`, and custom `Join` are
> implemented, tested, and governed, but they are not part of the v1 front-door
> promise. See [`COMPATIBILITY.md`](COMPATIBILITY.md#governed-pre-v1). The same applies to runtime
> `Attach`/`Detach`/`Rebranch` referenced in the "Runtime attach" column — that
> column documents behaviour available through `rt.Build` and its `LiveTask`,
> which is advanced surface beyond the `rt.Run` front door.

Joins are the N-to-1 side of the same grammar: each constructor takes two or
more Streams, lowers to one planned convergence node, and returns a Stream.
Arms are intentionally narrower than input streams: an arm supports stream
selection, `.Decode()`, and `.Tap(...)`, and `goav.At(stream, x, y)` places it
when it feeds `Composite`; transforms and encode belong after the join. A
join's output runs alone in one run today: other Outputs beside it are refused
with `stream_beside_join`.

| Constructor | Arm input shape | Joined output shape | Allowed domain | Inserted conversions | Primary refusals | Runtime attach |
|---|---|---|---|---|---|---|
| `Mix(streams...)` | Audio streams. Packet-domain arms must call `.Decode()` before the mix; frame arms must carry compatible audio facts. | One audio frame stream named from the planned mix node. `.Encode(...)` can turn it into packets for muxed destinations. | Audio frame output. | Arm decode is explicit. Later arms resample/convert to the first arm's audio format through the join arm policy. Output `.Auto(...)` can insert conversions before encode or forks. | `mix_inputs`, `mix_arm`, `mix_destination`, `mix_tap_arm`, decoder/transform/encoder adapter codes. | The mixed output can be tapped and used as a runtime attach anchor: `task.Tap(name)` Outputs use the ordinary operations; the join itself is built with the base task. |
| `Composite(layers...)` | Video streams. Packet-domain arms must call `.Decode()` before the composite; `goav.At(stream, x, y)` on arms or nested composites sets canvas placement. | One video frame stream whose geometry is the bounding box of all layers. `.Encode(...)` can turn it into packets for muxed destinations. | Video frame output. | Arm decode is explicit. Arm layout is planned from layer positions; output `.Auto(...)` can insert resize/convert work before encode or forks. | `composite_inputs`, `composite_arm`, `composite_destination`, `composite_tap_arm`, decoder/transform/encoder adapter codes. | The composited output can be tapped and used as a runtime attach anchor. Attached Outputs start after the joined frame point, not inside the arm set. |
| `Select(streams...)` | Frame or packet arms with compatible single-stream facts. Unlike Mix/Composite, Select forwards the active arm as-is. | One passthrough stream whose facts come from the first arm and whose active arm can switch with `SelectActive`. | Frame or packet, following the arms. | None; Select does not decode, encode, resample, or resize arms. | `select_inputs`, `select_arm`, `select_destination`, `select_tap_arm`. | The selected output can be tapped for attached Outputs. `SelectActive(id)` is a task control, not a stream operation, and rides the data path to the selector. |
| `Join(name, stage, streams...)` | Stage-defined. A stage with frame-domain `InputShapes` requires packet-domain arms to call `.Decode()` first; media-agnostic stages are passthrough. | Stage-defined through `OutputShapes`, or the first arm's facts under `name` when no output contract is declared. | Stage-defined. | Contract-derived arm conversions use the same shape solver and implicit arm policy as built-ins. | `join_name_invalid`, `join_stage_invalid`, custom `<name>_inputs`, `<name>_arm`, and destination refusals derived from the custom profile. | Custom joins have no private power: their tapped joined output is the same runtime-attach anchor shape that built-ins expose. |

## Joined Stream Operations

After `Mix`, `Composite`, `Select`, or custom `Join`, the returned stream point
accepts the same terminal grammar where the constructor supports it:

| Operation | Applies to | Notes |
|---|---|---|
| `.Tap(name)` | All joins. | Names the joined output as a stable attach/control/diagnostic point. |
| Reuse, other operations | All joins. | Reusing the joined Stream, or an operation not listed here (`.Resize`, `.Do`, `.Buffer`, ...), runs in a planned fork of the joined output. |
| `.To(destinations...)` | All joins. | `Mix` and `Composite` can deliver raw frames to sinks or encoded packets to muxers when `.Encode(...)` is present. `Select` and custom `Join` are sink-first. |
| `.Encode(codecSpec)` | `Mix` and `Composite`. | Terminal encode for the joined frame stream. Forking after the join's own encode is refused because each fork owns its terminal delivery. |
| `.Auto(...)`, `.Require(...)`, `.Prefer(...)` | `Mix` and `Composite`. | Shape-solver hints on the joined output before encode or planned forks. |
| `.SyncByPTS()` | `Mix` and `Composite` output only. | Aligns arms by presentation timestamp instead of arrival order; elsewhere it is refused with `stream_invalid`. |
| `goav.At(stream, x, y)` | Input streams, nested `Composite` and `Select` joins used as `Composite` layers. | Places the layer's top-left corner on the outer composite canvas. |

## Explain And Describe

`Describe` returns the planned graph shape after operation lowering. `Explain`
uses the same planner and adds diagnostics for decisions that did not fail the
build:

- `shape_conversion_inserted`: an `.Auto(...)` policy allowed a real transform
  insertion.
- `shape_preference_applied`: a `.Prefer(...)` value affected target facts or
  adapter choice.
- `shape_preference_ignored`: a preference could not apply, but the build
  still succeeded.
- `packet_copy`, `frame_source`, `decode_required`, `encode_required`, and
  `stream_rule`: planner decisions about domain and dynamic stream handling.
- `buffered_execution`: the build runs the graph buffered although the runtime's
  own policy is direct (live inputs, a join, a declared `.Buffer`, or an
  encode ladder), so forks and arms run in parallel.

Every refusal that happens before resources open should be a `*goav.BuildError`
with a stable family, detailed code, operation, node, reason, machine-readable
details through `Detail[T](key)`, rendered detail/fix lines, and a sentinel cause
when one exists. The checked catalog is
[`docs/ERROR_CATALOG.md`](ERROR_CATALOG.md).

## Composition laws

These laws keep goav one grammar instead of several workflow APIs: a change
that breaks one adds a workflow surface rather than extending the grammar.
The tests are the executable evidence; this table is the reviewer's map.

| Law | Evidence |
|---|---|
| Reusable work is a Go function over a Stream: it owns ordered operations only; sources, destinations, runtime, lifecycle, and fork ownership stay outside it. | `TestChainHelperComposes` |
| Build-time forks and Outputs added with `task.Attach` use the same operations and lowering. | `TestValueAttachFromTap`, `TestTaskAttachRuntimeFlowCopyBranchFromPacketTap`, `TestTaskAttachRuntimeDecodeResampleEncodeMuxBranchFromPacketTap`, `TestTaskAttachRuntimeFlowCustomEncodeMuxBranch` |
| `rt.Describe` is the graph shape that `rt.Build` installs. | `TestStreamRecipeDescribeMatchesBuiltGraph`, `TestBranchCompositionRecipeDescribeMatchesBuiltGraph`, `TestJoinDescribeEqualsBuildMix`, `TestJoinDescribeEqualsBuildNestedMix` |
| A deterministic generated corpus holds the structural laws: Describe equals the built graph, routing one Stream to two Outputs equals one Output with both destinations, `Attach` installs the same subgraph as a build-time fork, and nested mixes match their flat form where no per-stage clamp occurs. | `TestCompositionLawsHoldForGeneratedRecipes` |
| `Explain` reports solver insertions and soft preferences without opening resources. | `TestAutoInsertsFormatConvertThroughRegisteredAdapter`, `TestRequireSatisfiedByAutoConversion`, `TestPreferUnsatisfiableIgnoredWithDiagnostic` |
| Destination identity is the handle: one value reused from several Outputs feeds one mux or sink; separately constructed same-name handles refuse. | `TestValueGrammarMuxesStreamsIntoOneFile`, `TestForkGraphs`, `TestSameHandleSharesOneMux`, `TestWithKeepsHandleIdentity`, `TestDistinctHandlesWithOneNameAreRefused`, `TestFromMultiInputPlanDedupesMuxDestination`, `TestTaskAttachRuntimeBranchGroupSharesOneSinkHandle`, `TestTaskAttachRuntimeBranchGroupSharesMuxDestination` |
| Fork-local policy and mutation cannot corrupt siblings. | `TestCopyContractMutableFanoutBranchCannotCorruptSibling`, `TestFromMultiInputChainsKeepIndependentAutoPolicies`, `TestBranchBufferIsNormalBranchAPI` |
| Runtime attach failures roll back atomically and close prepared components. | `TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure`, `TestTaskAttachRollsBackRuntimeFilterWhenGraphConnectFails`, `TestTaskAttachRollsBackRuntimeTerminalStageWhenGraphConnectFails` |
| External adapters and joins have no private power unavailable to built-ins. | `TestExternalAdaptersComposeThroughPublicGrammar`, `TestExternalJoinComposesThroughPublicGrammar`; every `examples/*` module builds and tests on each commit (ci.yml "Examples modules") |
| A join arm `.Tap(name)` is an ordinary stream point: one decode feeds both the join and the tap. | `TestMixChainArmTapDecodesOnceAndMixes` |
| Work before a fork runs once, joins plan beside other streams and other joins, and a join arm reads a stream other work reads. | `TestCompilerOracle`, `TestForkGraphs`, `TestStreamBesideJoinRuns` |
| A fork's frame transform after the parent's `.Decode()` lowers like the equivalent unforked decoded stream. | `TestBranchAutoInsertMatchesDirectChain` |
| Nested select routing is explicit: `control.SelectActive(...).AtTap(name)` targets a tap declared at the inner select output; untargeted nested select controls refuse as ambiguous. | `TestNestedSelectRoutingPinned` |
| Taps owned by a detached branch disappear with it; later attaches use a surviving `task.Taps()` entry or reattach the publishing branch. | `TestAttachAfterSiblingDetachRefusalNamesFix` |
| Join outputs are ordinary Streams and fan out by reuse into independent forks. | `TestJoinOutputBranches`, `TestMixBranchesFanOutMixedStream`, `TestMixBranchesEncodeAndMonitorIndependently` |

Propose a front-door feature by adding its law here before adding a public
symbol. If it cannot be stated as input shape, ordered operation, tap, fork,
destination, task, or runtime attachment behavior, it belongs outside the
public grammar.

## Metrics

`metrics.Handler(task)` is an `http.Handler` serving the task's `Stats()` and
`Snapshot()` in Prometheus text format: `goav_node_{in,out}_{messages,packets,frames}_total{node}`,
`goav_node_dropped_total{node,reason}`, graph totals, and one-hot
`goav_task_state`, `goav_branch_state`, `goav_destination_state` gauges.
`metrics.Publish(name, task)` exposes the same data through `expvar`. Scrapes
are lock-free reads and cost the data plane nothing.
