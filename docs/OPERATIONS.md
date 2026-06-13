# Operations

This is the checked reference for the front-door operation grammar. It
describes what each chain method consumes, produces, can ask the shape solver
to insert, and how the same operation behaves when it appears inside a runtime
branch attached with `Task.Attach`.

The invariant is the same one used by the planner:

```text
From(inputs...) -> stream selection -> operations -> taps -> branches -> destinations -> task
```

Operation records are cold-path declarations. `Build`, `Explain`, `Describe`,
planned `Branches`, `Task.Attach`, and `Attachment.Rebranch` all lower those
records through the same shape validation and runtime registration checks
before resources are opened or the live graph is mutated.

## Scopes

| Scope | Owns source? | Owns destination? | Runtime attach? | Notes |
|---|---|---|---|---|
| Stream chain | Yes, through `From(...).Audio()/Video()/Stream(...)` | Yes, through `.To(...)` or `.Branches(...)` | No, it builds the base `WorkPlan` | Frame-consuming methods on stream chains can make decode implicit when the input is packet-domain. |
| Branch | Optional anchor through `.From(tapOrExpertHandle)` | Yes, through `.To(...)` | Yes, `BranchSpec` is the `Task.Attach` input | A branch starts from its anchor shape; packet taps need `.Decode()` before frame operations. |
| Flow | No | No | Yes, after `.Apply(flow)` on a stream or branch | A flow is only reusable operations. It cannot open sources, destinations, runtimes, or lifecycle. |

## Operation Matrix

| Operation | Input shape | Output shape | Allowed domain | Inserted conversions | Primary refusals | Runtime attach |
|---|---|---|---|---|---|---|
| `.Apply(flow)` | The current shape must satisfy the flow's first operation. Flow media must match the chain media when declared. | The flow's final shape. | Whatever the applied flow accepts. | The applied flow can carry `.Auto(...)`, `.Require(...)`, `.Prefer(...)`, explicit transforms, and encoders. | `flow_invalid`, `flow_media_mismatch`, `flow_decode_duplicate`, `flow_decode_order_invalid`, `flow_copy_domain_mismatch`, `stream_step_after_encode`. | Attach sees the expanded operation list exactly as if each flow operation were written inline. |
| `.Decode(options...)` | Packet-domain audio/video. The packet codec comes from stream facts, input provider metadata, or decode options. | Frame-domain audio/video. | Packet only. Frame-domain custom sources are already decoded. | None. Decode opens a decoder adapter; it does not use the shape solver. | `source_shape_mismatch`, `stream_codec_missing`, `decode_adapter_missing`, `decode_adapter_unavailable`, `decode_adapter_incompatible`, `flow_decode_duplicate`, `branch_decode_duplicate`, `branch_decode_order_invalid`, `stream_step_after_encode`. | Valid from a packet tap or packet stream anchor. Refused from a frame tap because the branch is already frame-domain. |
| `.Copy()` | Packet-domain audio/video. | Packet-domain audio/video with the same encoded payload. | Packet only. | None. `.Copy()` lowers to `Encode(codec.Copy())`. | `source_shape_mismatch`, `operation_shape_mismatch`, `flow_copy_domain_mismatch`, `branch_decode_copy_invalid`, `copy_unsupported`, `copy_branch_source_invalid`, `stream_step_after_encode`. | Valid from a packet tap or packet stream anchor. Use it for packet-preserving runtime recording or fanout. |
| `.Shape(spec)` | Any shape; it records facts about the current media point. | The same media point with the stated facts merged into shape validation. | Packet, frame, or event when the spec describes that domain. | None. It is a fact annotation, not a runtime node. | `stream_step_after_encode`, `source_shape_invalid`, `source_shape_unsupported`, shape mismatch codes if later operations contradict the stated facts. | Applies to the branch operation list before attach mutation. It is validated cold-path and lowers to no runtime node. |
| `.Auto(policies...)` | Any shape. | No immediate shape change. | Packet or frame, but it only matters when a later operation has a shape delta the solver can satisfy. | Allows solver-inserted transforms such as resample, resize, and convert classes covered by `shape.Policy`. Insertions appear in `Describe` and `Explain`. | `shape_conversion_refused`, `shape_adapter_missing`, `shape_adapter_ambiguous`, `shape_requirement_unmet` when no active policy covers a needed conversion. | Same as build: conversions are planned before graph mutation; failed attach rolls back. |
| `.Require(spec)` | Current shape must already satisfy `spec`, or be convertible under an active `.Auto(...)` policy. | Same shape after any allowed inserted conversion. | Packet or frame according to `spec`. | May trigger a solver insertion when `.Auto(...)` allows the conversion needed to satisfy the requirement. | `shape_requirement_unmet`, `shape_conversion_refused`, `shape_adapter_missing`, `shape_adapter_ambiguous`, `operation_shape_mismatch`. | Checked during attach planning. It lowers to no runtime node after any required conversion is inserted. |
| `.Prefer(spec)` | Any shape. | No immediate shape change. | Packet or frame according to the preferred facts. | Can bias solver target facts or adapter choice when an open choice exists. | Preferences do not fail builds. Unused preferences appear as `shape_preference_ignored` diagnostics. | Same diagnostics and soft behavior during attach planning. |
| `.Resize(width, height, options...)` | Video frames. Stream chains can insert decode before resize when selected packets need frames. | Video frames with the requested geometry and optional pixel-format facts. | Frame-domain video. | Explicit resize is the declared transform. `.Auto(shape.AllowResize())` can insert resize before a later operation that needs different geometry. | `transform_invalid`, `transform_media_mismatch`, `transform_adapter_missing`, `transform_adapter_incompatible`, `operation_shape_mismatch`, `packet_branch_transform_unsupported`, `stream_step_after_encode`. | Valid from a frame tap or after `.Decode()` from a packet tap. Requires a registered resize filter before mutation. |
| `.Resample(sampleRate, channels, options...)` | Audio frames. Stream chains can insert decode before resample when selected packets need frames. | Audio frames with the requested sample rate, channel count, and optional sample format. | Frame-domain audio. | Explicit resample is the declared transform. `.Auto(shape.AllowResample())` can insert resample before a later operation such as an encoder requirement. | `transform_invalid`, `transform_media_mismatch`, `transform_adapter_missing`, `transform_adapter_incompatible`, `operation_shape_mismatch`, `packet_branch_transform_unsupported`, `stream_step_after_encode`. | Valid from a frame tap or after `.Decode()` from a packet tap. Requires a registered resample filter before mutation. |
| `.Do(stage...)` | Whatever each custom stage contract declares. `FrameFunc` consumes frames, `PacketFunc` consumes packets, and `EventFunc` consumes events. Stream chains can insert decode before frame stages. | Whatever the stage emits through `Emit`. | Stage-defined. | Stage contracts can participate in shape solving when they expose shape facts. | `stage_missing`, `operation_shape_mismatch`, `stream_step_after_encode`, stage-open/runtime errors wrapped at the task boundary. | Valid when the branch anchor satisfies the stage contract. Custom diagnostic branches are usually `Branch(...).From(FrameTap(...)).Do(...).To(Sink(...))`. |
| `.Tap(tap)` | Current media point. Untyped `Tap` infers from context; `FrameTap` and `PacketTap` assert domain. | Same shape; the tap names this point for later branches, attach, controls, and diagnostics. | Packet or frame. | None. | `tap_invalid`, `tap_domain_mismatch`, runtime tap lookup errors such as `runtime_branch_tap_missing` or `runtime_branch_tap_duplicate`. | Taps are the normal runtime attachment anchors. A packet tap supports packet copy or decode; a frame tap supports transforms, stages, sinks, and encode. |
| `.Encode(codecSpec)` | Frame-domain audio/video for real codecs. `codec.Copy()` is packet-domain passthrough and is normally written as `.Copy()`. Stream chains can insert decode before real encode. | Packet-domain audio/video with codec facts from the encoder. | Frame for real codecs, packet for `codec.Copy()`. | `.Auto(...)` can insert needed frame conversions before opening the encoder. Encode itself opens a codec adapter. | `encode_missing`, `encode_duplicate`, `encode_parameter_invalid`, `encode_auto_unresolved`, `encode_work_in_progress`, `encode_adapter_missing`, `encode_adapter_unavailable`, `encode_adapter_incompatible`, `packet_branch_encode_unsupported`, `stream_step_after_encode`. | Valid from a frame tap, or after `.Decode()` from a packet tap. Encoder open and shape checks happen before live graph mutation. |
| `.Branches(branches...)` | The current stream point. Branches inherit the current point unless a branch uses `.From(tap)`. | No single output shape; each branch owns its operation list and destinations. | Packet or frame depending on parent and branch operations. | Each branch has its own `.Auto(...)` policy and insertions. Branch-local buffer policy cannot corrupt siblings. | `branch_missing`, `input_count_unsupported`, `output_scope_mixed`, `branch_duplicate`, `branch_tap_missing`, `branch_destination_invalid`, `destination_mux_incompatible`, `decode_config_conflict`, `decode_policy_conflict`. | Planned branches are build-time only. Runtime equivalents use `Task.Attach(ctx, Branch(...))` with the same branch grammar. |
| `.To(destinations...)` | Final chain or branch shape. Mux/file/URI/writer destinations consume packets; sink destinations can consume frames, packets, or events depending on the sink. | Terminal delivery. | Packet for muxed byte destinations; frame/packet/event for sinks. | None directly. A stream chain that ends in a sink without an encoder can make decode implicit for packet inputs. | `output_missing`, `destination_missing`, `destination_invalid`, `destination_duplicate`, `destination_format_unknown`, `destination_muxer_missing`, `destination_mux_incompatible`, `destination_shape_mismatch`, `encode_missing`. | Runtime branches open destinations before graph mutation. Reusing one destination value groups branches into one mux/sink output; attach failure rolls back opens and graph changes. |

## Branch-Only Methods

| Operation | Input shape | Output shape | Allowed domain | Inserted conversions | Primary refusals | Runtime attach |
|---|---|---|---|---|---|---|
| `Branch(name).From(anchor)` | The anchor is a `TapRef`, `input.Stream(av.Stream)`, or expert route handle. | Sets the branch start point. | Packet, frame, or event depending on the anchor. | None. | `branch_source_invalid`, `runtime_branch_anchor_missing`, `runtime_branch_tap_missing`, `runtime_branch_invalid`. | This is the normal attach anchor. The runtime validates the anchor and tap/source-stream domain before mutation. |
| `Branch(name).Stream(id)` | Packet or frame stream routed by stream id. | Same media point, narrowed by stream id. | Packet or frame. | None. | Stream/anchor mismatch errors when no matching route exists. | Useful when an expert/source anchor exposes several streams and a runtime branch should read only one. |
| `Branch(name).Event(type)` | Event route from the anchor. | Event-domain messages of the selected type. | Event. | None. | Stream/anchor mismatch errors when no matching route exists. | Useful for stats or control-event diagnostic branches. |
| `Branch(name).Buffer(policy)` | The branch input shape. | Same shape, queued according to `flow.BranchBuffer`. | Packet, frame, or event. | None. | `branch_buffer_invalid`, `branch_buffer_unsupported`, `buffer_payload_unsafe`, `buffer_payload_too_large`. | Buffer policy is branch-local. Blocking, dropping, and latest policies affect only that runtime branch. |

## Join Constructors

Joins are the N-to-1 side of the same grammar: each constructor consumes two or
more `JoinArm` values, lowers to one planned convergence node, and returns a
normal stream point. Arms are intentionally narrower than stream chains: arm
chains support stream selection, `.Decode()`, `.Tap(...)`, and `Region(...)`
when they feed `Composite`; transforms and encode belong after the join unless
they are expressed in a separate upstream branch.

| Constructor | Arm input shape | Joined output shape | Allowed domain | Inserted conversions | Primary refusals | Runtime attach |
|---|---|---|---|---|---|---|
| `Mix(arms...)` | Audio arms. Packet arms are decoded before the mix stage; frame arms must carry compatible audio facts. | One audio frame stream named from the planned mix node. `.Encode(...)` can turn it into packets for muxed destinations. | Audio frame output. | Packet-arm decode is automatic. Later arms resample/convert to the first arm's audio format through the join arm policy. Output `.Auto(...)` can insert conversions before encode or branches. | `mix_inputs`, `mix_arm`, `mix_destination`, `mix_tap_arm`, decoder/transform/encoder adapter codes. | The mixed output can be tapped and used as a runtime branch anchor. Runtime attach starts from that tap and uses the ordinary branch grammar; the join itself is built with the base task. |
| `Composite(arms...)` | Video arms. Packet arms are decoded before the composite stage; `Region(x, y)` on arms or nested composites sets canvas placement. | One video frame stream whose geometry is the bounding box of all regions. `.Encode(...)` can turn it into packets for muxed destinations. | Video frame output. | Packet-arm decode is automatic. Arm layout is planned from regions; output `.Auto(...)` can insert resize/convert work before encode or branches. | `composite_inputs`, `composite_arm`, `composite_destination`, `composite_tap_arm`, decoder/transform/encoder adapter codes. | The composited output can be tapped and used as a runtime branch anchor. Runtime branches attach after the joined frame point, not into the arm set. |
| `Select(arms...)` | Frame or packet arms with compatible single-stream facts. Unlike Mix/Composite, Select forwards the active arm as-is. | One passthrough stream whose facts come from the first arm and whose active arm can switch with `SelectActive`. | Frame or packet, following the arms. | None; Select does not decode, encode, resample, or resize arms. | `select_inputs`, `select_arm`, `select_destination`, `select_tap_arm`. | The selected output can be tapped for runtime branches. `SelectActive(id)` is a task control, not a branch operation, and rides the data path to the selector. |
| `Join(name, stage, arms...)` | Stage-defined. A stage with frame-domain `InputShapes` makes packet arms decode first; a media-agnostic stage is passthrough. | Stage-defined through `OutputShapes`, or the first arm's facts under `name` when no output contract is declared. | Stage-defined. | Contract-derived arm conversions use the same shape solver and implicit arm policy as built-ins. | `join_name_invalid`, `join_stage_invalid`, custom `<name>_inputs`, `<name>_arm`, and destination refusals derived from the custom profile. | Custom joins have no private power: their tapped joined output is the same runtime-attach anchor shape that built-ins expose. |

## Joined Stream Operations

After `Mix`, `Composite`, `Select`, or custom `Join`, the returned stream point
accepts the same terminal grammar where the constructor supports it:

| Operation | Applies to | Notes |
|---|---|---|
| `.Tap(tap)` | All joins. | Names the joined output as a stable attach/control/diagnostic point. |
| `.Branches(branches...)` | All joins. | Fans the joined output into ordinary branch specs. Use this when `Select` or custom `Join` output must be encoded before muxed destinations. |
| `.To(destinations...)` | All joins. | `Mix` and `Composite` can deliver raw frames to sinks or encoded packets to muxers when `.Encode(...)` is present. `Select` and custom `Join` are sink-first unless branches encode. |
| `.Encode(codecSpec)` | `Mix` and `Composite`. | Terminal encode for the joined frame stream. Encode before `.Branches(...)` is refused because branches own their own terminal delivery. |
| `.Auto(...)`, `.Require(...)`, `.Prefer(...)` | `Mix` and `Composite`. | Shape-solver hints on the joined output before encode or planned branches. |
| `.SyncByPTS()` | `Mix` and `Composite`. | Aligns arms by presentation timestamp instead of arrival order. |
| `.Region(x, y)` | Source/video chains, nested `Composite`, nested `Select`, and tap arms when used inside `Composite`. | Places the arm's top-left corner on the outer composite canvas. |

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

Every refusal that happens before resources open should be a `*goav.BuildError`
with an `errcode.Code`, operation, node, reason, details, suggestions, and a
sentinel cause when one exists. The checked catalog is
[`docs/ERROR_CATALOG.md`](ERROR_CATALOG.md).
