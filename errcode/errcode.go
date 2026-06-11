// Package errcode is the error-code catalog: every refusal a build, attach,
// or control path can raise carries exactly one Code from this package. The
// constants are the stable, greppable contract — match them with
// errors.As(err, *goav.BuildError) and a Code comparison; the rendered text
// may improve over time, the codes do not. See docs/ERRORS.md for the
// contract and matching recipes.
package errcode

// Code identifies one class of refusal or diagnostic. Every BuildError
// carries one; Explain diagnostics and plan decisions reuse the same
// vocabulary (as plain strings in the plan package).
//
// Code is an open string, not a closed enum: external components emit their
// own vendor-prefixed codes through the same BuildError shape, and they flow
// through matching and rendering unchanged. The catalog below is goav's own
// vocabulary, not the universe of valid errcode.
type Code string

// Input and source errcode.
const (
	// InputInvalid fires when an input spec is empty or carries a
	// construction error (a nil reader, a failed provider option).
	InputInvalid Code = "input_invalid"
	// InputMissing fires when a job reaches Build with no input configured.
	InputMissing Code = "input_missing"
	// InputDuplicate fires when two realtime inputs declare the same name.
	InputDuplicate Code = "input_duplicate"
	// InputUnknown fires when goav.InputName(...) names no job input.
	InputUnknown Code = "input_unknown"
	// InputCountUnsupported fires when a recipe form that takes one input
	// (transcode, branch fanout) is given several.
	InputCountUnsupported Code = "input_count_unsupported"
	// InputFormatUnknown fires when probing cannot detect the input format.
	InputFormatUnknown Code = "input_format_unknown"
	// InputDemuxerMissing fires when the input format was detected but the
	// runtime has no demuxer registered for it.
	InputDemuxerMissing Code = "input_demuxer_missing"
	// MultiInputUnsupported fires when several recipe inputs mix kinds the
	// multi-input compiler does not support yet.
	MultiInputUnsupported Code = "multi_input_unsupported"
	// UnsupportedInput fires when a live provider input reaches the
	// transcode recipe compiler, which does not accept it yet.
	UnsupportedInput Code = "unsupported_input"
	// SourceCallbackMissing fires when a custom source declares no push
	// callback.
	SourceCallbackMissing Code = "source_callback_missing"
	// SourceShapeInvalid fires when a custom source shape has no media kind.
	SourceShapeInvalid Code = "source_shape_invalid"
	// SourceShapeUnsupported fires when a custom source declares a media
	// domain other than packet, frame, or event.
	SourceShapeUnsupported Code = "source_shape_unsupported"
	// SourceShapeMismatch fires when a chain operation contradicts the
	// declared custom-source shape (decoding already-decoded frames, packet
	// copy from a frame source).
	SourceShapeMismatch Code = "source_shape_mismatch"
	// ExplicitGraphSourceMissing fires when an expert graph plan has no
	// source node.
	ExplicitGraphSourceMissing Code = "explicit_graph_source_missing"
)

// Stream selection errcode.
const (
	// StreamMissing fires when no stream matches the chain's selector.
	StreamMissing Code = "stream_missing"
	// StreamAmbiguous fires when several streams match the chain's
	// selector and goav cannot pick one.
	StreamAmbiguous Code = "stream_ambiguous"
	// StreamDuplicate fires when two stream chains or branches resolve to
	// the same name or selection.
	StreamDuplicate Code = "stream_duplicate"
	// StreamNameMissing fires when a planned branch has no stable name.
	StreamNameMissing Code = "stream_name_missing"
	// StreamSelectorInvalid fires when a selector is malformed (negative
	// stream index).
	StreamSelectorInvalid Code = "stream_selector_invalid"
	// StreamCodecMissing fires when the selected packet stream carries no
	// codec id, so it cannot be decoded.
	StreamCodecMissing Code = "stream_codec_missing"
	// StreamOperationMissing fires when a chain selects a stream but
	// requests no decode, processing stage, or encoder.
	StreamOperationMissing Code = "stream_operation_missing"
	// StreamStepAfterEncode fires when a processing step is declared after
	// the terminal encoder.
	StreamStepAfterEncode Code = "stream_step_after_encode"
	// StageMissing fires when a custom stream stage is nil.
	StageMissing Code = "stage_missing"
)

// Flow codes (reusable Flow fragments applied to chains).
const (
	// FlowInvalid fires when a nil flow is applied.
	FlowInvalid Code = "flow_invalid"
	// FlowMediaMismatch fires when a flow built for one media kind is
	// applied to a stream of another.
	FlowMediaMismatch Code = "flow_media_mismatch"
	// FlowDecodeDuplicate fires when a flow decodes a stream that already
	// decodes.
	FlowDecodeDuplicate Code = "flow_decode_duplicate"
	// FlowDecodeOrderInvalid fires when a flow's decode is not its first
	// operation.
	FlowDecodeOrderInvalid Code = "flow_decode_order_invalid"
	// FlowDecodeDomainMismatch fires when a flow decodes a point that is
	// not packet-domain.
	FlowDecodeDomainMismatch Code = "flow_decode_domain_mismatch"
	// FlowCopyDomainMismatch fires when a flow copies packets from a point
	// that is not packet-domain.
	FlowCopyDomainMismatch Code = "flow_copy_domain_mismatch"
)

// Transform and shape errcode.
const (
	// TransformInvalid fires when a transform is empty, conflicting, or
	// malformed (one transform declaring both resize and resample).
	TransformInvalid Code = "transform_invalid"
	// TransformMediaMismatch fires when a transform is applied to the
	// wrong media kind (resize on audio, resample on video).
	TransformMediaMismatch Code = "transform_media_mismatch"
	// TransformAdapterMissing fires when no filter adapter is registered
	// for the requested transform.
	TransformAdapterMissing Code = "transform_adapter_missing"
	// TransformAdapterIncompatible fires when the registered filter
	// adapter does not support the requested configuration.
	TransformAdapterIncompatible Code = "transform_adapter_incompatible"
	// TranscodeResizeInvalid fires when a transcode resize has invalid
	// dimensions.
	TranscodeResizeInvalid Code = "transcode_resize_invalid"
	// OperationShapeMismatch fires when an operation cannot consume the
	// media shape the chain produces at that point.
	OperationShapeMismatch Code = "operation_shape_mismatch"
	// ShapeRequirementUnmet fires when a .Require(...) assertion is not
	// satisfied and no active .Auto(...) policy covers the conversion.
	ShapeRequirementUnmet Code = "shape_requirement_unmet"
	// ShapeConversionRefused fires when a needed conversion exists but the
	// chain's .Auto(...) policy does not allow it.
	ShapeConversionRefused Code = "shape_conversion_refused"
	// ShapeAdapterMissing fires when no registered filter can perform a
	// needed conversion.
	ShapeAdapterMissing Code = "shape_adapter_missing"
	// ShapeAdapterAmbiguous fires when several registered filters could
	// perform a needed conversion and no .Prefer(...) resolves the choice.
	ShapeAdapterAmbiguous Code = "shape_adapter_ambiguous"
)

// Codec adapter codes (decode/encode availability and capability).
const (
	// DecodeAdapterMissing fires when no decoder adapter is registered for
	// the stream's codec.
	DecodeAdapterMissing Code = "decode_adapter_missing"
	// DecodeAdapterUnavailable fires when the decoder adapter is
	// descriptor-only in this build (missing build tag).
	DecodeAdapterUnavailable Code = "decode_adapter_unavailable"
	// DecodeAdapterIncompatible fires when the decoder adapter does not
	// support the requested media or frame format.
	DecodeAdapterIncompatible Code = "decode_adapter_incompatible"
	// EncodeAdapterMissing fires when no encoder adapter is registered for
	// the requested codec.
	EncodeAdapterMissing Code = "encode_adapter_missing"
	// EncodeAdapterUnavailable fires when the encoder adapter is
	// descriptor-only in this build (missing build tag).
	EncodeAdapterUnavailable Code = "encode_adapter_unavailable"
	// EncodeAdapterIncompatible fires when the encoder adapter does not
	// support the requested media or frame format.
	EncodeAdapterIncompatible Code = "encode_adapter_incompatible"
	// CodecChangePolicyUnsupported fires when a custom codec-change policy
	// is requested; only the built-in policies exist today.
	CodecChangePolicyUnsupported Code = "codec_change_policy_unsupported"
)

// Encode errcode.
const (
	// EncodeMissing fires when decoded frames are routed to a muxed
	// destination without an encoder.
	EncodeMissing Code = "encode_missing"
	// EncodeDuplicate fires when a chain declares a second terminal
	// encoder.
	EncodeDuplicate Code = "encode_duplicate"
	// EncodeParameterInvalid fires when an encode setting is out of range
	// (negative bitrate, non-positive FPS).
	EncodeParameterInvalid Code = "encode_parameter_invalid"
	// EncodeAutoUnresolved fires when automatic codec selection is
	// requested where it is not implemented.
	EncodeAutoUnresolved Code = "encode_auto_unresolved"
	// EncodeWorkInProgress fires when the requested recipe encoder codec
	// is not implemented yet.
	EncodeWorkInProgress Code = "encode_work_in_progress"
	// EncodeStreamMismatch fires when the encoder's stream selector does
	// not match the stream the chain decoded.
	EncodeStreamMismatch Code = "encode_stream_mismatch"
	// EncodeDestinationMissing fires when an encode stage is built with no
	// target codec.
	EncodeDestinationMissing Code = "encode_destination_missing"
	// EncodeBranchSourceInvalid fires when a planned branch anchors after
	// a terminal stream encoder.
	EncodeBranchSourceInvalid Code = "encode_branch_source_invalid"
)

// Destination and output errcode.
const (
	// OutputInvalid fires when a destination is empty or carries a
	// construction error (a nil sink or writer).
	OutputInvalid Code = "output_invalid"
	// OutputMissing fires when a job, chain, or route reaches Build with
	// no destination attached.
	OutputMissing Code = "output_missing"
	// OutputDuplicate fires when two outputs declare the same name.
	OutputDuplicate Code = "output_duplicate"
	// OutputScopeMixed fires when job-level and branch-local destinations
	// are mixed in one recipe.
	OutputScopeMixed Code = "output_scope_mixed"
	// OutputKindMixed fires when one stream mixes sink and muxed outputs.
	OutputKindMixed Code = "output_kind_mixed"
	// OutputWriterMissing fires when a file output has no writer.
	OutputWriterMissing Code = "output_writer_missing"
	// OutputFormatMissing fires when a writer-backed output gives no name,
	// URI, MIME type, or explicit format to derive a container from.
	OutputFormatMissing Code = "output_format_missing"
	// OutputFormatUnknown fires when the output format cannot be detected.
	OutputFormatUnknown Code = "output_format_unknown"
	// OutputMuxerMissing fires when the selected output format has no
	// registered muxer.
	OutputMuxerMissing Code = "output_muxer_missing"
	// OutputDestinationMissing fires when an output has no URI, writer, or
	// sink at all.
	OutputDestinationMissing Code = "output_destination_missing"
	// DestinationInvalid fires when a branch destination is empty or
	// unnamed.
	DestinationInvalid Code = "destination_invalid"
	// DestinationMissing fires when a branch has no destination or
	// references an undefined one.
	DestinationMissing Code = "destination_missing"
	// DestinationDuplicate fires when one destination label is bound to
	// two different destination handles, or routed twice from one branch.
	DestinationDuplicate Code = "destination_duplicate"
	// DestinationFormatUnknown fires when a destination's format cannot be
	// detected.
	DestinationFormatUnknown Code = "destination_format_unknown"
	// DestinationMuxerMissing fires when the destination format has no
	// registered muxer.
	DestinationMuxerMissing Code = "destination_muxer_missing"
	// DestinationMuxIncompatible fires when the planned mux group violates
	// the container's stream/codec contract (two streams into Annex B).
	DestinationMuxIncompatible Code = "destination_mux_incompatible"
	// DestinationShapeMismatch fires when frame-domain media is routed to
	// a byte or mux destination; those consume packets.
	DestinationShapeMismatch Code = "destination_shape_mismatch"
)

// Planned branch codes (.Branches fanout declared at build time).
const (
	// BranchInvalid fires when a branch spec is nil.
	BranchInvalid Code = "branch_invalid"
	// BranchMissing fires when .Branches(...) is called with no branches.
	BranchMissing Code = "branch_missing"
	// BranchDuplicate fires when two branches share one name.
	BranchDuplicate Code = "branch_duplicate"
	// BranchSourceInvalid fires when a branch anchors from something other
	// than a typed tap or expert graph handle.
	BranchSourceInvalid Code = "branch_source_invalid"
	// BranchTapMissing fires when a branch's .From(...) tap is not
	// declared on the parent stream.
	BranchTapMissing Code = "branch_tap_missing"
	// BranchDecodeDuplicate fires when a branch decodes an already-decoded
	// input.
	BranchDecodeDuplicate Code = "branch_decode_duplicate"
	// BranchDecodeOrderInvalid fires when a branch's decode is not its
	// first operation.
	BranchDecodeOrderInvalid Code = "branch_decode_order_invalid"
	// BranchDecodeDomainMismatch fires when a branch decodes a point that
	// is not packet-domain.
	BranchDecodeDomainMismatch Code = "branch_decode_domain_mismatch"
	// BranchDecodeCopyInvalid fires when a branch decodes and then copies
	// the original packets.
	BranchDecodeCopyInvalid Code = "branch_decode_copy_invalid"
	// BranchTransformMediaMismatch fires when a branch transform targets
	// the wrong media kind.
	BranchTransformMediaMismatch Code = "branch_transform_media_mismatch"
	// BranchDestinationInvalid fires when a branch destination is
	// malformed.
	BranchDestinationInvalid Code = "branch_destination_invalid"
	// BranchDestinationUnmatched fires when a declared destination selects
	// no branches.
	BranchDestinationUnmatched Code = "branch_destination_unmatched"
	// BranchOperationChainUnsupported fires when a branch's operation
	// chain uses an unsupported combination.
	BranchOperationChainUnsupported Code = "branch_operation_chain_unsupported"
	// BranchComposePlanEmpty fires when branch composition planned no
	// routes; an internal planner invariant.
	BranchComposePlanEmpty Code = "branch_compose_plan_empty"
	// BranchBufferInvalid fires when a branch buffer policy is malformed.
	BranchBufferInvalid Code = "branch_buffer_invalid"
	// BranchBufferUnsupported fires when an unbounded branch buffer is
	// requested; the runtime does not support it yet.
	BranchBufferUnsupported Code = "branch_buffer_unsupported"
	// BufferPayloadUnsafe fires when buffered execution receives a mutable
	// payload that the configured copy policy refuses to copy.
	BufferPayloadUnsafe Code = "buffer_payload_unsafe"
	// BufferPayloadTooLarge fires when buffered execution receives a payload
	// larger than the configured copy bounds.
	BufferPayloadTooLarge Code = "buffer_payload_too_large"
	// CopyBranchSourceInvalid fires when a packet-copy branch starts from
	// a point that is not packet-domain.
	CopyBranchSourceInvalid Code = "copy_branch_source_invalid"
	// CopyUnsupported fires when a copy branch is requested from a
	// frame-domain stream point.
	CopyUnsupported Code = "copy_unsupported"
	// PacketBranchEncodeUnsupported fires when a packet-domain branch
	// encodes without decoding first.
	PacketBranchEncodeUnsupported Code = "packet_branch_encode_unsupported"
	// PacketBranchTransformUnsupported fires when a packet-domain branch
	// resizes or resamples without decoding first.
	PacketBranchTransformUnsupported Code = "packet_branch_transform_unsupported"
	// DecodeConfigConflict fires when branches sharing one decoder declare
	// different decode configs.
	DecodeConfigConflict Code = "decode_config_conflict"
	// DecodePolicyConflict fires when branches sharing one decoder declare
	// different codec-change policies.
	DecodePolicyConflict Code = "decode_policy_conflict"
)

// Tap errcode.
const (
	// TapInvalid fires when a tap name is empty.
	TapInvalid Code = "tap_invalid"
	// TapDomainMismatch fires when a typed tap's domain (frame/packet)
	// does not match the chain point it is declared on.
	TapDomainMismatch Code = "tap_domain_mismatch"
)

// Join codes (Mix, Composite, Select, and custom goav.Join kinds). Each join
// kind raises its own per-kind code, derived as <kind>_<family> by the goav
// root joinErrorCode helper; the full enumeration for the built-in kinds is
// declared here. A custom join named "crossfade" raises the same families
// under its own name (crossfade_inputs, crossfade_arm, crossfade_destination,
// crossfade_tap_arm) — the snake-safe name requirement (JoinNameInvalid)
// keeps those derived codes well-formed.
const (
	// JoinNameInvalid fires when a custom join (goav.Join) declares an
	// unusable name: empty, not snake-safe ([a-z][a-z0-9_]*), reserved by a
	// built-in kind (mix, composite, select), or colliding with another join
	// node in the same join tree.
	JoinNameInvalid Code = "join_name_invalid"
	// JoinStageInvalid fires when a custom join's convergence stage is nil
	// or reports a Name() different from the join's name.
	JoinStageInvalid Code = "join_stage_invalid"
	// MixInputs fires when Mix is given fewer than two arms.
	MixInputs Code = "mix_inputs"
	// MixArm fires when a Mix arm is invalid: wrong media, duplicate
	// stream ids, an unconvertible format, or a nested arm carrying .Encode.
	MixArm Code = "mix_arm"
	// MixDestination fires when a Mix without .Encode is routed to a
	// non-sink destination.
	MixDestination Code = "mix_destination"
	// MixTapArm fires when a Mix tap arm references a tap no earlier arm
	// declares.
	MixTapArm Code = "mix_tap_arm"
	// CompositeInputs fires when Composite is given fewer than two arms.
	CompositeInputs Code = "composite_inputs"
	// CompositeArm fires when a Composite arm is invalid (see MixArm).
	CompositeArm Code = "composite_arm"
	// CompositeDestination fires when a Composite without .Encode is
	// routed to a non-sink destination.
	CompositeDestination Code = "composite_destination"
	// CompositeTapArm fires when a Composite tap arm references a tap no
	// earlier arm declares.
	CompositeTapArm Code = "composite_tap_arm"
	// SelectInputs fires when Select is given fewer than two arms.
	SelectInputs Code = "select_inputs"
	// SelectArm fires when a Select arm is invalid (see MixArm).
	SelectArm Code = "select_arm"
	// SelectDestination fires when a Select's output cannot reach the
	// declared destination kind.
	SelectDestination Code = "select_destination"
	// SelectTapArm fires when a Select tap arm references a tap no earlier
	// arm declares.
	SelectTapArm Code = "select_tap_arm"
)

// Runtime attach codes (Task.Attach / Rebranch refusals).
const (
	// RuntimeBranchInvalid fires when a runtime branch spec is nil or
	// malformed.
	RuntimeBranchInvalid Code = "runtime_branch_invalid"
	// RuntimeBranchAnchorMissing fires when the branch's source node does
	// not exist in the running task graph.
	RuntimeBranchAnchorMissing Code = "runtime_branch_anchor_missing"
	// RuntimeBranchTapMissing fires when the branch's source tap does not
	// exist in the running task.
	RuntimeBranchTapMissing Code = "runtime_branch_tap_missing"
	// RuntimeBranchTapDuplicate fires when the branch declares a tap name
	// that already exists in the task.
	RuntimeBranchTapDuplicate Code = "runtime_branch_tap_duplicate"
	// RuntimeBranchNodeDuplicate fires when a branch node name already
	// exists in the task graph.
	RuntimeBranchNodeDuplicate Code = "runtime_branch_node_duplicate"
	// RuntimeBranchEncodeMissing fires when a muxed runtime branch has
	// neither packet copy nor an encoder.
	RuntimeBranchEncodeMissing Code = "runtime_branch_encode_missing"
	// RuntimeBranchEncodeDomainMismatch fires when a runtime branch
	// encodes from a packet tap; encoding needs a frame tap.
	RuntimeBranchEncodeDomainMismatch Code = "runtime_branch_encode_domain_mismatch"
	// RuntimeBranchDecodeDomainMismatch fires when a runtime branch
	// decodes from a frame tap; decoding needs a packet tap.
	RuntimeBranchDecodeDomainMismatch Code = "runtime_branch_decode_domain_mismatch"
	// RuntimeBranchDecodeCodecMissing fires when a runtime branch decode
	// has no packet codec metadata to open a decoder with.
	RuntimeBranchDecodeCodecMissing Code = "runtime_branch_decode_codec_missing"
	// RuntimeBranchCopyDomainMismatch fires when a runtime branch copies
	// packets from a frame tap.
	RuntimeBranchCopyDomainMismatch Code = "runtime_branch_copy_domain_mismatch"
	// RuntimeBranchMuxCodecMissing fires when a runtime branch's mux
	// destination has no codec metadata for the muxed stream.
	RuntimeBranchMuxCodecMissing Code = "runtime_branch_mux_codec_missing"
	// RuntimeBranchTransformError fires when a runtime branch transform
	// stage cannot be opened.
	RuntimeBranchTransformError Code = "runtime_branch_transform_error"
	// RuntimeBranchTransformMediaMismatch fires when a runtime branch
	// transform targets the wrong media kind for its tap.
	RuntimeBranchTransformMediaMismatch Code = "runtime_branch_transform_media_mismatch"
	// RuntimeBranchGraphError fires when the live graph rejects the branch
	// attachment.
	RuntimeBranchGraphError Code = "runtime_branch_graph_error"
)

// Stream rule codes (OnStream grammar).
const (
	// StreamRuleInvalid fires when an OnStream rule is malformed (no
	// matcher, no reaction, or an unusable discovered stream).
	StreamRuleInvalid Code = "stream_rule_invalid"
)

// Job and compiler errcode.
const (
	// JobInvalid fires when a nil job or join reaches the recipe compiler;
	// an internal invariant.
	JobInvalid Code = "job_invalid"
	// RuntimeMissing fires when a job reaches Build with no runtime
	// configured.
	RuntimeMissing Code = "runtime_missing"
	// RuntimeUnsupported fires when recipe compilation is asked to run on
	// a non-goav runtime implementation.
	RuntimeUnsupported Code = "runtime_unsupported"
	// CompilerPassInvalid fires when a recipe compiler pass is nil; an
	// internal invariant.
	CompilerPassInvalid Code = "compiler_pass_invalid"
	// CompilerPassFailed fires when a compiler pass fails without its own
	// diagnostic; an internal invariant.
	CompilerPassFailed Code = "compiler_pass_failed"
	// RecipeGraphUnsupported fires when the recipe intent matches no
	// supported graph plan.
	RecipeGraphUnsupported Code = "recipe_graph_unsupported"
	// RecipeAttachmentMismatch fires when the recipe intent and its
	// concrete attachments disagree; an internal invariant.
	RecipeAttachmentMismatch Code = "recipe_attachment_mismatch"
	// GraphPlanInvalid fires when a media graph plan is internally
	// inconsistent; an internal invariant.
	GraphPlanInvalid Code = "graph_plan_invalid"
	// BufferBudgetMissing fires when a recipe needs a buffered graph but the
	// propagated stream facts cannot size graph-owned packet/frame copies.
	BufferBudgetMissing Code = "buffer_budget_missing"
)

// Diagnostic and decision codes: these never fail a build. They appear in
// Explain reports (plan.Diagnostic / plan.Decision, as plain strings) to
// record what the planner chose and why.
const (
	// ShapeConversionInserted records a conversion the shape solver
	// inserted under an .Auto(...) policy or a join's arm policy.
	ShapeConversionInserted Code = "shape_conversion_inserted"
	// ShapePreferenceApplied records a .Prefer(...) the solver used to
	// resolve an open choice.
	ShapePreferenceApplied Code = "shape_preference_applied"
	// ShapePreferenceIgnored records a .Prefer(...) the solver could not
	// honor; preferences never fail a build.
	ShapePreferenceIgnored Code = "shape_preference_ignored"
	// DecodeCodecDeferred records a decode whose codec resolves at run
	// time (live inputs without probes).
	DecodeCodecDeferred Code = "decode_codec_deferred"
	// ExplainPreflightError records a non-BuildError preflight failure on
	// an Explain report.
	ExplainPreflightError Code = "explain_preflight_error"
	// PacketCopy records the decision to keep a stream packet-encoded.
	PacketCopy Code = "packet_copy"
	// FrameSource records the decision that a frame source flows through
	// without decode.
	FrameSource Code = "frame_source"
	// EventSource records the decision that an event source flows through
	// untouched.
	EventSource Code = "event_source"
	// DecodeRequired records the decision that the declared operations
	// need decoded frames.
	DecodeRequired Code = "decode_required"
	// EncodeRequired records the decision that the chain re-encodes before
	// delivery.
	EncodeRequired Code = "encode_required"
	// StreamRule records an OnStream rule wired onto a task.
	StreamRule Code = "stream_rule"
)
