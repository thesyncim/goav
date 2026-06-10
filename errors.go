// The error-code catalog: every refusal a build, attach, or control path can
// raise carries exactly one ErrorCode from this file. The constants are the
// stable, greppable contract — match them with errors.As(err, *BuildError)
// and a Code comparison; the rendered text may improve over time, the codes
// do not. See docs/ERRORS.md for the contract and matching recipes.

package goav

// ErrorCode identifies one class of refusal or diagnostic. Every BuildError
// carries one; Explain diagnostics and plan decisions reuse the same
// vocabulary (as plain strings in the info package).
type ErrorCode string

// Input and source codes.
const (
	// CodeInputInvalid fires when an input spec is empty or carries a
	// construction error (a nil reader, a failed provider option).
	CodeInputInvalid ErrorCode = "input_invalid"
	// CodeInputMissing fires when a job reaches Build with no input configured.
	CodeInputMissing ErrorCode = "input_missing"
	// CodeInputDuplicate fires when two realtime inputs declare the same name.
	CodeInputDuplicate ErrorCode = "input_duplicate"
	// CodeInputUnknown fires when goav.InputName(...) names no job input.
	CodeInputUnknown ErrorCode = "input_unknown"
	// CodeInputCountUnsupported fires when a recipe form that takes one input
	// (transcode, branch fanout) is given several.
	CodeInputCountUnsupported ErrorCode = "input_count_unsupported"
	// CodeInputFormatUnknown fires when probing cannot detect the input format.
	CodeInputFormatUnknown ErrorCode = "input_format_unknown"
	// CodeInputDemuxerMissing fires when the input format was detected but the
	// runtime has no demuxer registered for it.
	CodeInputDemuxerMissing ErrorCode = "input_demuxer_missing"
	// CodeMultiInputUnsupported fires when several recipe inputs mix kinds the
	// multi-input compiler does not support yet.
	CodeMultiInputUnsupported ErrorCode = "multi_input_unsupported"
	// CodeUnsupportedInput fires when a live provider input reaches the
	// transcode recipe compiler, which does not accept it yet.
	CodeUnsupportedInput ErrorCode = "unsupported_input"
	// CodeSourceCallbackMissing fires when a custom source declares no push
	// callback.
	CodeSourceCallbackMissing ErrorCode = "source_callback_missing"
	// CodeSourceShapeInvalid fires when a custom source shape has no media kind.
	CodeSourceShapeInvalid ErrorCode = "source_shape_invalid"
	// CodeSourceShapeUnsupported fires when a custom source declares a media
	// domain other than packet, frame, or event.
	CodeSourceShapeUnsupported ErrorCode = "source_shape_unsupported"
	// CodeSourceShapeMismatch fires when a chain operation contradicts the
	// declared custom-source shape (decoding already-decoded frames, packet
	// copy from a frame source).
	CodeSourceShapeMismatch ErrorCode = "source_shape_mismatch"
	// CodeExplicitGraphSourceMissing fires when an expert graph plan has no
	// source node.
	CodeExplicitGraphSourceMissing ErrorCode = "explicit_graph_source_missing"
)

// Stream selection codes.
const (
	// CodeStreamMissing fires when no stream matches the chain's selector.
	CodeStreamMissing ErrorCode = "stream_missing"
	// CodeStreamAmbiguous fires when several streams match the chain's
	// selector and goav cannot pick one.
	CodeStreamAmbiguous ErrorCode = "stream_ambiguous"
	// CodeStreamDuplicate fires when two stream chains or branches resolve to
	// the same name or selection.
	CodeStreamDuplicate ErrorCode = "stream_duplicate"
	// CodeStreamNameMissing fires when a planned branch has no stable name.
	CodeStreamNameMissing ErrorCode = "stream_name_missing"
	// CodeStreamSelectorInvalid fires when a selector is malformed (negative
	// stream index).
	CodeStreamSelectorInvalid ErrorCode = "stream_selector_invalid"
	// CodeStreamCodecMissing fires when the selected packet stream carries no
	// codec id, so it cannot be decoded.
	CodeStreamCodecMissing ErrorCode = "stream_codec_missing"
	// CodeStreamOperationMissing fires when a chain selects a stream but
	// requests no decode, processing stage, or encoder.
	CodeStreamOperationMissing ErrorCode = "stream_operation_missing"
	// CodeStreamStepAfterEncode fires when a processing step is declared after
	// the terminal encoder.
	CodeStreamStepAfterEncode ErrorCode = "stream_step_after_encode"
	// CodeStageMissing fires when a custom stream stage is nil.
	CodeStageMissing ErrorCode = "stage_missing"
)

// Flow codes (reusable Flow fragments applied to chains).
const (
	// CodeFlowInvalid fires when a nil flow is applied.
	CodeFlowInvalid ErrorCode = "flow_invalid"
	// CodeFlowMediaMismatch fires when a flow built for one media kind is
	// applied to a stream of another.
	CodeFlowMediaMismatch ErrorCode = "flow_media_mismatch"
	// CodeFlowDecodeDuplicate fires when a flow decodes a stream that already
	// decodes.
	CodeFlowDecodeDuplicate ErrorCode = "flow_decode_duplicate"
	// CodeFlowDecodeOrderInvalid fires when a flow's decode is not its first
	// operation.
	CodeFlowDecodeOrderInvalid ErrorCode = "flow_decode_order_invalid"
	// CodeFlowDecodeDomainMismatch fires when a flow decodes a point that is
	// not packet-domain.
	CodeFlowDecodeDomainMismatch ErrorCode = "flow_decode_domain_mismatch"
	// CodeFlowCopyDomainMismatch fires when a flow copies packets from a point
	// that is not packet-domain.
	CodeFlowCopyDomainMismatch ErrorCode = "flow_copy_domain_mismatch"
)

// Transform and shape codes.
const (
	// CodeTransformInvalid fires when a transform is empty, conflicting, or
	// malformed (one transform declaring both resize and resample).
	CodeTransformInvalid ErrorCode = "transform_invalid"
	// CodeTransformMediaMismatch fires when a transform is applied to the
	// wrong media kind (resize on audio, resample on video).
	CodeTransformMediaMismatch ErrorCode = "transform_media_mismatch"
	// CodeTransformAdapterMissing fires when no filter adapter is registered
	// for the requested transform.
	CodeTransformAdapterMissing ErrorCode = "transform_adapter_missing"
	// CodeTransformAdapterIncompatible fires when the registered filter
	// adapter does not support the requested configuration.
	CodeTransformAdapterIncompatible ErrorCode = "transform_adapter_incompatible"
	// CodeTranscodeResizeInvalid fires when a transcode resize has invalid
	// dimensions.
	CodeTranscodeResizeInvalid ErrorCode = "transcode_resize_invalid"
	// CodeOperationShapeMismatch fires when an operation cannot consume the
	// media shape the chain produces at that point.
	CodeOperationShapeMismatch ErrorCode = "operation_shape_mismatch"
	// CodeShapeRequirementUnmet fires when a .Require(...) assertion is not
	// satisfied and no active .Auto(...) policy covers the conversion.
	CodeShapeRequirementUnmet ErrorCode = "shape_requirement_unmet"
	// CodeShapeConversionRefused fires when a needed conversion exists but the
	// chain's .Auto(...) policy does not allow it.
	CodeShapeConversionRefused ErrorCode = "shape_conversion_refused"
	// CodeShapeAdapterMissing fires when no registered filter can perform a
	// needed conversion.
	CodeShapeAdapterMissing ErrorCode = "shape_adapter_missing"
	// CodeShapeAdapterAmbiguous fires when several registered filters could
	// perform a needed conversion and no .Prefer(...) resolves the choice.
	CodeShapeAdapterAmbiguous ErrorCode = "shape_adapter_ambiguous"
)

// Codec adapter codes (decode/encode availability and capability).
const (
	// CodeDecodeAdapterMissing fires when no decoder adapter is registered for
	// the stream's codec.
	CodeDecodeAdapterMissing ErrorCode = "decode_adapter_missing"
	// CodeDecodeAdapterUnavailable fires when the decoder adapter is
	// descriptor-only in this build (missing build tag).
	CodeDecodeAdapterUnavailable ErrorCode = "decode_adapter_unavailable"
	// CodeDecodeAdapterIncompatible fires when the decoder adapter does not
	// support the requested media or frame format.
	CodeDecodeAdapterIncompatible ErrorCode = "decode_adapter_incompatible"
	// CodeEncodeAdapterMissing fires when no encoder adapter is registered for
	// the requested codec.
	CodeEncodeAdapterMissing ErrorCode = "encode_adapter_missing"
	// CodeEncodeAdapterUnavailable fires when the encoder adapter is
	// descriptor-only in this build (missing build tag).
	CodeEncodeAdapterUnavailable ErrorCode = "encode_adapter_unavailable"
	// CodeEncodeAdapterIncompatible fires when the encoder adapter does not
	// support the requested media or frame format.
	CodeEncodeAdapterIncompatible ErrorCode = "encode_adapter_incompatible"
	// CodeCodecChangePolicyUnsupported fires when a custom codec-change policy
	// is requested; only the built-in policies exist today.
	CodeCodecChangePolicyUnsupported ErrorCode = "codec_change_policy_unsupported"
)

// Encode codes.
const (
	// CodeEncodeMissing fires when decoded frames are routed to a muxed
	// destination without an encoder.
	CodeEncodeMissing ErrorCode = "encode_missing"
	// CodeEncodeDuplicate fires when a chain declares a second terminal
	// encoder.
	CodeEncodeDuplicate ErrorCode = "encode_duplicate"
	// CodeEncodeParameterInvalid fires when an encode setting is out of range
	// (negative bitrate, non-positive FPS).
	CodeEncodeParameterInvalid ErrorCode = "encode_parameter_invalid"
	// CodeEncodeAutoUnresolved fires when automatic codec selection is
	// requested where it is not implemented.
	CodeEncodeAutoUnresolved ErrorCode = "encode_auto_unresolved"
	// CodeEncodeWorkInProgress fires when the requested recipe encoder codec
	// is not implemented yet.
	CodeEncodeWorkInProgress ErrorCode = "encode_work_in_progress"
	// CodeEncodeStreamMismatch fires when the encoder's stream selector does
	// not match the stream the chain decoded.
	CodeEncodeStreamMismatch ErrorCode = "encode_stream_mismatch"
	// CodeEncodeDestinationMissing fires when an encode stage is built with no
	// target codec.
	CodeEncodeDestinationMissing ErrorCode = "encode_destination_missing"
	// CodeEncodeBranchSourceInvalid fires when a planned branch anchors after
	// a terminal stream encoder.
	CodeEncodeBranchSourceInvalid ErrorCode = "encode_branch_source_invalid"
)

// Destination and output codes.
const (
	// CodeOutputInvalid fires when a destination is empty or carries a
	// construction error (a nil sink or writer).
	CodeOutputInvalid ErrorCode = "output_invalid"
	// CodeOutputMissing fires when a job, chain, or route reaches Build with
	// no destination attached.
	CodeOutputMissing ErrorCode = "output_missing"
	// CodeOutputDuplicate fires when two outputs declare the same name.
	CodeOutputDuplicate ErrorCode = "output_duplicate"
	// CodeOutputScopeMixed fires when job-level and branch-local destinations
	// are mixed in one recipe.
	CodeOutputScopeMixed ErrorCode = "output_scope_mixed"
	// CodeOutputKindMixed fires when one stream mixes sink and muxed outputs.
	CodeOutputKindMixed ErrorCode = "output_kind_mixed"
	// CodeOutputWriterMissing fires when a file output has no writer.
	CodeOutputWriterMissing ErrorCode = "output_writer_missing"
	// CodeOutputFormatMissing fires when a writer-backed output gives no name,
	// URI, MIME type, or explicit format to derive a container from.
	CodeOutputFormatMissing ErrorCode = "output_format_missing"
	// CodeOutputFormatUnknown fires when the output format cannot be detected.
	CodeOutputFormatUnknown ErrorCode = "output_format_unknown"
	// CodeOutputMuxerMissing fires when the selected output format has no
	// registered muxer.
	CodeOutputMuxerMissing ErrorCode = "output_muxer_missing"
	// CodeOutputDestinationMissing fires when an output has no URI, writer, or
	// sink at all.
	CodeOutputDestinationMissing ErrorCode = "output_destination_missing"
	// CodeDestinationInvalid fires when a branch destination is empty or
	// unnamed.
	CodeDestinationInvalid ErrorCode = "destination_invalid"
	// CodeDestinationMissing fires when a branch has no destination or
	// references an undefined one.
	CodeDestinationMissing ErrorCode = "destination_missing"
	// CodeDestinationDuplicate fires when one destination label is bound to
	// two different destination handles, or routed twice from one branch.
	CodeDestinationDuplicate ErrorCode = "destination_duplicate"
	// CodeDestinationFormatUnknown fires when a destination's format cannot be
	// detected.
	CodeDestinationFormatUnknown ErrorCode = "destination_format_unknown"
	// CodeDestinationMuxerMissing fires when the destination format has no
	// registered muxer.
	CodeDestinationMuxerMissing ErrorCode = "destination_muxer_missing"
	// CodeDestinationMuxIncompatible fires when the planned mux group violates
	// the container's stream/codec contract (two streams into Annex B).
	CodeDestinationMuxIncompatible ErrorCode = "destination_mux_incompatible"
	// CodeDestinationShapeMismatch fires when frame-domain media is routed to
	// a byte or mux destination; those consume packets.
	CodeDestinationShapeMismatch ErrorCode = "destination_shape_mismatch"
)

// Planned branch codes (.Branches fanout declared at build time).
const (
	// CodeBranchInvalid fires when a branch spec is nil.
	CodeBranchInvalid ErrorCode = "branch_invalid"
	// CodeBranchMissing fires when .Branches(...) is called with no branches.
	CodeBranchMissing ErrorCode = "branch_missing"
	// CodeBranchDuplicate fires when two branches share one name.
	CodeBranchDuplicate ErrorCode = "branch_duplicate"
	// CodeBranchSourceInvalid fires when a branch anchors from something other
	// than a typed tap or expert graph handle.
	CodeBranchSourceInvalid ErrorCode = "branch_source_invalid"
	// CodeBranchTapMissing fires when a branch's .From(...) tap is not
	// declared on the parent stream.
	CodeBranchTapMissing ErrorCode = "branch_tap_missing"
	// CodeBranchTapDomainUnsupported fires when a planned branch anchors on a
	// post-encode tap; those anchor runtime attachments only.
	CodeBranchTapDomainUnsupported ErrorCode = "branch_tap_domain_unsupported"
	// CodeBranchDecodeDuplicate fires when a branch decodes an already-decoded
	// input.
	CodeBranchDecodeDuplicate ErrorCode = "branch_decode_duplicate"
	// CodeBranchDecodeOrderInvalid fires when a branch's decode is not its
	// first operation.
	CodeBranchDecodeOrderInvalid ErrorCode = "branch_decode_order_invalid"
	// CodeBranchDecodeDomainMismatch fires when a branch decodes a point that
	// is not packet-domain.
	CodeBranchDecodeDomainMismatch ErrorCode = "branch_decode_domain_mismatch"
	// CodeBranchDecodeCopyInvalid fires when a branch decodes and then copies
	// the original packets.
	CodeBranchDecodeCopyInvalid ErrorCode = "branch_decode_copy_invalid"
	// CodeBranchTransformMediaMismatch fires when a branch transform targets
	// the wrong media kind.
	CodeBranchTransformMediaMismatch ErrorCode = "branch_transform_media_mismatch"
	// CodeBranchDestinationInvalid fires when a branch destination is
	// malformed.
	CodeBranchDestinationInvalid ErrorCode = "branch_destination_invalid"
	// CodeBranchDestinationUnmatched fires when a declared destination selects
	// no branches.
	CodeBranchDestinationUnmatched ErrorCode = "branch_destination_unmatched"
	// CodeBranchOperationChainUnsupported fires when a branch's operation
	// chain uses an unsupported combination.
	CodeBranchOperationChainUnsupported ErrorCode = "branch_operation_chain_unsupported"
	// CodeBranchComposePlanEmpty fires when branch composition planned no
	// routes; an internal planner invariant.
	CodeBranchComposePlanEmpty ErrorCode = "branch_compose_plan_empty"
	// CodeBranchBufferInvalid fires when a branch buffer policy is malformed.
	CodeBranchBufferInvalid ErrorCode = "branch_buffer_invalid"
	// CodeBranchBufferUnsupported fires when an unbounded branch buffer is
	// requested; the runtime does not support it yet.
	CodeBranchBufferUnsupported ErrorCode = "branch_buffer_unsupported"
	// CodeCopyBranchSourceInvalid fires when a packet-copy branch starts from
	// a point that is not packet-domain.
	CodeCopyBranchSourceInvalid ErrorCode = "copy_branch_source_invalid"
	// CodeCopyUnsupported fires when a copy branch is requested from a
	// frame-domain stream point.
	CodeCopyUnsupported ErrorCode = "copy_unsupported"
	// CodePacketBranchEncodeUnsupported fires when a packet-domain branch
	// encodes without decoding first.
	CodePacketBranchEncodeUnsupported ErrorCode = "packet_branch_encode_unsupported"
	// CodePacketBranchTransformUnsupported fires when a packet-domain branch
	// resizes or resamples without decoding first.
	CodePacketBranchTransformUnsupported ErrorCode = "packet_branch_transform_unsupported"
	// CodeDecodeConfigConflict fires when branches sharing one decoder declare
	// different decode configs.
	CodeDecodeConfigConflict ErrorCode = "decode_config_conflict"
	// CodeDecodePolicyConflict fires when branches sharing one decoder declare
	// different codec-change policies.
	CodeDecodePolicyConflict ErrorCode = "decode_policy_conflict"
)

// Tap codes.
const (
	// CodeTapInvalid fires when a tap name is empty.
	CodeTapInvalid ErrorCode = "tap_invalid"
	// CodeTapDomainMismatch fires when a typed tap's domain (frame/packet)
	// does not match the chain point it is declared on.
	CodeTapDomainMismatch ErrorCode = "tap_domain_mismatch"
)

// Join codes (Mix, Composite, Select). Each join kind raises its own
// per-kind code, derived as <kind>_<family> by joinErrorCode below; the full
// enumeration for the built-in kinds is declared here.
const (
	// CodeMixInputs fires when Mix is given fewer than two arms.
	CodeMixInputs ErrorCode = "mix_inputs"
	// CodeMixArm fires when a Mix arm is invalid: wrong media, duplicate
	// stream ids, an unconvertible format, or a nested arm carrying .Encode.
	CodeMixArm ErrorCode = "mix_arm"
	// CodeMixDestination fires when a Mix without .Encode is routed to a
	// non-sink destination.
	CodeMixDestination ErrorCode = "mix_destination"
	// CodeMixTapArm fires when a Mix tap arm references a tap no earlier arm
	// declares.
	CodeMixTapArm ErrorCode = "mix_tap_arm"
	// CodeCompositeInputs fires when Composite is given fewer than two arms.
	CodeCompositeInputs ErrorCode = "composite_inputs"
	// CodeCompositeArm fires when a Composite arm is invalid (see CodeMixArm).
	CodeCompositeArm ErrorCode = "composite_arm"
	// CodeCompositeDestination fires when a Composite without .Encode is
	// routed to a non-sink destination.
	CodeCompositeDestination ErrorCode = "composite_destination"
	// CodeCompositeTapArm fires when a Composite tap arm references a tap no
	// earlier arm declares.
	CodeCompositeTapArm ErrorCode = "composite_tap_arm"
	// CodeSelectInputs fires when Select is given fewer than two arms.
	CodeSelectInputs ErrorCode = "select_inputs"
	// CodeSelectArm fires when a Select arm is invalid (see CodeMixArm).
	CodeSelectArm ErrorCode = "select_arm"
	// CodeSelectDestination fires when a Select's output cannot reach the
	// declared destination kind.
	CodeSelectDestination ErrorCode = "select_destination"
	// CodeSelectTapArm fires when a Select tap arm references a tap no earlier
	// arm declares.
	CodeSelectTapArm ErrorCode = "select_tap_arm"
)

// joinErrorCode derives a join refusal code from the join kind and family:
// joinErrorCode("mix", "arm") == CodeMixArm. The values for the built-in
// kinds (mix, composite, select) are enumerated above; nested joins of a
// repeated kind carry their claimed node name (mix-2_arm), and the "kind"
// family marks the internal unknown-join-kind invariant.
func joinErrorCode(kind string, family string) ErrorCode {
	return ErrorCode(kind + "_" + family)
}

// Runtime attach codes (Task.Attach / Rebranch refusals).
const (
	// CodeRuntimeBranchInvalid fires when a runtime branch spec is nil or
	// malformed.
	CodeRuntimeBranchInvalid ErrorCode = "runtime_branch_invalid"
	// CodeRuntimeBranchAnchorMissing fires when the branch's source node does
	// not exist in the running task graph.
	CodeRuntimeBranchAnchorMissing ErrorCode = "runtime_branch_anchor_missing"
	// CodeRuntimeBranchTapMissing fires when the branch's source tap does not
	// exist in the running task.
	CodeRuntimeBranchTapMissing ErrorCode = "runtime_branch_tap_missing"
	// CodeRuntimeBranchTapDuplicate fires when the branch declares a tap name
	// that already exists in the task.
	CodeRuntimeBranchTapDuplicate ErrorCode = "runtime_branch_tap_duplicate"
	// CodeRuntimeBranchNodeDuplicate fires when a branch node name already
	// exists in the task graph.
	CodeRuntimeBranchNodeDuplicate ErrorCode = "runtime_branch_node_duplicate"
	// CodeRuntimeBranchEncodeMissing fires when a muxed runtime branch has
	// neither packet copy nor an encoder.
	CodeRuntimeBranchEncodeMissing ErrorCode = "runtime_branch_encode_missing"
	// CodeRuntimeBranchEncodeDomainMismatch fires when a runtime branch
	// encodes from a packet tap; encoding needs a frame tap.
	CodeRuntimeBranchEncodeDomainMismatch ErrorCode = "runtime_branch_encode_domain_mismatch"
	// CodeRuntimeBranchDecodeDomainMismatch fires when a runtime branch
	// decodes from a frame tap; decoding needs a packet tap.
	CodeRuntimeBranchDecodeDomainMismatch ErrorCode = "runtime_branch_decode_domain_mismatch"
	// CodeRuntimeBranchDecodeCodecMissing fires when a runtime branch decode
	// has no packet codec metadata to open a decoder with.
	CodeRuntimeBranchDecodeCodecMissing ErrorCode = "runtime_branch_decode_codec_missing"
	// CodeRuntimeBranchCopyDomainMismatch fires when a runtime branch copies
	// packets from a frame tap.
	CodeRuntimeBranchCopyDomainMismatch ErrorCode = "runtime_branch_copy_domain_mismatch"
	// CodeRuntimeBranchMuxCodecMissing fires when a runtime branch's mux
	// destination has no codec metadata for the muxed stream.
	CodeRuntimeBranchMuxCodecMissing ErrorCode = "runtime_branch_mux_codec_missing"
	// CodeRuntimeBranchTransformError fires when a runtime branch transform
	// stage cannot be opened.
	CodeRuntimeBranchTransformError ErrorCode = "runtime_branch_transform_error"
	// CodeRuntimeBranchTransformMediaMismatch fires when a runtime branch
	// transform targets the wrong media kind for its tap.
	CodeRuntimeBranchTransformMediaMismatch ErrorCode = "runtime_branch_transform_media_mismatch"
	// CodeRuntimeBranchGraphError fires when the live graph rejects the branch
	// attachment.
	CodeRuntimeBranchGraphError ErrorCode = "runtime_branch_graph_error"
)

// Stream rule codes (OnStream grammar).
const (
	// CodeStreamRuleInvalid fires when an OnStream rule is malformed (no
	// matcher, no reaction, or an unusable discovered stream).
	CodeStreamRuleInvalid ErrorCode = "stream_rule_invalid"
)

// Job and compiler codes.
const (
	// CodeJobInvalid fires when a nil job or join reaches the recipe compiler;
	// an internal invariant.
	CodeJobInvalid ErrorCode = "job_invalid"
	// CodeRuntimeMissing fires when a job reaches Build with no runtime
	// configured.
	CodeRuntimeMissing ErrorCode = "runtime_missing"
	// CodeRuntimeUnsupported fires when recipe compilation is asked to run on
	// a non-goav runtime implementation.
	CodeRuntimeUnsupported ErrorCode = "runtime_unsupported"
	// CodeCompilerPassInvalid fires when a recipe compiler pass is nil; an
	// internal invariant.
	CodeCompilerPassInvalid ErrorCode = "compiler_pass_invalid"
	// CodeCompilerPassFailed fires when a compiler pass fails without its own
	// diagnostic; an internal invariant.
	CodeCompilerPassFailed ErrorCode = "compiler_pass_failed"
	// CodeRecipeGraphUnsupported fires when the recipe intent matches no
	// supported graph plan.
	CodeRecipeGraphUnsupported ErrorCode = "recipe_graph_unsupported"
	// CodeRecipeAttachmentMismatch fires when the recipe intent and its
	// concrete attachments disagree; an internal invariant.
	CodeRecipeAttachmentMismatch ErrorCode = "recipe_attachment_mismatch"
	// CodeGraphPlanInvalid fires when a media graph plan is internally
	// inconsistent; an internal invariant.
	CodeGraphPlanInvalid ErrorCode = "graph_plan_invalid"
)

// Diagnostic and decision codes: these never fail a build. They appear in
// Explain reports (info.Diagnostic / info.Decision, as plain strings) to
// record what the planner chose and why.
const (
	// CodeShapeConversionInserted records a conversion the shape solver
	// inserted under an .Auto(...) policy or a join's arm policy.
	CodeShapeConversionInserted ErrorCode = "shape_conversion_inserted"
	// CodeShapePreferenceApplied records a .Prefer(...) the solver used to
	// resolve an open choice.
	CodeShapePreferenceApplied ErrorCode = "shape_preference_applied"
	// CodeShapePreferenceIgnored records a .Prefer(...) the solver could not
	// honor; preferences never fail a build.
	CodeShapePreferenceIgnored ErrorCode = "shape_preference_ignored"
	// CodeDecodeCodecDeferred records a decode whose codec resolves at run
	// time (live inputs without probes).
	CodeDecodeCodecDeferred ErrorCode = "decode_codec_deferred"
	// CodeExplainPreflightError records a non-BuildError preflight failure on
	// an Explain report.
	CodeExplainPreflightError ErrorCode = "explain_preflight_error"
	// CodePacketCopy records the decision to keep a stream packet-encoded.
	CodePacketCopy ErrorCode = "packet_copy"
	// CodeFrameSource records the decision that a frame source flows through
	// without decode.
	CodeFrameSource ErrorCode = "frame_source"
	// CodeEventSource records the decision that an event source flows through
	// untouched.
	CodeEventSource ErrorCode = "event_source"
	// CodeDecodeRequired records the decision that the declared operations
	// need decoded frames.
	CodeDecodeRequired ErrorCode = "decode_required"
	// CodeEncodeRequired records the decision that the chain re-encodes before
	// delivery.
	CodeEncodeRequired ErrorCode = "encode_required"
	// CodeStreamRule records an OnStream rule wired onto a task.
	CodeStreamRule ErrorCode = "stream_rule"
)
