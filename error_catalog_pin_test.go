package goav_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type errorCatalogEntry struct {
	Section  string
	Constant string
	Code     string
	Kind     string
	When     string
}

type errorCatalogExample struct {
	Code          string
	Test          string
	BadRecipe     string
	RenderedError string
	Fix           string
	Cause         string
}

var errorCatalogExamples = []errorCatalogExample{
	{
		Code:          "input_invalid",
		Test:          "TestErrorAcceptanceNilProviderInput",
		BadRecipe:     `goav.From(goav.Input(nil)).Audio().To(...)`,
		RenderedError: "input constructor fixes and ErrNilSource cause are asserted by the test",
		Fix:           "pass a non-nil provider to goav.Input(provider), or use FileInput/URIInput/Source",
		Cause:         "goav.ErrNilSource",
	},
	{
		Code:          "input_missing",
		Test:          "TestErrorAcceptanceMissingInput",
		BadRecipe:     `goav.From().Copy().To(goav.File(...))`,
		RenderedError: "missing-input rendered error and start-from-input suggestion are asserted by the test",
		Fix:           `start the recipe from goav.From(goav.FileInput("in.webm", reader))`,
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "source_callback_missing",
		Test:          "TestErrorAcceptanceCustomSourceMissingCallback",
		BadRecipe:     `goav.Source("mic", shape.Frame(av.MediaAudio), nil)`,
		RenderedError: "custom source callback guidance and ErrNilSource cause are asserted by the test",
		Fix:           "pass a non-nil callback to goav.Source(name, shape, fn)",
		Cause:         "goav.ErrNilSource",
	},
	{
		Code:          "source_shape_invalid",
		Test:          "TestErrorAcceptanceCustomSourceShapeInvalid",
		BadRecipe:     `goav.Source("bad", shape.New(shape.Domain(shape.DomainPacket)), fn)`,
		RenderedError: "missing media-kind suggestions are asserted by the test",
		Fix:           "use shape.Packet(av.MediaAudio, codec) or add shape.Media(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "source_shape_unsupported",
		Test:          "TestErrorAcceptanceCustomSourceShapeUnsupported",
		BadRecipe:     `goav.Source("bytes", shape.New(shape.Domain("bytes"), shape.Media(av.MediaAudio)), fn)`,
		RenderedError: "unsupported-domain details and supported source-shape constructors are asserted by the test",
		Fix:           "use shape.Packet(...), shape.Frame(...), or shape.Event(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "source_shape_mismatch",
		Test:          "TestErrorAcceptanceFrameSourceDecodeMismatch",
		BadRecipe:     `goav.Source("frames", shape.Frame(av.MediaAudio), fn).Audio().Decode().To(...)`,
		RenderedError: "frame-source domain details and decode-removal suggestion are asserted by the test",
		Fix:           "remove .Decode() for frame-domain sources, or use shape.Packet(...) for encoded packets",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "flow_invalid",
		Test:          "TestErrorAcceptanceNilFlow",
		BadRecipe:     `.Apply(nilFlow).To(...)`,
		RenderedError: "nil-flow rendered error and Flow(name).Audio/Video creation guidance are asserted by the test",
		Fix:           "build flows with goav.Flow(name).Audio() or goav.Flow(name).Video()",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "flow_media_mismatch",
		Test:          "TestErrorAcceptanceFlowMediaMismatch",
		BadRecipe:     `.Audio().Apply(goav.Flow("thumbs").Video().Resize(...)).To(...)`,
		RenderedError: "media mismatch rendered error and matching-flow-media suggestions are asserted by the test",
		Fix:           "use goav.Flow(name).Audio() with .Audio(), or Video with .Video()",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "flow_decode_duplicate",
		Test:          "TestErrorAcceptanceFlowDecodeDuplicate",
		BadRecipe:     `goav.Flow("voice").Audio().Decode().Decode()`,
		RenderedError: "duplicate-flow-decode rendered error and single-decode suggestions are asserted by the test",
		Fix:           "call .Decode() once at the start of the flow",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "flow_decode_order_invalid",
		Test:          "TestErrorAcceptanceFlowDecodeOrderInvalid",
		BadRecipe:     `goav.Flow("voice").Audio().Resample(...).Decode()`,
		RenderedError: "flow decode order rendered error and Decode-first suggestion are asserted by the test",
		Fix:           "write goav.Flow(name).Audio().Decode().Resample(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "flow_decode_domain_mismatch",
		Test:          "TestErrorAcceptanceFlowDecodeDomainMismatch",
		BadRecipe:     `.Audio().Decode().Apply(goav.Flow("voice").Audio().Decode()).To(...)`,
		RenderedError: "frame-domain decode mismatch and omit-Decode suggestion are asserted by the test",
		Fix:           "omit .Decode() when applying the flow after stream decode",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "flow_copy_domain_mismatch",
		Test:          "TestErrorAcceptanceFlowCopyDomainMismatch",
		BadRecipe:     `goav.Flow("packets").Audio().Resample(...).Copy()`,
		RenderedError: "flow packet-copy domain mismatch and packet/encode alternatives are asserted by the test",
		Fix:           "start packet-preserving reusable work with goav.Flow(name).Audio().Copy() or encode transformed frames",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_missing",
		Test:          "TestErrorAcceptanceBranchMissing",
		BadRecipe:     `.Audio().Branches().Build(ctx)`,
		RenderedError: "empty Branches rendered error and named-branch suggestion are asserted by the test",
		Fix:           "pass branches with goav.Branch(name).Encode(...).To(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_name_missing",
		Test:          "TestErrorAcceptanceBranchNameMissing",
		BadRecipe:     `.Audio().Branches(goav.BranchSpec{}).Build(ctx)`,
		RenderedError: "stable-name refusal and branch-name guidance are asserted by the test",
		Fix:           "use branch names as handles for graph inspection and destination planning",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "input_unknown",
		Test:          "TestFromMultiInputUnknownInputNameListsInputs",
		BadRecipe:     `.Audio(goav.InputName("microphone")).To(...)` + " with inputs named mic-a and mic-b",
		RenderedError: "available input names and unsupported-build cause are asserted by the test",
		Fix:           "choose one of the listed input names",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_duplicate",
		Test:          "TestStreamRecipeRejectsSecondStreamSelectionBeforeRouting",
		BadRecipe:     `job.Audio(); job.Video(); job.Build(ctx)`,
		RenderedError: "duplicate selected-stream details and branch guidance are asserted by the test",
		Fix:           "route the first chain before selecting another, or use .Branches(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_selector_invalid",
		Test:          "TestStreamRecipeRejectsNegativeStreamIndex",
		BadRecipe:     `.Audio(goav.StreamIndex(-1)).To(...)`,
		RenderedError: "negative-index details and stream-selector suggestions are asserted by the test",
		Fix:           "use goav.StreamIndex(0) or a stable StreamID/StreamName selector",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_operation_missing",
		Test:          "TestStreamRecipeRequiresOperationForMuxOutput",
		BadRecipe:     `.Audio().To(goav.File("archive.ogg", ...))`,
		RenderedError: "missing-operation refusal is asserted by the test",
		Fix:           "add Decode/Copy/Encode work before routing to a muxed destination",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stage_missing",
		Test:          "TestStreamRecipeRejectsNilCustomStage",
		BadRecipe:     `.Audio().Do(nil).To(goav.Sink(...))`,
		RenderedError: "nil stage sentinel and FrameFunc/PacketFunc/EventFunc guidance are asserted by the test",
		Fix:           "pass a non-nil stage to .Do(stage)",
		Cause:         "goav.ErrNilStage",
	},
	{
		Code:          "multi_input_unsupported",
		Test:          "TestRecipeAndRejectsMultipleFileInputs",
		BadRecipe:     `goav.From(goav.FileInput("a.ivf", ...)).And(goav.FileInput("b.ivf", ...)).To(...)`,
		RenderedError: "multiple file input refusal and unsupported-build cause are asserted by the test",
		Fix:           "use realtime/custom sources for multi-input recipes or explicit graph composition",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_tap_missing",
		Test:          "TestErrorAcceptanceBranchTapMissing",
		BadRecipe:     `.Audio().Decode().Branches(goav.Branch("levels").From(goav.FrameTap("audio.missing")).To(...))`,
		RenderedError: "missing planned tap rendered error and tap/current-point fixes are asserted by the test",
		Fix:           `add .Tap(goav.FrameTap("audio.missing")) or omit .From(...)`,
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_decode_duplicate",
		Test:          "TestErrorAcceptanceBranchDecodeDuplicate",
		BadRecipe:     `goav.Branch("bad").Decode().Decode().To(...)`,
		RenderedError: "duplicate branch decode rendered error and single-decode suggestions are asserted by the test",
		Fix:           "call .Decode() once before frame operations",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_decode_order_invalid",
		Test:          "TestErrorAcceptanceBranchDecodeOrderInvalid",
		BadRecipe:     `goav.Branch("bad").Resample(...).Decode().To(...)`,
		RenderedError: "branch decode order rendered error and Decode-first suggestions are asserted by the test",
		Fix:           "write goav.Branch(name).Decode().Resample(...).To(target)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_decode_domain_mismatch",
		Test:          "TestErrorAcceptanceBranchDecodeDomainMismatch",
		BadRecipe:     `.Audio().Decode().Branches(goav.Branch("bad").Decode().To(...))`,
		RenderedError: "branch decode domain mismatch and parent-copy alternative are asserted by the test",
		Fix:           "omit .Decode() when the branch already starts after stream decode",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_decode_copy_invalid",
		Test:          "TestErrorAcceptanceBranchDecodeCopyInvalid",
		BadRecipe:     `goav.Branch("bad").Decode().Copy().To(...)`,
		RenderedError: "decode-then-copy rendered error and packet/frame alternatives are asserted by the test",
		Fix:           "use .Copy() for packet-preserving branches, or .Decode().Encode(codec).To(destination)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "packet_branch_encode_unsupported",
		Test:          "TestErrorAcceptancePacketBranchEncodeUnsupported",
		BadRecipe:     `.Audio().Copy().Branches(goav.Branch("bad").Encode(codec.Opus()).To(...))`,
		RenderedError: "packet-branch encode refusal and decode/copy alternatives are asserted by the test",
		Fix:           "decode before encoding, or keep packet branches as Copy",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "packet_branch_transform_unsupported",
		Test:          "TestErrorAcceptancePacketBranchTransformUnsupported",
		BadRecipe:     `.Audio().Copy().Branches(goav.Branch("bad").Resample(...).To(...))`,
		RenderedError: "packet-branch transform refusal and Decode-before-transform suggestion are asserted by the test",
		Fix:           "use .Decode().Branches(...) when branch variants need frame transforms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "transform_media_mismatch",
		Test:          "TestTransformHelperErrorContracts",
		BadRecipe:     `Resize(...) on an audio stream or Resample(...) on a video stream`,
		RenderedError: "wrong-media transform guidance is asserted by the test",
		Fix:           "use .Video().Resize(...) for video or .Audio().Resample(...) for audio",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "transform_invalid",
		Test:          "TestStreamRecipeRejectsInvalidResize",
		BadRecipe:     `.Video().Resize(0, 720).To(...)`,
		RenderedError: "invalid resize dimensions and unsupported-build cause are asserted by the test",
		Fix:           "use positive width and height",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "operation_shape_mismatch",
		Test:          "TestErrorAcceptanceCopyAfterDecode",
		BadRecipe:     `.Audio().Decode().Copy().To(goav.File(...))`,
		RenderedError: "full BuildError fields plus rendered suggestions are asserted by the test",
		Fix:           "move .Copy() before decode, or use .Encode(codec...) instead of .Copy()",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "operation_shape_mismatch",
		Test:          "TestBuildAndAttachReturnSameErrorForSameInvalidBranch",
		BadRecipe:     `Resize(...) on an audio branch at build time or runtime attach`,
		RenderedError: "shared branch wrong-media shape code and shape details are asserted by the test",
		Fix:           "use a video frame point before .Resize(...) or choose an audio transform",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_missing",
		Test:          "TestErrorAcceptanceFramesIntoContainerWithoutEncode",
		BadRecipe:     `.Audio().Decode().To(goav.File(...))`,
		RenderedError: "full BuildError fields plus rendered suggestions are asserted by the test",
		Fix:           "add .Encode(codec.Opus(...)) or route frames to goav.Sink(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_duplicate",
		Test:          "TestStreamRecipeRejectsDuplicateEncoder",
		BadRecipe:     `.Audio().Encode(codec.Opus()).Encode(codec.VP9()).To(...)`,
		RenderedError: "first/second encoder details and one-terminal-encoder guidance are asserted by the test",
		Fix:           "choose one output codec or use .Branches(...) for multiple encoded outputs",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_parameter_invalid",
		Test:          "TestBranchRecipeRejectsNegativeEncodeBitrate",
		BadRecipe:     `.Video("bad").Encode(codec.VP9(codec.Bitrate(-1))).To(...)`,
		RenderedError: "invalid bitrate details and unsupported-build cause are asserted by the test",
		Fix:           "use a non-negative bitrate",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "operation_shape_mismatch",
		Test:          "TestErrorAcceptanceTransformAfterCopy",
		BadRecipe:     `.Video().Copy().Resize(...).To(goav.File(...))`,
		RenderedError: "full BuildError fields plus rendered suggestions are asserted by the test",
		Fix:           "call .Decode() before .Resize(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_format_unknown",
		Test:          "TestErrorAcceptanceDestinationFormatUnknown",
		BadRecipe:     `.To(goav.File("out.weird", ...))`,
		RenderedError: "full BuildError fields plus rendered suggestions are asserted by the test",
		Fix:           "pass goav.Format(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_muxer_missing",
		Test:          "TestErrorAcceptanceDestinationMuxerMissing",
		BadRecipe:     `.To(goav.File("out.ogg", ...))` + " with no Ogg muxer registered",
		RenderedError: "full BuildError fields plus rendered suggestions are asserted by the test",
		Fix:           "register a muxer with goav.WithMuxer(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_invalid",
		Test:          "TestDecodeRecipeRejectsNilSinkDestination",
		BadRecipe:     `goav.Sink(nil)`,
		RenderedError: "nil sink destination guidance and ErrNilSink cause are asserted by the test",
		Fix:           "pass a non-nil sink",
		Cause:         "goav.ErrNilSink",
	},
	{
		Code:          "output_missing",
		Test:          "TestRecordRecipeRejectsMissingOutput",
		BadRecipe:     `goav.From(input).Copy().Build(ctx)`,
		RenderedError: "missing output guidance is asserted by the test",
		Fix:           "finish the recipe with .To(destination)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_scope_mixed",
		Test:          "TestStreamRecipeRejectsGenericAndStreamOutputs",
		BadRecipe:     `goav.From(input).To(jobOutput).Audio().Encode(...).To(streamOutput)`,
		RenderedError: "mixed output-scope guidance and branch alternatives are asserted by the test",
		Fix:           "keep outputs stream-local or use branches",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_kind_mixed",
		Test:          "TestStreamRecipeRejectsMixedSinkAndFile",
		BadRecipe:     `.Audio().To(goav.Sink(...), goav.File(...))`,
		RenderedError: "mixed sink/muxed output guidance is asserted by the test",
		Fix:           "use branches when one stream needs separate decoded and encoded outputs",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_writer_missing",
		Test:          "TestBranchRecipeRejectsInvalidDestination",
		BadRecipe:     `goav.File("preview.webm", nil)`,
		RenderedError: "nil writer refusal and unsupported-build cause are asserted by the test",
		Fix:           "pass a non-nil writer",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_missing",
		Test:          "TestBranchRecipeRequiresBranchDestination",
		BadRecipe:     `branchJob(input).Video("360p").Encode(...).Build(ctx)`,
		RenderedError: "missing branch destination and goav.File guidance are asserted by the test",
		Fix:           "finish the branch with .To(goav.File(...)) or .To(goav.Sink(...))",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_duplicate",
		Test:          "TestBranchRecipeRejectsDuplicateDestinations",
		BadRecipe:     `branchJob(input).Video("720p").To(File("web.webm")).Video("360p").To(File("web.webm"))`,
		RenderedError: "duplicate destination details and reuse-same-destination guidance are asserted by the test",
		Fix:           "reuse the same destination value or pass goav.DestinationGroup(...) for mux/sink groups",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_ambiguous",
		Test:          "TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates",
		BadRecipe:     `From(micA, micB).Audio().To(...)`,
		RenderedError: "candidate listing and narrowing suggestions are asserted by the test",
		Fix:           `narrow with .Audio(goav.InputName("mic-a")), goav.StreamID(...), or an index/name selector`,
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_tap_missing",
		Test:          "TestErrorAcceptanceAttachUnknownTapListsDeclaredTaps",
		BadRecipe:     `task.Attach(ctx, goav.Branch("late").From(goav.FrameTap("nope")).To(...))`,
		RenderedError: "declared tap listing and attach suggestions are asserted by the test",
		Fix:           `add .Tap(goav.FrameTap("nope")) or call Inspectable.Taps() before attaching`,
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "tap_domain_mismatch",
		Test:          "TestErrorAcceptanceTypedTapAtWrongDomain",
		BadRecipe:     `.Encode(codec.Opus()).Tap(goav.FrameTap("post-encode"))`,
		RenderedError: "domain details and typed-tap suggestions are asserted by the test",
		Fix:           "use goav.PacketTap(name) after packet-domain operations, or goav.FrameTap(name) after decode",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_adapter_missing",
		Test:          "TestErrorAcceptanceEncoderAdapterMissing",
		BadRecipe:     `.Encode(codec.Codec("weird", av.MediaAudio))`,
		RenderedError: "codec details and registration suggestion are asserted by the test",
		Fix:           "register an encoder with goav.WithEncoder(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "shape_conversion_refused",
		Test:          "TestErrorAcceptanceShapeConversionRefused",
		BadRecipe:     `.Audio().Auto().Encode(codec.Opus())` + " from 44.1kHz stereo",
		RenderedError: "conversion refusal and policy suggestions are asserted by the test",
		Fix:           "add .Auto(shape.AllowResample()) or insert .Resample(48000, 2) explicitly",
		Cause:         "goav.ErrUnsupportedBuild",
	},
}

var errorCatalogAdditionalExamples = []errorCatalogExample{
	{
		Code:          "input_duplicate",
		Test:          "TestDuplicateRealtimeInputNameErrorContract",
		BadRecipe:     `goav.From(goav.Input(named("mic"))).And(goav.Input(named("mic")))`,
		RenderedError: "duplicate realtime input name and conflicting name details are asserted by the test",
		Fix:           "give every realtime input a unique goav.Name(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "input_count_unsupported",
		Test:          "TestBranchesRejectMultiInputJobs",
		BadRecipe:     `.Branches(...) on a job with more than one input`,
		RenderedError: "single-input branch fanout requirement is asserted by the test",
		Fix:           "select one input before branch fanout, or use supported multi-input join grammar",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "input_format_unknown",
		Test:          "TestRuntimeFormatErrorContracts",
		BadRecipe:     `goav.FileInput("input.unknown", reader)` + " with no detectable format",
		RenderedError: "format-probe failure details and explicit format guidance are asserted by the test",
		Fix:           "pass a known goav.Format(...) or register a prober for the bytes",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "input_demuxer_missing",
		Test:          "TestRuntimeFormatErrorContracts",
		BadRecipe:     `goav.FileInput("input.ogg", reader)` + " with no Ogg demuxer registered",
		RenderedError: "missing input demuxer and adapter registration guidance are asserted by the test",
		Fix:           "register a demuxer with goav.WithFormatAdapter(...) or use std.MustNew(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "unsupported_input",
		Test:          "TestTranscodeAttachmentsPassRejectsInvalidConcreteAttachments",
		BadRecipe:     "a transcode/branch-composition recipe fed by a live provider input",
		RenderedError: "unsupported concrete input attachment is asserted by the test",
		Fix:           "use file/custom sources supported by that recipe shape, or regular stream grammar for live inputs",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "explicit_graph_source_missing",
		Test:          "TestRuntimeBuilderExplicitGraphValidation",
		BadRecipe:     "an expert runtime graph with stages but no source node",
		RenderedError: "explicit graph source-node requirement is asserted by the test",
		Fix:           "add a source node before connecting downstream graph stages",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_missing",
		Test:          "TestAllInputBoundStreamsAndMissingSelectionDetails",
		BadRecipe:     `.Video(goav.StreamID("camera"))` + " when no selected input has that stream",
		RenderedError: "missing stream details and available candidates are asserted by the test",
		Fix:           "choose a stream id/name/index that exists in the probed or live input metadata",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_codec_missing",
		Test:          "TestSelectDecodeStreamRequiresCodecMetadata",
		BadRecipe:     `.Decode()` + " on a selected stream whose codec id is empty",
		RenderedError: "codec metadata requirement and stream details are asserted by the test",
		Fix:           "provide codec metadata on the input stream or declare the receive codec on the provider",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_step_after_encode",
		Test:          "TestStreamRecipeRejectsProcessingAfterEncoder",
		BadRecipe:     `.Audio().Encode(codec.Opus()).Resample(...)`,
		RenderedError: "terminal-encode ordering refusal and branch guidance are asserted by the test",
		Fix:           "move transforms before .Encode(...) or use .Branches(...) for post-encode fanout",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "transform_adapter_missing",
		Test:          "TestTransformHelperErrorContracts",
		BadRecipe:     `.Audio().Resample(...)` + " without a resample filter adapter",
		RenderedError: "missing transform adapter details and registration guidance are asserted by the test",
		Fix:           "register the filter with goav.WithFilterAdapter(...) or use std.MustNewFilters(...)",
		Cause:         "filter.ErrNotFound",
	},
	{
		Code:          "transform_adapter_incompatible",
		Test:          "TestExplainReportsIncompatibleFilterDescriptor",
		BadRecipe:     `.Audio().Resample(...)` + " with a descriptor that rejects the requested shape",
		RenderedError: "incompatible filter descriptor warning/refusal is asserted by the test",
		Fix:           "register a filter descriptor compatible with the requested media and shape",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "transcode_resize_invalid",
		Test:          "TestBranchComposeTargetNamesAndResizeContracts",
		BadRecipe:     `goav.Resize(0, 720)` + " in a branch-compose target",
		RenderedError: "invalid resize dimensions are asserted by the test",
		Fix:           "use positive width and height in resize operations",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "shape_requirement_unmet",
		Test:          "TestRequireViolatedFailsWithFix",
		BadRecipe:     `.Require(shape.Frame(...))` + " that the chain cannot satisfy",
		RenderedError: "requirement mismatch and Auto/explicit-conversion guidance are asserted by the test",
		Fix:           "relax the requirement, add .Auto(...), or insert the conversion explicitly",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "shape_adapter_missing",
		Test:          "TestAutoFailsWithoutRegisteredAdapter",
		BadRecipe:     `.Auto(shape.AllowResample())` + " where no resample adapter is registered",
		RenderedError: "missing shape adapter and registration guidance are asserted by the test",
		Fix:           "register a filter that can perform the needed shape conversion",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "shape_adapter_ambiguous",
		Test:          "TestPreferResolvesAdapterAmbiguity",
		BadRecipe:     `.Auto(...)` + " with several equally valid conversion adapters and no .Prefer(...)",
		RenderedError: "ambiguous adapter candidates and Prefer guidance are asserted by the test",
		Fix:           "add .Prefer(...) to pick one conversion path",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "decode_adapter_missing",
		Test:          "TestStreamRecipeReportsProbedFileMissingDecoderBeforeOpeningInput",
		BadRecipe:     `.Audio().Decode()` + " for a probed codec with no decoder registered",
		RenderedError: "missing decoder adapter and pre-resource ordering are asserted by the test",
		Fix:           "register a decoder with goav.WithCodecAdapter(...) or use std.MustNew(...)",
		Cause:         "codec.ErrNotFound",
	},
	{
		Code:          "decode_adapter_unavailable",
		Test:          "TestStreamRecipeReportsMissingDecodeAdapterBeforeOpeningLiveInput",
		BadRecipe:     `.Video().Decode()` + " for a descriptor-only decoder in this build",
		RenderedError: "unavailable decoder adapter and build-tag guidance are asserted by the test",
		Fix:           "enable the adapter build tag or register an available decoder",
		Cause:         "codec.ErrUnavailable",
	},
	{
		Code:          "decode_adapter_incompatible",
		Test:          "TestExplainReportsIncompatibleDecodeDescriptor",
		BadRecipe:     `.Decode()` + " with a decoder descriptor incompatible with stream media/format",
		RenderedError: "incompatible decoder descriptor diagnostics are asserted by the test",
		Fix:           "register a decoder descriptor whose media and capabilities match the stream",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_adapter_unavailable",
		Test:          "TestExplainAdapterStatusContracts",
		BadRecipe:     `.Encode(codec.Opus())` + " when the descriptor is unavailable in this build",
		RenderedError: "unavailable adapter status mapping is asserted by the test",
		Fix:           "enable the adapter build tag or register an available encoder",
		Cause:         "codec.ErrUnavailable",
	},
	{
		Code:          "encode_adapter_incompatible",
		Test:          "TestExplainReportsIncompatibleEncodeDescriptor",
		BadRecipe:     `.Encode(codec.Opus(...))` + " with incompatible frame shape/capabilities",
		RenderedError: "incompatible encoder descriptor diagnostics are asserted by the test",
		Fix:           "register an encoder descriptor compatible with the requested media and frame shape",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "codec_change_policy_unsupported",
		Test:          "TestStreamRecipeRejectsUnsupportedCodecChangePolicy",
		BadRecipe:     `.Decode(codec.CodecChangePolicy(...))` + " with a custom policy",
		RenderedError: "unsupported codec-change policy refusal is asserted by the test",
		Fix:           "use one of the built-in codec-change policies",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_auto_unresolved",
		Test:          "TestStreamRecipeRejectsUnresolvedEncodeIntents",
		BadRecipe:     `.Encode(codec.Auto())`,
		RenderedError: "unresolved automatic encoder intent is asserted by the test",
		Fix:           "choose a concrete codec such as codec.Opus(...) or codec.VP8(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_work_in_progress",
		Test:          "TestReservedEncodeWorkInProgressCodeStaysExplicit",
		BadRecipe:     "reserved staged-encoder feature code, not emitted for normal codec availability",
		RenderedError: "reserved-code documentation and literal value are asserted by the test",
		Fix:           "normal encoder availability failures use encode_adapter_missing, encode_adapter_unavailable, or encode_adapter_incompatible",
		Cause:         "none (reserved)",
	},
	{
		Code:          "encode_stream_mismatch",
		Test:          "TestPrepareEncodeConfigRequiresMatchingStream",
		BadRecipe:     "prepare an encoder config for a stream different from the selected chain stream",
		RenderedError: "stream mismatch refusal is asserted by the test",
		Fix:           "prepare the encoder against the same stream facts the chain selected",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_destination_missing",
		Test:          "TestPrepareEncodeConfigRequiresTargetCodec",
		BadRecipe:     "prepare an encode stage with no target codec",
		RenderedError: "target-codec requirement is asserted by the test",
		Fix:           "pass a concrete codec spec to .Encode(codec...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "encode_branch_source_invalid",
		Test:          "TestBranchCompositionRejectsStreamEncodeBeforeBranches",
		BadRecipe:     `.Encode(...).Branches(...)` + " after the terminal stream encoder",
		RenderedError: "branch source after encode refusal is asserted by the test",
		Fix:           "branch before the terminal encoder, or move encoding into branch-local specs",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_duplicate",
		Test:          "TestRecordRecipeRejectsDuplicateOutputs",
		BadRecipe:     `.To(out, out)` + " with the same destination handle twice",
		RenderedError: "duplicate output handle/details are asserted by the test",
		Fix:           "list each destination once, or create distinct destination handles",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_format_missing",
		Test:          "TestRecordRecipeRejectsUnnamedFileWithoutFormat",
		BadRecipe:     `goav.File("", writer)` + " without explicit format",
		RenderedError: "missing output format derivation is asserted by the test",
		Fix:           "name the file with an extension or pass goav.Format(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_format_unknown",
		Test:          "TestRuntimeFormatErrorContracts",
		BadRecipe:     `goav.File("out.unknown", writer)` + " with no detectable output format",
		RenderedError: "unknown output format details and explicit-format guidance are asserted by the test",
		Fix:           "pass goav.Format(...) or use a known output extension/MIME type",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_muxer_missing",
		Test:          "TestRuntimeFormatErrorContracts",
		BadRecipe:     `goav.File("out.ogg", writer)` + " with no Ogg muxer registered",
		RenderedError: "missing output muxer and registration guidance are asserted by the test",
		Fix:           "register a muxer with goav.WithFormatAdapter(...) or use std.MustNew(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "output_destination_missing",
		Test:          "TestRecordRecipeRejectsFormatOnlyDestination",
		BadRecipe:     `goav.Destination{Format: av.FormatOgg}` + " without URI, writer, or sink",
		RenderedError: "missing destination endpoint is asserted by the test",
		Fix:           "provide a URI, writer-backed File destination, or Sink destination",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_invalid",
		Test:          "TestBranchToRefusesEmptyDestination",
		BadRecipe:     `goav.Branch("preview").To(goav.Destination{})`,
		RenderedError: "empty branch destination refusal is asserted by the test",
		Fix:           "use goav.File(...), goav.Sink(...), or another valid destination constructor",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_mux_incompatible",
		Test:          "TestDescriptorMuxCompatibilityContracts",
		BadRecipe:     "route streams/codecs to a muxer descriptor that rejects their combination",
		RenderedError: "mux compatibility issue code, format, and destination are asserted by the test",
		Fix:           "choose a compatible container/codec set or split outputs into separate destinations",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "destination_shape_mismatch",
		Test:          "TestRecipeDestinationShapePassRejectsFrameShapeForMuxDestination",
		BadRecipe:     "route frame-domain media directly to a mux destination",
		RenderedError: "frame-to-mux shape mismatch is asserted by the test",
		Fix:           "encode frames before muxing, or route raw frames to a sink",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_invalid",
		Test:          "TestNilBranchBuilderToIsRefused",
		BadRecipe:     "call methods on a nil branch builder",
		RenderedError: "nil branch builder refusal is asserted by the test",
		Fix:           "construct branches with goav.Branch(name)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_duplicate",
		Test:          "TestBranchComposeStructuredErrorContracts",
		BadRecipe:     "two planned branches share one branch name",
		RenderedError: "duplicate branch name details are asserted by the test",
		Fix:           "give every branch a unique stable name",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_source_invalid",
		Test:          "TestBranchCompositionRejectsGraphNodeSource",
		BadRecipe:     `goav.Branch("preview").From(invalidSource).To(...)`,
		RenderedError: "invalid branch source refusal is asserted by the test",
		Fix:           "anchor branches with goav.FrameTap(...), goav.PacketTap(...), or input.Stream(track)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_transform_media_mismatch",
		Test:          "TestBranchComposeStructuredErrorContracts",
		BadRecipe:     "put an audio transform on a video branch or video transform on audio",
		RenderedError: "branch transform media mismatch is asserted by the test",
		Fix:           "use transforms that match the branch media kind",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_destination_invalid",
		Test:          "TestBranchComposeStructuredErrorContracts",
		BadRecipe:     "branch destination spec is malformed",
		RenderedError: "invalid branch destination details are asserted by the test",
		Fix:           "use a valid destination constructor and stable branch destination name",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_destination_unmatched",
		Test:          "TestBranchComposeStructuredErrorContracts",
		BadRecipe:     "declare a destination selector that matches no branch",
		RenderedError: "unmatched destination selector is asserted by the test",
		Fix:           "update the destination selector or branch names so they match",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_operation_chain_unsupported",
		Test:          "TestBranchComposeStructuredErrorContracts",
		BadRecipe:     "branch compose operations grouped in an unsupported chain",
		RenderedError: "unsupported branch operation chain is asserted by the test",
		Fix:           "split unsupported grouped operations into supported stream/branch grammar",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_compose_plan_empty",
		Test:          "TestPrepareBranchComposePlanEmptyContracts",
		BadRecipe:     "branch composition planner produced no routes",
		RenderedError: "empty branch composition plan invariant is asserted by the test",
		Fix:           "ensure the recipe has at least one branch route before lowering",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_buffer_invalid",
		Test:          "TestBranchBufferRejectsInvalidPolicies",
		BadRecipe:     `goav.Branch("preview").Buffer(flow.DropOldest(0))`,
		RenderedError: "invalid branch buffer policy details are asserted by the test",
		Fix:           "use a positive capacity and non-conflicting copy options",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "branch_buffer_unsupported",
		Test:          "TestBranchBufferRejectsInvalidPolicies",
		BadRecipe:     `goav.Branch("preview").Buffer(flow.Unbounded())`,
		RenderedError: "unsupported unbounded branch buffer refusal is asserted by the test",
		Fix:           "use bounded Blocking, DropOldest, DropNewest, or Latest buffer policies",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "buffer_payload_unsafe",
		Test:          "TestCopyContractCopyNeverIsSafeOnly",
		BadRecipe:     "buffered execution receives mutable payload while copy policy forbids copying",
		RenderedError: "unsafe payload refusal and pipeline sentinel are asserted by the test",
		Fix:           "use immutable payloads or allow bounded payload copying",
		Cause:         "pipeline.ErrBufferedMessageUnsafe",
	},
	{
		Code:          "buffer_payload_too_large",
		Test:          "TestCopyContractTooSmallBoundsAreStructured",
		BadRecipe:     "buffered payload exceeds configured copy bounds",
		RenderedError: "too-large payload refusal and size sentinel are asserted by the test",
		Fix:           "raise copy bounds or use smaller immutable payloads",
		Cause:         "pipeline.ErrMessageTooLarge",
	},
	{
		Code:          "copy_branch_source_invalid",
		Test:          "TestBranchesRefuseCopyParentAfterDecode",
		BadRecipe:     `.Audio().Decode().Branches(goav.Branch("copy").Copy().To(...))`,
		RenderedError: "copy branch source-domain refusal is asserted by the test",
		Fix:           "copy from packet-domain taps or encode frame-domain branches",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "copy_unsupported",
		Test:          "TestTranscodeIntentShapePassRejectsInvalidPublicShape",
		BadRecipe:     "request packet copy from a frame-domain branch composition point",
		RenderedError: "unsupported copy shape is asserted by the test",
		Fix:           "copy before decode or encode frames before muxing",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "decode_config_conflict",
		Test:          "TestBranchCompositionRejectsConflictingDecodeConfigs",
		BadRecipe:     "branches sharing one decoder request incompatible decode configs",
		RenderedError: "decode config conflict is asserted by the test",
		Fix:           "make shared branches agree on decode config or split the decoder path",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "decode_policy_conflict",
		Test:          "TestBranchComposeStructuredErrorContracts",
		BadRecipe:     "branches sharing one decoder request different codec-change policies",
		RenderedError: "decode policy conflict is asserted by the test",
		Fix:           "use one codec-change policy for branches that share a decoder",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "tap_invalid",
		Test:          "TestStreamChainRejectsInvalidPostEncodeAndTapContracts",
		BadRecipe:     `.Tap(goav.Tap(""))`,
		RenderedError: "empty tap name refusal is asserted by the test",
		Fix:           "use a non-empty typed tap such as goav.FrameTap(\"audio.frames\")",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "join_name_invalid",
		Test:          "TestJoinRejectsInvalidNames",
		BadRecipe:     `goav.Join("Cross Fade", stage, arms...)`,
		RenderedError: "invalid custom join name variants are asserted by the test",
		Fix:           "use a snake-safe non-reserved custom join name",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "join_stage_invalid",
		Test:          "TestJoinRejectsStageMismatch",
		BadRecipe:     `goav.Join("funnel", nil, arms...)` + " or stage.Name() mismatch",
		RenderedError: "nil/mismatched join stage refusal is asserted by the test",
		Fix:           "pass a non-nil stage whose Name() equals the join name",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_inputs",
		Test:          "TestMixRequiresTwoArms",
		BadRecipe:     `goav.Mix(oneArm).To(...)`,
		RenderedError: "Mix minimum arm count is asserted by the test",
		Fix:           "pass at least two Mix arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_arm",
		Test:          "TestJoinArmsRejectMultiInputChains",
		BadRecipe:     "a Mix arm has invalid media, duplicate stream ids, or unsupported nested operations",
		RenderedError: "Mix arm validation details are asserted by the test",
		Fix:           "make every Mix arm an audio chain with a distinct stream id and supported operations",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_destination",
		Test:          "TestMixRawFramesRequireSinkDestination",
		BadRecipe:     `goav.Mix(a, b).To(goav.File("mix.ogg", writer))` + " without Encode",
		RenderedError: "raw Mix destination refusal and encode-or-sink guidance are asserted by the test",
		Fix:           "call .Encode(codec.Opus(...)) before file output or route raw frames to goav.Sink(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_tap_arm",
		Test:          "TestMixTapArmUnknownTapListsDeclaredTaps",
		BadRecipe:     `goav.Mix(chain.Tap(goav.FrameTap("dry")), goav.FrameTap("nope"))`,
		RenderedError: "unknown Mix tap arm and declared taps are asserted by the test",
		Fix:           "declare the tap on an earlier arm or reorder the arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_inputs",
		Test:          "TestCompositeRequiresTwoArms",
		BadRecipe:     `goav.Composite(oneArm).To(...)`,
		RenderedError: "Composite minimum arm count is asserted by the test",
		Fix:           "pass at least two Composite arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_arm",
		Test:          "TestCompositeRejectsDuplicateStreamIDs",
		BadRecipe:     "Composite arms declare duplicate stream ids or invalid video arms",
		RenderedError: "Composite arm validation details are asserted by the test",
		Fix:           "make every Composite arm a valid video chain with a distinct stream id",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_destination",
		Test:          "TestCompositeRawFramesRequireSinkDestination",
		BadRecipe:     `goav.Composite(a, b).To(goav.File("canvas.ivf", writer))` + " without Encode",
		RenderedError: "raw Composite destination refusal and encode-or-sink guidance are asserted by the test",
		Fix:           "call .Encode(codec.VP8(...)) before file output or route raw frames to goav.Sink(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_tap_arm",
		Test:          "TestCompositeTapArmUnknownTapListsDeclaredTaps",
		BadRecipe:     `goav.Composite(chain.Tap(goav.FrameTap("cam.frames")), goav.FrameTap("missing").Region(...))`,
		RenderedError: "unknown Composite tap arm and declared taps are asserted by the test",
		Fix:           "declare the tap on an earlier Composite arm or reorder the arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_inputs",
		Test:          "TestSelectRequiresTwoArms",
		BadRecipe:     `goav.Select(oneArm).To(...)`,
		RenderedError: "Select minimum arm count is asserted by the test",
		Fix:           "pass at least two Select arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_arm",
		Test:          "TestSelectRequiresDistinctArmIDs",
		BadRecipe:     "Select arms declare duplicate stream ids or invalid arms",
		RenderedError: "Select arm validation details are asserted by the test",
		Fix:           "make every Select arm valid and give each a distinct stream id",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_destination",
		Test:          "TestSelectRequiresSinkDestination",
		BadRecipe:     `goav.Select(a, b).To(goav.File("selected.ogg", writer))`,
		RenderedError: "Select sink-only destination refusal is asserted by the test",
		Fix:           "deliver selected frames to goav.Sink(...) or use .Branches(...) for muxed outputs",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_tap_arm",
		Test:          "TestSelectTapArmUnknownTapListsDeclaredTaps",
		BadRecipe:     `goav.Select(chain.Tap(goav.FrameTap("selected.frames")), goav.FrameTap("missing"))`,
		RenderedError: "unknown Select tap arm and declared taps are asserted by the test",
		Fix:           "declare the tap on an earlier Select arm or reorder the arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_invalid",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     `task.Attach(ctx, goav.BranchSpec{})`,
		RenderedError: "runtime branch invalid shape is asserted by the test",
		Fix:           "attach a named branch built with goav.Branch(name)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_anchor_missing",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a runtime branch from a graph anchor that no longer exists",
		RenderedError: "missing runtime anchor is asserted by the test",
		Fix:           "anchor runtime branches on taps listed by Inspectable.Taps()",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_tap_duplicate",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a runtime branch that declares a tap name already in the task",
		RenderedError: "duplicate runtime tap refusal is asserted by the test",
		Fix:           "choose a new tap name for the runtime branch",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_node_duplicate",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a runtime branch whose planned node name already exists",
		RenderedError: "duplicate runtime node refusal is asserted by the test",
		Fix:           "choose a branch/tap/destination name that produces unique runtime node names",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_encode_missing",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a muxed runtime branch with no copy or encoder",
		RenderedError: "runtime encode-missing refusal is asserted by the test",
		Fix:           "add .Copy() for packet branches or .Encode(codec...) for frame branches",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_encode_domain_mismatch",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach an encoder from a packet tap",
		RenderedError: "runtime encode domain mismatch is asserted by the test",
		Fix:           "encode from a frame tap, or decode before runtime encoding",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_decode_domain_mismatch",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a decoder from a frame tap",
		RenderedError: "runtime decode domain mismatch is asserted by the test",
		Fix:           "decode from a packet tap, or omit decode when starting from frames",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_decode_codec_missing",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a runtime decode branch from packets without codec metadata",
		RenderedError: "runtime decode codec metadata requirement is asserted by the test",
		Fix:           "publish packet taps with codec facts or declare the codec on the branch",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_copy_domain_mismatch",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a packet-copy runtime branch from a frame tap",
		RenderedError: "runtime copy domain mismatch is asserted by the test",
		Fix:           "copy from packet taps or route frame taps to sinks/encoders",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_mux_codec_missing",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a runtime mux branch without mux stream codec metadata",
		RenderedError: "runtime mux codec metadata requirement is asserted by the test",
		Fix:           "copy or encode with codec facts before muxing",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_transform_error",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "attach a runtime branch whose transform stage cannot open",
		RenderedError: "runtime transform-open failure is asserted by the test",
		Fix:           "register a compatible transform adapter before attaching",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_branch_graph_error",
		Test:          "TestRuntimeBranchStructuredErrorContracts",
		BadRecipe:     "live graph rejects a prepared runtime branch attachment",
		RenderedError: "runtime graph error and rollback path are asserted by the test",
		Fix:           "fix the branch graph shape; prepared runtime components are closed on failure",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_rule_invalid",
		Test:          "TestOnStreamValidation",
		BadRecipe:     `goav.OnStream(...)` + " with no matcher, no branch, or malformed stream",
		RenderedError: "invalid OnStream rule variants are asserted by the test",
		Fix:           "provide a stream matcher and a valid branch spec",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "job_invalid",
		Test:          "TestRecipeDiagnosticHelperContracts",
		BadRecipe:     "compile a nil job or nil join intent",
		RenderedError: "job invalid diagnostic helper fields are asserted by the test",
		Fix:           "start from goav.From(...), goav.Mix(...), goav.Composite(...), or goav.Select(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_missing",
		Test:          "TestRecipeDiagnosticHelperContracts",
		BadRecipe:     "compile a recipe without a runtime in an internal path that requires one",
		RenderedError: "runtime missing diagnostic helper fields are asserted by the test",
		Fix:           "attach a runtime with .UseRuntime(...) or use the default runtime path",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "compiler_pass_invalid",
		Test:          "TestBuildErrorAndCompilerPassErrorContracts",
		BadRecipe:     "recipe compiler assembled with a nil pass",
		RenderedError: "nil compiler pass invariant is asserted by the test",
		Fix:           "keep compiler pass lists free of nil entries",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "compiler_pass_failed",
		Test:          "TestBuildErrorAndCompilerPassErrorContracts",
		BadRecipe:     "compiler pass returns an empty BuildError with no diagnostic",
		RenderedError: "compiler pass failed wrapper is asserted by the test",
		Fix:           "make compiler passes return a fully populated BuildError diagnostic",
		Cause:         "*goav.BuildError",
	},
	{
		Code:          "recipe_graph_unsupported",
		Test:          "TestRecipeResolvedUnsupportedContracts",
		BadRecipe:     "describe/build a recipe shape that has no graph plan",
		RenderedError: "unsupported recipe graph helper is asserted by the test",
		Fix:           "use a supported front-door recipe shape",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "recipe_attachment_mismatch",
		Test:          "TestRecipeAttachmentConsistencyRejectsMismatches",
		BadRecipe:     "recipe intent and concrete attachments disagree",
		RenderedError: "attachment mismatch invariant is asserted by the test",
		Fix:           "keep intent inputs/outputs aligned with their concrete attachments",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "graph_plan_invalid",
		Test:          "TestValidateBranchComposeBranchOperationContracts",
		BadRecipe:     "media graph plan is internally inconsistent",
		RenderedError: "graph-plan invalid invariant is asserted by the test",
		Fix:           "repair the planner invariant before lowering the graph",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "buffer_budget_missing",
		Test:          "TestBufferBudgetDecisionHelperContracts",
		BadRecipe:     "buffered graph needs copy budget facts that were not propagated",
		RenderedError: "missing buffer budget refusal is asserted by the test",
		Fix:           "propagate packet/frame copy budgets before lowering buffered work",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "shape_conversion_inserted",
		Test:          "TestAutoInsertsResampleBeforeEncode",
		BadRecipe:     `.Auto(shape.AllowResample()).Encode(codec.Opus())` + " from incompatible source facts",
		RenderedError: "inserted conversion Explain diagnostic is asserted by the test",
		Fix:           "not a failure; Explain reports the inserted conversion",
		Cause:         "not an error (Explain diagnostic)",
	},
	{
		Code:          "shape_preference_applied",
		Test:          "TestPreferResolvesAdapterAmbiguity",
		BadRecipe:     `.Prefer(...)` + " resolves an otherwise open conversion choice",
		RenderedError: "applied preference Explain diagnostic is asserted by the test",
		Fix:           "not a failure; the preference was honored",
		Cause:         "not an error (Explain diagnostic)",
	},
	{
		Code:          "shape_preference_ignored",
		Test:          "TestPreferUnsatisfiableIgnoredWithDiagnostic",
		BadRecipe:     `.Prefer(...)` + " asks for a conversion the active Auto policy cannot perform",
		RenderedError: "ignored preference Explain diagnostic is asserted by the test",
		Fix:           "not a failure; add an Auto policy if the preference should become actionable",
		Cause:         "not an error (Explain diagnostic)",
	},
	{
		Code:          "decode_codec_deferred",
		Test:          "TestExplainDecodeCodecDeferredWarning",
		BadRecipe:     `.Decode()` + " where codec metadata will resolve only after input open",
		RenderedError: "deferred decode-codec Explain warning is asserted by the test",
		Fix:           "declare provider codec intent or provide probe metadata when known",
		Cause:         "not an error (Explain diagnostic)",
	},
	{
		Code:          "explain_preflight_error",
		Test:          "TestExplainPreflightErrorWarning",
		BadRecipe:     "Explain preflight returns a non-BuildError failure",
		RenderedError: "generic preflight warning mapping is asserted by the test",
		Fix:           "inspect the warning message; preflight should prefer structured BuildError when possible",
		Cause:         "not an error (Explain diagnostic)",
	},
	{
		Code:          "packet_copy",
		Test:          "TestPlanOperationSpecsContracts",
		BadRecipe:     "packet source with no frame work requested",
		RenderedError: "packet-copy planner decision is asserted by the test",
		Fix:           "not a failure; Explain records that packets are preserved",
		Cause:         "not an error (Explain decision)",
	},
	{
		Code:          "frame_source",
		Test:          "TestPlanOperationSpecsContracts",
		BadRecipe:     "custom frame source selected without decode",
		RenderedError: "frame-source planner decision is asserted by the test",
		Fix:           "not a failure; Explain records that the source already produces frames",
		Cause:         "not an error (Explain decision)",
	},
	{
		Code:          "event_source",
		Test:          "TestPlanOperationSpecsContracts",
		BadRecipe:     "custom event source selected for sink delivery",
		RenderedError: "event-source planner decision is asserted by the test",
		Fix:           "not a failure; Explain records event pass-through",
		Cause:         "not an error (Explain decision)",
	},
	{
		Code:          "decode_required",
		Test:          "TestPlanOperationSpecsContracts",
		BadRecipe:     "declared transform/encode work requires decoded frames",
		RenderedError: "decode-required planner decision is asserted by the test",
		Fix:           "not a failure; planner records that decode is required",
		Cause:         "not an error (Explain decision)",
	},
	{
		Code:          "encode_required",
		Test:          "TestPlanOperationSpecsContracts",
		BadRecipe:     "declared frame work routes to encoded output",
		RenderedError: "encode-required planner decision is asserted by the test",
		Fix:           "not a failure; planner records that encode is required",
		Cause:         "not an error (Explain decision)",
	},
	{
		Code:          "stream_rule",
		Test:          "TestOnStreamRuleVisibleInExplain",
		BadRecipe:     `goav.OnStream(...)` + " rule wired onto a task",
		RenderedError: "stream-rule Explain decision is asserted by the test",
		Fix:           "not a failure; Explain records the dynamic stream rule",
		Cause:         "not an error (Explain decision)",
	},
}

func allErrorCatalogExamples() []errorCatalogExample {
	examples := append([]errorCatalogExample(nil), errorCatalogExamples...)
	examples = append(examples, errorCatalogAdditionalExamples...)
	return examples
}

func TestErrorCatalogDocMatchesErrcodeCatalog(t *testing.T) {
	entries := readErrorCatalogEntries(t)
	generated := []byte(renderErrorCatalogDoc(entries))
	const path = "docs/ERROR_CATALOG.md"
	if os.Getenv("UPDATE_ERROR_CATALOG") == "1" {
		if err := os.WriteFile(path, generated, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v; run UPDATE_ERROR_CATALOG=1 go test -run TestErrorCatalogDocMatchesErrcodeCatalog .", path, err)
	}
	if string(current) != string(generated) {
		t.Fatalf("%s is out of date with errcode/errcode.go; run UPDATE_ERROR_CATALOG=1 go test -run TestErrorCatalogDocMatchesErrcodeCatalog .", path)
	}
}

func TestErrorCatalogEveryErrcodeHasExampleCoverage(t *testing.T) {
	entries := readErrorCatalogEntries(t)
	coverage := errorCatalogCoverage()
	for _, entry := range entries {
		if len(coverage[entry.Code]) == 0 {
			t.Fatalf("%s (%s) has no acceptance snippet coverage", entry.Constant, entry.Code)
		}
	}
}

func TestErrorCatalogCoverageMetadataIsComplete(t *testing.T) {
	knownCodes := make(map[string]bool)
	for _, entry := range readErrorCatalogEntries(t) {
		knownCodes[entry.Code] = true
	}
	knownTests := testFunctionNames(t)
	seen := make(map[string]bool)
	for _, example := range allErrorCatalogExamples() {
		label := example.Code + "/" + example.Test
		if seen[label] {
			t.Fatalf("duplicate error catalog coverage row for %s", label)
		}
		seen[label] = true
		if !knownCodes[example.Code] {
			t.Fatalf("coverage row names unknown errcode %q", example.Code)
		}
		if !knownTests[example.Test] {
			t.Fatalf("coverage row for %s names missing test %q", example.Code, example.Test)
		}
		for field, value := range map[string]string{
			"bad recipe":     example.BadRecipe,
			"rendered error": example.RenderedError,
			"fix":            example.Fix,
			"cause":          example.Cause,
		} {
			if strings.TrimSpace(value) == "" {
				t.Fatalf("coverage row for %s/%s has empty %s", example.Code, example.Test, field)
			}
			if strings.Contains(strings.ToLower(value), "todo") {
				t.Fatalf("coverage row for %s/%s leaves TODO in %s: %s", example.Code, example.Test, field, value)
			}
		}
	}
}

func TestErrorGuideDescribesCompleteCatalogCoverage(t *testing.T) {
	body, err := os.ReadFile("docs/ERRORS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "Every current catalog row names coverage") ||
		!strings.Contains(text, "If a future row appears as `catalog-only`") {
		t.Fatal("docs/ERRORS.md should describe the complete catalog coverage contract")
	}
	if strings.Contains(text, "Rows marked\n`catalog-only` still need") {
		t.Fatal("docs/ERRORS.md still describes catalog-only rows as current work")
	}
}

func readErrorCatalogEntries(t *testing.T) []errorCatalogEntry {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errcode/errcode.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var entries []errorCatalogEntry
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		section := firstDocLine(gen.Doc)
		kind := "refusal"
		if strings.HasPrefix(section, "Diagnostic and decision") {
			kind = "diagnostic"
		}
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || !isErrcodeCodeSpec(valueSpec) {
				continue
			}
			when := compactDoc(valueSpec.Doc)
			if when == "" {
				when = compactDoc(valueSpec.Comment)
			}
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					t.Fatalf("errcode/errcode.go: %s has no explicit value", name.Name)
				}
				value, ok := valueSpec.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("errcode/errcode.go: %s value is not a literal", name.Name)
				}
				entries = append(entries, errorCatalogEntry{
					Section:  section,
					Constant: "errcode." + name.Name,
					Code:     strings.Trim(value.Value, `"`),
					Kind:     kind,
					When:     when,
				})
			}
		}
	}
	if len(entries) == 0 {
		t.Fatal("no errcode.Code constants found")
	}
	return entries
}

func isErrcodeCodeSpec(spec *ast.ValueSpec) bool {
	ident, ok := spec.Type.(*ast.Ident)
	return ok && ident.Name == "Code"
}

func testFunctionNames(t *testing.T) map[string]bool {
	t.Helper()
	names := make(map[string]bool)
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "bench-results":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			if strings.HasPrefix(fn.Name.Name, "Test") {
				names[fn.Name.Name] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("no Test functions discovered")
	}
	return names
}

func renderErrorCatalogDoc(entries []errorCatalogEntry) string {
	coverage := errorCatalogCoverage()
	var b strings.Builder
	b.WriteString("# Error Catalog\n\n")
	b.WriteString("<!-- Code generated from errcode/errcode.go by TestErrorCatalogDocMatchesErrcodeCatalog; DO NOT EDIT BY HAND. -->\n\n")
	b.WriteString("This catalog is the checked index of goav's error and diagnostic codes. ")
	b.WriteString("The `Code`, `Constant`, `Section`, `Kind`, and `When it fires` columns are generated from `errcode/errcode.go`, so a new code must update the source catalog and this checked document together.\n\n")
	b.WriteString("Every current catalog row names coverage. If a future row is marked `catalog-only`, it still needs a dedicated bad recipe, rendered golden error, fixed recipe, sentinel/cause, and test name before the v1 error catalog is complete. ")
	b.WriteString("Rows naming tests already have public grammar snippets, rendered-error assertions, fix coverage, or sentinel checks in the named test.\n\n")
	b.WriteString("## Acceptance Snippet Coverage\n\n")
	b.WriteString("| Code | Test | Bad recipe | Rendered error | Fix coverage | Cause |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, example := range allErrorCatalogExamples() {
		b.WriteString(fmt.Sprintf("| `%s` | `%s` | %s | %s | %s | `%s` |\n",
			escapeMarkdown(example.Code),
			escapeMarkdown(example.Test),
			escapeMarkdown(example.BadRecipe),
			escapeMarkdown(example.RenderedError),
			escapeMarkdown(example.Fix),
			escapeMarkdown(example.Cause),
		))
	}
	b.WriteString("\n## Full Code Index\n\n")
	b.WriteString("| Section | Constant | Code | Kind | When it fires | Example coverage |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, entry := range entries {
		examples := coverage[entry.Code]
		if len(examples) == 0 {
			examples = []string{"catalog-only"}
		}
		b.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | %s | %s | %s |\n",
			escapeMarkdown(entry.Section),
			escapeMarkdown(entry.Constant),
			escapeMarkdown(entry.Code),
			escapeMarkdown(entry.Kind),
			escapeMarkdown(entry.When),
			escapeMarkdown(strings.Join(examples, ", ")),
		))
	}
	return b.String()
}

func errorCatalogCoverage() map[string][]string {
	coverage := make(map[string][]string)
	for _, example := range allErrorCatalogExamples() {
		coverage[example.Code] = append(coverage[example.Code], example.Test)
	}
	for code := range coverage {
		sort.Strings(coverage[code])
	}
	return coverage
}

func firstDocLine(group *ast.CommentGroup) string {
	text := compactDoc(group)
	if text == "" {
		return ""
	}
	if i := strings.Index(text, ". "); i >= 0 {
		return text[:i+1]
	}
	return text
}

func compactDoc(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	return strings.Join(strings.Fields(group.Text()), " ")
}

func escapeMarkdown(text string) string {
	text = strings.ReplaceAll(text, "|", `\|`)
	text = strings.ReplaceAll(text, "\n", " ")
	return text
}
