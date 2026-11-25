package goav

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func runtimeBranchInvalidError(reason string, suggestion string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: "attach runtime branch",
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			suggestion,
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchAnchorMissingError(node string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchAnchorMissingCode),
		Code:      runtimeBranchAnchorMissingCode,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch source node does not exist in the running task graph",
		fixes: buildErrorFixes([]string{
			"call task.Taps() and attach from task.Tap(name) for stable media outlets",
			"keep the GraphNode returned by expert.Graph(runtime).Source/Stage/Sink for expert graph attachments",
			"attach from a stable decoded-frame tap when the branch needs raw frames",
		}),
		cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchTapMissingError(name string, taps []TapSnapshot) error {
	details := make([]string, 0, len(taps))
	for i := range taps {
		details = append(details, taps[i].Name+": "+string(taps[i].Domain)+" "+string(taps[i].MediaKind))
	}
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchTapMissingCode),
		Code:      runtimeBranchTapMissingCode,
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch source tap does not exist in the running task",
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"add .Tap(" + strconv.Quote(name) + ") at the point you want to attach",
			"call task.Taps() before attaching runtime branches",
			"if the tap came from a detached runtime branch, reattach that branch or choose a tap still listed by task.Taps()",
			"use task.Route(node.Out().Route()) only for expert graph-node attachments",
		}),
		cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchNodeDuplicateError(node string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchNodeDuplicateCode),
		Code:      runtimeBranchNodeDuplicateCode,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch node name already exists in the task graph",
		fixes: buildErrorFixes([]string{
			"give each attached Output a unique name with .As(name)",
			"use distinct stage and sink names inside repeated runtime branches",
		}),
		cause: pipeline.ErrNodeExists,
	}
}

func duplicateRuntimeBranchDestinationRefError(branch string, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(destinationDuplicateCode),
		Code:      destinationDuplicateCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", label),
		fields:    errDetails(errNote(fmt.Sprintf("first destination index: %d", firstIndex)), errNote(fmt.Sprintf("second destination index: %d", secondIndex))),
		fixes: buildErrorFixes([]string{
			"list each destination once in .To(...)",
			"route one runtime branch to multiple destinations with distinct values such as .To(archive, monitor)",
			"pass the same destination value to each grouped attachment branch",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchTapDuplicateError(name string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchTapDuplicateCode),
		Code:      runtimeBranchTapDuplicateCode,
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch tap name already exists in the task",
		fixes: buildErrorFixes([]string{
			"use a unique tap name for each runtime branch outlet",
			"call task.Taps() before attaching to inspect the current tap names",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchTransformMediaError(branch string, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(transformMediaMismatchCode),
		Code:      transformMediaMismatchCode,
		Operation: "attach runtime branch",
		Node:      branch,
		Reason:    transform + " applies to " + string(expected) + " frame taps",
		fields:    errDetails(errDetail("expected_shape", shape.Frame(expected).String()), errDetail("actual_shape", shape.Frame(actual).String())),
		fixes: buildErrorFixes([]string{
			"use in.Video().Decode().Tap(name) or a video transform tap before attaching .Resize(...)",
			"use in.Audio().Decode().Tap(name) or an audio transform tap before attaching .Resample(...)",
			"call task.Taps() and choose a tap with matching media kind",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchTransformError(node string, cause error) error {
	if cause == nil {
		return nil
	}
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchTransformErrorCode),
		Code:      runtimeBranchTransformErrorCode,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "runtime branch transform could not be opened",
		fixes: buildErrorFixes([]string{
			"register a matching resize or resample filter adapter",
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), or goav.New(bundle.Filters(), ...) for the bundled filters alone",
			"attach from a frame tap with media shape that match the requested transform",
		}),
		cause: cause,
	}
}

func runtimeBranchEncodeMissingError(branch string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchEncodeMissingCode),
		Code:      runtimeBranchEncodeMissingCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "muxed runtime branches need packet copy or an encoder",
		fixes: buildErrorFixes([]string{
			"attach from a packet tap without .Decode() to copy its packets",
			"call " + encodeHint("") + " when attaching from a frame tap",
			"use .To(goav.Sink(...)) when the runtime branch should receive raw frames",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchEncodeDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchEncodeDomainMismatchCode),
		Code:      runtimeBranchEncodeDomainMismatchCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch encoding requires a frame tap",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a tap declared after Decode, Resize, Resample, or a frame-stage .Do(...)",
			"route a packet tap without .Encode(...) when no re-encode is intended: task.Tap(name).To(dst)",
			"call task.Taps() and choose a tap with domain=frame",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchDecodeDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchDecodeDomainMismatchCode),
		Code:      runtimeBranchDecodeDomainMismatchCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decoding requires a packet tap",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a tap declared before Decode (on received packets) or after Encode",
			"omit .Decode() when attaching from a frame tap",
			"call task.Taps() and choose a tap with domain=packet",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchDecodeCodecMissingError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchDecodeCodecMissingCode),
		Code:      runtimeBranchDecodeCodecMissingCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decode needs packet codec metadata",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a recipe tap with codec shape",
			"declare the input codec (provider codec intent or file metadata) before building the task",
			"call task.Taps() and choose a packet tap that reports codec=...",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchCopyDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchCopyDomainMismatchCode),
		Code:      runtimeBranchCopyDomainMismatchCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch packet copy requires a packet tap",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a tap declared before Decode or after Encode",
			"encode frame taps with " + encodeHint(shape.MediaKind) + " before writing a muxed destination",
			"call task.Taps() and choose a tap with domain=packet",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchMuxCodecMissingError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchMuxCodecMissingCode),
		Code:      runtimeBranchMuxCodecMissingCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch mux destination needs codec metadata",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a recipe tap with codec shape",
			"set an explicit encoder with " + encodeHint(shape.MediaKind),
			"use .To(goav.Sink(...)) when the branch should stay raw",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchShapeDetails(shape shape.Spec) []string {
	details := []string{
		"domain=" + string(shape.Domain),
		"media=" + string(shape.MediaKind),
	}
	if shape.Codec != "" {
		details = append(details, "codec="+string(shape.Codec))
	}
	if shape.StreamID != "" {
		details = append(details, "stream="+string(shape.StreamID))
	}
	return details
}

func runtimeBranchGraphError(operation string, node string, cause error) error {
	if errors.Is(cause, pipeline.ErrUnsafeFaultDomain) {
		return unsafeIsolationError("attachment would leave an isolated failure boundary", cause)
	}
	fixes := []string{
		"build the branch before Run when using buffered execution",
	}
	if errors.Is(cause, pipeline.ErrBufferedEdgesUnsupported) {
		// A buffered branch (.Buffer(...)) needs a buffered task graph; offline
		// file tasks run direct.
		fixes = []string{
			"drop the branch's .Buffer(...) policy: this task runs a direct (offline) graph",
			"or build the task buffered: declare the buffered fork in the recipe itself (v.Buffer(...).To(dst)), or build the runtime with runconfig.WithBufferPolicy(...)",
		}
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(runtimeBranchGraphErrorCode),
		Code:      runtimeBranchGraphErrorCode,
		Operation: operation,
		Node:      node,
		Reason:    "runtime graph rejected the branch attachment",
		fixes:     buildErrorFixes(fixes),
		cause:     cause,
	}
}
