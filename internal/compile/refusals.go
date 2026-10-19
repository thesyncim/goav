package compile

import (
	"cmp"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func validateRecipeStreamSelector(operation string, node string, selector av.StreamSelector) error {
	if selector.Index >= 0 {
		return nil
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.StreamSelectorInvalidCode),
		Code:      builderr.StreamSelectorInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream index must be non-negative",
		Details:   builderr.Details(builderr.KV("index", fmt.Sprintf("%d", selector.Index))),
		Fixes: builderr.Fixes([]string{
			"use goav.StreamIndex(0) for the first matching stream",
			"use goav.StreamID(...) when stream metadata is stable",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func codecIntentSet(spec codec.Spec) bool {
	return spec.ID != "" || spec.Auto || spec.Copy
}

func chainStepAfterEncodeError(operation string, node string, step string, encode codec.Spec) error {
	if encode.Copy && step != "decode" {
		return chainStepOnPacketCopyError(operation, node, step)
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.StreamStepAfterEncodeCode),
		Code:      builderr.StreamStepAfterEncodeCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream processing steps must be declared before the encoder",
		Details:   builderr.Details(builderr.Note("step: "+step), builderr.Note("encoder: "+codecIntentName(encode))),
		Fixes: builderr.Fixes([]string{
			"place .Do(...), .Resize(...), or .Resample(...) before .Encode(...)",
			"call .To(...) after the encoder to attach outputs",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// chainStepOnPacketCopyError is the packet-domain transform refusal: .Copy()
// keeps the stream packet-encoded, so frame operations declared after it have
// no decoded frames to consume. The fix is the domain rule itself: decode
// first, or keep the chain a pure packet copy.
func chainStepOnPacketCopyError(operation string, node string, step string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.OperationShapeMismatchCode),
		Code:      builderr.OperationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    step + " needs decoded frames, but a stream without .Decode() copies its packets",
		Details:   builderr.Details(builderr.Note("step: "+step), builderr.KV("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String()), builderr.KV("expected_shape", shape.New(shape.Domain(shape.DomainFrame)).String())),
		Fixes: builderr.Fixes([]string{
			"call .Decode() before .Resize(...), .Resample(...), or .Do(...) — transforms run on decoded frames",
			"remove the processing step to keep a pure packet copy",
			"reuse the Stream to fork a packet copy and a processed stream: s.To(copy), s.Decode()...To(out)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func chainFrameInputRequiredError(operation string, node string, step string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.OperationShapeMismatchCode),
		Code:      builderr.OperationShapeMismatchCode,
		Operation: operation,
		Node:      cmp.Or(node, "stream"),
		Reason:    step + " needs decoded frames, but the selected stream is still packet-domain",
		Details:   builderr.Details(builderr.KV("step", step), builderr.KV("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String()), builderr.KV("expected_shape", shape.New(shape.Domain(shape.DomainFrame)).String())),
		Fixes: builderr.Fixes([]string{
			"write .Decode()." + streamStepMethodName(step) + "(...) for decoded-frame processing",
			"keep a packet copy by leaving out .Decode() and the frame-domain processing",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func streamStepMethodName(step string) string {
	switch step {
	case "custom stage":
		return "Do"
	case "flow":
		return "Apply"
	case "encode":
		return "Encode"
	default:
		if step == "" {
			return "Decode"
		}
		return string(step[0]-('a'-'A')) + step[1:]
	}
}

func duplicateStreamEncodeError(operation string, node string, first codec.Spec, second codec.Spec) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.EncodeDuplicateCode),
		Code:      builderr.EncodeDuplicateCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream recipes allow one terminal encoder",
		Details:   builderr.Details(builderr.Note("first encoder: "+codecIntentName(first)), builderr.Note("second encoder: "+codecIntentName(second))),
		Fixes: builderr.Fixes([]string{
			"choose one output codec for the stream chain",
			"fork the decoded Stream when one input needs several encodings: v.Encode(...).To(a), v.Encode(...).To(b)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func copyEncodeSpellingError(operation string, node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.EncodeParameterInvalidCode),
		Code:      builderr.EncodeParameterInvalidCode,
		Operation: operation,
		Node:      cmp.Or(node, "stream"),
		Reason:    "codec.Copy() is the internal packet-copy codec; a recipe stream copies its packets when it does not decode",
		Fixes: builderr.Fixes([]string{
			"drop .Decode() and .Encode(codec.Copy()): a stream routed without .Decode() copies its packets",
			"use " + builderr.EncodeHint("") + " for real encoders",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func codecIntentName(spec codec.Spec) string {
	switch {
	case spec.Auto:
		return "auto"
	case spec.Copy:
		return "copy"
	case spec.ID != "":
		return string(spec.ID)
	default:
		return "none"
	}
}

// stageRequiresFrameInput reports whether a custom .Do(stage) needs decoded
// frames, so a packet-domain chain must .Decode() first. Per the operation
// contract only frame stages do: a PacketFunc consumes packets and an EventFunc
// consumes events directly. Stages that declare FrameOnlyInput (the component
// func stages) answer for themselves; any other custom stage keeps the
// conservative default of requiring frames, since its domain is unknown here
// and the shape solver still validates it.
func stageRequiresFrameInput(stage pipeline.Stage) bool {
	if domained, ok := stage.(interface{ FrameOnlyInput() bool }); ok {
		return domained.FrameOnlyInput()
	}
	return true
}

func frameSourceDecodeError(operation string, node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.SourceShapeMismatchCode),
		Code:      builderr.SourceShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources are already decoded frames",
		Details:   builderr.Details(builderr.KV("source_domain", "frame"), builderr.KV("operation", "decode")),
		Fixes: builderr.Fixes([]string{
			"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
			"use shape.Packet(...) when the custom source pushes encoded packets",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func StreamSelectFromAV(selector av.StreamSelector) plan.StreamSelect {
	return plan.StreamSelect{
		ID:       selector.ID,
		Index:    selector.Index,
		UseIndex: selector.UseIndex,
		Type:     selector.Type,
		Codec:    selector.Codec,
		Name:     selector.Name,
	}
}

func duplicateBranchDecodeError(node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.BranchDecodeDuplicateCode),
		Code:      builderr.BranchDecodeDuplicateCode,
		Operation: "build branch",
		Node:      node,
		Reason:    "branch already decodes its input packets",
		Fixes: builderr.Fixes([]string{
			"call .Decode() once before frame operations",
			"remove the second .Decode() call",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func branchDecodeDomainError(node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.BranchDecodeDomainMismatchCode),
		Code:      builderr.BranchDecodeDomainMismatchCode,
		Operation: "build branches",
		Node:      node,
		Reason:    "branch decoding requires a packet-domain stream point",
		Fixes: builderr.Fixes([]string{
			"omit .Decode() when the fork already starts after the stream's .Decode()",
			"fork before .Decode() when a packet copy and a decoding fork share the stream: s := in.Video()",
			"attach runtime decode branches from packet taps",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func branchDestinationInvalidError(name string, reason string) error {
	return DestinationInvalidError("build branch", cmp.Or(name, "branch"), reason)
}

func DestinationInvalidError(operation string, node string, reason string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DestinationInvalidCode),
		Code:      builderr.DestinationInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			"use goav.Write(...), goav.URI(...), or goav.Sink(...) for a real destination",
			"pass one destination value to every chain that should share it",
			"use distinct destination values for independent outputs",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func GraphPlanInvalidError(reason string, details []string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.GraphPlanInvalidCode),
		Code:      builderr.GraphPlanInvalidCode,
		Operation: "build graph plan",
		Reason:    reason,
		Details:   builderr.DetailLines(append([]string(nil), details...)),
		Fixes: builderr.Fixes([]string{
			"compile recipes through Streams, joins, and Outputs: rt.Run(ctx, outputs...)",
			"keep graph-plan nodes, edges, operations, and destinations in sync",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func StreamRuleInvalidError(node string, reason string, suggestion string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.StreamRuleInvalid),
		Code:      errcode.StreamRuleInvalid,
		Operation: "build stream rule",
		Node:      cmp.Or(node, "rule"),
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			suggestion,
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}
