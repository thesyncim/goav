// Stream selection options and the refusals the stream validator raises.

package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func validateRecipeStreamSelector(operation string, node string, selector av.StreamSelector) error {
	if selector.Index >= 0 {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamSelectorInvalidCode),
		Code:      streamSelectorInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream index must be non-negative",
		fields:    errDetails(errDetail("index", fmt.Sprintf("%d", selector.Index))),
		fixes: buildErrorFixes([]string{
			"use goav.StreamIndex(0) for the first matching stream",
			"use goav.StreamID(...) when stream metadata is stable",
		}),
		cause: errUnsupportedBuild,
	}
}

func codecIntentSet(spec codec.Spec) bool {
	return spec.ID != "" || spec.Auto || spec.Copy
}

func chainStepAfterEncodeError(operation string, node string, step string, encode codec.Spec) error {
	if encode.Copy && step != "decode" {
		return chainStepOnPacketCopyError(operation, node, step)
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamStepAfterEncodeCode),
		Code:      streamStepAfterEncodeCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream processing steps must be declared before the encoder",
		fields:    errDetails(errNote("step: "+step), errNote("encoder: "+codecIntentName(encode))),
		fixes: buildErrorFixes([]string{
			"place .Do(...), .Resize(...), or .Resample(...) before .Encode(...)",
			"call .To(...) after the encoder to attach outputs",
		}),
		cause: errUnsupportedBuild,
	}
}

// chainStepOnPacketCopyError is the packet-domain transform refusal: .Copy()
// keeps the stream packet-encoded, so frame operations declared after it have
// no decoded frames to consume. The fix is the domain rule itself: decode
// first, or keep the chain a pure packet copy.
func chainStepOnPacketCopyError(operation string, node string, step string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    step + " needs decoded frames, but a stream without .Decode() copies its packets",
		fields:    errDetails(errNote("step: "+step), errDetail("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String()), errDetail("expected_shape", shape.New(shape.Domain(shape.DomainFrame)).String())),
		fixes: buildErrorFixes([]string{
			"call .Decode() before .Resize(...), .Resample(...), or .Do(...) — transforms run on decoded frames",
			"remove the processing step to keep a pure packet copy",
			"reuse the Stream to fork a packet copy and a processed stream: s.To(copy), s.Decode()...To(out)",
		}),
		cause: errUnsupportedBuild,
	}
}

func chainFrameInputRequiredError(operation string, node string, step string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    step + " needs decoded frames, but the selected stream is still packet-domain",
		fields:    errDetails(errDetail("step", step), errDetail("actual_shape", shape.New(shape.Domain(shape.DomainPacket)).String()), errDetail("expected_shape", shape.New(shape.Domain(shape.DomainFrame)).String())),
		fixes: buildErrorFixes([]string{
			"write .Decode()." + streamStepMethodName(step) + "(...) for decoded-frame processing",
			"keep a packet copy by leaving out .Decode() and the frame-domain processing",
		}),
		cause: errUnsupportedBuild,
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
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeDuplicateCode),
		Code:      encodeDuplicateCode,
		Operation: operation,
		Node:      node,
		Reason:    "stream recipes allow one terminal encoder",
		fields:    errDetails(errNote("first encoder: "+codecIntentName(first)), errNote("second encoder: "+codecIntentName(second))),
		fixes: buildErrorFixes([]string{
			"choose one output codec for the stream chain",
			"fork the decoded Stream when one input needs several encodings: v.Encode(...).To(a), v.Encode(...).To(b)",
		}),
		cause: errUnsupportedBuild,
	}
}

func copyEncodeSpellingError(operation string, node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeParameterInvalidCode),
		Code:      encodeParameterInvalidCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    "codec.Copy() is the internal packet-copy codec; a recipe stream copies its packets when it does not decode",
		fixes: buildErrorFixes([]string{
			"drop .Decode() and .Encode(codec.Copy()): a stream routed without .Decode() copies its packets",
			"use " + encodeHint("") + " for real encoders",
		}),
		cause: errUnsupportedBuild,
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

// StreamOption narrows the stream selection made by the input selectors
// (input.Audio, input.Video, input.Stream); construct values with StreamID or
// StreamIndex. It is sealed: the config it acts on is unexported, so those
// constructors are the only way to build one.
type StreamOption func(*streamSelectConfig)

type streamSelectConfig struct {
	selector av.StreamSelector
}

func newStreamSelectConfig(media av.MediaType, options ...StreamOption) streamSelectConfig {
	config := streamSelectConfig{selector: av.StreamSelector{Type: media}}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return config
}

// StreamID narrows a stream selection to the stream with the given id, as
// probed or declared by the input.
func StreamID(id av.StreamID) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.ID = id
	}
}

// StreamIndex narrows a stream selection to the stream at the given probe
// index (0-based).
func StreamIndex(index int) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.Index = index
		config.selector.UseIndex = true
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
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(sourceShapeMismatchCode),
		Code:      sourceShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources are already decoded frames",
		fields:    errDetails(errDetail("source_domain", "frame"), errDetail("operation", "decode")),
		fixes: buildErrorFixes([]string{
			"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
			"use shape.Packet(...) when the custom source pushes encoded packets",
		}),
		cause: errUnsupportedBuild,
	}
}

func streamSelectFromAV(selector av.StreamSelector) plan.StreamSelect {
	return plan.StreamSelect{
		ID:       selector.ID,
		Index:    selector.Index,
		UseIndex: selector.UseIndex,
		Type:     selector.Type,
		Codec:    selector.Codec,
		Name:     selector.Name,
	}
}

// joinLayerNilError refuses a nil Composite layer: every layer is a Stream
// or a Stream placed with At.
func joinLayerNilError(join string, index int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Node:      join,
		Reason:    fmt.Sprintf("%s input %d is nil", join, index+1),
		fixes: buildErrorFixes([]string{
			"pass every Composite layer as a Stream or a placed Stream: goav.Composite(cam, goav.At(screen, 640, 0))",
		}),
		cause: errUnsupportedBuild,
	}
}
