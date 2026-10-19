package compile

import (
	"cmp"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func ValidateRecipeIROperationShapes(operation string, stream recipeir.Stream, operations []recipeir.Operation, initial shape.Spec) error {
	shape := NormalizeTapShape(initial)
	if shape.MediaKind == "" {
		shape.MediaKind = stream.Selector.Type
	}
	if shape.Codec == "" {
		shape.Codec = stream.Selector.Codec
	}
	node := cmp.Or(stream.Name, string(stream.Selector.ID), string(stream.Selector.Type), "stream")
	for i := range operations {
		next := operations[i]
		if next.Kind == plan.OpTap {
			shape = RecipeIROperationOutputShape(shape, next)
			continue
		}
		if next.Kind == plan.OpShape && next.Require == nil {
			if err := ValidateShapeAnnotationDomain(operation, node, i, shape, next.Shape); err != nil {
				return err
			}
			shape = RecipeIROperationOutputShape(shape, next)
			continue
		}
		expected := RecipeIROperationInputShapes(next)
		if len(expected) != 0 && !expected.Accepts(shape) {
			return recipeIROperationShapeFailureError(operation, node, i, next, expected, shape)
		}
		shape = RecipeIROperationOutputShape(shape, next)
	}
	return nil
}

func RecipeIROperationInputShapes(operation recipeir.Operation) shape.Set {
	switch operation.Kind {
	case plan.OpDecode:
		codecID := FirstNonEmptyCodec(operation.Decode.ID, av.CodecID(operation.Component))
		media := FirstNonEmptyMedia(operation.Decode.Type, operation.Decode.Parameters.Type, CodecMedia(codecID))
		return shape.Set{shape.Packet(media, codecID)}
	case plan.OpShape:
		if operation.Require != nil {
			return shape.Set{*operation.Require}
		}
		return nil
	case plan.OpTransform:
		return recipeIRTransformInputShapes(operation.Transform)
	case plan.OpStage:
		if contract, ok := operation.Stage.(shape.Contract); ok {
			return contract.InputShapes()
		}
		return nil
	case plan.OpEncode, plan.OpCopy:
		return codecSpecInputShapes(operation.Encode)
	default:
		return nil
	}
}

func RecipeIROperationOutputShape(input shape.Spec, operation recipeir.Operation) shape.Spec {
	out := RecipeIROperationOutputShapes(input, operation)
	if len(out) == 0 {
		return input
	}
	return out[0]
}

func RecipeIROperationOutputShapes(input shape.Spec, operation recipeir.Operation) shape.Set {
	switch operation.Kind {
	case plan.OpDecode:
		input.Domain = shape.DomainFrame
		return shape.Set{input}
	case plan.OpShape:
		return shape.Set{shape.Merge(input, operation.Shape)}
	case plan.OpTransform:
		return recipeIRTransformOutputShapes(operation.Transform, input)
	case plan.OpStage:
		if contract, ok := operation.Stage.(shape.Contract); ok {
			if out := contract.OutputShapes(input); len(out) != 0 {
				return out
			}
		}
		return shape.Set{input}
	case plan.OpEncode, plan.OpCopy:
		return codecSpecOutputShapes(operation.Encode, input)
	default:
		return shape.Set{input}
	}
}

func recipeIRTransformInputShapes(transform recipeir.Transform) shape.Set {
	switch transform.Kind {
	case recipeir.TransformResize:
		return shape.Set{shape.Frame(av.MediaVideo)}
	case recipeir.TransformResample:
		return shape.Set{shape.Frame(av.MediaAudio)}
	default:
		return nil
	}
}

func recipeIRTransformOutputShapes(transform recipeir.Transform, input shape.Spec) shape.Set {
	out := input
	switch transform.Kind {
	case recipeir.TransformResize:
		out.Domain = shape.DomainFrame
		out.MediaKind = av.MediaVideo
		out.Width = transform.Resize.Width
		out.Height = transform.Resize.Height
		if transform.Resize.PixelFormat != "" {
			out.PixelFormat = transform.Resize.PixelFormat
		}
	case recipeir.TransformResample:
		out.Domain = shape.DomainFrame
		out.MediaKind = av.MediaAudio
		out.SampleRate = transform.Resample.SampleRate
		out.Channels = transform.Resample.Channels
		if transform.Resample.SampleFormat != "" {
			out.SampleFormat = transform.Resample.SampleFormat
		}
	}
	return shape.Set{out}
}

func recipeIROperationComponent(operation recipeir.Operation) string {
	switch operation.Kind {
	case plan.OpDecode:
		return cmp.Or(string(operation.Decode.ID), operation.Component, "decode")
	case plan.OpTransform:
		return cmp.Or(RecipeIRTransformFactoryName(operation.Transform), "transform")
	case plan.OpEncode:
		return cmp.Or(string(operation.Encode.ID), operation.Component, "encode")
	case plan.OpCopy:
		return "packet-copy"
	default:
		return operation.Component
	}
}

func RecipeIRTransformEmpty(transform recipeir.Transform) bool {
	return transform.Kind == "" || RecipeIRTransformFactoryName(transform) == ""
}

func recipeIROperationShapeFailureError(operation string, node string, index int, step recipeir.Operation, expected shape.Set, actual shape.Spec) error {
	if step.Kind == plan.OpShape && step.Require != nil {
		return recipeIRShapeRequirementUnmetError(operation, node, index, step, expected, actual)
	}
	return recipeIROperationShapeMismatchError(operation, node, index, step, expected, actual)
}

func recipeIRShapeRequirementUnmetError(operation string, node string, index int, step recipeir.Operation, expected shape.Set, actual shape.Spec) error {
	required := shape.Spec{}
	if step.Require != nil {
		required = *step.Require
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.ShapeRequirementUnmetCode),
		Code:      builderr.ShapeRequirementUnmetCode,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf(".Require(...) is not satisfied: the stream is %s, required %s",
			HumanizeShape(actual), HumanizeShape(required)),
		Details: builderr.Details(builderr.KV("operation_index", fmt.Sprintf("%d", index)), builderr.KV("operation", "require"), builderr.KV("source", HumanizeShape(actual)), builderr.KV("actual_shape", actual.String()), builderr.KV("expected_shape", shapeSetString(expected))),
		Fixes: builderr.Fixes([]string{
			"adjust the chain so the stream satisfies the required shape before .Require(...)",
			"relax or remove the .Require(...) assertion",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func recipeIROperationShapeMismatchError(operation string, node string, index int, step recipeir.Operation, expected shape.Set, actual shape.Spec) error {
	component := cmp.Or(step.Component, recipeIROperationComponent(step), string(step.Kind), "operation")
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.OperationShapeMismatchCode),
		Code:      builderr.OperationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    component + " cannot consume the current media shape",
		Details:   builderr.Details(builderr.KV("operation_index", fmt.Sprintf("%d", index)), builderr.KV("operation", string(step.Kind)), builderr.KV("expected_shape", shapeSetString(expected)), builderr.KV("actual_shape", actual.String())),
		Fixes:     builderr.Fixes(recipeIROperationShapeMismatchSuggestions(step)),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

func recipeIROperationShapeMismatchSuggestions(operation recipeir.Operation) []string {
	switch operation.Kind {
	case plan.OpDecode:
		return []string{
			"decode only consumes packet-domain media",
			"remove duplicate .Decode() calls after a frame tap",
			"attach from a packet tap (before .Decode()) when a runtime branch should decode",
		}
	case plan.OpTransform:
		return []string{
			"call .Decode() before frame transforms when starting from packets",
			"use .Video().Resize(...) for video frames",
			"use .Audio().Resample(...) for audio frames",
		}
	case plan.OpEncode:
		return []string{
			"call .Decode() before encoding when starting from packets",
			"keep .Shape(...) annotations in the frame domain before encoders",
			"leave out .Decode() and the encoder for packet-preserving fanout",
		}
	case plan.OpCopy:
		return []string{
			"copy only consumes packet-domain media",
			"leave out .Decode() to copy, or attach from a packet tap (before .Decode())",
			"use .Encode(codec...) to turn decoded frames back into packets",
			"use a sink destination when the branch should remain decoded",
		}
	default:
		return []string{
			"inspect Explain(ctx) to see operation shapes",
			"keep structural facts in goav.Shape(...) and codec behavior in codec.Spec options",
		}
	}
}

func OpCopy() recipeir.Operation {
	return recipeir.Operation{Kind: plan.OpCopy, Component: "packet-copy", Detail: "explicit packet copy", Encode: codec.Copy()}
}

// operationsAutoPolicy unions a stream's .Auto(...) policies; the second
// result reports whether any is present — an empty .Auto() activates solving
// while allowing nothing, so refusals name the exact policy to add.
func operationsAutoPolicy(operations []recipeir.Operation) (shape.Policy, bool) {
	var policy shape.Policy
	active := false
	for i := range operations {
		if operations[i].Auto == nil {
			continue
		}
		active = true
		policy = policy.Union(*operations[i].Auto)
	}
	return policy, active
}

func OperationsHaveDecode(operations []recipeir.Operation) bool {
	return recipeIROperationsContainKind(operations, plan.OpDecode)
}

// OperationsEncodeSpec is a stream's encoder (or packet copy).
func OperationsEncodeSpec(operations []recipeir.Operation) codec.Spec {
	for i := range operations {
		if operations[i].Kind == plan.OpEncode || operations[i].Kind == plan.OpCopy {
			return operations[i].Encode
		}
	}
	return codec.Spec{}
}

func RecipeIRTransformFactoryName(transform recipeir.Transform) string {
	switch transform.Kind {
	case recipeir.TransformResize:
		return filter.FactoryResize
	case recipeir.TransformResample:
		return filter.FactoryResample
	default:
		return ""
	}
}
