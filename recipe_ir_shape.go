package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func validateRecipeIROperationShapes(operation string, stream streamIntent, operations []recipeir.Operation, initial shape.Spec) error {
	shape := normalizeTapShape(initial)
	if shape.MediaKind == "" {
		shape.MediaKind = stream.Select.Type
	}
	if shape.Codec == "" {
		shape.Codec = stream.Select.Codec
	}
	node := firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
	for i := range operations {
		next := operations[i]
		if next.Kind == plan.OpTap || (next.Kind == plan.OpShape && next.Require == nil) {
			shape = recipeIROperationOutputShape(shape, next)
			continue
		}
		expected := recipeIROperationInputShapes(next)
		if len(expected) != 0 && !expected.Accepts(shape) {
			return recipeIROperationShapeFailureError(operation, node, i, next, expected, shape)
		}
		shape = recipeIROperationOutputShape(shape, next)
	}
	return nil
}

func recipeIROperationInputShapes(operation recipeir.Operation) shape.Set {
	switch operation.Kind {
	case plan.OpDecode:
		codecID := firstNonEmptyCodec(operation.Decode.ID, av.CodecID(operation.Component))
		media := firstNonEmptyMedia(operation.Decode.Type, operation.Decode.Parameters.Type, codecMedia(codecID))
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

func recipeIROperationOutputShape(input shape.Spec, operation recipeir.Operation) shape.Spec {
	out := recipeIROperationOutputShapes(input, operation)
	if len(out) == 0 {
		return input
	}
	return out[0]
}

func recipeIROperationOutputShapes(input shape.Spec, operation recipeir.Operation) shape.Set {
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

func recipeIROperationCodec(operation recipeir.Operation) codec.CodecSpec {
	switch operation.Kind {
	case plan.OpDecode:
		return cloneCodecSpec(operation.Decode)
	case plan.OpEncode, plan.OpCopy:
		return cloneCodecSpec(operation.Encode)
	default:
		return codec.CodecSpec{}
	}
}

func recipeIROperationComponent(operation recipeir.Operation) string {
	switch operation.Kind {
	case plan.OpDecode:
		return firstNonEmpty(string(operation.Decode.ID), operation.Component, "decode")
	case plan.OpTransform:
		return firstNonEmpty(recipeIRTransformFactoryName(operation.Transform), "transform")
	case plan.OpEncode:
		return firstNonEmpty(string(operation.Encode.ID), operation.Component, "encode")
	case plan.OpCopy:
		return "packet-copy"
	default:
		return operation.Component
	}
}

func recipeIROperationTapIsTerminalPacket(operation recipeir.Operation) bool {
	if operation.Kind != plan.OpTap {
		return false
	}
	return operation.Tap.Domain == shape.DomainPacket &&
		(operation.Tap.After == plan.OpEncode || operation.Tap.After == plan.OpCopy)
}

func recipeIRTransformFactoryName(transform recipeir.Transform) string {
	switch transform.Kind {
	case recipeir.TransformResize:
		return filter.FactoryResize
	case recipeir.TransformResample:
		return filter.FactoryResample
	default:
		return ""
	}
}

func recipeIRTransformEmpty(transform recipeir.Transform) bool {
	return transform.Kind == "" || recipeIRTransformFactoryName(transform) == ""
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
	return &BuildError{
		Family:    errcode.FamilyForCode(shapeRequirementUnmetCode),
		Code:      shapeRequirementUnmetCode,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf(".Require(...) is not satisfied: the stream is %s, required %s",
			humanizeShape(actual), humanizeShape(required)),
		fields: buildErrorFields([]string{
			fmt.Sprintf("operation_index=%d", index),
			"operation=require",
			"source=" + humanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + shapeSetString(expected),
		}),
		fixes: buildErrorFixes([]string{
			"adjust the chain so the stream satisfies the required shape before .Require(...)",
			"relax or remove the .Require(...) assertion",
		}),
		cause: errUnsupportedBuild,
	}
}

func recipeIROperationShapeMismatchError(operation string, node string, index int, step recipeir.Operation, expected shape.Set, actual shape.Spec) error {
	component := firstNonEmpty(step.Component, recipeIROperationComponent(step), string(step.Kind), "operation")
	return &BuildError{
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    component + " cannot consume the current media shape",
		fields: buildErrorFields([]string{
			fmt.Sprintf("operation_index=%d", index),
			"operation=" + string(step.Kind),
			"expected_shape=" + shapeSetString(expected),
			"actual_shape=" + actual.String(),
		}),
		fixes: buildErrorFixes(recipeIROperationShapeMismatchSuggestions(step)),
		cause: errUnsupportedBuild,
	}
}

func recipeIROperationShapeMismatchSuggestions(operation recipeir.Operation) []string {
	switch operation.Kind {
	case plan.OpDecode:
		return []string{
			"decode only consumes packet-domain media",
			"remove duplicate .Decode() calls after a frame tap",
			"start from goav.PacketTap(name) when a runtime branch should decode",
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
			"use .Copy() instead of an encoder for packet-preserving fanout",
		}
	case plan.OpCopy:
		return []string{
			"copy only consumes packet-domain media",
			"move .Copy() before decode or start from goav.PacketTap(name)",
			"use .Encode(codec...) instead of .Copy() to turn decoded frames back into packets",
			"use a sink destination when the branch should remain decoded",
		}
	default:
		return []string{
			"inspect Explain(ctx) to see operation shapes",
			"keep structural facts in goav.Shape(...) and codec behavior in codec.CodecSpec options",
		}
	}
}
