package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// mediaShapeFromCodecSpec is the shape.Spec view of a codec spec. It stays in
// goav (not the shape package) because it crosses codec.CodecSpec and the codecMedia/
// firstNonEmptyMedia helpers that live in the goav root.
func mediaShapeFromCodecSpec(spec codec.CodecSpec, domain shape.MediaDomain) shape.Spec {
	out := shape.FromCodecParameters(spec.Parameters)
	out.Domain = domain
	if out.MediaKind == "" {
		out.MediaKind = firstNonEmptyMedia(spec.Type, codecMedia(spec.ID))
	}
	if out.Codec == "" {
		out.Codec = spec.ID
	}
	return out
}

// codecSpecInputShapes is the shape.Set view of a codec spec's input. It is a
// goav function (not a method) because codec.CodecSpec's data lives in the codec
// package and shape.Set is reached through the goav-only codecMedia helper.
func codecSpecInputShapes(spec codec.CodecSpec) shape.Set {
	if spec.Copy {
		return shape.Set{shape.New(shape.Domain(shape.DomainPacket))}
	}
	media := firstNonEmptyMedia(spec.Type, spec.Parameters.Type, codecMedia(spec.ID))
	if spec.ID == "" && !spec.Auto && media == "" {
		return nil
	}
	return shape.Set{shape.Frame(media)}
}

func codecSpecOutputShapes(spec codec.CodecSpec, input shape.Spec) shape.Set {
	if spec.Copy {
		input.Domain = shape.DomainPacket
		return shape.Set{input}
	}
	out := input
	out.Domain = shape.DomainPacket
	out = shape.Merge(out, mediaShapeFromCodecSpec(spec, shape.DomainPacket))
	return shape.Set{out}
}

func (spec TransformSpec) InputShapes() shape.Set {
	switch {
	case spec.Resize != nil:
		return shape.Set{shape.Frame(av.MediaVideo)}
	case spec.Resample != nil:
		return shape.Set{shape.Frame(av.MediaAudio)}
	default:
		return nil
	}
}

func (spec TransformSpec) OutputShapes(input shape.Spec) shape.Set {
	out := input
	switch {
	case spec.Resize != nil:
		out.Domain = shape.DomainFrame
		out.MediaKind = av.MediaVideo
		out.Width = spec.Resize.Width
		out.Height = spec.Resize.Height
		if spec.Resize.PixelFormat != "" {
			out.PixelFormat = spec.Resize.PixelFormat
		}
	case spec.Resample != nil:
		out.Domain = shape.DomainFrame
		out.MediaKind = av.MediaAudio
		out.SampleRate = spec.Resample.SampleRate
		out.Channels = spec.Resample.Channels
		if spec.Resample.SampleFormat != "" {
			out.SampleFormat = spec.Resample.SampleFormat
		}
	}
	return shape.Set{out}
}

func (operation OperationSpec) InputShapes() shape.Set {
	switch operation.Kind {
	case plan.OpDecode:
		codecID := firstNonEmptyCodec(operation.Decode.ID, av.CodecID(operation.Component))
		media := firstNonEmptyMedia(operation.Decode.Type, operation.Decode.Parameters.Type, codecMedia(codecID))
		return shape.Set{shape.Packet(media, codecID)}
	case plan.OpShape:
		// A .Require(...) assertion IS an input contract: the shape walk fails
		// when the propagated shape does not satisfy it. Plain annotations
		// (.Shape facts, .Auto policies, .Prefer hints) accept anything.
		if operation.Require != nil {
			return shape.Set{*operation.Require}
		}
		return nil
	case plan.OpTransform:
		return operation.Transform.InputShapes()
	case plan.OpStage:
		// Custom stages may declare their compatibility contract; stages without
		// one accept anything (the historical behavior).
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

func (operation OperationSpec) OutputShapes(input shape.Spec) shape.Set {
	switch operation.Kind {
	case plan.OpDecode:
		input.Domain = shape.DomainFrame
		return shape.Set{input}
	case plan.OpShape:
		return shape.Set{shape.Merge(input, operation.Shape)}
	case plan.OpTransform:
		return operation.Transform.OutputShapes(input)
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
