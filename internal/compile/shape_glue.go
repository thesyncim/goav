package compile

import (
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

// mediaShapeFromCodecSpec is the shape.Spec view of a codec spec. It stays in
// goav (not the shape package) because it crosses codec.Spec and the codecMedia/
// firstNonEmptyMedia helpers that live in the goav root.
func MediaShapeFromCodecSpec(spec codec.Spec, domain shape.MediaDomain) shape.Spec {
	out := shape.FromCodecParameters(spec.Parameters)
	out.Domain = domain
	if out.MediaKind == "" {
		out.MediaKind = FirstNonEmptyMedia(spec.Type, CodecMedia(spec.ID))
	}
	if out.Codec == "" {
		out.Codec = spec.ID
	}
	return out
}

// codecSpecInputShapes is the shape.Set view of a codec spec's input. It is a
// goav function (not a method) because codec.Spec's data lives in the codec
// package and shape.Set is reached through the goav-only codecMedia helper.
func codecSpecInputShapes(spec codec.Spec) shape.Set {
	if spec.Copy {
		return shape.Set{shape.New(shape.Domain(shape.DomainPacket))}
	}
	media := FirstNonEmptyMedia(spec.Type, spec.Parameters.Type, CodecMedia(spec.ID))
	if spec.ID == "" && !spec.Auto && media == "" {
		return nil
	}
	return shape.Set{shape.Frame(media)}
}

func codecSpecOutputShapes(spec codec.Spec, input shape.Spec) shape.Set {
	if spec.Copy {
		input.Domain = shape.DomainPacket
		return shape.Set{input}
	}
	out := input
	out.Domain = shape.DomainPacket
	out = shape.Merge(out, MediaShapeFromCodecSpec(spec, shape.DomainPacket))
	return shape.Set{out}
}
