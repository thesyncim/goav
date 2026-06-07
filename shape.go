package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
)

type ShapeContract interface {
	InputShapes() ShapeSet
	OutputShapes(MediaShape) ShapeSet
}

type ShapeOption func(*MediaShape)

func Shape(options ...ShapeOption) MediaShape {
	var shape MediaShape
	for i := range options {
		if options[i] != nil {
			options[i](&shape)
		}
	}
	return shape
}

func PacketShape(media av.MediaType, codec av.CodecID, options ...ShapeOption) MediaShape {
	options = append([]ShapeOption{ShapeDomain(DomainPacket), ShapeMedia(media), ShapeCodec(codec)}, options...)
	return Shape(options...)
}

func FrameShape(media av.MediaType, options ...ShapeOption) MediaShape {
	options = append([]ShapeOption{ShapeDomain(DomainFrame), ShapeMedia(media)}, options...)
	return Shape(options...)
}

func EventShape(options ...ShapeOption) MediaShape {
	options = append([]ShapeOption{ShapeDomain(DomainEvent)}, options...)
	return Shape(options...)
}

func ShapeDomain(domain MediaDomain) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.Domain = domain
		}
	}
}

func ShapeMedia(media av.MediaType) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.MediaKind = media
		}
	}
}

func ShapeStream(stream av.StreamID) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.StreamID = stream
		}
	}
}

func ShapeCodec(codec av.CodecID) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.Codec = codec
		}
	}
}

func ShapeFormat(format av.FormatID) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.Format = format
		}
	}
}

func ShapeVideo(width int, height int, pixelFormat string) ShapeOption {
	return func(shape *MediaShape) {
		if shape == nil {
			return
		}
		shape.MediaKind = av.MediaVideo
		shape.Width = width
		shape.Height = height
		shape.PixelFormat = pixelFormat
	}
}

func ShapeAudio(sampleRate int, channels int, sampleFormat string) ShapeOption {
	return func(shape *MediaShape) {
		if shape == nil {
			return
		}
		shape.MediaKind = av.MediaAudio
		shape.SampleRate = sampleRate
		shape.Channels = channels
		shape.SampleFormat = sampleFormat
	}
}

func ShapeFramerate(num int, den int) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.Framerate = Rational{Num: num, Den: den}
		}
	}
}

func ShapeRealtime(realtime bool) ShapeOption {
	return func(shape *MediaShape) {
		if shape != nil {
			shape.Realtime = realtime
		}
	}
}

func (s ShapeSet) Clone() ShapeSet {
	if len(s) == 0 {
		return nil
	}
	out := make(ShapeSet, len(s))
	copy(out, s)
	return out
}

func (s ShapeSet) Accepts(actual MediaShape) bool {
	if len(s) == 0 {
		return true
	}
	for i := range s {
		if actual.CompatibleWith(s[i]) {
			return true
		}
	}
	return false
}

func (shape MediaShape) CompatibleWith(expected MediaShape) bool {
	if expected.Domain != "" && shape.Domain != expected.Domain {
		return false
	}
	if expected.MediaKind != "" && shape.MediaKind != expected.MediaKind {
		return false
	}
	if expected.StreamID != "" && shape.StreamID != expected.StreamID {
		return false
	}
	if expected.Codec != "" && shape.Codec != expected.Codec {
		return false
	}
	if expected.Format != "" && shape.Format != expected.Format {
		return false
	}
	if expected.Width != 0 && shape.Width != expected.Width {
		return false
	}
	if expected.Height != 0 && shape.Height != expected.Height {
		return false
	}
	if expected.PixelFormat != "" && shape.PixelFormat != expected.PixelFormat {
		return false
	}
	if expected.Framerate != (Rational{}) && shape.Framerate != expected.Framerate {
		return false
	}
	if expected.SampleRate != 0 && shape.SampleRate != expected.SampleRate {
		return false
	}
	if expected.Channels != 0 && shape.Channels != expected.Channels {
		return false
	}
	if expected.SampleFormat != "" && shape.SampleFormat != expected.SampleFormat {
		return false
	}
	if expected.Realtime && !shape.Realtime {
		return false
	}
	return true
}

func (shape MediaShape) String() string {
	parts := make([]string, 0, 8)
	if shape.Domain != "" {
		parts = append(parts, "domain="+string(shape.Domain))
	}
	if shape.MediaKind != "" {
		parts = append(parts, "media="+string(shape.MediaKind))
	}
	if shape.StreamID != "" {
		parts = append(parts, "stream="+string(shape.StreamID))
	}
	if shape.Codec != "" {
		parts = append(parts, "codec="+string(shape.Codec))
	}
	if shape.Format != "" {
		parts = append(parts, "format="+string(shape.Format))
	}
	if shape.Width != 0 || shape.Height != 0 {
		parts = append(parts, fmt.Sprintf("size=%dx%d", shape.Width, shape.Height))
	}
	if shape.PixelFormat != "" {
		parts = append(parts, "pixel_format="+shape.PixelFormat)
	}
	if shape.Framerate != (Rational{}) {
		parts = append(parts, fmt.Sprintf("framerate=%d/%d", shape.Framerate.Num, shape.Framerate.Den))
	}
	if shape.SampleRate != 0 {
		parts = append(parts, fmt.Sprintf("sample_rate=%d", shape.SampleRate))
	}
	if shape.Channels != 0 {
		parts = append(parts, fmt.Sprintf("channels=%d", shape.Channels))
	}
	if shape.SampleFormat != "" {
		parts = append(parts, "sample_format="+shape.SampleFormat)
	}
	if shape.Realtime {
		parts = append(parts, "realtime=true")
	}
	if len(parts) == 0 {
		return "any"
	}
	return strings.Join(parts, " ")
}

func MediaShapeFromStream(stream av.Stream, domain MediaDomain) MediaShape {
	shape := MediaShape{Domain: domain}
	if stream.Type != "" {
		shape.MediaKind = stream.Type
	}
	if stream.ID != "" {
		shape.StreamID = stream.ID
	}
	return mergeMediaShape(shape, MediaShapeFromCodecParameters(stream.Codec))
}

func MediaShapeFromCodecParameters(parameters av.CodecParameters) MediaShape {
	return MediaShape{
		MediaKind:    parameters.Type,
		Codec:        parameters.ID,
		Width:        parameters.Width,
		Height:       parameters.Height,
		PixelFormat:  parameters.PixelFormat,
		SampleRate:   parameters.SampleRate,
		Channels:     parameters.Channels,
		SampleFormat: parameters.SampleFormat,
	}
}

func MediaShapeFromCodecSpec(spec CodecSpec, domain MediaDomain) MediaShape {
	shape := MediaShapeFromCodecParameters(spec.Parameters)
	shape.Domain = domain
	if shape.MediaKind == "" {
		shape.MediaKind = firstNonEmptyMedia(spec.Type, codecMedia(spec.ID))
	}
	if shape.Codec == "" {
		shape.Codec = spec.ID
	}
	return shape
}

func (spec CodecSpec) InputShapes() ShapeSet {
	if spec.Copy {
		return ShapeSet{Shape(ShapeDomain(DomainPacket))}
	}
	media := firstNonEmptyMedia(spec.Type, spec.Parameters.Type, codecMedia(spec.ID))
	if spec.ID == "" && !spec.Auto && media == "" {
		return nil
	}
	return ShapeSet{FrameShape(media)}
}

func (spec CodecSpec) OutputShapes(input MediaShape) ShapeSet {
	if spec.Copy {
		input.Domain = DomainPacket
		return ShapeSet{input}
	}
	shape := input
	shape.Domain = DomainPacket
	shape = mergeMediaShape(shape, MediaShapeFromCodecSpec(spec, DomainPacket))
	return ShapeSet{shape}
}

func (spec TransformSpec) InputShapes() ShapeSet {
	switch {
	case spec.Resize != nil:
		return ShapeSet{FrameShape(av.MediaVideo)}
	case spec.Resample != nil:
		return ShapeSet{FrameShape(av.MediaAudio)}
	default:
		return nil
	}
}

func (spec TransformSpec) OutputShapes(input MediaShape) ShapeSet {
	shape := input
	switch {
	case spec.Resize != nil:
		shape.Domain = DomainFrame
		shape.MediaKind = av.MediaVideo
		shape.Width = spec.Resize.Width
		shape.Height = spec.Resize.Height
		if spec.Resize.PixelFormat != "" {
			shape.PixelFormat = spec.Resize.PixelFormat
		}
	case spec.Resample != nil:
		shape.Domain = DomainFrame
		shape.MediaKind = av.MediaAudio
		shape.SampleRate = spec.Resample.SampleRate
		shape.Channels = spec.Resample.Channels
		if spec.Resample.SampleFormat != "" {
			shape.SampleFormat = spec.Resample.SampleFormat
		}
	}
	return ShapeSet{shape}
}

func (operation OperationSpec) InputShapes() ShapeSet {
	switch operation.Kind {
	case OpDecode:
		codecID := firstNonEmptyCodec(operation.Decode.ID, av.CodecID(operation.Component))
		media := firstNonEmptyMedia(operation.Decode.Type, operation.Decode.Parameters.Type, codecMedia(codecID))
		return ShapeSet{PacketShape(media, codecID)}
	case OpShape:
		return nil
	case OpTransform:
		return operation.Transform.InputShapes()
	case OpEncode, OpCopy:
		return operation.Encode.InputShapes()
	default:
		return nil
	}
}

func (operation OperationSpec) OutputShapes(input MediaShape) ShapeSet {
	switch operation.Kind {
	case OpDecode:
		input.Domain = DomainFrame
		return ShapeSet{input}
	case OpShape:
		return ShapeSet{mergeMediaShape(input, operation.Shape)}
	case OpTransform:
		return operation.Transform.OutputShapes(input)
	case OpEncode, OpCopy:
		return operation.Encode.OutputShapes(input)
	default:
		return ShapeSet{input}
	}
}

func mergeMediaShape(base MediaShape, next MediaShape) MediaShape {
	if next.Domain != "" {
		base.Domain = next.Domain
	}
	if next.MediaKind != "" {
		base.MediaKind = next.MediaKind
	}
	if next.StreamID != "" {
		base.StreamID = next.StreamID
	}
	if next.Codec != "" {
		base.Codec = next.Codec
	}
	if next.Format != "" {
		base.Format = next.Format
	}
	if next.Width != 0 {
		base.Width = next.Width
	}
	if next.Height != 0 {
		base.Height = next.Height
	}
	if next.PixelFormat != "" {
		base.PixelFormat = next.PixelFormat
	}
	if next.Framerate != (Rational{}) {
		base.Framerate = next.Framerate
	}
	if next.SampleRate != 0 {
		base.SampleRate = next.SampleRate
	}
	if next.Channels != 0 {
		base.Channels = next.Channels
	}
	if next.SampleFormat != "" {
		base.SampleFormat = next.SampleFormat
	}
	base.Realtime = base.Realtime || next.Realtime
	return base
}
