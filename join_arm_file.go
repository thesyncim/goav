// Arms bound when their input opens: a file's streams, and the streams of a
// provider that declares none, are known only once the input opens. Such an
// arm is planned by its selection and its planned id, so Describe shows the
// whole graph without opening anything; the build binds the selected stream,
// republishes it under the arm id, and converts it to the join's format as
// frames arrive.

package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// joinConformStage converts one arm's frames to the join's format as they
// arrive: it builds its converter from the first frame's format through the
// shape solver, rebuilds it when the format changes, and passes frames already
// in the format through untouched. Target facts left zero keep the frame's.
type joinConformStage struct {
	name     string
	detail   string
	service  *builder
	target   shape.Spec
	realtime bool
	// format is the frame format the converter was built for; converter is
	// nil while frames pass through.
	format    shape.Spec
	bound     bool
	converter pipeline.Stage
}

func newJoinConformStage(service *builder, name string, detail string, target shape.Spec, realtime bool) *joinConformStage {
	return &joinConformStage{name: name, detail: detail, service: service, target: target, realtime: realtime}
}

func (s *joinConformStage) Name() string { return s.name }

func (s *joinConformStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *joinConformStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg != nil && msg.Kind == pipeline.MessageFrame && msg.Frame != nil {
		if format := frameFormat(msg.Frame); !s.bound || format != s.format {
			if err := s.bind(ctx, format, msg.Frame.StreamID); err != nil {
				return err
			}
		}
	}
	if s.converter == nil {
		return emitter.Emit(ctx, msg)
	}
	return s.converter.Handle(ctx, msg, emitter)
}

// bind builds the converter from format to the target, or none when format
// already is the target.
func (s *joinConformStage) bind(ctx context.Context, format shape.Spec, id av.StreamID) error {
	if s.converter != nil {
		s.converter.Close()
		s.converter = nil
	}
	s.format, s.bound = format, true
	expected := conformedShape(format, s.target)
	if shape.Conversions(format, expected).Empty() {
		return nil
	}
	conversion, planned, err := refuse2(compile.PlanShapeConversion(s.service.runtime.compileEnv(), format, expected, s.realtime))
	if err == nil && !planned {
		err = fmt.Errorf("no conversion from %s", compile.HumanizeShape(format))
	}
	if err != nil {
		return publicError(compile.JoinArmError(s.name, string(id), fmt.Sprintf("arm %q cannot be converted to the join format (%s -> %s): %v", id, compile.HumanizeShape(format), compile.HumanizeShape(expected), err),
			"register a resample or resize adapter that converts "+compile.HumanizeShape(format)+" (bundle.Filters())"))
	}
	transform := compile.MediaTransform{Name: s.name, Factory: conversion.Factory, Video: compile.TransformResize(conversion.Transform), Audio: compile.TransformResample(conversion.Transform)}
	stage, _, err := s.service.newMediaTransformStageNamed(ctx, s.name, transform, streamFromFrameFormat(id, format), s.realtime)
	if err != nil {
		return err
	}
	s.converter = stage
	return nil
}

func (s *joinConformStage) Close() error {
	if s.converter != nil {
		return s.converter.Close()
	}
	return nil
}

// frameFormat is the format facts a frame carries.
func frameFormat(frame *av.Frame) shape.Spec {
	switch {
	case frame.Audio != nil:
		return shape.Frame(av.MediaAudio, shape.Audio(frame.Audio.SampleRate, frame.Audio.Channels, frame.Audio.SampleFormat))
	case frame.Video != nil:
		return shape.Frame(av.MediaVideo, shape.Video(frame.Video.Width, frame.Video.Height, frame.Video.PixelFormat))
	default:
		return shape.Frame(frame.Type)
	}
}

// conformedShape is format with the facts target fixes.
func conformedShape(format shape.Spec, target shape.Spec) shape.Spec {
	out := format
	if target.SampleRate > 0 {
		out.SampleRate = target.SampleRate
	}
	if target.Channels > 0 {
		out.Channels = target.Channels
	}
	if target.SampleFormat != "" {
		out.SampleFormat = target.SampleFormat
	}
	if target.Width > 0 && target.Height > 0 {
		out.Width, out.Height = target.Width, target.Height
	}
	if target.PixelFormat != "" {
		out.PixelFormat = target.PixelFormat
	}
	return out
}

func streamFromFrameFormat(id av.StreamID, format shape.Spec) av.Stream {
	stream := av.Stream{ID: id, Type: format.MediaKind, Codec: av.CodecParameters{
		Type:         format.MediaKind,
		SampleRate:   format.SampleRate,
		Channels:     format.Channels,
		SampleFormat: format.SampleFormat,
		Width:        format.Width,
		Height:       format.Height,
		PixelFormat:  format.PixelFormat,
	}}
	if format.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(format.SampleRate)
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(format.SampleRate)}
	}
	return stream
}
