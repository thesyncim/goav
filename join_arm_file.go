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
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// joinBindArmPlan is the bind node of an arm bound when its input opens: it
// selects the stream selector matches and republishes it under the arm id.
type joinBindArmPlan struct {
	selector av.StreamSelector
	node     string
}

// joinConformPlan is an arm's run-time conversion to the join format: target
// holds the facts known at plan time (zero facts follow the arriving frame,
// or the first arm's stream when the join's format follows it).
type joinConformPlan struct {
	node   string
	target shape.Spec
	// adapter is the conversion adapter the solver would pick for the arm's
	// media, reported by Explain ("" when the runtime has none).
	adapter string
}

// joinArmBindsAtOpen reports whether an arm binds its stream when its input
// opens: a demuxed input always does (its stream ids are the container's,
// which two files share), and so does any input whose streams the plan does
// not know.
func joinArmBindsAtOpen(input compileInput, set inputStreamSet) bool {
	return input.Demuxed || !set.known || len(set.streams) == 0
}

// planJoinBindArm plans an arm bound when its input opens: by the chain's
// selection (its media kind defaults to the join's), under the input's name
// as its arm id, with its format left to the build.
func planJoinBindArm(name string, kind string, profile joinProfile, selector av.StreamSelector, set inputStreamSet) (joinArmPlan, error) {
	if selector.Type == "" {
		selector.Type = profile.media
	}
	if profile.media != "" && selector.Type != profile.media {
		return joinArmPlan{}, joinArmError(name, set.name,
			fmt.Sprintf("%s arm %q selects %s, want %s", kind, set.name, selector.Type, profile.media),
			"select the input's "+string(profile.media)+": input."+mediaSelectorCall(profile.media)+"()")
	}
	id := av.StreamID(set.name)
	return joinArmPlan{
		inputName: set.name,
		stream:    av.Stream{ID: id, Type: selector.Type, Codec: av.CodecParameters{Type: selector.Type}},
		domain:    set.domain,
		late:      true,
		bind:      &joinBindArmPlan{selector: selector, node: name + "-select-" + set.name},
	}, nil
}

func mediaSelectorCall(media av.MediaType) string {
	if media == av.MediaVideo {
		return "Video"
	}
	return "Audio"
}

// planArmConform plans an arm's run-time conversion to the join format,
// against the first arm's planned stream (the arm itself when it is first).
func (p *joinPlan) planArmConform(arm joinArmPlan) *joinConformPlan {
	first := arm.stream
	if len(p.arms) != 0 {
		first = p.arms[0].joinStream()
	}
	target := p.profile.conformTarget(p, first)
	return &joinConformPlan{node: p.name + "-conform-" + string(arm.stream.ID), target: target, adapter: conformAdapter(p.runtime, target)}
}

// conformAdapter names the adapter the solver picks to convert frames to
// target: adapter choice turns on the media and the formats, not on rates or
// sizes.
func conformAdapter(rt *Runtime, target shape.Spec) string {
	if rt == nil {
		return ""
	}
	var transform recipeir.Transform
	switch target.MediaKind {
	case av.MediaAudio:
		transform.Kind, transform.Resample = recipeir.TransformResample, filter.ResampleConfig{SampleFormat: target.SampleFormat}
	case av.MediaVideo:
		transform.Kind, transform.Resize = recipeir.TransformResize, filter.ResizeConfig{PixelFormat: target.PixelFormat, Mode: filter.ResizeExact}
	default:
		return ""
	}
	descriptor, _, _, err := selectShapeConversionAdapter(rt, target.MediaKind, transform, rt.realtime, shape.Spec{})
	if err != nil {
		return ""
	}
	return descriptor.Name
}

// conformedStream is stream after a conversion to target: the facts target
// fixes replace stream's.
func conformedStream(stream av.Stream, target shape.Spec) av.Stream {
	out := stream
	switch target.MediaKind {
	case av.MediaAudio:
		if target.SampleRate > 0 {
			out.Codec.SampleRate = target.SampleRate
			out.Codec.ClockRate = uint32(target.SampleRate)
			out.TimeBase = av.TimeBase{Num: 1, Den: int64(target.SampleRate)}
		}
		if target.Channels > 0 {
			out.Codec.Channels = target.Channels
		}
		if target.SampleFormat != "" {
			out.Codec.SampleFormat = target.SampleFormat
		}
	case av.MediaVideo:
		if target.Width > 0 && target.Height > 0 {
			out.Codec.Width, out.Codec.Height = target.Width, target.Height
		}
		if target.PixelFormat != "" {
			out.Codec.PixelFormat = target.PixelFormat
		}
	}
	return out
}

func bindArmNodeDetail(selector av.StreamSelector) string {
	return joinSpecDetail("bind on open", selectorDetail(selector))
}

func conformNodeDetail(target shape.Spec) string {
	parts := []string{"conform"}
	if target.SampleRate > 0 {
		parts = append(parts, "rate="+strconv.Itoa(target.SampleRate))
	}
	if target.Channels > 0 {
		parts = append(parts, "channels="+strconv.Itoa(target.Channels))
	}
	if target.Width > 0 && target.Height > 0 {
		parts = append(parts, "size="+strconv.Itoa(target.Width)+"x"+strconv.Itoa(target.Height))
	}
	for _, format := range []string{target.SampleFormat, target.PixelFormat} {
		if format != "" {
			parts = append(parts, "format="+format)
		}
	}
	return joinSpecDetail(parts...)
}

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
	conversion, planned, err := planShapeConversion(s.service.runtime, format, expected, s.realtime)
	if err == nil && !planned {
		err = fmt.Errorf("no conversion from %s", humanizeShape(format))
	}
	if err != nil {
		return joinArmError(s.name, string(id), fmt.Sprintf("arm %q cannot be converted to the join format (%s -> %s): %v", id, humanizeShape(format), humanizeShape(expected), err),
			"register a resample or resize adapter that converts "+humanizeShape(format)+" (bundle.Filters())")
	}
	transform := mediaTransform{name: s.name, factory: conversion.factory, video: transformResize(conversion.transform), audio: transformResample(conversion.transform)}
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

// resolvedJoinedStream is the joined stream of a join whose format is known
// only once its arms open: derived, the way planning derives it, from the
// streams the arm nodes resolved while building.
func (p *joinPlan) resolvedJoinedStream(streams map[pipeline.NodeRef]av.Stream, armRefs []pipeline.NodeRef) av.Stream {
	resolved := *p
	resolved.arms = append([]joinArmPlan(nil), p.arms...)
	for i := range resolved.arms {
		if i < len(armRefs) {
			resolved.arms[i].stream = streams[armRefs[i]]
		}
		resolved.arms[i].conform = nil
		resolved.arms[i].late = true
	}
	return p.profile.joinedStream(&resolved)
}
