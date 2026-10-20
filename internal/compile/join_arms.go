package compile

import (
	"cmp"
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// joinArmTap is one tap a chain arm declared: the resolved ref plus the
// operation it follows (decode, or nothing on frame-domain sources).
type joinArmTap struct {
	ref   TapRef
	after plan.OperationKind
}

// validateJoinArmOperations keeps arm chains honest: a join arm decodes, taps,
// and transforms its own frames before the join (a picture-in-picture inset
// resizes on its arm: goav.At(screen.Video().Decode().Resize(480, 270), x,
// y)); stages, encoders, copies, and shape steps are refused with the
// supported alternatives.
func validateJoinArmOperations(join string, arm string, media av.MediaType, domain shape.MediaDomain, operations []recipeir.Operation) error {
	node := cmp.Or(arm, join)
	transformed := false
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpDecode:
			domain = shape.DomainFrame
		case plan.OpTap:
			if transformed {
				return JoinArmError(join, node, "a join arm's taps sit at its decoded point, but this one follows a transform on the arm",
					"tap the arm before its transform: a.Video().Decode().Tap(name).Resize(w, h)")
			}
		case plan.OpTransform:
			transformed = true
			if domain != shape.DomainFrame {
				return JoinArmError(join, node, join+" arm transforms decoded frames, but this arm is still packet-domain",
					"decode the arm before transforming it: a.Video().Decode().Resize(w, h)")
			}
			if want := armTransformMedia(operations[i].Transform); media != "" && want != media {
				return JoinArmError(join, node, fmt.Sprintf("%s arms carry %s, but .%s transforms %s", join, media, armTransformCall(operations[i].Transform), want),
					"transform the arm's own media: .Resize(w, h) for video, .Resample(rate, channels) for audio")
			}
		default:
			return JoinArmError(join, node, fmt.Sprintf(
				"%s arm chains support .Decode(), .Tap(...), .Resize(...), and .Resample(...) only (%s is not lowered on an arm)",
				join, string(operations[i].Kind)),
				"encode, or run stages on, the JOINED stream after the join: goav.Mix(a, b).Encode(...)",
				"use a nested join arm when one arm needs its own convergence")
		}
	}
	return nil
}

func armTransformMedia(transform recipeir.Transform) av.MediaType {
	if transform.Kind == recipeir.TransformResample {
		return av.MediaAudio
	}
	return av.MediaVideo
}

func armTransformCall(transform recipeir.Transform) string {
	if transform.Kind == recipeir.TransformResample {
		return "Resample(...)"
	}
	return "Resize(...)"
}

// plannedArmTransform is the stream an arm carries after one of its
// transforms, and whether its facts are known before the input opens: a fit
// resize's geometry follows its input's, known only when that is.
func plannedArmTransform(stream av.Stream, transform recipeir.Transform) (av.Stream, bool) {
	switch transform.Kind {
	case recipeir.TransformResize:
		resize := transform.Resize
		if resize.PixelFormat != "" {
			stream.Codec.PixelFormat = resize.PixelFormat
		}
		switch resize.Mode {
		case filter.ResizePassthrough:
			return stream, true
		case filter.ResizeFit:
			stream.Codec.Width, stream.Codec.Height = 0, 0
			return stream, false
		}
		if resize.Width > 0 && resize.Height > 0 {
			stream.Codec.Width, stream.Codec.Height = resize.Width, resize.Height
			return stream, true
		}
		return stream, false
	case recipeir.TransformResample:
		resample := transform.Resample
		if resample.SampleRate > 0 {
			stream.Codec.SampleRate, stream.Codec.ClockRate = resample.SampleRate, uint32(resample.SampleRate)
			stream.TimeBase = av.TimeBase{Num: 1, Den: int64(resample.SampleRate)}
		}
		if resample.Channels > 0 {
			stream.Codec.Channels = resample.Channels
		}
		if resample.SampleFormat != "" {
			stream.Codec.SampleFormat = resample.SampleFormat
		}
		return stream, true
	}
	return stream, true
}

// armTransform is the stage one arm transform lowers to, built against the
// stream the arm resolves to; the lowering names it.
func armTransform(operation recipeir.Operation) MediaTransform {
	switch operation.Transform.Kind {
	case recipeir.TransformResample:
		resample := operation.Transform.Resample
		return MediaTransform{Factory: cmp.Or(operation.Component, filter.FactoryResample), Audio: &resample}
	default:
		resize := operation.Transform.Resize
		return MediaTransform{Factory: cmp.Or(operation.Component, filter.FactoryResize), Video: &resize}
	}
}

// joinChainArmTaps validates and collects the taps a chain arm declared. Arm
// taps live at the arm's decoded point (or directly on a frame-domain
// source), so they are frame-domain: a tap before a packet arm's .Decode()
// is refused.
func joinChainArmTaps(join string, arm string, domain shape.MediaDomain, operations []recipeir.Operation) ([]joinArmTap, error) {
	var taps []joinArmTap
	for i := range operations {
		if operations[i].Kind == plan.OpDecode {
			domain = shape.DomainFrame
		}
		if operations[i].Kind != plan.OpTap {
			continue
		}
		tap := operations[i].Tap
		if tap.Name == "" {
			return nil, tapNameEmptyError("build "+join, cmp.Or(arm, join))
		}
		ref := TapRef{Name: tap.Name, Domain: shape.DomainFrame}
		if domain != shape.DomainFrame {
			return nil, joinArmPacketTapError(join, cmp.Or(arm, join), tap.Name)
		}
		taps = append(taps, joinArmTap{ref: ref, after: tap.After})
	}
	return taps, nil
}

// joinArmPacketTapError refuses a tap on a join arm's packets: arm taps sit
// where the arm carries frames.
func joinArmPacketTapError(join string, arm string, tap string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.TapDomainMismatch),
		Code:      errcode.TapDomainMismatch,
		Operation: "build " + join,
		Node:      arm,
		Reason:    "a join arm's taps carry frames, but this tap sits before the arm's .Decode()",
		Details:   builderr.Details(builderr.KV("tap", tap), builderr.KV("wanted", string(shape.DomainFrame)), builderr.KV("actual", string(shape.DomainPacket))),
		Fixes: builderr.Fixes([]string{
			"tap the arm after its decode: a.Audio().Decode().Tap(name)",
			"tap the packets on a stream that does not feed the join: s.Tap(name).To(destination)",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// joinBindArmPlan is the bind node of an arm bound when its input opens: it
// selects the stream selector matches and republishes it under the arm id.
type joinBindArmPlan struct {
	Selector av.StreamSelector
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
func joinArmBindsAtOpen(input Input, set inputStreamSet) bool {
	return input.Demuxed || !set.Known || len(set.Streams) == 0
}

// planJoinBindArm plans an arm bound when its input opens: by the chain's
// selection (its media kind defaults to the join's), under the input's name
// as its arm id, with its format left to the build.
func planJoinBindArm(name string, kind string, profile joinProfile, selector av.StreamSelector, set inputStreamSet) (joinArmPlan, error) {
	if selector.Type == "" {
		selector.Type = profile.media
	}
	if profile.media != "" && selector.Type != profile.media {
		return joinArmPlan{}, JoinArmError(name, set.Name,
			fmt.Sprintf("%s arm %q selects %s, want %s", kind, set.Name, selector.Type, profile.media),
			"select the input's "+string(profile.media)+": input."+mediaSelectorCall(profile.media)+"()")
	}
	id := av.StreamID(set.Name)
	return joinArmPlan{
		inputName: set.Name,
		Stream:    av.Stream{ID: id, Type: selector.Type, Codec: av.CodecParameters{Type: selector.Type}},
		Domain:    set.Domain,
		late:      true,
		Bind:      &joinBindArmPlan{Selector: selector, node: name + "-select-" + set.Name},
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
func (p *JoinPlan) planArmConform(arm joinArmPlan) *joinConformPlan {
	first := arm.Stream
	if len(p.Arms) != 0 {
		first = p.Arms[0].joinStream()
	}
	target := p.Profile.ConformTarget(p, first)
	return &joinConformPlan{node: p.Name + "-conform-" + string(arm.Stream.ID), target: target, adapter: conformAdapter(p.env, target)}
}

// conformAdapter names the adapter the solver picks to convert frames to
// target: adapter choice turns on the media and the formats, not on rates or
// sizes.
func conformAdapter(env Env, target shape.Spec) string {
	if !env.Present {
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
	descriptor, _, _, err := selectShapeConversionAdapter(env, target.MediaKind, transform, env.Realtime, shape.Spec{})
	if err != nil {
		return ""
	}
	return descriptor.Name
}

// ConformedStream is stream after a conversion to target: the facts target
// fixes replace stream's.
func ConformedStream(stream av.Stream, target shape.Spec) av.Stream {
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
	return JoinSpecDetail("bind on open", selectorDetail(selector))
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
	return JoinSpecDetail(parts...)
}

// ResolvedJoinedStream is the joined stream of a join whose format is known
// only once its arms open: derived, the way planning derives it, from the
// streams the arm nodes resolved while building.
func (p *JoinPlan) ResolvedJoinedStream(streams map[pipeline.NodeRef]av.Stream, armRefs []pipeline.NodeRef) av.Stream {
	resolved := *p
	resolved.Arms = append([]joinArmPlan(nil), p.Arms...)
	for i := range resolved.Arms {
		if i < len(armRefs) {
			resolved.Arms[i].Stream = streams[armRefs[i]]
		}
		resolved.Arms[i].conform = nil
		resolved.Arms[i].late = true
	}
	return p.Profile.joinedStream(&resolved)
}

// TapRef is a typed handle to a stable media attach point.
type TapRef struct {
	Name   string
	Domain shape.MediaDomain
}
