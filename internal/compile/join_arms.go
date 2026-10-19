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

// validateJoinArmOperations keeps arm chains honest: a join arm supports
// .Decode() and .Tap(...) before the join — everything else (transforms,
// stages, encoders, copies, shape steps) was silently dropped before and is
// now rejected with the supported alternatives.
func validateJoinArmOperations(join string, arm string, operations []recipeir.Operation) error {
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpDecode, plan.OpTap:
		default:
			return JoinArmError(join, cmp.Or(arm, join), fmt.Sprintf(
				"%s arm chains support .Decode() and .Tap(...) only (%s is not lowered on an arm)",
				join, string(operations[i].Kind)),
				"transform or encode the JOINED stream after the join: goav.Mix(a, b).Resample(...).Encode(...)",
				"use a nested join arm when one arm needs its own convergence")
		}
	}
	return nil
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
func joinArmBindsAtOpen(input Input, set InputStreamSet) bool {
	return input.Demuxed || !set.Known || len(set.Streams) == 0
}

// planJoinBindArm plans an arm bound when its input opens: by the chain's
// selection (its media kind defaults to the join's), under the input's name
// as its arm id, with its format left to the build.
func planJoinBindArm(name string, kind string, profile joinProfile, selector av.StreamSelector, set InputStreamSet) (JoinArmPlan, error) {
	if selector.Type == "" {
		selector.Type = profile.media
	}
	if profile.media != "" && selector.Type != profile.media {
		return JoinArmPlan{}, JoinArmError(name, set.Name,
			fmt.Sprintf("%s arm %q selects %s, want %s", kind, set.Name, selector.Type, profile.media),
			"select the input's "+string(profile.media)+": input."+mediaSelectorCall(profile.media)+"()")
	}
	id := av.StreamID(set.Name)
	return JoinArmPlan{
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
func (p *JoinPlan) planArmConform(arm JoinArmPlan) *joinConformPlan {
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

func BindArmNodeDetail(selector av.StreamSelector) string {
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
	resolved.Arms = append([]JoinArmPlan(nil), p.Arms...)
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
