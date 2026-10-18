// Join arms: what an arm may declare, its taps, and the restamp that
// republishes an arm under its id.

package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// joinArmTap is one tap a chain arm declared: the resolved ref plus the
// operation it follows (decode, or nothing on frame-domain sources).
type joinArmTap struct {
	ref   tapRef
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
			return joinArmError(join, firstNonEmpty(arm, join), fmt.Sprintf(
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
// source), so they are frame-domain by construction.
func joinChainArmTaps(join string, arm string, operations []recipeir.Operation) ([]joinArmTap, error) {
	var taps []joinArmTap
	for i := range operations {
		if operations[i].Kind != plan.OpTap {
			continue
		}
		intent := operations[i].Tap
		ref := tapWithDomain(tapRef{name: intent.Name, domain: intent.Domain}, shape.DomainFrame)
		if err := validateTapDomain("build "+join, firstNonEmpty(arm, join), ref, shape.DomainFrame); err != nil {
			return nil, err
		}
		taps = append(taps, joinArmTap{ref: ref, after: intent.After})
	}
	return taps, nil
}

// armRestampStage republishes an arm's stream under the arm's id — the
// runtime body of a tap arm and of an arm bound when its input opens. Join
// stages identify arms by stream id: a tap's own media keeps flowing down its
// declaring arm unchanged, and two files carry the same track ids, so the
// converged copy is re-stamped: a shallow per-message copy (payloads are
// shared exactly like every other fanout; downstream joins clone what they
// buffer). With in set, only that stream and stream-less events pass: a
// file's source carries all its streams.
type armRestampStage struct {
	name   string
	detail string
	in     av.StreamID
	out    av.StreamID
}

func newArmRestampStage(name string, detail string, in av.StreamID, out av.StreamID) *armRestampStage {
	return &armRestampStage{name: name, detail: detail, in: in, out: out}
}

func (s *armRestampStage) Name() string { return s.name }

// DescribeNode reports the same detail the join planner records on the
// planned restamp node, keeping Describe() ≡ Build().
func (s *armRestampStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *armRestampStage) passes(id av.StreamID, event bool) bool {
	return s.in == "" || id == s.in || (event && id == "")
}

func (s *armRestampStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil || !s.passes(msg.Frame.StreamID, false) {
			return nil
		}
		frame := *msg.Frame
		frame.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame})
	case pipeline.MessagePacket:
		if msg.Packet == nil || !s.passes(msg.Packet.StreamID, false) {
			return nil
		}
		packet := *msg.Packet
		packet.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet})
	case pipeline.MessageEvent:
		if msg.Event == nil {
			return nil
		}
		if msg.Event.Reason == selectorActiveReason {
			// Control-plane events ride the data path unchanged: a SelectActive
			// heading for a downstream selector carries its target arm in
			// Event.StreamID — re-stamping it would erase the target.
			return emitter.Emit(ctx, msg)
		}
		if !s.passes(msg.Event.StreamID, true) {
			return nil
		}
		event := *msg.Event
		event.StreamID = s.out
		if event.Stream != nil {
			stream := *event.Stream
			stream.ID = s.out
			event.Stream = &stream
		}
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
	default:
		return emitter.Emit(ctx, msg)
	}
}

func (s *armRestampStage) Close() error { return nil }
