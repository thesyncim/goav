// Tap arms: a TapRef as one arm of a join. The tap-declaring chain rides into
// the join as an ordinary arm (its .Decode()/.Tap(...) operations are honored
// and the tap installs on the task); a TapRef arm then converges that
// already-flowing point again — anchored on the tap's planned node, no source
// re-opened — the join-side dual of Branch().From(tap).

package goav

import (
	"context"
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// joinArm lets a declared tap stand as one arm of a join: the arm attaches to
// the tap's node — declared by an EARLIER arm of the same join expression —
// instead of opening a source, so the tapped stream converges mid-graph
// (decoded once). The arm's stream id is the tap name.
func (t TapRef) joinArm() joinArmSpec {
	tap := t
	return joinArmSpec{tap: &tap}
}

// Region places this tap arm's frames at (x, y) on the composite canvas —
// the tap-arm form of the chain arm's .Region(x, y). It has no effect on arms
// outside a Composite.
func (t TapRef) Region(x, y int) JoinArm {
	return tapArmRef{tap: t, region: compositeRegion{x: x, y: y}}
}

// tapArmRef is a TapRef arm carrying its composite placement; it stays behind
// the sealed JoinArm interface like every other arm shape.
type tapArmRef struct {
	tap    TapRef
	region compositeRegion
}

func (t tapArmRef) joinArm() joinArmSpec {
	tap := t.tap
	region := t.region
	return joinArmSpec{tap: &tap, region: &region}
}

// joinArmTap is one tap a chain arm declared: the resolved ref plus the
// operation it follows (decode, or nothing on frame-domain sources).
type joinArmTap struct {
	ref   TapRef
	after plan.OperationKind
}

// joinTapArmPlan is one planned tap arm: the resolved tap, its anchor in the
// join tree, and the restamp node that republishes the tapped media under the
// tap name (join stages identify arms by stream id, so the converged copy
// must carry its own).
type joinTapArmPlan struct {
	tap    TapRef
	anchor joinTapAnchor
	node   string
}

// joinTapAnchor locates a declared tap inside the planned join tree: the plan
// that owns the anchor node and the arm index (-1 anchors on the owner's join
// node — a nested join's output tap). stream is the media at the tap point.
type joinTapAnchor struct {
	owner  *joinPlan
	arm    int
	domain shape.MediaDomain
	stream av.Stream
}

// node names the planned node the tap anchors on. Arm source names are
// assigned after the whole tree is planned, so this resolves lazily at
// spec/lower/work time.
func (a joinTapAnchor) node() string {
	if a.arm < 0 {
		return a.owner.name
	}
	return a.owner.arms[a.arm].armTapAnchorNode()
}

// joinTapAnchors tracks the taps declared by already-planned arms of one join
// tree, in depth-first arm order — the same order the lowering walks — so a
// TapRef arm always anchors strictly upstream. declared lists every tap name
// in the whole spec (including not-yet-planned arms) for error candidates.
type joinTapAnchors struct {
	declared []string
	entries  map[string]joinTapAnchor
}

func newJoinTapAnchors(declared []string) *joinTapAnchors {
	return &joinTapAnchors{declared: declared, entries: make(map[string]joinTapAnchor)}
}

// declare registers a tap anchor; the first declaration of a name wins,
// mirroring the task-level tap install (tapInfosFromPlan).
func (a *joinTapAnchors) declare(name string, anchor joinTapAnchor) {
	if name == "" {
		return
	}
	if _, ok := a.entries[name]; ok {
		return
	}
	a.entries[name] = anchor
}

func (a *joinTapAnchors) resolve(name string) (joinTapAnchor, bool) {
	anchor, ok := a.entries[name]
	return anchor, ok
}

// planJoinTapArm plans one TapRef arm: the named tap must already be anchored
// (declared by an earlier arm of the tree), its typed domain must match, and
// the arm's stream is the tapped stream re-stamped under the tap name.
func planJoinTapArm(name string, kind string, profile joinProfile, tap TapRef, anchors *joinTapAnchors) (joinArmPlan, error) {
	if tap.name == "" {
		return joinArmPlan{}, joinArmError(name, name, "tap arm has no name",
			"use a named typed tap ref: goav.FrameTap(\"voice.decoded\") or goav.PacketTap(\"cam.packets\")")
	}
	anchor, ok := anchors.resolve(tap.name)
	if !ok {
		return joinArmPlan{}, joinTapArmMissingError(name, tap, anchors.declared)
	}
	if err := validateTapDomain("build "+name, tap.name, tap, anchor.domain); err != nil {
		return joinArmPlan{}, err
	}
	if profile.media != "" && anchor.stream.Type != profile.media {
		return joinArmPlan{}, joinArmError(name, tap.name,
			fmt.Sprintf("%s tap arm %q carries %s, want %s", kind, tap.name, anchor.stream.Type, profile.media),
			"reference a tap declared on a "+string(profile.media)+" arm chain")
	}
	stream := anchor.stream
	stream.ID = av.StreamID(tap.name)
	return joinArmPlan{
		inputName: tap.name,
		stream:    stream,
		domain:    anchor.domain,
		tapArm: &joinTapArmPlan{
			tap:    tapWithDomain(tap, anchor.domain),
			anchor: anchor,
			node:   name + "-tap-" + tap.name,
		},
	}, nil
}

// chainArmOperations reads a chain arm's declared operation list.
func chainArmOperations(chain *jobStreamBuilder) []operationSpec {
	if chain == nil || chain.stream == nil {
		return nil
	}
	return chain.stream.operations
}

// validateJoinArmOperations keeps arm chains honest: a join arm supports
// .Decode() and .Tap(...) before the join — everything else (transforms,
// stages, encoders, copies, shape steps) was silently dropped before and is
// now rejected with the supported alternatives.
func validateJoinArmOperations(join string, arm string, operations []operationSpec) error {
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpDecode, plan.OpTap:
		default:
			return joinArmError(join, firstNonEmpty(arm, join), fmt.Sprintf(
				"%s arm chains support .Decode() and .Tap(...) only (%s is not lowered on an arm)",
				join, string(operations[i].Kind)),
				"transform or encode the JOINED stream after the join: goav.Mix(a, b).Encode(...) / .Branches(...)",
				"use a nested join arm when one arm needs its own convergence")
		}
	}
	return nil
}

// joinChainArmTaps validates and collects the taps a chain arm declared. Arm
// taps live at the arm's decoded point (or directly on a frame-domain
// source), so they are frame-domain by construction.
func joinChainArmTaps(join string, arm string, operations []operationSpec) ([]joinArmTap, error) {
	var taps []joinArmTap
	for i := range operations {
		if operations[i].Kind != plan.OpTap {
			continue
		}
		intent := operations[i].Tap
		ref := tapWithDomain(TapRef{name: intent.Name, domain: intent.Domain}, shape.DomainFrame)
		if err := validateTapDomain("build "+join, firstNonEmpty(arm, join), ref, shape.DomainFrame); err != nil {
			return nil, err
		}
		taps = append(taps, joinArmTap{ref: ref, after: intent.After})
	}
	return taps, nil
}

// declaredJoinTapNames walks the join spec tree and lists every tap name an
// arm could anchor — chain-arm taps plus nested joins' output taps (the
// root's own output taps are excluded: a join cannot feed itself). The list
// feeds the unresolved-tap-arm error candidates.
func declaredJoinTapNames(spec *joinSpec) []string {
	var names []string
	seen := make(map[string]struct{})
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	var walk func(s *joinSpec, root bool)
	walk = func(s *joinSpec, root bool) {
		for _, arm := range s.arms {
			if arm == nil {
				continue
			}
			resolved := arm.joinArm()
			switch {
			case resolved.join != nil:
				walk(resolved.join, false)
			case resolved.chain != nil:
				for _, op := range chainArmOperations(resolved.chain) {
					if op.Kind == plan.OpTap {
						add(op.Tap.Name)
					}
				}
			}
		}
		if root {
			return
		}
		for _, tap := range s.taps {
			add(tap.name)
		}
	}
	walk(spec, true)
	return names
}

// joinTapArmMissingError is the unresolved tap arm refusal: the referenced
// tap is not anchored by any earlier arm. It lists every tap the join
// expression declares, in the established candidates style.
func joinTapArmMissingError(join string, tap TapRef, declared []string) error {
	details := []string{"tap=" + tap.name}
	for _, name := range declared {
		details = append(details, "declared="+name)
	}
	if len(declared) == 0 {
		details = append(details, "declared taps: none")
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(joinErrorCode(join, "tap_arm")),
		Code:      joinErrorCode(join, "tap_arm"),
		Operation: "build " + join,
		Node:      firstNonEmpty(tap.name, join),
		Reason:    "tap arm references a tap that no earlier arm declares",
		Fields:    buildErrorFields(details),
		Fixes: buildErrorFixes([]string{
			"declare the tap on an arm chain: goav.From(input).Audio().Decode().Tap(goav.FrameTap(" + strconv.Quote(tap.name) + "))",
			"order arms so the tap-declaring chain comes before the tap arm",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

// tapArmNodeDetail renders the restamp node's planned/built graph detail; the
// planner and tapArmStage.DescribeNode share it, keeping Describe() ≡ Build().
func tapArmNodeDetail(tap string) string {
	return joinSpecDetail("tap arm", "tap="+tap)
}

// tapArmStage republishes a tapped stream under the tap name — the runtime
// body of a tap arm. Join stages identify arms by stream id, and the tap's
// own media keeps flowing down its declaring arm unchanged, so the converged
// copy is re-stamped: a shallow per-message copy (payloads are shared exactly
// like every other tap fanout; downstream joins clone what they buffer).
type tapArmStage struct {
	name string
	out  av.StreamID
}

func newTapArmStage(name string, out av.StreamID) *tapArmStage {
	return &tapArmStage{name: name, out: out}
}

func (s *tapArmStage) Name() string { return s.name }

// DescribeNode reports the same detail the join planner records on the
// planned restamp node, keeping Describe() ≡ Build().
func (s *tapArmStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: tapArmNodeDetail(string(s.out))}
}

func (s *tapArmStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		if msg.Frame == nil {
			return nil
		}
		frame := *msg.Frame
		frame.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame})
	case pipeline.MessagePacket:
		if msg.Packet == nil {
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
		event := *msg.Event
		event.StreamID = s.out
		return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
	default:
		return emitter.Emit(ctx, msg)
	}
}

func (s *tapArmStage) Close() error { return nil }
