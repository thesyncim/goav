package compile

import (
	"cmp"
	"fmt"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
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
