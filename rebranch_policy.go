package goav

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
)

// RebranchArg is one argument to Attachment.Rebranch: a replacement Output
// built from task.Tap or task.Track, a switch boundary from [SwitchAt], or a
// [DetachOption] for the replaced branch. It is sealed: only goav values
// implement it.
type RebranchArg interface {
	rebranchArg()
}

// SwitchBoundary is the stream position where a rebranch hands delivery over
// from the replaced branch to its replacements: [NextFrame], [NextKeyframe],
// or [AtMediaTime].
type SwitchBoundary struct {
	kind      switchBoundaryKind
	mediaTime time.Duration
}

// NextFrame switches at the next media message: the replacement branch starts
// delivering with the first frame or packet that reaches it.
func NextFrame() SwitchBoundary {
	return SwitchBoundary{kind: switchNextFrame}
}

// NextKeyframe switches at the next decodable sync point. Raw frames are
// independently decodable, so on frame streams the next frame qualifies.
func NextKeyframe() SwitchBoundary {
	return SwitchBoundary{kind: switchNextKeyframe}
}

// AtMediaTime switches when the replacement branch sees a packet or frame at
// or after the given media timestamp.
func AtMediaTime(position time.Duration) SwitchBoundary {
	return SwitchBoundary{kind: switchMediaTime, mediaTime: position}
}

// SwitchAt delays the rebranch switch to the given stream boundary. Without
// SwitchAt, rebranch switches immediately.
func SwitchAt(boundary SwitchBoundary) RebranchArg {
	return switchAt(boundary)
}

type switchAt SwitchBoundary

func (switchAt) rebranchArg() {}

type switchBoundaryKind string

const (
	switchImmediate    switchBoundaryKind = ""
	switchNextFrame    switchBoundaryKind = "next-frame"
	switchNextKeyframe switchBoundaryKind = "next-keyframe"
	switchMediaTime    switchBoundaryKind = "media-time"
)

// rebranchPolicy is the collected Rebranch configuration: the replacement
// branches, the switch boundary, and the old branch's destination disposition.
type rebranchPolicy struct {
	specs       []attachBranch
	boundary    switchBoundaryKind
	mediaTime   time.Duration
	invalid     string
	disposition oldBranchDisposition
}

// oldBranchDisposition selects how a removed branch's destinations are
// finalized when it detaches. The zero value is a plain detach (destinations
// close without a recorded outcome).
type oldBranchDisposition string

const (
	oldBranchDetach oldBranchDisposition = ""
	oldBranchDrain  oldBranchDisposition = "drain"
	oldBranchAbort  oldBranchDisposition = "abort"
)

func rebranchPolicyFromOptions(options []RebranchArg) rebranchPolicy {
	var policy rebranchPolicy
	for _, option := range options {
		switch option := option.(type) {
		case attachBranch:
			policy.specs = append(policy.specs, option)
		case switchAt:
			switch option.kind {
			case switchNextFrame, switchNextKeyframe:
				policy.boundary = option.kind
			case switchMediaTime:
				if option.mediaTime < 0 {
					policy.invalid = "media-time switch boundary must be non-negative"
				}
				policy.boundary = option.kind
				policy.mediaTime = option.mediaTime
			case switchImmediate:
			default:
				policy.invalid = "switch boundary is invalid"
			}
		case DetachOption:
			if disposition := option.detachDisposition(); disposition != oldBranchDetach {
				policy.disposition = disposition
			}
		}
	}
	return policy
}

// switchGroup coordinates one boundary rebranch: every replacement branch
// carries a gate from the group, and the group reports the first gate opening
// (the boundary arrived — detach the old branch) or every gate closing before
// any opened (the switch was abandoned by teardown).
type switchGroup struct {
	boundary  switchBoundaryKind
	mediaTime time.Duration
	gates     atomic.Int64
	open      atomic.Bool
	opened    chan struct{}
	abandoned chan struct{}
}

func newSwitchGroup(boundary switchBoundaryKind, mediaTime ...time.Duration) *switchGroup {
	var at time.Duration
	if len(mediaTime) != 0 {
		at = mediaTime[0]
	}
	return &switchGroup{
		boundary:  boundary,
		mediaTime: at,
		opened:    make(chan struct{}),
		abandoned: make(chan struct{}),
	}
}

func (g *switchGroup) gate() *switchGate {
	g.gates.Add(1)
	return &switchGate{group: g}
}

func (g *switchGroup) markOpen() {
	if g.open.CompareAndSwap(false, true) {
		close(g.opened)
	}
}

func (g *switchGroup) gateClosed() {
	if g.gates.Add(-1) == 0 {
		close(g.abandoned)
	}
}

// switchGate is the head stage of a replacement branch during a boundary
// rebranch: it sheds media messages until the switch boundary arrives, then
// opens permanently. The hot path is lock-free — one atomic load per message —
// and the open flip is decided inside the node's serial Handle on the first
// boundary message, so no mutex ever guards delivery. Events always pass so
// EOS and stream events reach the replacement's destinations while gated.
type switchGate struct {
	group  *switchGroup
	open   atomic.Bool
	closed atomic.Bool
}

func (g *switchGate) Name() string {
	return "switch-gate"
}

func (g *switchGate) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	if !g.open.Load() {
		if msg.Kind == pipeline.MessageEvent {
			return emitter.Emit(ctx, msg)
		}
		if !switchGroupBoundaryReached(g.group, msg) {
			return nil
		}
		g.open.Store(true)
		g.group.markOpen()
	}
	return emitter.Emit(ctx, msg)
}

func (g *switchGate) Close() error {
	if g.closed.CompareAndSwap(false, true) {
		g.group.gateClosed()
	}
	return nil
}

func switchBoundaryReached(boundary switchBoundaryKind, msg *pipeline.Message) bool {
	return switchBoundaryReachedAt(boundary, 0, msg)
}

func switchGroupBoundaryReached(group *switchGroup, msg *pipeline.Message) bool {
	if group == nil {
		return true
	}
	return switchBoundaryReachedAt(group.boundary, group.mediaTime, msg)
}

func switchBoundaryReachedAt(boundary switchBoundaryKind, mediaTime time.Duration, msg *pipeline.Message) bool {
	switch boundary {
	case switchNextFrame:
		return msg.Kind == pipeline.MessageFrame || msg.Kind == pipeline.MessagePacket
	case switchNextKeyframe:
		if msg.Kind == pipeline.MessageFrame {
			return true
		}
		return msg.Kind == pipeline.MessagePacket && msg.Packet != nil && msg.Packet.Keyframe
	case switchMediaTime:
		_, pts, ok := gateMessageTime(msg)
		return ok && pts >= mediaTime
	default:
		return true
	}
}

// gatedBranchSpecs clones the replacement specs with a switch gate from the
// group prepended as each branch's head operation, so every replacement sheds
// until its boundary while the rest of the chain stays exactly as written.
func gatedBranchSpecs(specs []attachBranch, group *switchGroup) []attachBranch {
	out := make([]attachBranch, len(specs))
	for i := range specs {
		out[i] = specs[i]
		operations := make([]recipeir.Operation, 0, len(specs[i].operations)+1)
		gate := opStage(group.gate())
		// The gate is the replacement's own node, not one of the Streams'.
		gate.Node = -1
		operations = append(operations, gate)
		operations = append(operations, specs[i].operations...)
		out[i].operations = operations
	}
	return out
}
