package goav

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
)

type switchBoundaryKind string

const (
	switchImmediate    switchBoundaryKind = ""
	switchNextFrame    switchBoundaryKind = "next-frame"
	switchNextKeyframe switchBoundaryKind = "next-keyframe"
	switchMediaTime    switchBoundaryKind = "media-time"
)

// RebranchArg lets BranchSpec ride the lifecycle.RebranchArg list accepted by
// Attachment.Rebranch as a replacement branch.
func (s BranchSpec) RebranchArg() {}

// rebranchPolicy is the collected Rebranch configuration: the replacement
// branches, the switch boundary, and the old branch's destination disposition.
type rebranchPolicy struct {
	specs       []BranchSpec
	boundary    switchBoundaryKind
	mediaTime   time.Duration
	invalid     string
	disposition oldBranchDisposition
}

// oldBranchDisposition selects how the replaced branch's destinations are
// finalized when it detaches. The zero value is a plain detach (destinations
// close without a recorded outcome) — exactly what Detach does today.
type oldBranchDisposition string

const (
	oldBranchDetach oldBranchDisposition = ""
	oldBranchDrain  oldBranchDisposition = "drain"
	oldBranchAbort  oldBranchDisposition = "abort"
)

type lifecycleRebranchPolicy interface {
	Boundary() string
	MediaTime() time.Duration
	Invalid() string
	Disposition() string
}

func rebranchPolicyFromOptions(options []lifecycle.RebranchArg) rebranchPolicy {
	var policy rebranchPolicy
	for _, option := range options {
		if option == nil {
			continue
		}
		switch option := option.(type) {
		case BranchSpec:
			policy.specs = append(policy.specs, option)
		case lifecycleRebranchPolicy:
			if invalid := option.Invalid(); invalid != "" {
				policy.invalid = invalid
			}
			switch switchBoundaryKind(option.Boundary()) {
			case switchNextFrame, switchNextKeyframe, switchMediaTime:
				policy.boundary = switchBoundaryKind(option.Boundary())
				policy.mediaTime = option.MediaTime()
			case switchImmediate:
			default:
				policy.invalid = "switch boundary is invalid"
			}
			switch oldBranchDisposition(option.Disposition()) {
			case oldBranchDrain:
				policy.disposition = oldBranchDrain
			case oldBranchAbort:
				policy.disposition = oldBranchAbort
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
func gatedBranchSpecs(specs []BranchSpec, group *switchGroup) []BranchSpec {
	out := make([]BranchSpec, len(specs))
	for i := range specs {
		out[i] = specs[i]
		operations := make([]operationSpec, 0, len(specs[i].operations)+1)
		operations = append(operations, operationSpecForStage(group.gate()))
		operations = append(operations, cloneOperationSpecs(specs[i].operations)...)
		out[i].operations = operations
	}
	return out
}
