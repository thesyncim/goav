package goav

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func operationSpecForSync(policy flow.SyncPolicy) operationSpec {
	gate := newSyncGate(policy)
	return operationSpec{Kind: plan.OpStage, Component: gate.Name(), Stage: gate}
}

func newSyncGate(policy flow.SyncPolicy) *syncGate {
	return &syncGate{policy: policy.Normalize()}
}

type syncGate struct {
	policy  flow.SyncPolicy
	dropped atomic.Uint64
}

func (g *syncGate) Name() string {
	return syncStageName(g.policy.Name())
}

func (g *syncGate) DescribeNode() pipeline.NodeSpec {
	detail := "sync"
	if tolerance := g.policy.Tolerance(); tolerance > 0 {
		detail += " tolerance=" + tolerance.String()
	}
	if g.policy.DropLate() {
		detail += " drop-late"
	} else {
		detail += " hold-late"
	}
	return pipeline.NodeSpec{Name: g.Name(), Kind: pipeline.NodeStage, Detail: detail}
}

func (g *syncGate) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	if msg.Kind == pipeline.MessageEvent {
		g.policy.ObserveEvent(msg.Event)
		return emit.Emit(ctx, msg)
	}
	stream, pts, ok := syncMessageTime(msg)
	if !ok {
		return syncTimebaseError(g.policy, msg)
	}
	drop, err := g.policy.Admit(ctx, stream, pts)
	if err != nil || drop {
		if drop {
			g.dropped.Add(1)
		}
		return err
	}
	return emit.Emit(ctx, msg)
}

func (g *syncGate) Close() error {
	return nil
}

func (g *syncGate) DroppedMessages() uint64 {
	return g.dropped.Load()
}

func (g *syncGate) InputShapes() shape.Set {
	return nil
}

func (g *syncGate) OutputShapes(input shape.Spec) shape.Set {
	return shape.Set{input}
}

func syncMessageTime(msg *pipeline.Message) (av.StreamID, time.Duration, bool) {
	switch msg.Kind {
	case pipeline.MessagePacket:
		if msg.Packet == nil || !msg.Packet.PTS.Base.Valid() {
			return "", 0, false
		}
		pts, ok := msg.Packet.PTS.ToDuration()
		return msg.Packet.StreamID, pts, ok
	case pipeline.MessageFrame:
		if msg.Frame == nil || !msg.Frame.PTS.Base.Valid() {
			return "", 0, false
		}
		pts, ok := msg.Frame.PTS.ToDuration()
		return msg.Frame.StreamID, pts, ok
	default:
		return "", 0, false
	}
}

func syncStageName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "sync"
	}
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", "\t", "-", "\n", "-")
	return "sync-" + replacer.Replace(name)
}

func syncTimebaseError(policy flow.SyncPolicy, msg *pipeline.Message) error {
	node := syncStageName(policy.Name())
	kind := ""
	switch {
	case msg == nil:
	case msg.Packet != nil:
		kind = "packet"
	case msg.Frame != nil:
		kind = "frame"
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: "sync media timeline",
		Node:      node,
		Reason:    "media message has no valid PTS timebase",
		fields:    errDetails(errDetail("message", firstNonEmpty(kind, "unknown"))),
		fixes: buildErrorFixes([]string{
			"set Packet.PTS or Frame.PTS with a valid av.TimeBase before the sync gate",
			"declare live stream TimeBase facts on the input or discovered stream",
			"remove .Sync(...) from branches that cannot carry media timestamps",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateSyncPolicyForStream(operation string, branchName string, stream av.Stream, operations []operationSpec) error {
	if !operationSpecsContainSync(operations) {
		return nil
	}
	return validateSyncPolicyForBranch(operation, branchName, stream)
}

func validateSyncPolicyForRecipeIROperations(operation string, branchName string, stream av.Stream, operations []recipeir.Operation) error {
	if !recipeIROperationsContainSync(operations) {
		return nil
	}
	return validateSyncPolicyForBranch(operation, branchName, stream)
}

func validateSyncPolicyForBranch(operation string, branchName string, stream av.Stream) error {
	if stream.TimeBase.Valid() {
		return nil
	}
	if stream.Codec.ClockRate != 0 {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: operation,
		Node:      firstNonEmpty(branchName, string(stream.ID), "branch"),
		Reason:    "sync branches need stream timebase facts before graph mutation",
		fixes: buildErrorFixes([]string{
			"set av.Stream.TimeBase on the input.Stream(...) anchor or discovered stream",
			"set Codec.ClockRate so RTP-style timestamps can derive a timebase",
			"remove .Sync(...) from dynamic branches whose source has no media timeline",
		}),
		cause: errUnsupportedBuild,
	}
}

func operationSpecsContainSync(operations []operationSpec) bool {
	for i := range operations {
		if _, ok := operations[i].Stage.(*syncGate); ok {
			return true
		}
	}
	return false
}

func recipeIROperationsContainSync(operations []recipeir.Operation) bool {
	for i := range operations {
		if _, ok := operations[i].Stage.(*syncGate); ok {
			return true
		}
	}
	return false
}

func syncStageOperationName(operation operationSpec, branchName string) string {
	if _, ok := operation.Stage.(*syncGate); !ok {
		if operation.Stage != nil {
			return operation.Stage.Name()
		}
		return operation.Component
	}
	return syncStageName(firstNonEmpty(branchName, operation.Component, "branch"))
}

var _ pipeline.Stage = (*syncGate)(nil)
var _ pipeline.NodeDescriber = (*syncGate)(nil)
var _ pipeline.DropReporter = (*syncGate)(nil)
var _ shape.Contract = (*syncGate)(nil)
