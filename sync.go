package goav

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
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
	gate := &syncGate{policy: policy.Normalize()}
	gate.qosLast.Store(qosWindowUnset)
	return gate
}

// syncGate aligns a branch on its policy's shared media timeline. Sheds — a
// message trailing the timeline past tolerance — produce rate-limited EventQoS
// reports through the reporter bound at lowering time; admitted media pays
// nothing.
type syncGate struct {
	policy  flow.SyncPolicy
	report  func(av.Event)
	qosLast atomic.Int64
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
	stream, pts, ok := gateMessageTime(msg)
	if !ok {
		return syncTimebaseError(g.policy, msg)
	}
	late, drop, err := g.policy.Admit(ctx, stream, pts)
	if err != nil || drop {
		if drop {
			g.dropped.Add(1)
			if late > 0 {
				g.reportLate(stream, late)
			}
		}
		return err
	}
	return emit.Emit(ctx, msg)
}

// reportLate publishes one rate-limited QoS report for a shed message. Sync
// gates have no bound timeline, so the window runs on the process monotonic
// clock; the lateness itself is media time behind the shared sync timeline.
func (g *syncGate) reportLate(stream av.StreamID, late time.Duration) {
	if g.report == nil || !claimQoSWindow(&g.qosLast, qosWindowClock.Now()) {
		return
	}
	g.report(av.Event{
		Type:     av.EventQoS,
		StreamID: stream,
		Reason:   g.Name(),
		Metadata: av.QoSMetadata(late, true),
	})
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

func syncStageName(name string) string {
	return gateStageName("sync", name)
}

func syncTimebaseError(policy flow.SyncPolicy, msg *pipeline.Message) error {
	return gateTimebaseError("sync media timeline", syncStageName(policy.Name()), msg, []string{
		"set Packet.PTS or Frame.PTS with a valid av.TimeBase before the sync gate",
		"declare live stream TimeBase facts on the input or discovered stream",
		"remove .Sync(...) from branches that cannot carry media timestamps",
	})
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
	return validateGatePolicyForBranch(operation, branchName, stream,
		"sync branches need stream timebase facts before graph mutation", []string{
			"set av.Stream.TimeBase on the input.Stream(...) anchor or discovered stream",
			"set Codec.ClockRate so RTP-style timestamps can derive a timebase",
			"remove .Sync(...) from dynamic branches whose source has no media timeline",
		})
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
