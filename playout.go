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

func operationSpecForPlayout(policy flow.PlayoutPolicy) operationSpec {
	gate := newPlayoutGate(policy)
	return operationSpec{Kind: plan.OpStage, Component: gate.Name(), Stage: gate}
}

func newPlayoutGate(policy flow.PlayoutPolicy) *playoutGate {
	gate := &playoutGate{policy: policy.Normalize()}
	gate.qosLast.Store(qosWindowUnset)
	return gate
}

// playoutGate is the sink-side deliver-when-due stage: it holds packets and
// frames until their media time is due on the task timeline (plus the policy
// offset) and lets events through ungated. The clock and QoS reporter are
// bound to the task at lowering time, before the graph runs, so Handle reads
// them without synchronization. Overdue admits — delivered late or shed —
// produce rate-limited EventQoS reports; on-time media pays nothing.
type playoutGate struct {
	policy  flow.PlayoutPolicy
	clock   av.Clock
	report  func(av.Event)
	qosLast atomic.Int64
	dropped atomic.Uint64
}

func (g *playoutGate) Name() string {
	return playoutStageName(g.policy.Name())
}

func (g *playoutGate) DescribeNode() pipeline.NodeSpec {
	detail := "playout"
	if offset := g.policy.Offset(); offset > 0 {
		detail += " offset=" + offset.String()
	}
	if g.policy.DropLate() {
		detail += " drop-late"
		if tolerance := g.policy.Tolerance(); tolerance > 0 {
			detail += " tolerance=" + tolerance.String()
		}
	} else {
		detail += " hold-late"
	}
	return pipeline.NodeSpec{Name: g.Name(), Kind: pipeline.NodeStage, Detail: detail}
}

// bindClock hands the gate its pacing clock. Called only during lowering or
// attach planning, which happen-before the node's goroutines start.
func (g *playoutGate) bindClock(clock av.Clock) {
	if clock != nil {
		g.clock = clock
	}
}

// timeSource falls back to the monotonic clock for gates driven outside a
// build path (unit-test graphs); built tasks always bind the task timeline.
func (g *playoutGate) timeSource() av.Clock {
	if g.clock != nil {
		return g.clock
	}
	return av.MonotonicClock()
}

func (g *playoutGate) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
	if msg == nil {
		return nil
	}
	if msg.Kind == pipeline.MessageEvent {
		g.policy.ObserveEvent(msg.Event)
		return emit.Emit(ctx, msg)
	}
	stream, pts, ok := gateMessageTime(msg)
	if !ok {
		return playoutTimebaseError(g.policy, msg)
	}
	late, drop, err := g.policy.Admit(ctx, g.timeSource(), pts)
	if err != nil {
		return err
	}
	if late > 0 {
		g.reportLate(stream, late, drop)
	}
	if drop {
		g.dropped.Add(1)
		return nil
	}
	return emit.Emit(ctx, msg)
}

// reportLate publishes one rate-limited QoS report for an overdue admit. Cold
// side of the gate: it runs only when a message was already late, at most once
// per window.
func (g *playoutGate) reportLate(stream av.StreamID, late time.Duration, dropped bool) {
	if g.report == nil || !claimQoSWindow(&g.qosLast, g.timeSource().Now()) {
		return
	}
	g.report(av.Event{
		Type:     av.EventQoS,
		StreamID: stream,
		Reason:   g.Name(),
		Metadata: av.QoSMetadata(late, dropped),
	})
}

func (g *playoutGate) Close() error {
	return nil
}

func (g *playoutGate) DroppedMessages() uint64 {
	return g.dropped.Load()
}

func (g *playoutGate) InputShapes() shape.Set {
	return nil
}

func (g *playoutGate) OutputShapes(input shape.Spec) shape.Set {
	return shape.Set{input}
}

// playoutClock returns the clock attach-time playout gates bind: the running
// task's shared timeline. Nil for tasks assembled outside the build paths.
func (t *task) playoutClock() av.Clock {
	if t == nil || t.timeline == nil {
		return nil
	}
	return t.timeline
}

func playoutStageName(name string) string {
	return gateStageName("playout", name)
}

func playoutTimebaseError(policy flow.PlayoutPolicy, msg *pipeline.Message) error {
	return gateTimebaseError("schedule playout delivery", playoutStageName(policy.Name()), msg, []string{
		"set Packet.PTS or Frame.PTS with a valid av.TimeBase before the playout gate",
		"declare live stream TimeBase facts on the input or discovered stream",
		"remove .Playout(...) from branches that cannot carry media timestamps",
	})
}

func validatePlayoutPolicyForStream(operation string, branchName string, stream av.Stream, operations []operationSpec) error {
	if !operationSpecsContainPlayout(operations) {
		return nil
	}
	return validatePlayoutPolicyForBranch(operation, branchName, stream)
}

func validatePlayoutPolicyForRecipeIROperations(operation string, branchName string, stream av.Stream, operations []recipeir.Operation) error {
	if !recipeIROperationsContainPlayout(operations) {
		return nil
	}
	return validatePlayoutPolicyForBranch(operation, branchName, stream)
}

func validatePlayoutPolicyForBranch(operation string, branchName string, stream av.Stream) error {
	return validateGatePolicyForBranch(operation, branchName, stream,
		"playout branches need stream timebase facts before graph mutation", []string{
			"set av.Stream.TimeBase on the input.Stream(...) anchor or discovered stream",
			"set Codec.ClockRate so RTP-style timestamps can derive a timebase",
			"remove .Playout(...) from dynamic branches whose source has no media timeline",
		})
}

func operationSpecsContainPlayout(operations []operationSpec) bool {
	for i := range operations {
		if _, ok := operations[i].Stage.(*playoutGate); ok {
			return true
		}
	}
	return false
}

func recipeIROperationsContainPlayout(operations []recipeir.Operation) bool {
	for i := range operations {
		if _, ok := operations[i].Stage.(*playoutGate); ok {
			return true
		}
	}
	return false
}

func playoutStageOperationName(operation operationSpec, branchName string) string {
	if _, ok := operation.Stage.(*playoutGate); !ok {
		if operation.Stage != nil {
			return operation.Stage.Name()
		}
		return operation.Component
	}
	return playoutStageName(firstNonEmpty(branchName, operation.Component, "branch"))
}

var _ pipeline.Stage = (*playoutGate)(nil)
var _ pipeline.NodeDescriber = (*playoutGate)(nil)
var _ pipeline.DropReporter = (*playoutGate)(nil)
var _ shape.Contract = (*playoutGate)(nil)
