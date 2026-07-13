package goav

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/pipeline"
)

// qosReportWindow is the producer-side rate limit on QoS reports: each gate
// reports at most once per window per stream (gates sit on single-stream
// branches, so per gate is per stream), so a persistently late stream cannot
// storm the event channel. The window is measured on the producer's clock —
// the task timeline for playout gates, the process monotonic clock for sync
// gates — so it needs no extra time source.
const qosReportWindow = time.Second

// qosWindowUnset marks a gate that never reported; the first late message
// always claims the window, even near a fake clock's zero origin.
const qosWindowUnset = math.MinInt64

// qosPolicyRefusalReason is the Reason on the EventQoS a task publishes when
// its QoS policy issued a control the task refused (Cause carries the error).
const qosPolicyRefusalReason = "qos policy control refused"

// qosWindowClock rate-limits producers with no bound timeline (sync gates).
var qosWindowClock = av.MonotonicClock()

// claimQoSWindow claims the report window ending at now: it succeeds when the
// last report is at least qosReportWindow old. One load when suppressed, one
// CAS when claiming — the gate path pays no lock. Gates handle messages on one
// serial worker, so the CAS is uncontended and a failed CAS means a concurrent
// claim already covered the window.
func claimQoSWindow(last *atomic.Int64, now time.Duration) bool {
	previous := last.Load()
	if previous != qosWindowUnset && now-time.Duration(previous) < qosReportWindow {
		return false
	}
	return last.CompareAndSwap(previous, int64(now))
}

// qosReporter is the late-binding seam between gates bound at lowering time
// (before the task exists) and the task's cold-path event surface: the build
// threads one reporter through the runtime clone next to the timeline, and the
// finished task attaches itself. Reports published before attach are dropped —
// nothing flows before the task exists.
type qosReporter struct {
	task atomic.Pointer[task]
}

func newQoSReporter() *qosReporter {
	return &qosReporter{}
}

func (r *qosReporter) attach(t *task) {
	if r != nil && t != nil {
		r.task.Store(t)
	}
}

// publish forwards one QoS report to the owning task's Watch surface — the
// same non-blocking task-originated publish attach errors use.
func (r *qosReporter) publish(event av.Event) {
	if r == nil {
		return
	}
	if t := r.task.Load(); t != nil {
		t.watch.publish(event)
	}
}

// qosReportFunc is the reporter as a bindable func, nil when the runtime
// carries no reporter (explicit-graph builders lower no gates).
func (r *runtime) qosReportFunc() func(av.Event) {
	if r == nil || r.qos == nil {
		return nil
	}
	return r.qos.publish
}

// qosReportFunc is the attach-time twin of the runtime seam: gates attached to
// a running task publish straight to its Watch surface.
func (t *task) qosReportFunc() func(av.Event) {
	if t == nil {
		return nil
	}
	return func(event av.Event) { t.watch.publish(event) }
}

// bindGateDeps threads the task-scoped dependencies into a lowered playout or
// sync gate: the pacing clock (playout only — the stage twin of the provider
// UseClock seam, marking the timeline paced so task rate controls apply) and
// the cold-path QoS publisher. Called only during lowering or attach planning,
// which happen-before the node's goroutines start, so gates read both without
// synchronization.
func bindGateDeps(stage pipeline.Stage, clock av.Clock, report func(av.Event)) {
	if named, ok := stage.(namedStage); ok {
		stage = named.stage
	}
	switch gate := stage.(type) {
	case *playoutGate:
		gate.bindClock(clock)
		markTimelinePaced(clock)
		if report != nil {
			gate.report = report
		}
	case *syncGate:
		if report != nil {
			gate.report = report
		}
	}
}

// startQoSPolicy runs the runtime's opt-in QoS policy on the task's event
// stream: a Watch subscription filtered to EventQoS feeds the policy on the
// cold-path delivery goroutine (never the data plane), and every control it
// returns goes through the normal task.Control path with its refusals — the
// packaged form of what an application does by hand with Watch + Control. A
// refused control is published back as an EventQoS carrying the error on
// Cause; refusal events carry no lateness payload, so EventQoSReport screens
// them out and the policy never reacts to its own refusals. The goroutine
// exits when the subscription closes with the task.
func (t *task) startQoSPolicy(policy func(av.Event) []control.Control) {
	if t == nil || policy == nil {
		return
	}
	sub := t.Watch(func(event av.Event) bool { return event.Type == av.EventQoS })
	go t.runQoSPolicy(sub, policy)
}

func (t *task) runQoSPolicy(sub inspect.Subscription, policy func(av.Event) []control.Control) {
	for event := range sub.Events() {
		if _, _, ok := av.EventQoSReport(&event); !ok {
			continue
		}
		for _, ctrl := range policy(event) {
			if err := t.Control(context.Background(), ctrl); err != nil {
				t.watch.publish(av.Event{
					Type:     av.EventQoS,
					StreamID: event.StreamID,
					Reason:   qosPolicyRefusalReason,
					Cause:    err,
				})
			}
		}
	}
}
