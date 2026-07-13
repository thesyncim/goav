package pipeline

import (
	"sync/atomic"

	"github.com/thesyncim/goav/av"
)

// readiness is the once-per-run countdown behind av.EventTaskReady, shared by
// both runners: Run arms it with the sinks that have not yet seen media, the
// sink delivery path counts each one out on its first packet or frame (Remove
// counts out a sink that leaves unfed), and whoever takes the count to zero
// publishes the event — at most once, guarded by fired.
type readiness struct {
	remaining atomic.Int64
	fired     atomic.Bool
}

// arm snapshots the countdown at Run start and reports whether the graph is
// ready as it starts: sinks exist and every one already saw media. With no
// sinks there is nothing to wait for, so a sink-less graph never reports.
func (r *readiness) arm(sinks, unfed int64) bool {
	r.remaining.Store(unfed)
	return sinks > 0 && unfed == 0
}

// note counts a sink out exactly once (the CAS keeps the countdown
// single-shot per sink) and reports whether it was the last — the caller
// publishes readiness.
func (r *readiness) note(sawMedia *atomic.Bool) bool {
	if !sawMedia.CompareAndSwap(false, true) {
		return false
	}
	return r.remaining.Add(-1) == 0
}

// claim reports whether the caller wins the single readiness publish.
func (r *readiness) claim() bool {
	return r.fired.CompareAndSwap(false, true)
}

// taskReadyEvent is the graph-level av.EventTaskReady both runners publish.
func taskReadyEvent() av.Event {
	return av.Event{Type: av.EventTaskReady, Reason: "every sink received its first media message"}
}
