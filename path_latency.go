package goav

import (
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/internal/recipeir"
)

// planPathLatencyDecision composes the declared latency contributions along
// one planned branch into a single plan decision (code "path_latency_budget"):
// every sync tolerance and playout offset the path declares, plus the
// runtime's declared buffered queue MaxLatency, sum to the path's latency
// budget. Queue capacity is a message count, not a duration, so it is
// reported as a separate fact instead of a fake duration. The row is emitted
// only for paths that declare a sync or playout gate, and every figure is
// declared policy — Explain never measures. Contributions the root cannot see
// (a transport provider's internal jitter buffer, e.g. rtpav's) are absent by
// construction: no provider seam declares latency today.
func planPathLatencyDecision(rt *runtime, branchName string, operations []recipeir.Operation) (planDecision, bool) {
	var syncTolerance, playoutOffset time.Duration
	gated := false
	for i := range operations {
		switch gate := operations[i].Stage.(type) {
		case *syncGate:
			gated = true
			syncTolerance += gate.policy.Tolerance()
		case *playoutGate:
			gated = true
			playoutOffset += gate.policy.Offset()
		}
	}
	if !gated {
		return planDecision{}, false
	}
	total := syncTolerance + playoutOffset
	terms := make([]string, 0, 3)
	if syncTolerance > 0 {
		terms = append(terms, "sync tolerance "+syncTolerance.String())
	}
	if playoutOffset > 0 {
		terms = append(terms, "playout offset "+playoutOffset.String())
	}
	var facts []string
	if rt != nil && !rt.buffer.IsDirect() {
		if rt.buffer.MaxLatency > 0 {
			total += rt.buffer.MaxLatency
			terms = append(terms, "queue max latency "+rt.buffer.MaxLatency.String())
		}
		if rt.buffer.Capacity > 0 {
			facts = append(facts, "queue capacity "+strconv.Itoa(rt.buffer.Capacity)+" messages per node (a count, not a duration)")
		}
	}
	message := "declared latency budget " + total.String()
	if len(terms) > 0 {
		message += " = " + strings.Join(terms, " + ")
	}
	if len(facts) > 0 {
		message += "; " + strings.Join(facts, "; ")
	}
	return planDecision{
		Code:    diagnosticPathLatencyBudget,
		Branch:  branchName,
		Message: message,
	}, true
}
