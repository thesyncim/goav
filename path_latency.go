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
func planPathLatencyDecision(env compileEnv, branchName string, operations []recipeir.Operation) (planDecision, bool) {
	var syncTolerance, playoutOffset time.Duration
	gated := false
	for i := range operations {
		if gate, ok := operations[i].Stage.(gateStage); ok {
			gated = true
			sync, playout := gate.DeclaredLatency()
			syncTolerance += sync
			playoutOffset += playout
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
	if env.Present && !env.Buffer.IsDirect() {
		if env.Buffer.MaxLatency > 0 {
			total += env.Buffer.MaxLatency
			terms = append(terms, "queue max latency "+env.Buffer.MaxLatency.String())
		}
		if env.Buffer.Capacity > 0 {
			facts = append(facts, "queue capacity "+strconv.Itoa(env.Buffer.Capacity)+" messages per node (a count, not a duration)")
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

// gateStage is a sync or playout gate a recipe declares (.Sync, .Playout):
// GateName names its node on a branch, and DeclaredLatency is the latency
// its policy declares: a sync tolerance and a playout offset.
type gateStage interface {
	GateName(branch string) string
	DeclaredLatency() (sync, playout time.Duration)
}
