package interleave

// Ordering is the scheduling decision shared by the runtime and its planner.
// A plain blocking Admitter cannot participate: only Gate lets the scheduler
// inspect arm readiness without parking a delivery that holds the turn.
type Ordering struct {
	Realtime, Buffered                   bool
	Sources, TimedSources                int
	Converging, BlockingAdmitter, Merges bool
}

func (p Ordering) Enabled() bool {
	if p.Realtime || p.TimedSources < 2 || p.BlockingAdmitter {
		return false
	}
	if (p.Buffered || p.Converging) && p.TimedSources != p.Sources {
		return false
	}
	return !p.Buffered || p.Merges
}
