package compile

// joinSyncMode selects how a convergent join (Mix/Composite) aligns its arms.
type JoinSyncMode int

const (
	// joinSyncArrival pairs arm frames by arrival: one buffered frame per arm
	// per step, no timestamp math. Right for live sources on one clock; the
	// default.
	JoinSyncArrival JoinSyncMode = iota
	// joinSyncPTS aligns arm frames by presentation time on a common
	// nanosecond clock — opt-in via LegacyMix(...).SyncByPTS() and
	// LegacyComposite(...).SyncByPTS().
	JoinSyncPTS
)

// joinSyncNodeDetail renders the join node's planned/built graph detail.
// Arrival is the default and stays unannotated, so existing plans are
// byte-identical; the planner and the stages' DescribeNode share this one
// function, keeping Describe() ≡ Build().
func JoinSyncNodeDetail(mode JoinSyncMode) string {
	if mode == JoinSyncPTS {
		return "sync=pts"
	}
	return ""
}
