// Package interleave is the contract between the offline pipeline runner and
// the nodes it may order by time. A non-realtime graph with several such
// sources advances the one whose next packet is earliest, so a run's output
// is the same on every run. The contract stays inside the module: no other
// package can name Token, so none can opt in by accident.
package interleave

// Token keeps Source and Gate unimplementable outside the module.
type Token struct{}

// Source is a graph source whose Start loop blocks only on Emit and on
// bounded input reads, and whose packets carry their times: a file demuxer
// pumping offline. ByTime reports whether this run of the source qualifies
// (a paced, realtime pump does not).
type Source interface {
	ByTime(Token) bool
}

// Gate is a converging stage (pipeline.Admitter) whose arm bound the time
// order can enforce at the turn instead of in Admit: Arm names the arm msg,
// the *pipeline.Message being delivered, is a frame of (false for messages
// the stage never holds back), and Admits reports, without blocking, whether
// the stage would take more of that arm now. An ordered run never calls
// Admit on a Gate; it passes over a source whose arm is held back, so which
// source runs next is decided by the order, never by the Go scheduler.
type Gate interface {
	Arm(tok Token, msg any) (int, bool)
	Admits(tok Token, arm int) bool
}
