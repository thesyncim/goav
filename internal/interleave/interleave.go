// Package interleave is the contract between the offline pipeline runner and
// the sources it may interleave by time. A direct, non-realtime graph with
// several such sources advances the one whose next packet is earliest, so a
// run's output is the same on every run. The contract stays inside the
// module: no other package can name Token, so none can opt in by accident.
package interleave

// Token keeps Source unimplementable outside the module.
type Token struct{}

// Source is a graph source whose Start loop blocks only on Emit and on
// bounded input reads, and whose packets carry their times: a file demuxer
// pumping offline. ByTime reports whether this run of the source qualifies
// (a paced, realtime pump does not).
type Source interface {
	ByTime(Token) bool
}
