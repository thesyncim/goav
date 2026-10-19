//go:build race

package av

// raceEnabled reports the race detector, under which sync.Pool drops a
// share of what it is given, so pool-reuse allocation pins skip.
const raceEnabled = true
