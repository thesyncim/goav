// Package raceflag reports whether the race detector is compiled in, so an
// allocation pin that relies on av.NewBuffer's pool reuse can skip under
// -race: there sync.Pool drops a share of what it is given, on purpose.
package raceflag

// SkipReason is the skip message for pool-backed allocation pins.
const SkipReason = "sync.Pool drops items under the race detector; the pin runs without -race"
