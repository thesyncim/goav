// The deterministic av.Clock realtime pacing runs on in tests.

package goavtest

import (
	"context"
	"sync"
	"time"

	"github.com/thesyncim/goav/av"
)

// Clock is a deterministic av.Clock for paced pipelines: Sleep never blocks —
// it records the requested wait and advances Now by it — so a realtime task
// runs instantly while a test asserts the exact sleep sequence the pacer
// asked for. Pair it with goav.WithClock (Runtime injects one by default).
// Advance moves the reading manually for code that polls Now without
// sleeping. Safe for concurrent use.
type Clock struct {
	mu     sync.Mutex
	now    time.Duration
	sleeps []time.Duration
}

var _ av.Clock = (*Clock)(nil)

// NewClock returns a Clock reading zero with no recorded sleeps.
func NewClock() *Clock {
	return &Clock{}
}

// Now reports the elapsed time on the fake timeline: the sum of every
// positive Sleep plus every Advance so far.
func (c *Clock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep implements av.Clock without blocking: it records d and advances Now
// by it when positive, returning ctx.Err() only when ctx is already done.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	if d > 0 {
		c.now += d
	}
	c.mu.Unlock()
	return nil
}

// Advance moves Now forward by d without recording a sleep.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	c.mu.Unlock()
}

// Sleeps returns every wait requested through Sleep, in order — the pacing
// trace a test asserts against.
func (c *Clock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}
