package av

import (
	"context"
	"time"
)

// Clock is the monotonic time source used by paced sources, playout gates, and
// task timelines. Tests inject a fake so nothing sleeps for real.
//
// Implementations must be safe for concurrent use.
type Clock interface {
	// Now reports the elapsed monotonic time since the clock's arbitrary,
	// fixed origin. Only differences between readings are meaningful.
	Now() time.Duration
	// Sleep blocks until d has elapsed on this clock or ctx is done, returning
	// ctx.Err() when interrupted and nil when the wait completed. A
	// non-positive d returns immediately (still honouring an already-done ctx).
	Sleep(ctx context.Context, d time.Duration) error
}

// MonotonicClock returns the standard Clock: Now reads the process monotonic
// clock and Sleep waits on a timer, interrupted by ctx. This is the default a
// realtime pacer uses when no clock is injected.
func MonotonicClock() Clock {
	return &monotonicClock{origin: time.Now()}
}

// IsMonotonicClock reports whether c is the standard process monotonic clock
// returned by MonotonicClock.
func IsMonotonicClock(c Clock) bool {
	_, ok := c.(*monotonicClock)
	return ok
}

type monotonicClock struct {
	origin time.Time
}

func (c *monotonicClock) Now() time.Duration {
	// time.Since uses the monotonic reading carried by origin, so wall-clock
	// adjustments never move the pacing timeline.
	return time.Since(c.origin)
}

func (c *monotonicClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
