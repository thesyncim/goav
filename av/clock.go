package av

import (
	"context"
	"time"
)

// Clock is the time source paced delivery runs on: a monotonic reading plus an
// interruptible wait. It is deliberately tiny — the realtime demux pacer is its
// first consumer, and a future pipeline-wide clock service can implement the
// same contract without churn. Tests inject a fake so nothing sleeps for real.
//
// Implementations must be safe for concurrent use: a pacing loop reads the
// clock while controls arrive from other goroutines.
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
