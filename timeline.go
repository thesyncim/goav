package goav

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
)

// timeline is the task-wide clock service: one shared av.Clock per task that
// maps the runtime clock onto media run time through an atomically readable
// rate and pause epoch. Paced consumers — the realtime demux pacer, clock-aware
// provider sources, and (later slices) playout gates — read and sleep on the
// timeline, so one control.Rate re-anchors every consumer coherently instead
// of each source keeping a private rate.
//
// Concurrency: Now is lock-free (one atomic epoch load, arithmetic only), per
// the data-plane rule. SetRate/Pause/Resume serialize on a cold-side mutex,
// re-anchor the epoch atomically, and rotate a broadcast channel so in-flight
// Sleeps recompute their remaining wall wait under the new epoch.
type timeline struct {
	inner av.Clock

	// epoch is the immutable rate/pause anchor Now computes from.
	epoch atomic.Pointer[timelineEpoch]
	// change is closed and replaced on every epoch change, waking sleepers.
	change atomic.Pointer[chan struct{}]
	// consumers records that at least one paced consumer waits on the
	// timeline; task rate controls are refused before anything paces.
	consumers atomic.Bool

	// mu serializes epoch changes (control side only; never on Now/Sleep's
	// fast path).
	mu sync.Mutex
}

// timelineEpoch is one immutable rate/pause segment: timeline time is
// mediaAnchor plus rate-scaled inner-clock time since wallAnchor, frozen at
// mediaAnchor while paused.
type timelineEpoch struct {
	wallAnchor  time.Duration
	mediaAnchor time.Duration
	rate        float64
	paused      bool
}

var _ av.Clock = (*timeline)(nil)

// newTimeline anchors a timeline at zero on the inner clock's current reading,
// running at 1x. A nil inner clock falls back to av.MonotonicClock, matching
// the pacing default.
func newTimeline(inner av.Clock) *timeline {
	if inner == nil {
		inner = av.MonotonicClock()
	}
	t := &timeline{inner: inner}
	t.epoch.Store(&timelineEpoch{wallAnchor: inner.Now(), rate: 1})
	change := make(chan struct{})
	t.change.Store(&change)
	return t
}

// Now reports the timeline reading: the integral of rate over unpaused inner
// time since construction. Lock-free — one epoch load plus arithmetic.
func (t *timeline) Now() time.Duration {
	return t.epoch.Load().readingAt(t.inner.Now())
}

// readingAt maps one inner-clock reading onto the epoch's timeline segment.
func (e *timelineEpoch) readingAt(wall time.Duration) time.Duration {
	if e.paused {
		return e.mediaAnchor
	}
	return e.mediaAnchor + time.Duration(float64(wall-e.wallAnchor)*e.rate)
}

// Rate reports the current playback-rate multiplier.
func (t *timeline) Rate() float64 {
	return t.epoch.Load().rate
}

// SetRate re-anchors the timeline at the current reading with a new rate.
// In-flight Sleeps wake and recompute their remaining wall wait, so every
// paced consumer changes pace together, mid-sleep included.
func (t *timeline) SetRate(rate float64) error {
	if !(rate > 0) || math.IsInf(rate, 0) {
		return fmt.Errorf("goav: timeline needs a positive, finite playback rate, got %v", rate)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	epoch := t.epoch.Load()
	// One inner reading anchors both fields, so the switch is seamless: the
	// new segment starts exactly where the old one ends.
	wall := t.inner.Now()
	t.setEpochLocked(timelineEpoch{
		wallAnchor:  wall,
		mediaAnchor: epoch.readingAt(wall),
		rate:        rate,
		paused:      epoch.paused,
	})
	return nil
}

// Pause freezes the timeline at its current reading. Sleeps hold until Resume
// (or their context ends). Pausing a paused timeline is a no-op.
func (t *timeline) Pause() {
	t.mu.Lock()
	defer t.mu.Unlock()
	epoch := t.epoch.Load()
	if epoch.paused {
		return
	}
	t.setEpochLocked(timelineEpoch{
		mediaAnchor: epoch.readingAt(t.inner.Now()),
		rate:        epoch.rate,
		paused:      true,
	})
}

// Resume continues a paused timeline from the frozen reading at the current
// rate. Resuming a running timeline is a no-op.
func (t *timeline) Resume() {
	t.mu.Lock()
	defer t.mu.Unlock()
	epoch := t.epoch.Load()
	if !epoch.paused {
		return
	}
	t.setEpochLocked(timelineEpoch{
		wallAnchor:  t.inner.Now(),
		mediaAnchor: epoch.mediaAnchor,
		rate:        epoch.rate,
	})
}

// setEpochLocked publishes the new epoch, then rotates the change broadcast so
// sleepers observe it: a sleeper that loads the old channel is woken by its
// close; one that loads the new channel already sees the new epoch (atomic
// loads/stores order the pair).
func (t *timeline) setEpochLocked(next timelineEpoch) {
	t.epoch.Store(&next)
	fresh := make(chan struct{})
	old := t.change.Swap(&fresh)
	close(*old)
}

// markPaced records that a paced consumer (demux pacer, clock-aware source)
// waits on this timeline; the task refuses rate controls until one exists.
func (t *timeline) markPaced() {
	t.consumers.Store(true)
}

// paced reports whether any paced consumer waits on the timeline.
func (t *timeline) paced() bool {
	return t.consumers.Load()
}

// Sleep blocks until d of timeline time has elapsed or ctx is done. d is a
// timeline duration: at rate 2 a 100ms sleep costs 50ms of inner time, and a
// rate or pause change mid-sleep re-derives the remaining wall wait instead of
// finishing on the stale conversion. While paused it holds for the next epoch
// change. All wall waits go through the inner clock's Sleep so fake clocks
// keep pacing deterministic.
func (t *timeline) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	target := t.Now() + d
	for {
		// Load the broadcast channel before reading the epoch: an epoch change
		// after this load closes the loaded channel, and one before it is
		// already visible in the epoch read below.
		change := *t.change.Load()
		epoch := t.epoch.Load()
		remaining := target - t.Now()
		if remaining <= 0 {
			return nil
		}
		if epoch.paused {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-change:
			}
			continue
		}
		wall := time.Duration(float64(remaining) / epoch.rate)
		if wall <= 0 {
			// Sub-nanosecond remainder at a high rate: take the smallest
			// representable wait so the loop always advances the inner clock.
			wall = 1
		}
		if err := t.sleepInner(ctx, change, wall); err != nil {
			return err
		}
	}
}

// sleepInner waits wall time on the inner clock, waking early when the epoch
// changes or ctx ends. The inner Sleep runs in a goroutine with its own
// cancelable context so an abandoned wait releases fake-clock waiters instead
// of leaking them.
func (t *timeline) sleepInner(ctx context.Context, change <-chan struct{}, wall time.Duration) error {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- t.inner.Sleep(waitCtx, wall)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-change:
		// Epoch changed: abandon the pending wait; the caller recomputes the
		// remaining timeline duration under the new epoch.
		return nil
	case err := <-done:
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
}
