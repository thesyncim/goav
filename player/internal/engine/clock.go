package engine

import (
	"context"
	"sync"
	"time"
)

// Device positions arrive in callback-sized steps (and oto counts a read a
// moment before it buffers it), so the clock does not follow them directly: it
// runs on the monotonic clock with its rate trimmed by up to maxSlew in
// proportion to the error against the device, converging over about
// slewWindow. A device more than snapAhead ahead is jumped to at once.
const (
	maxSlew    = 0.05
	slewWindow = 250 * time.Millisecond
	snapAhead  = 250 * time.Millisecond
)

// DeviceClock is the audio device's hardware clock as an av.Clock: Now is the
// time the device has spent playing (silence included), so it advances at the
// sample rate of the DAC rather than the system clock. Readings follow the
// monotonic clock slewed toward the device position, so they are smooth, never
// move backwards, and do not drift from the DAC.
//
// Handed to the goav runtime (runconfig.WithClock), it becomes the inner
// clock of the task timeline: the realtime pacer, playout gates, Pause, and
// Rate all run in the speaker's time, so media arrives exactly as fast as the
// device consumes it and the two never drift.
type DeviceClock struct {
	rate   int64
	played func() int64
	mono   func() time.Duration

	mu      sync.Mutex
	started bool
	lastAt  time.Duration
	last    time.Duration
}

// NewDeviceClock builds a clock over played (device frames played so far) at
// sampleRate. mono is the monotonic time source used for interpolation; nil
// means the process monotonic clock.
func NewDeviceClock(sampleRate int, played func() int64, mono func() time.Duration) *DeviceClock {
	if mono == nil {
		origin := time.Now()
		mono = func() time.Duration { return time.Since(origin) }
	}
	return &DeviceClock{rate: int64(sampleRate), played: played, mono: mono}
}

// Now reports elapsed device time since the device opened.
func (c *DeviceClock) Now() time.Duration {
	raw := framesToDuration(c.played(), c.rate)
	at := c.mono()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started {
		c.started, c.last, c.lastAt = true, raw, at
		return raw
	}
	dt := at - c.lastAt
	if dt <= 0 {
		return c.last
	}
	t := c.last + dt
	if drift := raw - t; drift > snapAhead {
		t = raw
	} else {
		k := max(-maxSlew, min(maxSlew, float64(drift)/float64(slewWindow)))
		t = c.last + time.Duration(float64(dt)*(1+k))
	}
	c.last, c.lastAt = t, at
	return t
}

// Sleep waits until d of device time has elapsed or ctx is done.
func (c *DeviceClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	target := c.Now() + d
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		remaining := target - c.Now()
		if remaining <= 0 {
			return nil
		}
		// A device running slow (or stalled) is waited out in short steps.
		timer.Reset(max(remaining, time.Millisecond))
	}
}

func framesToDuration(frames, rate int64) time.Duration {
	if rate <= 0 {
		return 0
	}
	return time.Duration(frames/rate*int64(time.Second)) + time.Duration(frames%rate*int64(time.Second)/rate)
}

func durationToFrames(d time.Duration) int64 {
	return int64(d/time.Second)*OutputRate + int64(d%time.Second)*OutputRate/int64(time.Second)
}
