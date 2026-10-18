package goav

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// recordingTestClock is a deterministic inner av.Clock for timeline tests:
// Sleep never blocks — it records the requested wall wait and advances Now by
// it — and Advance moves the reading manually (the goavtest.Clock shape,
// local because the root package cannot import goavtest).
type recordingTestClock struct {
	mu     sync.Mutex
	now    time.Duration
	sleeps []time.Duration
}

func (c *recordingTestClock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *recordingTestClock) Sleep(ctx context.Context, d time.Duration) error {
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

func (c *recordingTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	c.mu.Unlock()
}

func (c *recordingTestClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

// assertClockSleeps pins the exact wall-wait trace the fake clock recorded;
// call with no wants to pin that nothing slept.
func assertClockSleeps(t *testing.T, clock *recordingTestClock, want ...time.Duration) {
	t.Helper()
	got := clock.Sleeps()
	if len(got) != len(want) {
		t.Fatalf("clock sleeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("clock sleeps = %v, want %v", got, want)
		}
	}
}

// blockingTestClock is an inner av.Clock whose Sleep parks until the test
// releases it, so a test can hold a timeline.Sleep mid-wait and change the
// epoch underneath it. Every Sleep call reports its requested wall duration on
// requests; release answers exactly one parked sleeper.
type blockingTestClock struct {
	mu       sync.Mutex
	now      time.Duration
	requests chan time.Duration
	release  chan time.Duration // wall time to advance before returning
}

func newBlockingTestClock() *blockingTestClock {
	return &blockingTestClock{
		requests: make(chan time.Duration, 16),
		release:  make(chan time.Duration),
	}
}

func (c *blockingTestClock) Now() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *blockingTestClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	c.mu.Unlock()
}

func (c *blockingTestClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.requests <- d
	select {
	case <-ctx.Done():
		return ctx.Err()
	case advance := <-c.release:
		c.advance(advance)
		return nil
	}
}

func waitSleepRequest(t *testing.T, clock *blockingTestClock) time.Duration {
	t.Helper()
	select {
	case d := <-clock.requests:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("no inner sleep request arrived")
		return 0
	}
}

func TestTimelineRateChangeMidSleepWakesEarly(t *testing.T) {
	clock := newBlockingTestClock()
	timeline := newTimeline(clock)

	slept := make(chan error, 1)
	go func() {
		slept <- timeline.Sleep(context.Background(), time.Second)
	}()

	// The sleeper asks the inner clock for the full second at 1x and parks.
	if got := waitSleepRequest(t, clock); got != time.Second {
		t.Fatalf("first inner wait = %v, want 1s", got)
	}

	// A rate change mid-sleep must abandon the stale wait and re-derive the
	// remaining timeline duration: still 1s of timeline left, now 500ms of
	// inner time at 2x.
	if err := timeline.SetRate(2); err != nil {
		t.Fatal(err)
	}
	if got := waitSleepRequest(t, clock); got != 500*time.Millisecond {
		t.Fatalf("post-rate inner wait = %v, want 500ms", got)
	}

	// Completing the re-derived wait finishes the sleep with the timeline
	// having advanced exactly the requested duration.
	clock.release <- 500 * time.Millisecond
	if err := <-slept; err != nil {
		t.Fatalf("Sleep err = %v", err)
	}
	if got := timeline.Now(); got != time.Second {
		t.Fatalf("timeline.Now() = %v, want 1s", got)
	}
	// The abandoned 1s inner sleeper was cancelled, not leaked: its context
	// ended, so nothing waits on release.
}

func TestTimelinePauseFreezesNowAndResumeContinues(t *testing.T) {
	clock := &recordingTestClock{}
	timeline := newTimeline(clock)

	clock.Advance(100 * time.Millisecond)
	if got := timeline.Now(); got != 100*time.Millisecond {
		t.Fatalf("Now = %v, want 100ms", got)
	}

	timeline.Pause()
	clock.Advance(time.Second)
	if got := timeline.Now(); got != 100*time.Millisecond {
		t.Fatalf("paused Now = %v, want frozen 100ms", got)
	}
	// Pausing again is a no-op, not a re-anchor.
	timeline.Pause()
	if got := timeline.Now(); got != 100*time.Millisecond {
		t.Fatalf("double-paused Now = %v, want frozen 100ms", got)
	}

	timeline.Resume()
	clock.Advance(50 * time.Millisecond)
	if got := timeline.Now(); got != 150*time.Millisecond {
		t.Fatalf("resumed Now = %v, want 150ms (pause gap excluded)", got)
	}
	// Resuming a running timeline is a no-op.
	timeline.Resume()
	if got := timeline.Now(); got != 150*time.Millisecond {
		t.Fatalf("double-resumed Now = %v, want 150ms", got)
	}
}

func TestTimelinePauseHoldsSleepUntilResume(t *testing.T) {
	clock := newBlockingTestClock()
	timeline := newTimeline(clock)
	timeline.Pause()

	slept := make(chan error, 1)
	go func() {
		slept <- timeline.Sleep(context.Background(), 100*time.Millisecond)
	}()

	// Paused sleepers wait for an epoch change without consulting the inner
	// clock at all.
	select {
	case d := <-clock.requests:
		t.Fatalf("paused Sleep asked the inner clock for %v", d)
	case <-time.After(10 * time.Millisecond):
	}

	timeline.Resume()
	if got := waitSleepRequest(t, clock); got != 100*time.Millisecond {
		t.Fatalf("post-resume inner wait = %v, want 100ms", got)
	}
	clock.release <- 100 * time.Millisecond
	if err := <-slept; err != nil {
		t.Fatalf("Sleep err = %v", err)
	}
}

func TestTimelineSleepScalesWallTimeByRate(t *testing.T) {
	clock := &recordingTestClock{}
	timeline := newTimeline(clock)
	if err := timeline.SetRate(4); err != nil {
		t.Fatal(err)
	}
	if got := timeline.Rate(); got != 4.0 {
		t.Fatalf("Rate = %v, want 4", got)
	}
	if err := timeline.Sleep(context.Background(), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// 100ms of timeline at 4x costs 25ms of inner time.
	if got := clock.Sleeps(); len(got) != 1 || got[0] != 25*time.Millisecond {
		t.Fatalf("inner sleeps = %v, want [25ms]", got)
	}
	if got := timeline.Now(); got != 100*time.Millisecond {
		t.Fatalf("Now = %v, want 100ms", got)
	}
}

func TestTimelineSleepHonoursContext(t *testing.T) {
	clock := newBlockingTestClock()
	timeline := newTimeline(clock)

	ctx, cancel := context.WithCancel(context.Background())
	slept := make(chan error, 1)
	go func() {
		slept <- timeline.Sleep(ctx, time.Second)
	}()
	waitSleepRequest(t, clock)
	cancel()
	if err := <-slept; !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep err = %v, want context.Canceled", err)
	}

	done, cancelDone := context.WithCancel(context.Background())
	cancelDone()
	if err := timeline.Sleep(done, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep on done ctx = %v, want context.Canceled", err)
	}
}

func TestTimelineSetRateRejectsInvalidRates(t *testing.T) {
	timeline := newTimeline(&recordingTestClock{})
	for _, rate := range []float64{0, -1} {
		if err := timeline.SetRate(rate); err == nil {
			t.Fatalf("SetRate(%v) accepted, want rejection", rate)
		}
	}
	if got := timeline.Rate(); got != 1.0 {
		t.Fatalf("Rate after rejections = %v, want unchanged 1", got)
	}
}

func TestTimelineConcurrentNowUnderEpochChanges(t *testing.T) {
	clock := &recordingTestClock{}
	timeline := newTimeline(clock)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Lock-free readers race the epoch swaps; under -race this
				// pins the synchronization, and the reading must always stay
				// inside the advanced range (rates are 1..3 over 200ms of
				// inner time).
				now := timeline.Now()
				if now < 0 || now > 600*time.Millisecond {
					t.Errorf("timeline reading out of range: %v", now)
					return
				}
			}
		})
	}
	for i := range 200 {
		if err := timeline.SetRate(float64(1 + i%3)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Millisecond)
		timeline.Pause()
		timeline.Resume()
	}
	close(stop)
	wg.Wait()
}

// TestTimelineRealClockSleepAllocs pins the real-clock paced sleep at zero
// allocations: on av.MonotonicClock the timeline waits on a pooled timer, the
// change broadcast and ctx directly, with no goroutine, child context or
// channel per sleep (one per paced packet on the realtime path).
func TestTimelineRealClockSleepAllocs(t *testing.T) {
	for _, timeline := range []*timeline{newTimeline(nil), newTimeline(av.MonotonicClock())} {
		if !timeline.monotonic {
			t.Fatal("timeline on av.MonotonicClock does not take the direct timer path")
		}
		ctx := context.Background()
		if allocs := testing.AllocsPerRun(200, func() {
			if err := timeline.Sleep(ctx, 20*time.Microsecond); err != nil {
				t.Fatal(err)
			}
		}); allocs != 0 {
			t.Fatalf("real-clock timeline Sleep allocs = %v, want 0", allocs)
		}
	}
	if newTimeline(newBlockingTestClock()).monotonic {
		t.Fatal("a fake clock took the real-clock path; its Sleep would be bypassed")
	}
}

// TestTimelineRealClockSleepWakes pins the real-clock wait's three exits: a
// sleeper parks on its own goroutine only (none spawned per sleep), a rate
// change re-derives the remaining wait mid-sleep, and ctx interrupts it.
func TestTimelineRealClockSleepWakes(t *testing.T) {
	timeline := newTimeline(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := goruntime.NumGoroutine()
	slept := make(chan error, 1)
	go func() { slept <- timeline.Sleep(ctx, time.Hour) }()
	time.Sleep(20 * time.Millisecond)
	if during := goruntime.NumGoroutine(); during > before+1 {
		t.Fatalf("goroutines during one paced sleep = %d, want at most %d (the sleeper)", during, before+1)
	}
	// At a rate of 1e9 the remaining hour of timeline time is 3.6ms of wall.
	if err := timeline.SetRate(1e9); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-slept:
		if err != nil {
			t.Fatalf("Sleep = %v, want nil after the rate change shortened it", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rate change did not wake the real-clock sleeper")
	}

	if err := timeline.SetRate(1); err != nil {
		t.Fatal(err)
	}
	go func() { slept <- timeline.Sleep(ctx, time.Hour) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-slept:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not wake the real-clock sleeper")
	}
}
