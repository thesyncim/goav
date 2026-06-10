package av

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMonotonicClockNowNeverGoesBackwards(t *testing.T) {
	clock := MonotonicClock()
	previous := clock.Now()
	for i := 0; i < 100; i++ {
		now := clock.Now()
		if now < previous {
			t.Fatalf("Now went backwards: %v after %v", now, previous)
		}
		previous = now
	}
}

func TestMonotonicClockSleepHonoursCancellation(t *testing.T) {
	clock := MonotonicClock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// An already-done ctx interrupts immediately — even a giant wait never
	// actually sleeps.
	if err := clock.Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep err = %v, want context.Canceled", err)
	}
	// And a non-positive wait still reports the done ctx instead of nil.
	if err := clock.Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(0) on done ctx err = %v, want context.Canceled", err)
	}
}

func TestMonotonicClockSleepCompletes(t *testing.T) {
	clock := MonotonicClock()
	ctx := context.Background()
	if err := clock.Sleep(ctx, 0); err != nil {
		t.Fatalf("Sleep(0) err = %v, want nil", err)
	}
	if err := clock.Sleep(ctx, -time.Second); err != nil {
		t.Fatalf("Sleep(-1s) err = %v, want nil", err)
	}
	before := clock.Now()
	if err := clock.Sleep(ctx, time.Millisecond); err != nil {
		t.Fatalf("Sleep err = %v, want nil", err)
	}
	if elapsed := clock.Now() - before; elapsed < time.Millisecond {
		t.Fatalf("Sleep returned after %v, want at least 1ms", elapsed)
	}
}
