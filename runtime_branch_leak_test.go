package goav_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// assertGoroutinesSettle fails if the live goroutine count does not return to
// near base within a short window after teardown. The runtime starts a per-node
// worker for every attached branch; if Detach (Remove) or Task.Close failed to
// stop one, the count stays elevated. goleak would be the usual tool, but the
// root module pins its dependency set (TestRootModuleDependencyPurity), so this
// stays stdlib-only: a real leak keeps the count high for the whole window,
// while transient runtime goroutines drain inside it.
func assertGoroutinesSettle(t *testing.T, base int) {
	t.Helper()
	const tolerance = 6
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= base+tolerance {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak after teardown: %d live, want <= %d (baseline %d)", n, base+tolerance, base)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRuntimeBranchAttachDetachChurnDoesNotLeak churns many attach/detach cycles
// on one live task. Each cycle waits until the branch actually receives a frame
// (so its worker really started) before detaching, then the test asserts the
// goroutine count settles back to baseline once the task stops — proving Detach
// and Task.Close stop every per-branch worker they started, with no slow leak
// across repeated runtime mutation.
func TestRuntimeBranchAttachDetachChurnDoesNotLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := runtime.NumGoroutine()

	main := &frameCountSink{name: "main"}
	task := decodedVideoTapTask(t, ctx, liveMutableVideoPackets("camera", 64, 64, time.Millisecond), main)
	defer task.Close() // safety net if an assertion fails mid-churn

	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()
	waitFrameCount(ctx, &main.count, 1)

	const cycles = 30
	for i := 0; i < cycles; i++ {
		sink := &frameCountSink{name: fmt.Sprintf("churn-%d", i)}
		attachment := attachDropBranch(t, ctx, task, fmt.Sprintf("churn-%d", i), sink)
		waitFrameCount(ctx, &sink.count, 1)
		if sink.count.Load() == 0 {
			t.Fatalf("cycle %d: branch never received a frame before detach", i)
		}
		if err := task.Detach(ctx, attachment); err != nil {
			t.Fatalf("cycle %d: Detach error = %v", i, err)
		}
	}

	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertGoroutinesSettle(t, base)
}
