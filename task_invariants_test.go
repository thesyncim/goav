// Runtime lifecycle invariants, pinned consumer side: Task.Close is
// idempotent in every ordering (before, after, and during Run), a destination
// writer closes exactly once no matter how many times the task is closed,
// Snapshot stays race-safe while branches attach and detach, and finalize
// failures surface predictably — a transactional destination's Commit error
// returns from Task.Close on the Build path and from Job.Run on the one-shot
// path. Sibling pins: branch pause/shed (runtime_branch_control_test.go),
// attach rollback (graph_test.go), watcher shedding (watch_test.go).

package goav_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/provider"
)

// countingWriteCloser counts Close calls so tests can pin the
// exactly-one-close destination writer contract.
type countingWriteCloser struct {
	closes int
}

func (w *countingWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (w *countingWriteCloser) Close() error                { w.closes++; return nil }

// transactionalTestWriter is a provider.TransactionalWriter whose Commit
// can fail, for pinning where finalize errors surface.
type transactionalTestWriter struct {
	commitErr error
	commits   int
	aborts    int
	closes    int
}

func (w *transactionalTestWriter) Write(p []byte) (int, error)  { return len(p), nil }
func (w *transactionalTestWriter) Close() error                 { w.closes++; return nil }
func (w *transactionalTestWriter) Commit(context.Context) error { w.commits++; return w.commitErr }
func (w *transactionalTestWriter) Abort(context.Context) error  { w.aborts++; return nil }

func transactionalWriterDestination(writer *transactionalTestWriter) goav.Destination {
	return goav.Writer("mem://upload.ogg", func(context.Context, provider.Info) (io.WriteCloser, error) {
		return writer, nil
	}, goav.Format(av.FormatOgg))
}

// TestTaskCloseIsIdempotentAfterRun pins the everyday ordering: Run to
// completion, then Close twice. Both calls return nil and the destination
// writer closes exactly once.
func TestTaskCloseIsIdempotentAfterRun(t *testing.T) {
	ctx := context.Background()
	writer := &countingWriteCloser{}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).
		To(goav.File("out.ogg", writer)).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if writer.closes != 1 {
		t.Fatalf("writer closes = %d, want exactly 1 across double Close", writer.closes)
	}
}

// TestTaskCloseIsIdempotentWithoutRun pins Close on a built-but-never-run
// task: resources opened at Build still tear down exactly once, and the
// second Close stays nil.
func TestTaskCloseIsIdempotentWithoutRun(t *testing.T) {
	writer := &countingWriteCloser{}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).
		To(goav.File("out.ogg", writer)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if writer.closes != 1 {
		t.Fatalf("writer closes = %d, want exactly 1", writer.closes)
	}
}

// TestTaskCloseDuringRunStopsTheTask pins Close racing a live Run: the run
// unwinds without error or with ErrClosed (never a panic or a hang), and a
// second Close is still nil.
func TestTaskCloseDuringRunStopsTheTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := goavtest.NewCollector()
	task, err := goav.From(goavtest.LiveAudio("live", 48000, 1)).
		Audio().To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()
	if err := out.Wait(ctx, func(c *goavtest.Collector) bool { return len(c.S16()) > 0 }); err != nil {
		t.Fatalf("no traffic before Close: %v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("Close during Run: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, goav.ErrClosed) {
			t.Fatalf("Run after Close = %v, want nil or ErrClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not return after Close")
	}
	if err := task.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestTaskSnapshotIsRaceSafeDuringAttachDetach stresses Snapshot and Taps
// against live Attach/Detach mutation: bounded iterations, run under -race in
// the gate. Every snapshot must be self-consistent — a typed state and a spec
// whose node list is readable without tearing.
func TestTaskSnapshotIsRaceSafeDuringAttachDetach(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task, err := goav.From(goavtest.LiveAudio("live", 48000, 1)).
		Audio().Tap(goav.FrameTap("audio.decoded")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()

	const iterations = 50
	mutated := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer close(mutated)
		for i := 0; i < iterations; i++ {
			attachment, err := task.Attach(ctx,
				goav.Branch("stress").From(goav.FrameTap("audio.decoded")).To(goavtest.NewCollector().Sink()))
			if err != nil {
				t.Errorf("Attach %d: %v", i, err)
				return
			}
			if err := task.Detach(ctx, attachment); err != nil {
				t.Errorf("Detach %d: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			snap := task.Snapshot()
			if snap.State == "" {
				t.Error("Snapshot returned an untyped task state during mutation")
				return
			}
			for i := range snap.Spec.Nodes {
				if snap.Spec.Nodes[i].Name == "" {
					t.Error("Snapshot spec carries an unnamed node during mutation")
					return
				}
			}
			_ = task.Taps()
			select {
			case <-mutated:
				return
			default:
			}
		}
	}()
	wg.Wait()
	if err := task.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, goav.ErrClosed) {
			t.Fatalf("Run = %v, want nil or ErrClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not return after Close")
	}
}

// TestTransactionalCommitFailureSurfacesFromTaskClose pins the Build-path
// finalize contract: a successful run commits the transactional destination
// on Task.Close, and a Commit failure is THAT call's return value — Run stays
// the data-plane error, the second Close stays nil, and the writer still
// closes exactly once.
func TestTransactionalCommitFailureSurfacesFromTaskClose(t *testing.T) {
	ctx := context.Background()
	commitErr := errors.New("upload commit failed")
	writer := &transactionalTestWriter{commitErr: commitErr}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).
		To(transactionalWriterDestination(writer)).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil (the run itself succeeded)", err)
	}
	if err := task.Close(); !errors.Is(err, commitErr) {
		t.Fatalf("first Close = %v, want the commit failure %v", err, commitErr)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if writer.commits != 1 || writer.aborts != 0 || writer.closes != 1 {
		t.Fatalf("commits=%d aborts=%d closes=%d, want exactly one commit and one close",
			writer.commits, writer.aborts, writer.closes)
	}
}

// TestRunOneShotSurfacesCommitFailure pins the one-shot finalize contract:
// Job.Run closes the task it built, so a transactional destination's Commit
// failure after an otherwise clean run IS Run's return value — never silently
// swallowed.
func TestRunOneShotSurfacesCommitFailure(t *testing.T) {
	commitErr := errors.New("upload commit failed")
	writer := &transactionalTestWriter{commitErr: commitErr}
	err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).
		To(transactionalWriterDestination(writer)).
		UseRuntime(goavtest.Runtime()).
		Run(context.Background())
	if !errors.Is(err, commitErr) {
		t.Fatalf("Run = %v, want the commit failure %v", err, commitErr)
	}
	if writer.commits != 1 || writer.aborts != 0 || writer.closes != 1 {
		t.Fatalf("commits=%d aborts=%d closes=%d, want exactly one commit and one close",
			writer.commits, writer.aborts, writer.closes)
	}
}
