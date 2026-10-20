package goav

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type retirementSink struct {
	name                       string
	handleErr, closeErr        error
	closeStarted, releaseClose chan struct{}
	closes                     atomic.Int64
	onClose                    func()
}

func (s *retirementSink) Name() string                                    { return s.name }
func (s *retirementSink) Handle(context.Context, *pipeline.Message) error { return s.handleErr }
func (s *retirementSink) Close() error {
	s.closes.Add(1)
	if s.closeStarted != nil {
		close(s.closeStarted)
		<-s.releaseClose
	}
	if s.onClose != nil {
		s.onClose()
	}
	return s.closeErr
}

func waitBranch(t *testing.T, a Attachment, ready func(BranchSnapshot) bool) BranchSnapshot {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for {
		s := a.Snapshot()
		if ready(s) {
			return s
		}
		if time.Now().After(until) {
			t.Fatalf("branch did not settle: %+v", s)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIsolatedAttachmentRetirementReservesNamesUntilDetach(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	commands := make(chan struct{})
	var healthy atomic.Int64
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(ctx context.Context, push SourcePush) error {
		for {
			select {
			case _, ok := <-commands:
				if !ok {
					return nil
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			if _, err := push.Packet(&av.Packet{}); err != nil {
				return err
			}
		}
	}).Audio().Tap("packets").To(Sink(SinkFunc("healthy", func(context.Context, Message) error { healthy.Add(1); return nil }))))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	cause := errors.New("optional processing")
	firstSink := &retirementSink{name: "optional", handleErr: cause}
	first, err := task.Attach(ctx, task.Tap("packets").As("preview").To(Sink(firstSink)).OnFailure(IsolateBranch))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	commands <- struct{}{}
	waitBranch(t, first, func(s BranchSnapshot) bool { return s.Retired })
	if !errors.Is(first.Snapshot().Err, cause) {
		t.Fatal("lost isolated failure")
	}
	if _, err := task.Attach(ctx, task.Tap("packets").As("preview").To(Sink(&retirementSink{name: "optional"}))); err == nil {
		t.Fatal("reused a still-owned attachment node name")
	}
	if err := task.Detach(ctx, first); err != nil {
		t.Fatal(err)
	}
	secondSink := &retirementSink{name: "optional"}
	second, err := task.Attach(ctx, task.Tap("packets").As("preview").To(Sink(secondSink)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, first); err != nil {
		t.Fatal(err)
	}
	commands <- struct{}{}
	close(commands)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if secondSink.closes.Load() != 0 || second.Snapshot().State != BranchAttached || healthy.Load() != 2 {
		t.Fatalf("old handle affected replacement: closes=%d state=%s healthy=%d", secondSink.closes.Load(), second.Snapshot().State, healthy.Load())
	}
}

func TestIsolatedDetachWaitsForRetirementAndKeepsTaskRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	releaseSource := make(chan struct{})
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(ctx context.Context, push SourcePush) error {
		if _, err := push.Packet(&av.Packet{}); err != nil {
			return err
		}
		select {
		case <-releaseSource:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Audio().Tap("packets").To(lifecycleTestSink("healthy")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	cause, cleanup := errors.New("optional processing"), errors.New("optional finalizer")
	sink := &retirementSink{name: "optional", handleErr: cause, closeErr: cleanup, closeStarted: make(chan struct{}), releaseClose: make(chan struct{})}
	attachment, err := task.Attach(ctx, task.Tap("packets").As("preview").To(Sink(sink)).OnFailure(IsolateBranch))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	<-sink.closeStarted
	waitCtx, stop := context.WithTimeout(ctx, 15*time.Millisecond)
	if err := task.Detach(waitCtx, attachment); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending Detach=%v", err)
	}
	stop()
	if s := attachment.Snapshot(); s.PendingCleanup == 0 || s.Retired {
		t.Fatalf("unfinished retirement=%+v", s)
	}
	close(sink.releaseClose)
	if err := task.Detach(ctx, attachment); !errors.Is(err, cleanup) {
		t.Fatalf("later Detach=%v", err)
	}
	if s := task.Snapshot(); s.State != TaskRunning || !errors.Is(s.Err, cleanup) {
		t.Fatalf("cleanup stopped live task state: %+v", s)
	}
	root, _ := destinationSnapshotByName(task.Snapshot().Destinations, "healthy")
	if root.State != DestinationOpen {
		t.Fatalf("healthy destination=%+v", root)
	}
	close(releaseSource)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); !errors.Is(err, cleanup) {
		t.Fatalf("Close=%v", err)
	}
	if sink.closes.Load() != 1 {
		t.Fatal("finalized more than once")
	}
}

func TestHealthyIsolatedDetachDoesNotReappearAttached(t *testing.T) {
	task, err := mustNew().Build(t.Context(), lifecycleTestSource(func(context.Context, SourcePush) error { return nil }).Audio().Tap("packets").To(lifecycleTestSink("main")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	for range 8 {
		a, err := task.Attach(t.Context(), task.Tap("packets").As("preview").To(lifecycleTestSink("preview-sink")).OnFailure(IsolateBranch))
		if err != nil {
			t.Fatal(err)
		}
		if err := task.Detach(t.Context(), a); err != nil {
			t.Fatal(err)
		}
		if len(task.Snapshot().Branches) != 0 {
			t.Fatalf("healthy detached branch retained: %+v", task.Snapshot().Branches)
		}
	}
}

func TestCloseInitiatesGraphShutdownWhenAttachmentConsumesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	root := &retirementSink{name: "main"}
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(context.Context, SourcePush) error { return nil }).Audio().Tap("packets").To(Sink(root)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = task.Attach(ctx, task.Tap("packets").As("preview").To(Sink(&retirementSink{name: "cancel-on-close", onClose: cancel})))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.(interface{ CloseContext(context.Context) error }).CloseContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloseContext=%v", err)
	}
	if err := task.Close(); err != nil {
		t.Fatalf("later Close=%v", err)
	}
	if root.closes.Load() != 1 || task.Snapshot().State != TaskClosed {
		t.Fatalf("graph did not close: root=%d state=%s", root.closes.Load(), task.Snapshot().State)
	}
	if len(task.Spec().Nodes) != 0 {
		t.Fatal("reported closed with live nodes")
	}
}
