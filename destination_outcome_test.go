package goav

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
)

type outcomeSink struct {
	name             string
	closeErr         error
	entered, release chan struct{}
}

func (s *outcomeSink) Name() string { return s.name }
func (s *outcomeSink) Handle(context.Context, *pipeline.Message) error {
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return nil
}
func (s *outcomeSink) Close() error { return s.closeErr }

func TestDestinationFinalizationKeepsIndependentStickyOutcomes(t *testing.T) {
	closeErr := errors.New("second sink close")
	input := lifecycleTestSource(func(_ context.Context, push SourcePush) error { return lifecycleTestPush(push) }).Audio()
	task, err := mustNew().Build(t.Context(), input.To(
		Sink(&outcomeSink{name: "good"}),
		Sink(&outcomeSink{name: "bad", closeErr: closeErr}),
	))
	if err != nil {
		t.Fatal(err)
	}
	events := task.Watch(WatchTypes(av.EventDestinationCommitted, av.EventDestinationCommitError)).Events()
	if err := task.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		t.Fatalf("premature finalization event: %+v", e)
	default:
	}
	if task.Snapshot().State != TaskFinalizing {
		t.Fatal("successful Run finalized the task")
	}
	for range 2 {
		if err := task.Close(); !errors.Is(err, closeErr) || strings.Count(err.Error(), closeErr.Error()) != 1 {
			t.Fatalf("Close = %v", err)
		}
		snapshot := task.Snapshot()
		if !errors.Is(snapshot.Err, closeErr) || snapshot.State != TaskFailed {
			t.Fatalf("outcome = %+v", snapshot)
		}
		good, _ := destinationSnapshotByName(snapshot.Destinations, "good")
		bad, _ := destinationSnapshotByName(snapshot.Destinations, "bad")
		if good.State != DestinationCommitted || good.Err != nil || bad.State != DestinationFailed || !errors.Is(bad.Err, closeErr) {
			t.Fatalf("mixed outcomes: good=%+v bad=%+v", good, bad)
		}
	}
	count := 0
	for event := range events {
		count++
		if event.Metadata[av.MetadataDestinationName] == "bad" && (event.Type != av.EventDestinationCommitError || !errors.Is(event.Cause, closeErr)) {
			t.Fatalf("bad destination event = %+v", event)
		}
	}
	if count != 2 {
		t.Fatalf("finalization events = %d, want 2", count)
	}
}

func TestCloseWaitDoesNotBecomeFinalOutcome(t *testing.T) {
	sink := &outcomeSink{name: "late-success", entered: make(chan struct{}), release: make(chan struct{})}
	task, err := mustNew(runconfig.WithCloseWaitTimeout(time.Millisecond)).Build(t.Context(),
		lifecycleTestSource(func(_ context.Context, push SourcePush) error {
			_, err := push.Packet(&av.Packet{})
			return err
		}).Audio().To(Sink(sink)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- task.Run(t.Context()) }()
	<-sink.entered
	if err := task.Close(); !errors.Is(err, pipeline.ErrCloseWait) {
		t.Fatalf("Close = %v", err)
	}
	if got := task.Snapshot(); got.Err != nil || got.State != TaskFinalizing {
		t.Fatalf("wait polluted outcome: %+v", got)
	}
	close(sink.release)
	<-done
	if err := task.Close(); err != nil {
		t.Fatalf("later Close = %v", err)
	}
	if got := task.Snapshot(); got.Err != nil || got.State != TaskClosed || got.Stats.PendingCleanup != 0 {
		t.Fatalf("late success = %+v", got)
	}
}

func TestDeferredDestinationOutcomeSurvivesObserverClose(t *testing.T) {
	closeErr := errors.New("late sink close")
	sink := &outcomeSink{name: "late", closeErr: closeErr, entered: make(chan struct{}), release: make(chan struct{})}
	task, err := mustNew(runconfig.WithCloseWaitTimeout(time.Millisecond)).Build(t.Context(),
		lifecycleTestSource(func(_ context.Context, push SourcePush) error {
			_, err := push.Packet(&av.Packet{})
			return err
		}).Audio().To(Sink(sink)))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(t.Context()) }()
	<-sink.entered
	if err := task.Close(); err == nil {
		t.Fatal("Close did not report blocked handler")
	}
	before, _ := destinationSnapshotByName(task.Snapshot().Destinations, "late")
	if before.State == DestinationCommitted || before.State == DestinationAborted {
		t.Fatalf("unfinished close = %+v", before)
	}
	close(sink.release)
	<-runDone
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := task.Snapshot()
		if snapshot.Stats.PendingCleanup == 0 && errors.Is(snapshot.Err, closeErr) {
			destination, _ := destinationSnapshotByName(snapshot.Destinations, "late")
			if destination.State != DestinationFailed || !errors.Is(destination.Err, closeErr) {
				t.Fatalf("late outcome = %+v", destination)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("late outcome missing: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	if err := task.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close lost late error: %v", err)
	}
}

func TestFinalizationLedgerDistinguishesNodesSharingCause(t *testing.T) {
	cause := context.DeadlineExceeded
	task, err := mustNew().Build(t.Context(), lifecycleTestSource(func(context.Context, SourcePush) error { return nil }).Audio().To(
		Sink(&outcomeSink{name: "first", closeErr: cause}), Sink(&outcomeSink{name: "second", closeErr: cause})))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		err := task.Close()
		if !errors.Is(err, cause) || strings.Count(err.Error(), cause.Error()) != 2 || !strings.Contains(err.Error(), "first") || !strings.Contains(err.Error(), "second") {
			t.Fatalf("per-node outcomes=%v", err)
		}
	}
}
