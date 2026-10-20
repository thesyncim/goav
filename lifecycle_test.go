package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/shape"
)

func lifecycleTestSource(fn SourceFunc) InputSpec {
	return Source("gen",
		shape.Packet(av.MediaAudio, av.CodecOpus,
			shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
		),
		fn,
	)
}

func lifecycleTestPush(push SourcePush) error {
	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}
	if _, err := push.Packet(&packet); err != nil {
		return err
	}
	return push.EOS()
}

func lifecycleTestSink(name string) Destination {
	return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
}

func TestTaskSnapshotReportsTypedTaskLifecycle(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		close(started)
		<-release
		return lifecycleTestPush(push)
	}).Audio().To(lifecycleTestSink("packets")))
	if err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != TaskBuilt {
		t.Fatalf("built state = %q, want %q", state, TaskBuilt)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	<-started
	if state := task.Snapshot().State; state != TaskRunning {
		t.Fatalf("running state = %q, want %q", state, TaskRunning)
	}
	close(release)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != TaskFinalizing {
		t.Fatalf("finished state = %q, want %q", state, TaskFinalizing)
	}

	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != TaskClosed {
		t.Fatalf("closed state = %q, want %q", state, TaskClosed)
	}
}

func TestTaskSnapshotReportsClosedStateWithoutRun(t *testing.T) {
	ctx := context.Background()
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	}).Audio().To(lifecycleTestSink("packets")))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != TaskClosed {
		t.Fatalf("closed-before-run state = %q, want %q", state, TaskClosed)
	}
}

func TestTaskSnapshotReportsRootDestinationLifecycle(t *testing.T) {
	ctx := context.Background()
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	}).Audio().To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	before, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || before.State != DestinationOpen || !before.Open {
		t.Fatalf("root destination before run = %+v, want open base destination", before)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, _ := destinationSnapshotByName(task.Snapshot().Destinations, "base"); pending.State != DestinationFinalizing {
		t.Fatalf("before Close = %+v, want finalizing", pending)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	committed, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || committed.State != DestinationCommitted || committed.Open {
		t.Fatalf("root destination after run = %+v, want committed base destination", committed)
	}
}

func TestTaskSnapshotReportsFailedRootDestination(t *testing.T) {
	ctx := context.Background()
	sourceErr := errors.New("source failed")
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}
		if _, err := push.Packet(&packet); err != nil {
			return err
		}
		return sourceErr
	}).Audio().To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if err := task.Run(ctx); !errors.Is(err, sourceErr) {
		t.Fatalf("Run err = %v, want %v", err, sourceErr)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	closed, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || closed.State != DestinationClosed || closed.Open {
		t.Fatalf("plain sink after failed run = %+v, want closed base destination", closed)
	}
}

func TestTaskSnapshotReportsFinalizingDestinationAfterRun(t *testing.T) {
	ctx := context.Background()
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	}).Audio().
		Tap("audio.packets").
		To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, task.Tap("audio.packets").
		As("rec").
		To(lifecycleTestSink("rec")))
	if err != nil {
		t.Fatal(err)
	}
	before, ok := destinationSnapshotByName(task.Snapshot().Destinations, "rec")
	if !ok || before.State != DestinationOpen || !before.Open {
		t.Fatalf("destination before run = %+v, want open rec destination", before)
	}
	baseBefore, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || baseBefore.State != DestinationOpen || !baseBefore.Open {
		t.Fatalf("root destination before run = %+v, want open base destination alongside branch", baseBefore)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	snap := task.Snapshot()
	if snap.State != TaskFinalizing {
		t.Fatalf("task state = %q, want %q", snap.State, TaskFinalizing)
	}
	branch, ok := branchSnapshotByName(snap.Branches, "rec")
	if !ok || branch.State != BranchAttached {
		t.Fatalf("branch = %+v, want attached rec branch", branch)
	}
	committed, ok := destinationSnapshotByName(snap.Destinations, "rec")
	if !ok || committed.State != DestinationFinalizing || committed.Open {
		t.Fatalf("destination after run = %+v, want finalizing rec destination", committed)
	}
	baseCommitted, ok := destinationSnapshotByName(snap.Destinations, "base")
	if !ok || baseCommitted.State != DestinationFinalizing || baseCommitted.Open {
		t.Fatalf("root destination after run = %+v, want finalizing base destination alongside branch", baseCommitted)
	}

	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	detached := attachment.Snapshot()
	if detached.State != BranchDetached {
		t.Fatalf("detached branch state = %q, want %q", detached.State, BranchDetached)
	}
	closed, ok := destinationSnapshotByName(detached.Destinations, "rec")
	if !ok || closed.State != DestinationCommitted || closed.Open {
		t.Fatalf("destination after successful run and detach = %+v, want committed rec destination", closed)
	}
}

func TestTaskAttachDetachPublishesBranchLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	}).Audio().
		Tap("audio.packets").
		To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	events := task.Watch(WatchTypes(av.EventBranchAttached, av.EventBranchDetached)).Events()
	attachment, err := task.Attach(ctx, task.Tap("audio.packets").
		As("rec").
		To(lifecycleTestSink("rec")))
	if err != nil {
		t.Fatal(err)
	}

	attached := recvWatchEvent(t, events)
	if attached.Type != av.EventBranchAttached {
		t.Fatalf("attached event type = %q, want %q", attached.Type, av.EventBranchAttached)
	}
	if attached.Metadata[av.MetadataAttachmentID] != attachment.ID() || attached.Metadata[av.MetadataAttachmentName] != "rec" {
		t.Fatalf("attached metadata = %#v, want attachment id/name", attached.Metadata)
	}
	if _, ok := attached.Metadata[av.MetadataDetachDisposition]; ok {
		t.Fatalf("attached metadata includes detach disposition: %#v", attached.Metadata)
	}

	if err := task.Detach(ctx, attachment, DrainBranch()); err != nil {
		t.Fatal(err)
	}
	detached := recvWatchEvent(t, events)
	if detached.Type != av.EventBranchDetached {
		t.Fatalf("detached event type = %q, want %q", detached.Type, av.EventBranchDetached)
	}
	if detached.Metadata[av.MetadataAttachmentID] != attachment.ID() ||
		detached.Metadata[av.MetadataAttachmentName] != "rec" ||
		detached.Metadata[av.MetadataDetachDisposition] != "drain" {
		t.Fatalf("detached metadata = %#v, want id/name/drain disposition", detached.Metadata)
	}
}

func TestTaskDetachPublishesDestinationLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	}).Audio().
		Tap("audio.packets").
		To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	events := task.Watch(WatchTypes(av.EventDestinationCommitted, av.EventDestinationAborted, av.EventDestinationCommitError)).Events()
	attachment, err := task.Attach(ctx, task.Tap("audio.packets").
		As("rec").
		To(lifecycleTestSink("rec")))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, attachment, DrainBranch()); err != nil {
		t.Fatal(err)
	}
	event := recvWatchEvent(t, events)
	if event.Type != av.EventDestinationCommitted {
		t.Fatalf("destination event type = %q, want %q", event.Type, av.EventDestinationCommitted)
	}
	if event.Metadata[av.MetadataDestinationName] != "rec" ||
		event.Metadata[av.MetadataAttachmentID] != attachment.ID() ||
		event.Metadata[av.MetadataAttachmentName] != "rec" {
		t.Fatalf("destination event metadata = %#v, want destination and attachment facts", event.Metadata)
	}
}

func TestTaskFinalizerPublishesRootDestinationCommitError(t *testing.T) {
	commitErr := errors.New("commit failed")
	transaction := &destinationTransaction{name: "commit-error.ivf", requireSuccess: true}
	transaction.Fail()
	task := newTaskWithRootDestinations(newWatchTestGraph(1), nil, []compile.WorkDestination{{Name: "commit-error.ivf"}}, transaction)
	defer task.Close()

	events := task.Watch(WatchTypes(av.EventDestinationCommitted, av.EventDestinationAborted, av.EventDestinationCommitError)).Events()
	task.finishDestinations(commitErr)
	select {
	case event := <-events:
		t.Fatalf("execution published premature finalization: %+v", event)
	default:
	}
	transaction.finalize(DestinationAborted, commitErr)
	event := recvWatchEvent(t, events)
	if event.Type != av.EventDestinationCommitError {
		t.Fatalf("destination event type = %q, want %q", event.Type, av.EventDestinationCommitError)
	}
	if event.Metadata[av.MetadataDestinationName] != "commit-error.ivf" || !errors.Is(event.Cause, commitErr) {
		t.Fatalf("destination event = %+v, want commit-error.ivf with commit cause", event)
	}
}

func TestTaskDetachOptionsReportDestinationOutcome(t *testing.T) {
	ctx := context.Background()
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	}).Audio().
		Tap("audio.packets").
		To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	tests := []struct {
		name    string
		dest    string
		options []DetachOption
		want    DestinationState
	}{
		{name: "rec-drain", dest: "rec-drain", options: []DetachOption{DrainBranch()}, want: DestinationCommitted},
		{name: "rec-abort", dest: "rec-abort", options: []DetachOption{AbortBranch()}, want: DestinationClosed},
	}
	for _, tt := range tests {
		attachment, err := task.Attach(ctx, task.Tap("audio.packets").
			As(tt.name).
			To(lifecycleTestSink(tt.dest)))
		if err != nil {
			t.Fatalf("%s attach: %v", tt.name, err)
		}
		if err := task.Detach(ctx, attachment, tt.options...); err != nil {
			t.Fatalf("%s detach: %v", tt.name, err)
		}
		snap := attachment.Snapshot()
		destination, ok := destinationSnapshotByName(snap.Destinations, tt.dest)
		if !ok || destination.State != tt.want || destination.Open {
			t.Fatalf("%s destination = %+v, want closed %q destination", tt.name, destination, tt.want)
		}
	}
}

func TestTaskSnapshotReportsFailedTaskAndAbortedDestination(t *testing.T) {
	ctx := context.Background()
	sourceErr := errors.New("source failed")
	task, err := mustNew().Build(ctx, lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}
		if _, err := push.Packet(&packet); err != nil {
			return err
		}
		return sourceErr
	}).Audio().
		Tap("audio.packets").
		To(lifecycleTestSink("base")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if _, err := task.Attach(ctx, task.Tap("audio.packets").
		As("rec").
		To(lifecycleTestSink("rec"))); err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); !errors.Is(err, sourceErr) {
		t.Fatalf("run err = %v, want %v", err, sourceErr)
	}
	snap := task.Snapshot()
	if snap.State != TaskFailed {
		t.Fatalf("task state = %q, want %q", snap.State, TaskFailed)
	}
	aborted, ok := destinationSnapshotByName(snap.Destinations, "rec")
	if !ok || aborted.State != DestinationFinalizing || aborted.Open {
		t.Fatalf("destination after failed run = %+v, want finalizing rec destination", aborted)
	}
}
