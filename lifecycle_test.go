package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/lifecycle"
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
	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
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
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		close(started)
		<-release
		return lifecycleTestPush(push)
	})).Audio().Copy().To(lifecycleTestSink("packets")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != lifecycle.TaskBuilt {
		t.Fatalf("built state = %q, want %q", state, lifecycle.TaskBuilt)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	<-started
	if state := task.Snapshot().State; state != lifecycle.TaskRunning {
		t.Fatalf("running state = %q, want %q", state, lifecycle.TaskRunning)
	}
	close(release)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != lifecycle.TaskClosed {
		t.Fatalf("finished state = %q, want %q", state, lifecycle.TaskClosed)
	}

	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != lifecycle.TaskClosed {
		t.Fatalf("closed state = %q, want %q", state, lifecycle.TaskClosed)
	}
}

func TestTaskSnapshotReportsClosedStateWithoutRun(t *testing.T) {
	ctx := context.Background()
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	})).Audio().Copy().To(lifecycleTestSink("packets")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != lifecycle.TaskClosed {
		t.Fatalf("closed-before-run state = %q, want %q", state, lifecycle.TaskClosed)
	}
}

func TestTaskSnapshotReportsRootDestinationLifecycle(t *testing.T) {
	ctx := context.Background()
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	})).Audio().Copy().To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	before, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || before.State != lifecycle.DestinationOpen || !before.Open {
		t.Fatalf("root destination before run = %+v, want open base destination", before)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	committed, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || committed.State != lifecycle.DestinationCommitted || committed.Open {
		t.Fatalf("root destination after run = %+v, want committed base destination", committed)
	}
}

func TestTaskSnapshotReportsFailedRootDestination(t *testing.T) {
	ctx := context.Background()
	sourceErr := errors.New("source failed")
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
		if _, err := push.Packet(&packet); err != nil {
			return err
		}
		return sourceErr
	})).Audio().Copy().To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if err := task.Run(ctx); !errors.Is(err, sourceErr) {
		t.Fatalf("Run err = %v, want %v", err, sourceErr)
	}
	aborted, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || aborted.State != lifecycle.DestinationAborted || aborted.Open {
		t.Fatalf("root destination after failed run = %+v, want aborted base destination", aborted)
	}
}

func TestTaskSnapshotReportsCommittedDestinationAfterRun(t *testing.T) {
	ctx := context.Background()
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	})).Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
		To(lifecycleTestSink("rec")))
	if err != nil {
		t.Fatal(err)
	}
	before, ok := destinationSnapshotByName(task.Snapshot().Destinations, "rec")
	if !ok || before.State != lifecycle.DestinationOpen || !before.Open {
		t.Fatalf("destination before run = %+v, want open rec destination", before)
	}
	baseBefore, ok := destinationSnapshotByName(task.Snapshot().Destinations, "base")
	if !ok || baseBefore.State != lifecycle.DestinationOpen || !baseBefore.Open {
		t.Fatalf("root destination before run = %+v, want open base destination alongside branch", baseBefore)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	snap := task.Snapshot()
	if snap.State != lifecycle.TaskClosed {
		t.Fatalf("task state = %q, want %q", snap.State, lifecycle.TaskClosed)
	}
	branch, ok := branchSnapshotByName(snap.Branches, "rec")
	if !ok || branch.State != lifecycle.BranchAttached {
		t.Fatalf("branch = %+v, want attached rec branch", branch)
	}
	committed, ok := destinationSnapshotByName(snap.Destinations, "rec")
	if !ok || committed.State != lifecycle.DestinationCommitted || committed.Open {
		t.Fatalf("destination after run = %+v, want committed rec destination", committed)
	}
	baseCommitted, ok := destinationSnapshotByName(snap.Destinations, "base")
	if !ok || baseCommitted.State != lifecycle.DestinationCommitted || baseCommitted.Open {
		t.Fatalf("root destination after run = %+v, want committed base destination alongside branch", baseCommitted)
	}

	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	detached := attachment.Snapshot()
	if detached.State != lifecycle.BranchDetached {
		t.Fatalf("detached branch state = %q, want %q", detached.State, lifecycle.BranchDetached)
	}
	closed, ok := destinationSnapshotByName(detached.Destinations, "rec")
	if !ok || closed.State != lifecycle.DestinationClosed || closed.Open {
		t.Fatalf("destination after detach = %+v, want closed rec destination", closed)
	}
}

func TestTaskAttachDetachPublishesBranchLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	})).Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	events := task.Watch(WatchTypes(av.EventBranchAttached, av.EventBranchDetached))
	attachment, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
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

func TestTaskDetachOptionsReportDestinationOutcome(t *testing.T) {
	ctx := context.Background()
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		return lifecycleTestPush(push)
	})).Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	tests := []struct {
		name    string
		dest    string
		options []DetachOption
		want    lifecycle.DestinationState
	}{
		{name: "rec-drain", dest: "rec-drain", options: []DetachOption{DrainBranch()}, want: lifecycle.DestinationCommitted},
		{name: "rec-abort", dest: "rec-abort", options: []DetachOption{AbortBranch()}, want: lifecycle.DestinationAborted},
	}
	for _, tt := range tests {
		attachment, err := task.Attach(ctx, Branch(tt.name).
			From(PacketTap("audio.packets")).
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
	task, err := From(lifecycleTestSource(func(_ context.Context, push SourcePush) error {
		packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
		if _, err := push.Packet(&packet); err != nil {
			return err
		}
		return sourceErr
	})).Audio().Copy().
		Tap(PacketTap("audio.packets")).
		To(lifecycleTestSink("base")).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if _, err := task.Attach(ctx, Branch("rec").
		From(PacketTap("audio.packets")).
		To(lifecycleTestSink("rec"))); err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); !errors.Is(err, sourceErr) {
		t.Fatalf("run err = %v, want %v", err, sourceErr)
	}
	snap := task.Snapshot()
	if snap.State != lifecycle.TaskFailed {
		t.Fatalf("task state = %q, want %q", snap.State, lifecycle.TaskFailed)
	}
	aborted, ok := destinationSnapshotByName(snap.Destinations, "rec")
	if !ok || aborted.State != lifecycle.DestinationAborted || aborted.Open {
		t.Fatalf("destination after failed run = %+v, want aborted rec destination", aborted)
	}
}
