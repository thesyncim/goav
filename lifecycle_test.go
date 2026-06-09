package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/info"
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
	if state := task.Snapshot().State; state != info.TaskBuilt {
		t.Fatalf("built state = %q, want %q", state, info.TaskBuilt)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	<-started
	if state := task.Snapshot().State; state != info.TaskRunning {
		t.Fatalf("running state = %q, want %q", state, info.TaskRunning)
	}
	close(release)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != info.TaskClosed {
		t.Fatalf("finished state = %q, want %q", state, info.TaskClosed)
	}

	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state := task.Snapshot().State; state != info.TaskClosed {
		t.Fatalf("closed state = %q, want %q", state, info.TaskClosed)
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
	if state := task.Snapshot().State; state != info.TaskClosed {
		t.Fatalf("closed-before-run state = %q, want %q", state, info.TaskClosed)
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
	if !ok || before.State != info.DestinationOpen || !before.Open {
		t.Fatalf("destination before run = %+v, want open rec destination", before)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := task.Snapshot()
	if snapshot.State != info.TaskClosed {
		t.Fatalf("task state = %q, want %q", snapshot.State, info.TaskClosed)
	}
	branch, ok := branchSnapshotByName(snapshot.Branches, "rec")
	if !ok || branch.State != info.BranchAttached {
		t.Fatalf("branch = %+v, want attached rec branch", branch)
	}
	committed, ok := destinationSnapshotByName(snapshot.Destinations, "rec")
	if !ok || committed.State != info.DestinationCommitted || committed.Open {
		t.Fatalf("destination after run = %+v, want committed rec destination", committed)
	}

	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	detached := attachment.Snapshot()
	if detached.State != info.BranchDetached {
		t.Fatalf("detached branch state = %q, want %q", detached.State, info.BranchDetached)
	}
	closed, ok := destinationSnapshotByName(detached.Destinations, "rec")
	if !ok || closed.State != info.DestinationClosed || closed.Open {
		t.Fatalf("destination after detach = %+v, want closed rec destination", closed)
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
	snapshot := task.Snapshot()
	if snapshot.State != info.TaskFailed {
		t.Fatalf("task state = %q, want %q", snapshot.State, info.TaskFailed)
	}
	aborted, ok := destinationSnapshotByName(snapshot.Destinations, "rec")
	if !ok || aborted.State != info.DestinationAborted || aborted.Open {
		t.Fatalf("destination after failed run = %+v, want aborted rec destination", aborted)
	}
}
