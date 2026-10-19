package goav

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// cancelOnCloseSink cancels a context when the graph closes it.
type cancelOnCloseSink struct {
	name   string
	cancel context.CancelFunc
}

func (s *cancelOnCloseSink) Name() string                           { return s.name }
func (s *cancelOnCloseSink) Handle(context.Context, *Message) error { return nil }
func (s *cancelOnCloseSink) Close() error {
	s.cancel()
	return nil
}

// TestDetachReturnsWhenCanceledWhileStoppingChildren pins the progress rule
// of a recursive detach: a parent with two dependent children, its context
// canceled while the first child closes, returns the cancellation instead of
// reselecting the child it can no longer stop, and releases the task.
func TestDetachReturnsWhenCanceledWhileStoppingChildren(t *testing.T) {
	task, err := mustNew().Build(context.Background(), mixTestAudioSource("src", 1).Audio().Tap("base").To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	parent, err := task.Attach(context.Background(), task.Tap("base").Do(FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit Emit) error { return emit.Frame(frame) })).Tap("parent.frames").As("parent").To(Sink(SinkFunc("parent", func(context.Context, Message) error { return nil }))))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, name := range []string{"child-a", "child-b"} {
		if _, err := task.Attach(context.Background(), task.Tap("parent.frames").As(name).To(Sink(&cancelOnCloseSink{name: name, cancel: cancel}))); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- task.Detach(ctx, parent) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Detach = nil, want the cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Detach did not return after its context was canceled")
	}
	// The task is released: a later detach with a live context finishes the job.
	if err := task.Detach(context.Background(), parent); err != nil {
		t.Fatalf("second Detach: %v", err)
	}
	if got := len(task.Snapshot().Branches); got != 0 {
		t.Fatalf("branches after detach = %d, want 0", got)
	}
}
