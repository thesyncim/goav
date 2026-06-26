package goav

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

func TestTaskWatchUsesIndependentSubscription(t *testing.T) {
	graph := newWatchTestGraph(1)
	task := newTask(graph, nil)
	task.rules = &taskStreamRules{}
	defer task.Close()

	events := task.Watch().Events()
	if events == (<-chan av.Event)(graph.events) {
		t.Fatal("Watch should return an isolated subscription")
	}

	graph.events <- av.Event{Type: av.EventStats, StreamID: "video"}
	event := recvWatchEvent(t, events)
	if event.Type != av.EventStats || event.StreamID != "video" {
		t.Fatalf("event = %+v, want stats for video", event)
	}
}

func TestTaskDetachHelperContracts(t *testing.T) {
	task := newTask(newWatchTestGraph(1), nil)
	defer task.Close()

	attachment := &lifecycleFakeAttachment{name: "archive"}
	input, err := runtimeDetachInputFromArgs(context.Background(), attachment, []lifecycle.DetachOption{lifecycle.AbortBranch()})
	if err != nil {
		t.Fatal(err)
	}
	if input.attachment != attachment ||
		input.runtime != nil ||
		input.disposition != oldBranchAbort {
		t.Fatalf("detach input = %+v, want non-runtime attachment with abort disposition", input)
	}

	runtimeAttachment := &runtimeAttachment{name: "recording"}
	input, err = runtimeDetachInputFromArgs(context.Background(), runtimeAttachment, []lifecycle.DetachOption{lifecycle.DrainBranch()})
	if err != nil {
		t.Fatal(err)
	}
	if input.attachment != runtimeAttachment ||
		input.runtime != runtimeAttachment ||
		input.disposition != oldBranchDrain {
		t.Fatalf("runtime detach input = %+v, want runtime attachment with drain disposition", input)
	}
	direct := runtimeDetachInputForRuntimeAttachment(runtimeAttachment, oldBranchAbort)
	if direct.attachment != runtimeAttachment ||
		direct.runtime != runtimeAttachment ||
		direct.disposition != oldBranchAbort {
		t.Fatalf("direct runtime detach input = %+v, want runtime attachment with abort disposition", direct)
	}

	if err := task.Detach(context.Background(), nil); err == nil {
		t.Fatal("Detach(nil) succeeded")
	} else {
		var buildErr *BuildError
		if !errors.As(err, &buildErr) || buildErr.Code != runtimeBranchInvalidCode || buildErr.Reason != "attachment is nil" {
			t.Fatalf("Detach(nil) error = %v, want runtime branch invalid BuildError", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := task.Detach(ctx, attachment); !errors.Is(err, context.Canceled) {
		t.Fatalf("Detach canceled error = %v, want context.Canceled", err)
	}
	if attachment.closed {
		t.Fatal("Detach should not close an attachment after context cancellation")
	}

	if err := task.Detach(context.Background(), attachment); err != nil {
		t.Fatalf("Detach(non-runtime) error = %v", err)
	}
	if !attachment.closed {
		t.Fatal("Detach(non-runtime) did not call attachment.Close")
	}

	closeErr := errors.New("close failed")
	attachment = &lifecycleFakeAttachment{name: "broken", closeErr: closeErr}
	if err := task.Detach(context.Background(), attachment); !errors.Is(err, closeErr) {
		t.Fatalf("Detach close error = %v, want %v", err, closeErr)
	}
}

func TestTaskFinishDestinationsMarksTransactions(t *testing.T) {
	success := &destinationTransaction{requireSuccess: true}
	task := newTask(newWatchTestGraph(1), nil, nil, success)
	defer task.Close()

	task.finishDestinations(nil)
	if !success.succeeded || success.failed || success.ShouldAbort() {
		t.Fatalf("success transaction = %+v, want succeeded without abort", success)
	}

	failed := &destinationTransaction{requireSuccess: true}
	task = newTask(newWatchTestGraph(1), nil, failed)
	defer task.Close()

	task.finishDestinations(errors.New("run failed"))
	if failed.succeeded || !failed.failed || !failed.ShouldAbort() {
		t.Fatalf("failed transaction = %+v, want failed abort", failed)
	}

	pending := &destinationTransaction{requireSuccess: true}
	if !pending.ShouldAbort() {
		t.Fatal("requireSuccess transaction without Succeed should abort")
	}
	(*destinationTransaction)(nil).Succeed()
	(*destinationTransaction)(nil).Fail()
	if (*destinationTransaction)(nil).ShouldAbort() {
		t.Fatal("nil destination transaction should not abort")
	}
}

func TestBufferedPayloadCauseNames(t *testing.T) {
	if got := bufferedPayloadCauseName(fmt.Errorf("wrapped: %w", pipeline.ErrBufferedMessageUnsafe)); got != "pipeline.ErrBufferedMessageUnsafe" {
		t.Fatalf("unsafe cause = %q", got)
	}
	if got := bufferedPayloadCauseName(fmt.Errorf("wrapped: %w", pipeline.ErrMessageTooLarge)); got != "pipeline.ErrMessageTooLarge" {
		t.Fatalf("too-large cause = %q", got)
	}
	if got := bufferedPayloadCauseName(errors.New("custom payload refusal")); got != "custom payload refusal" {
		t.Fatalf("custom cause = %q", got)
	}
}

type lifecycleFakeAttachment struct {
	name     string
	closed   bool
	closeErr error
}

func (a *lifecycleFakeAttachment) ID() string {
	return "fake-" + a.name
}

func (a *lifecycleFakeAttachment) Name() string {
	return a.name
}

func (a *lifecycleFakeAttachment) Spec() pipeline.Spec {
	return pipeline.Spec{}
}

func (a *lifecycleFakeAttachment) Stats() pipeline.GraphStats {
	return pipeline.GraphStats{}
}

func (a *lifecycleFakeAttachment) Snapshot() snapshot.Branch {
	return snapshot.Branch{Name: a.name}
}

func (a *lifecycleFakeAttachment) Pause(context.Context) error {
	return nil
}

func (a *lifecycleFakeAttachment) Resume(context.Context) error {
	return nil
}

func (a *lifecycleFakeAttachment) Rebranch(context.Context, ...lifecycle.RebranchArg) (Attachment, error) {
	return a, nil
}

func (a *lifecycleFakeAttachment) Close(context.Context) error {
	a.closed = true
	return a.closeErr
}
