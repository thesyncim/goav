package goav_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

func TestDrainDoesNotCommitAfterTaskHandlerFailedBeforeRunReturns(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			cause := errors.New("required handler failed while draining")
			held := &finalizeHeldStage{entered: make(chan struct{}), release: make(chan struct{}), err: cause}
			sourceRelease := make(chan struct{})
			var sourceOnce, handlerOnce sync.Once
			releaseSource := func() { sourceOnce.Do(func() { close(sourceRelease) }) }
			releaseHandler := func() { handlerOnce.Do(func() { close(held.release) }) }
			defer releaseSource()
			defer releaseHandler()
			input := goav.Source("packet", shape.Packet(av.MediaAudio, av.CodecOpus), func(_ context.Context, push goav.SourcePush) error {
				_, _ = push.Packet(&av.Packet{Payload: av.Buffer{Bytes: []byte{0xf8, 0xff, 0xfe}}})
				<-sourceRelease
				return nil
			})
			options := []runconfig.Option{runconfig.WithCloseWaitTimeout(time.Second)}
			if buffered {
				options = append(options, runconfig.WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropBlock}))
			}
			live, err := goavtest.Runtime(options...).Build(t.Context(), input.Audio().Tap("packets").To(goav.Sink(goav.SinkFunc("root", func(context.Context, goav.Message) error { return nil }))))
			if err != nil {
				t.Fatal(err)
			}
			writer := &transactionalTestWriter{}
			attachment, err := live.Attach(t.Context(), live.Tap("packets").As("required").Do(held).To(transactionalWriterDestination(writer)))
			if err != nil {
				t.Fatal(err)
			}
			first := attachment.Spec().Nodes[0].Name
			done := make(chan error, 1)
			go func() { done <- live.Run(t.Context()) }()
			<-held.entered
			detached := make(chan error, 1)
			go func() { detached <- live.Detach(t.Context(), attachment, goav.DrainBranch()) }()
			deadline := time.Now().Add(time.Second)
			for {
				connected := false
				for _, edge := range live.Spec().Edges {
					connected = connected || edge.To.String() == first
				}
				if !connected {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("Detach did not remove ingress")
				}
				time.Sleep(time.Millisecond)
			}
			releaseHandler()
			detachErr := <-detached
			if !errors.Is(detachErr, cause) {
				t.Errorf("Detach=%v, want recorded handler error", detachErr)
			}
			if writer.commits != 0 || writer.aborts != 1 {
				t.Errorf("failed handler: commits=%d aborts=%d Detach=%v", writer.commits, writer.aborts, detachErr)
			}
			releaseSource()
			if err := <-done; !errors.Is(err, cause) {
				t.Errorf("Run=%v", err)
			}
			_ = live.Close()
		})
	}
}

// The attachment itself has no upstream stage to remove: its finalizer must
// consult the already-recorded failure before deciding to commit.
func TestDestinationOnlyDrainAbortsBeforeFailedRunReturns(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			cause := errors.New("other source failed")
			failureObserved, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseSource := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseSource()
			failed := goav.Source("failed", shape.Packet(av.MediaAudio, av.CodecOpus), func(context.Context, goav.SourcePush) error { return cause })
			remaining := goav.Source("remaining", shape.Packet(av.MediaAudio, av.CodecOpus), func(ctx context.Context, _ goav.SourcePush) error {
				<-ctx.Done() // runFailure records the source error before cancellation.
				close(failureObserved)
				<-release
				return nil
			})
			var options []runconfig.Option
			if buffered {
				options = append(options, runconfig.WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropBlock}))
			}
			live, err := goavtest.Runtime(options...).Build(t.Context(),
				failed.Audio().To(goav.Sink(goav.SinkFunc("failed-root", func(context.Context, goav.Message) error { return nil }))),
				remaining.Audio().Tap("packets").To(goav.Sink(goav.SinkFunc("remaining-root", func(context.Context, goav.Message) error { return nil }))),
			)
			if err != nil {
				t.Fatal(err)
			}
			writer := &transactionalTestWriter{}
			attachment, err := live.Attach(t.Context(), live.Tap("packets").As("required").To(transactionalWriterDestination(writer)))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- live.Run(t.Context()) }()
			<-failureObserved
			if err := live.Detach(t.Context(), attachment, goav.DrainBranch()); !errors.Is(err, cause) {
				t.Errorf("Detach=%v, want source failure", err)
			}
			if writer.commits != 0 || writer.aborts != 1 || writer.closes != 1 {
				t.Errorf("destination-only drain: commits=%d aborts=%d closes=%d", writer.commits, writer.aborts, writer.closes)
			}
			releaseSource()
			if err := <-done; !errors.Is(err, cause) {
				t.Errorf("Run=%v", err)
			}
			if err := live.Close(); err != nil {
				t.Fatalf("Close=%v", err)
			}
		})
	}
}
