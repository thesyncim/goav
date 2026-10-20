package goav

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/runconfig"
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

func TestCloseRemainsFinalizingUntilRunReturns(t *testing.T) {
	for _, runErr := range []error{pipeline.ErrClosed, errors.New("independent source failure")} {
		t.Run(runErr.Error(), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			task, err := mustNew().Build(t.Context(), lifecycleTestSource(func(context.Context, SourcePush) error {
				close(entered)
				<-release
				return runErr
			}).Audio().To(lifecycleTestSink("root")))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- task.Run(t.Context()) }()
			<-entered
			if err := task.Close(); err != nil {
				t.Fatal(err)
			}
			before := task.Snapshot()
			close(release)
			gotRun := <-done
			if before.State != TaskFinalizing {
				t.Errorf("outstanding Run state=%s", before.State)
			}
			after := task.Snapshot()
			if runErr == pipeline.ErrClosed {
				if gotRun != nil || after.State != TaskClosed || after.Err != nil {
					t.Fatalf("own Close: Run=%v state=%s outcome=%v", gotRun, after.State, after.Err)
				}
			} else if !errors.Is(gotRun, runErr) || after.State != TaskFailed || !errors.Is(after.Err, runErr) {
				t.Fatalf("independent failure: Run=%v state=%s outcome=%v", gotRun, after.State, after.Err)
			}
		})
	}
}

func TestCloseDeadlineWhileDrainDetachOwnsAttachmentLock(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			options := []runconfig.Option{runconfig.WithCloseWaitTimeout(time.Second)}
			if buffered {
				options = append(options, runconfig.WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropBlock}))
			}
			held := &outcomeSink{name: "draining", entered: make(chan struct{}), release: make(chan struct{})}
			live, err := mustNew(options...).Build(t.Context(), lifecycleTestSource(func(_ context.Context, push SourcePush) error {
				_, err := push.Packet(&av.Packet{})
				if buffered {
					<-held.release
				}
				return err
			}).Audio().Tap("packets").To(lifecycleTestSink("root")))
			if err != nil {
				t.Fatal(err)
			}
			owner := live.(*task)
			draining, err := live.Attach(t.Context(), live.Tap("packets").As("draining").To(Sink(held)))
			if err != nil {
				t.Fatal(err)
			}
			active, err := live.Attach(t.Context(), live.Tap("packets").As("active").To(lifecycleTestSink("active")))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- live.Run(t.Context()) }()
			<-held.entered
			detached := make(chan error, 1)
			go func() { detached <- live.Detach(t.Context(), draining, DrainBranch()) }()
			deadline := time.Now().Add(time.Second)
			for {
				owner.lifecycleMu.Lock()
				_, owned := owner.activeAttachments[draining.(*runtimeAttachment)]
				owner.lifecycleMu.Unlock()
				if !owned {
					break
				}
				if time.Now().After(deadline) {
					close(held.release)
					t.Fatal("Detach did not accept drain")
				}
				time.Sleep(time.Millisecond)
			}
			closeCtx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			started := time.Now()
			closeErr := owner.CloseContext(closeCtx)
			elapsed := time.Since(started)
			cancel()
			close(held.release)
			detachErr, runErr := <-detached, <-done
			if err := live.Close(); err != nil {
				t.Fatalf("later Close=%v", err)
			}
			if !errors.Is(closeErr, context.DeadlineExceeded) || elapsed >= 500*time.Millisecond {
				t.Errorf("Close=%v elapsed=%v", closeErr, elapsed)
			}
			if detachErr != nil || runErr != nil {
				t.Fatalf("Detach=%v Run=%v", detachErr, runErr)
			}
			if got := draining.Snapshot().Destinations[0].State; got != DestinationCommitted {
				t.Errorf("accepted DrainBranch outcome=%s", got)
			}
			if got := active.Snapshot().Destinations[0].State; got != DestinationClosed {
				t.Errorf("active attachment outcome=%s", got)
			}
			if len(live.Snapshot().Branches) != 0 {
				t.Fatal("shutdown left attached handles")
			}
		})
	}
}

func TestCloseKeepsCallerCancellationAndIndependentErrors(t *testing.T) {
	independent := errors.New("independent execution failure")
	if got := withoutShutdownError(errors.Join(pipeline.ErrClosed, independent)); !errors.Is(got, independent) || errors.Is(got, pipeline.ErrClosed) {
		t.Fatalf("joined shutdown classification=%v", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	entered, release := make(chan struct{}), make(chan struct{})
	live, err := mustNew().Build(t.Context(), lifecycleTestSource(func(context.Context, SourcePush) error { close(entered); <-release; return pipeline.ErrClosed }).Audio().To(lifecycleTestSink("root")))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- live.Run(ctx) }()
	<-entered
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run=%v", err)
	}
	if err := live.Run(t.Context()); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("Run after Close=%v", err)
	}
}

type heldAttachmentInstallGraph struct {
	pipeline.Graph
	entered chan pipeline.Sink
	release chan struct{}
}

func (g *heldAttachmentInstallGraph) AddSink(sink pipeline.Sink, policy pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	g.entered <- sink
	<-g.release
	return g.Graph.AddSink(sink, policy)
}

func (g *heldAttachmentInstallGraph) CloseContext(ctx context.Context) error {
	return g.Graph.(interface{ CloseContext(context.Context) error }).CloseContext(ctx)
}

func TestCloseAbandonsAttachmentWhileGraphInstallationIsBlocked(t *testing.T) {
	live, err := mustNew().Build(t.Context(), lifecycleTestSource(func(context.Context, SourcePush) error { return nil }).Audio().Tap("packets").To(lifecycleTestSink("root")))
	if err != nil {
		t.Fatal(err)
	}
	owner := live.(*task)
	graph := &heldAttachmentInstallGraph{Graph: owner.graph, entered: make(chan pipeline.Sink), release: make(chan struct{})}
	owner.graph = graph
	sink := &retirementSink{name: "installing"}
	attached := make(chan error, 1)
	go func() {
		_, err := live.Attach(t.Context(), live.Tap("packets").As("installing").To(Sink(sink)))
		attached <- err
	}()
	<-graph.entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	closeErr := owner.CloseContext(ctx)
	cancel()
	owner.lifecycleMu.Lock()
	owned, aborted := len(owner.activeAttachments), true
	for attachment := range owner.activeAttachments {
		for _, tx := range attachment.transactions {
			aborted = aborted && tx.Failed()
		}
	}
	owner.lifecycleMu.Unlock()
	close(graph.release)
	attachErr := <-attached
	if err := live.Close(); err != nil {
		t.Fatalf("later Close=%v", err)
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) || !errors.Is(attachErr, pipeline.ErrClosed) {
		t.Fatalf("Close=%v Attach=%v", closeErr, attachErr)
	}
	if owned != 1 || !aborted || sink.closes.Load() != 0 {
		t.Fatalf("in-flight ownership=%d abandoned=%t closes=%d", owned, aborted, sink.closes.Load())
	}
	if got := live.Snapshot(); got.State != TaskClosed || len(got.Branches) != 0 {
		t.Fatalf("settled task=%+v", got)
	}
}

type heldAttachmentStageInstallGraph struct {
	pipeline.Graph
	entered, release chan struct{}
}

func (g *heldAttachmentStageInstallGraph) AddStage(stage pipeline.Stage, policy pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	close(g.entered)
	<-g.release
	return g.Graph.AddStage(stage, policy)
}
func (g *heldAttachmentStageInstallGraph) CloseContext(ctx context.Context) error {
	return g.Graph.(interface{ CloseContext(context.Context) error }).CloseContext(ctx)
}
