package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/runconfig"
)

// This file is copied into an empty consumer module. It deliberately uses
// public APIs only, so it exercises the resolved dependency, not the checkout.
func TestResolvedClientKeepsReplyBeforeEOF(t *testing.T) {
	for range 20 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			var req struct {
				ID json.RawMessage `json:"id"`
			}
			if json.NewDecoder(conn).Decode(&req) == nil {
				_ = json.NewEncoder(conn).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": true})
			}
		}()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		client, err := ctlserver.Dial(ctx, "tcp://"+listener.Addr().String())
		if err != nil {
			cancel()
			listener.Close()
			t.Fatal(err)
		}
		got, err := client.Call[bool](ctx, "probe", nil)
		client.Close()
		cancel()
		listener.Close()
		if err != nil || !got {
			t.Fatalf("completed reply lost: %v %v", got, err)
		}
	}
}

func TestResolvedClientBoundsBlockedWrite(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer := make(chan net.Conn, 1)
	go func() { conn, _ := listener.Accept(); peer <- conn }()
	client, err := ctlserver.Dial(t.Context(), "tcp://"+listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn := <-peer
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Call[bool](ctx, "probe", make([]byte, 16<<20))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resolved client cannot cancel a blocked write")
	}
}

type heldSource struct {
	ready, stop chan struct{}
	emitter     pipeline.Emitter
}

func (*heldSource) Name() string { return "source" }
func (s *heldSource) Start(_ context.Context, e pipeline.Emitter) error {
	s.emitter = e
	close(s.ready)
	<-s.stop
	return nil
}
func (*heldSource) Close() error { return nil }

type heldSink struct {
	entered, release chan struct{}
	closes           atomic.Int32
	closeErr         error
}

func (*heldSink) Name() string { return "sink" }
func (s *heldSink) Handle(context.Context, *pipeline.Message) error {
	close(s.entered)
	<-s.release
	return nil
}
func (s *heldSink) Close() error { s.closes.Add(1); return s.closeErr }

func TestResolvedGraphFinalizesAfterTimeout(t *testing.T) {
	for _, policy := range []pipeline.BufferPolicy{{}, {Capacity: 1}} {
		g, err := pipeline.NewGraph(pipeline.GraphConfig{Buffer: policy, CloseWaitTimeout: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		source := &heldSource{ready: make(chan struct{}), stop: make(chan struct{})}
		finalization := errors.New("recording finalization failed")
		sink := &heldSink{entered: make(chan struct{}), release: make(chan struct{}), closeErr: finalization}
		if _, err := g.AddSource(source, policy); err != nil {
			t.Fatal(err)
		}
		if _, err := g.AddSink(sink, policy); err != nil {
			t.Fatal(err)
		}
		if err := g.Connect(pipeline.Route{From: "source", To: []string{"sink"}}); err != nil {
			t.Fatal(err)
		}
		run := make(chan error, 1)
		go func() { run <- g.Run(t.Context()) }()
		<-source.ready
		emit := make(chan error, 1)
		go func() {
			emit <- source.emitter.Emit(t.Context(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity}})
		}()
		select {
		case <-sink.entered:
		case <-time.After(time.Second):
			t.Fatal("no delivery")
		}
		if err := g.Remove("sink"); !errors.Is(err, pipeline.ErrCloseWait) {
			t.Fatalf("remove = %v", err)
		}
		close(sink.release)
		<-emit
		close(source.stop)
		<-run
		deadline := time.Now().Add(time.Second)
		for g.Stats().PendingCleanup != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if err := g.Close(); !errors.Is(err, finalization) {
			t.Fatalf("resolved graph lost deferred finalization error: %v", err)
		}
		if sink.closes.Load() != 1 {
			t.Fatalf("resolved graph abandoned Close: %d", sink.closes.Load())
		}
	}
}

// A published consumer must observe the real commit result, including after
// an idempotent retry of Close. Compiling this probe also catches stale pins
// that do not expose the finalization state/outcome contract.
type finalizationWriter struct {
	commitErr error
	commits   int
	closes    int
}

func (*finalizationWriter) Write(p []byte) (int, error)    { return len(p), nil }
func (w *finalizationWriter) Close() error                 { w.closes++; return nil }
func (w *finalizationWriter) Commit(context.Context) error { w.commits++; return w.commitErr }
func (*finalizationWriter) Abort(context.Context) error    { return nil }

func TestResolvedTaskRetainsFinalizationOutcome(t *testing.T) {
	failure := errors.New("upload commit failed")
	good := &finalizationWriter{}
	bad := &finalizationWriter{commitErr: failure}
	destination := func(name string, writer *finalizationWriter) goav.Destination {
		return goav.Writer(name, func(context.Context, provider.Info) (io.WriteCloser, error) {
			return writer, nil
		}, goav.Format(av.FormatOgg))
	}
	task, err := goavtest.Runtime().Build(t.Context(), goavtest.Audio(48000, 1, []int16{1}).
		Audio().Encode(codec.Opus()).To(destination("mem://good.ogg", good), destination("mem://bad.ogg", bad)))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := task.Snapshot()
	if before.State != goav.TaskFinalizing || good.commits != 0 || bad.commits != 0 {
		t.Fatalf("processing completion is not finalization: state=%s commits=%d/%d", before.State, good.commits, bad.commits)
	}
	for _, output := range before.Destinations {
		if output.State == goav.DestinationCommitted {
			t.Fatalf("premature commit: %+v", output)
		}
	}
	for range 2 {
		if err := task.Close(); !errors.Is(err, failure) {
			t.Fatalf("lost commit failure: %v", err)
		}
	}
	after := task.Snapshot()
	if after.State != goav.TaskFailed || !errors.Is(after.Err, failure) {
		t.Fatalf("terminal task outcome: state=%s error=%v", after.State, after.Err)
	}
	committed, failed := 0, 0
	for _, output := range after.Destinations {
		switch output.State {
		case goav.DestinationCommitted:
			committed++
		case goav.DestinationFailed:
			failed++
			if !errors.Is(output.Err, failure) {
				t.Fatalf("destination lost error identity: %v", output.Err)
			}
		}
	}
	if committed != 1 || failed != 1 || good.commits != 1 || bad.commits != 1 || good.closes != 1 || bad.closes != 1 {
		t.Fatalf("mixed outcomes or exactly-once cleanup lost: destinations=%+v good=%+v bad=%+v", after.Destinations, good, bad)
	}
}

type optionalFailureSink struct {
	cause  error
	closes atomic.Int32
}

func (*optionalFailureSink) Name() string                                      { return "optional-preview" }
func (s *optionalFailureSink) Handle(context.Context, *pipeline.Message) error { return s.cause }
func (s *optionalFailureSink) Close() error                                    { s.closes.Add(1); return nil }

func TestResolvedTaskIsolatesOptionalOutputAndRetainsFailure(t *testing.T) {
	for _, policy := range []pipeline.BufferPolicy{{}, {Capacity: 2, Drop: pipeline.DropBlock}} {
		t.Run(fmt.Sprintf("capacity-%d", policy.Capacity), func(t *testing.T) {
			cause := errors.New("preview disconnected")
			optional := &optionalFailureSink{cause: cause}
			healthy := goavtest.NewCollector()
			frames := [][]int16{{1, 2}, {3, 4}, {5, 6}, {7, 8}}
			source := goavtest.Audio(48000, 1, frames...).Audio()
			task, err := goavtest.Runtime(runconfig.WithBufferPolicy(policy)).Build(t.Context(),
				source.To(goav.Sink(optional)).OnFailure(goav.IsolateBranch), source.To(healthy.Sink()))
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			if err := task.Run(t.Context()); err != nil {
				t.Fatalf("optional failure escaped to Run: %v", err)
			}
			if err := task.Close(); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			if got := healthy.S16(); !reflect.DeepEqual(got, frames) {
				t.Fatalf("healthy sibling lost media: got %v want %v", got, frames)
			}
			if optional.closes.Load() != 1 {
				t.Fatalf("failed branch close count = %d", optional.closes.Load())
			}
			snapshot := task.Snapshot()
			found := false
			for _, branch := range snapshot.Branches {
				if branch.FailurePolicy != goav.IsolateBranch {
					continue
				}
				found = true
				if branch.State != goav.BranchFailed || !errors.Is(branch.Err, cause) || branch.PendingCleanup != 0 || !branch.Retired {
					t.Fatalf("failed branch outcome disappeared after retirement: %+v", branch)
				}
			}
			if !found {
				t.Fatal("isolated branch outcome missing")
			}
		})
	}
}

type heldFinalizerSink struct {
	entered, release chan struct{}
	closes           atomic.Int32
}

func (*heldFinalizerSink) Name() string                                    { return "held-finalizer" }
func (*heldFinalizerSink) Handle(context.Context, *pipeline.Message) error { return nil }
func (s *heldFinalizerSink) Close() error {
	s.closes.Add(1)
	close(s.entered)
	<-s.release
	return nil
}

func TestResolvedConcurrentCloseHonorsEachCallersDeadline(t *testing.T) {
	sink := &heldFinalizerSink{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(sink.release) }) }
	t.Cleanup(release)
	task, err := goavtest.Runtime().Build(t.Context(), goavtest.Audio(48000, 1, []int16{1}).Audio().To(goav.Sink(sink)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	closer, ok := task.(interface{ CloseContext(context.Context) error })
	if !ok {
		t.Fatal("built task has no CloseContext")
	}
	first := make(chan error, 1)
	go func() { first <- task.Close() }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("first close did not enter finalizer")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- closer.CloseContext(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second caller = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second CloseContext ignored its deadline behind another caller")
	}
	if err := task.Snapshot().Err; err != nil {
		t.Fatalf("caller timeout poisoned pending outcome: %v", err)
	}
	release()
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("successful finalizer = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first close did not finish")
	}
	if err := task.Close(); err != nil {
		t.Fatalf("caller timeout became sticky: %v", err)
	}
	if task.Snapshot().State != goav.TaskClosed || sink.closes.Load() != 1 {
		t.Fatalf("late successful close: state=%s closes=%d", task.Snapshot().State, sink.closes.Load())
	}
}
