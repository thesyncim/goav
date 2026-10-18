package pipeline

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

var errTenthHandle = errors.New("sink failed on its 10th message")

// liveEmitSource is a live source: it emits until its context ends and never
// reports its Emit errors (the hardest case for the runner's error contract).
// With idle set it stops emitting after the first ten messages and just waits
// for its context — a live input with nothing new to send — returning the
// cancellation error it then sees.
type liveEmitSource struct {
	name string
	idle bool
}

func (s *liveEmitSource) Name() string { return s.name }

func (s *liveEmitSource) Start(ctx context.Context, emitter Emitter) error {
	packet := immutablePacket(1)
	msg := Message{Kind: MessagePacket, Packet: &packet}
	for n := 0; ctx.Err() == nil; n++ {
		if s.idle && n == 10 {
			<-ctx.Done()
			return ctx.Err()
		}
		if err := emitter.Emit(ctx, &msg); err != nil && !errors.Is(err, ErrBackpressure) {
			return nil
		}
	}
	return nil
}

func (s *liveEmitSource) Close() error { return nil }

// tenthFailSink fails its 10th Handle and every later one.
type tenthFailSink struct {
	name    string
	handled atomic.Int64
}

func (s *tenthFailSink) Name() string { return s.name }

func (s *tenthFailSink) Handle(context.Context, *Message) error {
	if s.handled.Add(1) >= 10 {
		return errTenthHandle
	}
	return nil
}

func (s *tenthFailSink) Close() error { return nil }

type passStage struct{ name string }

func (s *passStage) Name() string { return s.name }
func (s *passStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	return emitter.Emit(ctx, msg)
}
func (s *passStage) Close() error { return nil }

// TestGraphRunFailsFastOnNodeError pins the shared Run contract: a sink
// failing under a live source stops the run within a second in both runners,
// with and without a stage between them, and Run returns the sink's error
// itself — not the context.Canceled the failure causes, and not wrapped,
// since the echoes of one failure are not further errors.
func TestGraphRunFailsFastOnNodeError(t *testing.T) {
	for _, mode := range []struct {
		name   string
		buffer BufferPolicy
		idle   bool
	}{
		{name: "direct"},
		{name: "buffered block", buffer: BufferPolicy{Capacity: 4, Drop: DropBlock}},
		{name: "buffered drop oldest", buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}},
		{name: "buffered idle source", buffer: BufferPolicy{Capacity: 4, Drop: DropBlock}, idle: true},
	} {
		for _, staged := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/staged=%v", mode.name, staged), func(t *testing.T) {
				graph, err := NewGraph(GraphConfig{Name: "fail-fast", Buffer: mode.buffer})
				if err != nil {
					t.Fatal(err)
				}
				defer graph.Close()
				sink := &tenthFailSink{name: "sink"}
				if _, err := graph.AddSource(&liveEmitSource{name: "live", idle: mode.idle}, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				upstream := "live"
				if staged {
					if _, err := graph.AddStage(&passStage{name: "stage"}, BufferPolicy{}); err != nil {
						t.Fatal(err)
					}
					if err := graph.Connect(route("live", "stage")); err != nil {
						t.Fatal(err)
					}
					upstream = "stage"
				}
				if err := graph.Connect(route(upstream, "sink")); err != nil {
					t.Fatal(err)
				}

				runDone := make(chan error, 1)
				go func() { runDone <- graph.Run(context.Background()) }()
				select {
				case err = <-runDone:
				case <-time.After(time.Second):
					t.Fatal("Run did not return within 1s of the sink failing")
				}
				if err != errTenthHandle {
					t.Fatalf("Run = %v, want the sink's error itself", err)
				}
				if got := sink.handled.Load(); got != 10 {
					t.Fatalf("sink handled %d messages, want 10: nothing is delivered after the failure", got)
				}
			})
		}
	}
}

// TestRunFailureKeepsFirstAndCountsTheRest pins the error record behind Run:
// the first error wins, independent later errors are counted on it rather
// than dropped, and echoes of the first failure (the error propagating back
// up a chain, the cancellation it caused) are not counted.
func TestRunFailureKeepsFirstAndCountsTheRest(t *testing.T) {
	first := errors.New("mux write failed")
	second := errors.New("encoder failed")
	var canceled atomic.Bool
	var f runFailure
	f.reset(func() { canceled.Store(true) })
	if f.err() != nil || f.failed.Load() {
		t.Fatal("a fresh record reports a failure")
	}
	f.report(nil)
	f.report(first)
	if !canceled.Load() || !f.failed.Load() {
		t.Fatal("the first error did not cancel the run")
	}
	if f.err() != first {
		t.Fatalf("err = %v, want the first error itself while it is alone", f.err())
	}
	f.report(fmt.Errorf("stage: %w", first))
	f.report(context.Canceled)
	f.report(fmt.Errorf("source: %w", context.DeadlineExceeded))
	f.report(second)
	f.report(ErrClosed)
	err := f.err()
	if !errors.Is(err, first) || errors.Is(err, second) {
		t.Fatalf("err = %v, want it to match the first error only", err)
	}
	if want := "mux write failed (and 2 more errors)"; err.Error() != want {
		t.Fatalf("err = %q, want %q", err.Error(), want)
	}
	f.reset(nil)
	if f.err() != nil || f.failed.Load() {
		t.Fatal("reset did not clear the record")
	}
}

// TestGraphBufferedInjectRacesDrain pins C5: Inject from another goroutine
// while Run waits for the drain after its sources end. The in-flight count
// may rise from zero during that wait (a WaitGroup forbids it; the race
// detector flags the old code under this interleaving), and Run still ends.
func TestGraphBufferedInjectRacesDrain(t *testing.T) {
	for range 50 {
		graph, err := NewGraph(GraphConfig{Name: "inject-drain", Buffer: BufferPolicy{Capacity: 8, Drop: DropOldest}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := graph.AddSource(&bufferedPacketSource{name: "source", packets: nil, done: make(chan struct{})}, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if _, err := graph.AddSink(&benchSink{name: "sink"}, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route("source", "sink")); err != nil {
			t.Fatal(err)
		}
		injector := graph.(NodeInjector)
		stop := make(chan struct{})
		injected := make(chan struct{})
		go func() {
			defer close(injected)
			packet := immutablePacket(7)
			msg := Message{Kind: MessagePacket, Packet: &packet}
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = injector.Inject(context.Background(), "sink", &msg)
				runtime.Gosched()
			}
		}()
		if err := graph.Run(context.Background()); err != nil {
			t.Fatalf("Run = %v", err)
		}
		close(stop)
		<-injected
		_ = graph.Close()
	}
}
