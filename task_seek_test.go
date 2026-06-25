package goav

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/control"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// seekTickSource is a pipeline.ControllableSource for goav-level seek tests:
// its Start loop emits frames whose PTS counts nanosecond ticks from zero, and
// control.Control(av.EventSeek) records the target in one atomic the loop reads — so a
// task.Control(ctx, control.Seek(...)) repositions it mid-run, with av.EventDiscontinuity
// emitted before the first repositioned frame per the interface contract.
type seekTickSource struct {
	name   string
	seekTo atomic.Int64 // pending seek tick; negative means none
}

func newSeekTickSource(name string) *seekTickSource {
	s := &seekTickSource{name: name}
	s.seekTo.Store(-1)
	return s
}

func (s *seekTickSource) Name() string { return s.name }

func (s *seekTickSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	pos := int64(0)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if target := s.seekTo.Swap(-1); target >= 0 {
			pos = target
			disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, Reason: "seek"}}
			for {
				err := emitter.Emit(ctx, disc)
				if err == nil {
					break
				}
				if !errors.Is(err, pipeline.ErrBackpressure) || ctx.Err() != nil {
					return nil
				}
			}
		}
		msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			if errors.Is(err, pipeline.ErrBackpressure) {
				continue
			}
			return nil
		}
		pos++
	}
}

func (s *seekTickSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil || msg.Event.Type != av.EventSeek {
		return errors.New("seekTickSource: unsupported control")
	}
	pos, ok := msg.Event.Timestamp.ToDuration()
	if !ok || pos < 0 {
		return errors.New("seekTickSource: bad seek position")
	}
	s.seekTo.Store(int64(pos))
	return nil
}

func (s *seekTickSource) Close() error { return nil }

// seekPlainSource is a source without the ControllableSource capability: it
// emits nothing and waits for cancellation, so an untargeted Seek must report
// it clearly while seekable siblings still reposition.
type seekPlainSource struct{ name string }

func (s *seekPlainSource) Name() string { return s.name }

func (s *seekPlainSource) Start(ctx context.Context, _ pipeline.Emitter) error {
	<-ctx.Done()
	return nil
}

func (s *seekPlainSource) Close() error { return nil }

// seekTaskSink records frame PTS ticks and discontinuities (-1) in arrival order.
type seekTaskSink struct {
	name string
	mu   sync.Mutex
	log  []int64
}

func (s *seekTaskSink) Name() string { return s.name }

func (s *seekTaskSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case msg.Kind == pipeline.MessageFrame && msg.Frame != nil:
		s.log = append(s.log, msg.Frame.PTS.Value)
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventDiscontinuity:
		s.log = append(s.log, -1)
	}
	return nil
}

func (s *seekTaskSink) Close() error { return nil }

func (s *seekTaskSink) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.log...)
}

func (s *seekTaskSink) waitForTick(ctx context.Context, min int64) error {
	for {
		for _, tick := range s.snapshot() {
			if tick >= min {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func TestTaskSeekRepositionsSourceMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name: "seek",
		// DropBlock keeps every message so the discontinuity cannot be shed.
		Buffer: pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropBlock},
	})
	if err != nil {
		t.Fatal(err)
	}

	ticker := newSeekTickSource("ticker")
	plain := &seekPlainSource{name: "plain"}
	sink := &seekTaskSink{name: "sink"}
	if _, err := graph.AddSource(ticker, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(plain, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "ticker", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "plain", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Let pre-seek frames flow; their arrival implies the graph is running.
	if err := sink.waitForTick(ctx, 3); err != nil {
		t.Fatalf("pre-seek frames never arrived: %v", err)
	}

	const position = time.Hour // tick 3.6e12: far beyond what the loop reaches naturally
	if err := task.Control(ctx, control.Seek(position).At("ticker")); err != nil {
		t.Fatalf("Seek controllable source: %v", err)
	}

	err = task.Control(ctx, control.Seek(position).At("plain"))
	if !errors.Is(err, pipeline.ErrInvalidLink) {
		t.Fatalf("Seek err = %v, want ErrInvalidLink for the uncontrollable source", err)
	}
	if !strings.Contains(err.Error(), `"plain"`) || !strings.Contains(err.Error(), "ControllableSource") {
		t.Fatalf("Seek err = %v, want it to name the source and the missing capability", err)
	}
	if strings.Contains(err.Error(), `"ticker"`) {
		t.Fatalf("Seek err = %v, must not blame the seekable source", err)
	}

	// The seekable source still repositioned: the sink sees the jump, preceded by
	// the discontinuity the contract requires.
	target := int64(position)
	if err := sink.waitForTick(ctx, target); err != nil {
		t.Fatalf("repositioned frame never arrived: %v", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}

	log := sink.snapshot()
	jump := -1
	for i, tick := range log {
		if tick >= target {
			jump = i
			break
		}
	}
	if jump < 1 {
		t.Fatalf("no repositioned frame in log %v", log)
	}
	if log[jump] != target {
		t.Fatalf("first repositioned tick = %d, want %d", log[jump], target)
	}
	if log[jump-1] != -1 {
		t.Fatalf("entry before the jump = %d, want a discontinuity (-1)", log[jump-1])
	}
}

func TestTaskSeekTargetsOneSourceAndDirectGraphs(t *testing.T) {
	ctx := context.Background()

	// A direct (unbuffered) graph delivers seeks too: sources are controlled by a
	// synchronous method call, not a queue, so control.SeekType does not depend on
	// per-node workers. Delivered before Run, the seek pre-positions the source.
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "seek-direct"})
	if err != nil {
		t.Fatal(err)
	}
	source := newSeekBoundedSource("ticker", 3)
	sink := &seekTaskSink{name: "sink"}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "ticker", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })

	// Expert targeting: At names the source node directly.
	if err := task.Control(ctx, control.Seek(100*time.Nanosecond).At("ticker")); err != nil {
		t.Fatalf("Seek on direct graph: %v", err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	want := []int64{-1, 100, 101, 102}
	got := sink.snapshot()
	if len(got) != len(want) {
		t.Fatalf("sink log = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sink log = %v, want %v", got, want)
		}
	}
}

// seekBoundedSource emits a fixed number of nanosecond ticks (honouring a
// pending seek first) and returns, so a direct Run terminates.
type seekBoundedSource struct {
	name   string
	count  int
	seekTo atomic.Int64
}

func newSeekBoundedSource(name string, count int) *seekBoundedSource {
	s := &seekBoundedSource{name: name, count: count}
	s.seekTo.Store(-1)
	return s
}

func (s *seekBoundedSource) Name() string { return s.name }

func (s *seekBoundedSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	pos := int64(0)
	for i := 0; i < s.count; i++ {
		if target := s.seekTo.Swap(-1); target >= 0 {
			pos = target
			disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, Reason: "seek"}}
			if err := emitter.Emit(ctx, disc); err != nil {
				return err
			}
		}
		msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			return err
		}
		pos++
	}
	return nil
}

func (s *seekBoundedSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil || msg.Event.Type != av.EventSeek {
		return errors.New("seekBoundedSource: unsupported control")
	}
	pos, ok := msg.Event.Timestamp.ToDuration()
	if !ok || pos < 0 {
		return errors.New("seekBoundedSource: bad seek position")
	}
	s.seekTo.Store(int64(pos))
	return nil
}

func (s *seekBoundedSource) Close() error { return nil }
