package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// tickSource is a ControllableSource whose Start loop emits frames with a
// monotonically increasing PTS tick. Control(av.EventSeek) records the target
// tick in a single atomic the loop reads — the contract under test: Control is
// called from the injector's goroutine concurrently with Start and only records
// the request; the loop repositions and emits av.EventDiscontinuity before the
// first frame at the new position.
type tickSource struct {
	name   string
	seekTo atomic.Int64 // pending seek tick; negative means none
}

func newTickSource(name string) *tickSource {
	s := &tickSource{name: name}
	s.seekTo.Store(-1)
	return s
}

func (s *tickSource) Name() string { return s.name }

func (s *tickSource) Start(ctx context.Context, emitter Emitter) error {
	pos := int64(0)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if target := s.seekTo.Swap(-1); target >= 0 {
			pos = target
			disc := &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, Reason: "seek"}}
			for {
				err := emitter.Emit(ctx, disc)
				if err == nil {
					break
				}
				if !errors.Is(err, ErrBackpressure) || ctx.Err() != nil {
					return nil
				}
			}
		}
		msg := &Message{Kind: MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			if errors.Is(err, ErrBackpressure) {
				continue
			}
			return nil
		}
		pos++
	}
}

func (s *tickSource) Control(_ context.Context, msg *Message) error {
	if msg == nil || msg.Kind != MessageEvent || msg.Event == nil || msg.Event.Type != av.EventSeek {
		return errors.New("tickSource: unsupported control")
	}
	target, ok := msg.Event.Timestamp.Rescale(av.TimeBase{Num: 1, Den: int64(time.Second)})
	if !ok || target.Value < 0 {
		return errors.New("tickSource: bad seek position")
	}
	s.seekTo.Store(target.Value)
	return nil
}

func (s *tickSource) Close() error { return nil }

// seekLogSink records the order in which frames (by PTS tick) and discontinuity
// events arrive, so a test can assert the reposition and its hygiene signal.
type seekLogSink struct {
	name string
	mu   sync.Mutex
	log  []int64 // frame PTS tick, or -1 for a discontinuity event
}

func (s *seekLogSink) Name() string { return s.name }

func (s *seekLogSink) Handle(_ context.Context, msg *Message) error {
	if msg == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case msg.Kind == MessageFrame && msg.Frame != nil:
		s.log = append(s.log, msg.Frame.PTS.Value)
	case msg.Kind == MessageEvent && msg.Event != nil && msg.Event.Type == av.EventDiscontinuity:
		s.log = append(s.log, -1)
	}
	return nil
}

func (s *seekLogSink) Close() error { return nil }

func (s *seekLogSink) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.log...)
}

func (s *seekLogSink) waitForTick(ctx context.Context, min int64) error {
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

func seekMessage(tick int64) *Message {
	return &Message{Kind: MessageEvent, Event: &av.Event{
		Type:      av.EventSeek,
		Timestamp: av.Timestamp{Value: tick, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
	}}
}

func TestGraphBufferedInjectSourceSeeksMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	source := newTickSource("source")
	sink := &seekLogSink{name: "sink"}

	// DropBlock keeps every message: the discontinuity event must not be shed by
	// a lossy queue between the source and the sink.
	graph, err := NewGraph(GraphConfig{Name: "seek", Buffer: BufferPolicy{Capacity: 8, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	injector, ok := graph.(SourceInjector)
	if !ok {
		t.Fatalf("graph %T does not implement SourceInjector", graph)
	}

	// Before Run the source's Start loop is not live: the buffered runner reports
	// that, exactly like Inject.
	const target = int64(1) << 40 // far beyond what the loop reaches naturally
	if err := injector.InjectSource(ctx, "source", seekMessage(target)); !errors.Is(err, ErrDynamicGraphUnsupported) {
		t.Fatalf("InjectSource before Run err = %v, want ErrDynamicGraphUnsupported", err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()

	// Let some pre-seek traffic flow, then seek. Frames at the sink imply the
	// graph is running, so the injection cannot race startup.
	if err := sink.waitForTick(ctx, 3); err != nil {
		t.Fatalf("pre-seek frames never arrived: %v", err)
	}
	if err := injector.InjectSource(ctx, "source", seekMessage(target)); err != nil {
		t.Fatalf("InjectSource: %v", err)
	}
	if err := sink.waitForTick(ctx, target); err != nil {
		t.Fatalf("repositioned frame never arrived: %v", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}

	// The sink saw the jump, and the discontinuity arrived after the last
	// pre-seek frame and before the first repositioned frame.
	log := sink.snapshot()
	jump := -1
	for i, tick := range log {
		if tick >= target {
			jump = i
			break
		}
	}
	if jump < 1 {
		t.Fatalf("no frame at tick >= %d in log %v", target, log)
	}
	if log[jump] != target {
		t.Fatalf("first repositioned frame tick = %d, want %d", log[jump], target)
	}
	if log[jump-1] != -1 {
		t.Fatalf("entry before the jump = %d, want a discontinuity (-1); log tail %v", log[jump-1], log[max(0, jump-3):jump+1])
	}
	for _, tick := range log[:jump-1] {
		if tick < 0 || tick >= target {
			t.Fatalf("unexpected pre-seek entry %d in log %v", tick, log[:jump])
		}
	}
}

func TestGraphBufferedInjectSourceRejectsUncontrollableAndNonSources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	release := make(chan struct{})
	plain := &blockingSource{name: "plain", release: release}
	stage := &recordingStage{name: "stage", want: 1, got: make(chan struct{})}
	sink := &seekLogSink{name: "sink"}

	graph, err := NewGraph(GraphConfig{Name: "seek-reject", Buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(plain, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("plain", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("stage", "sink")); err != nil {
		t.Fatal(err)
	}
	injector := graph.(SourceInjector)

	// Nil message and unknown nodes are rejected without touching the graph.
	if err := injector.InjectSource(ctx, "plain", nil); !errors.Is(err, ErrNilMessage) {
		t.Fatalf("InjectSource nil err = %v, want ErrNilMessage", err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	if err := waitInjectAccepted(ctx, graph.(NodeInjector), "stage"); err != nil {
		t.Fatal(err)
	}

	if err := injector.InjectSource(ctx, "missing", seekMessage(0)); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("InjectSource unknown err = %v, want ErrUnknownNode", err)
	}
	// A stage is not a source: same ErrInvalidLink as Inject reports for sources.
	if err := injector.InjectSource(ctx, "stage", seekMessage(0)); !errors.Is(err, ErrInvalidLink) {
		t.Fatalf("InjectSource stage err = %v, want ErrInvalidLink", err)
	}
	// A source without the ControllableSource capability reports ErrInvalidLink.
	if err := injector.InjectSource(ctx, "plain", seekMessage(0)); !errors.Is(err, ErrInvalidLink) {
		t.Fatalf("InjectSource uncontrollable err = %v, want ErrInvalidLink", err)
	}

	close(release)
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

func TestGraphDirectInjectSourcePrePositionsSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	source := newBoundedTickSource("source", 3)
	sink := &seekLogSink{name: "sink"}

	graph, err := NewGraph(GraphConfig{Name: "seek-direct"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "sink")); err != nil {
		t.Fatal(err)
	}

	injector, ok := graph.(SourceInjector)
	if !ok {
		t.Fatalf("graph %T does not implement SourceInjector", graph)
	}

	// The direct runner tracks no running state: a control delivered before Run
	// pre-positions the source, whose Start loop observes it.
	if err := injector.InjectSource(ctx, "source", seekMessage(100)); err != nil {
		t.Fatalf("InjectSource on direct graph: %v", err)
	}
	if err := injector.InjectSource(ctx, "missing", seekMessage(0)); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("InjectSource unknown err = %v, want ErrUnknownNode", err)
	}
	if err := injector.InjectSource(ctx, "sink", seekMessage(0)); !errors.Is(err, ErrInvalidLink) {
		t.Fatalf("InjectSource sink err = %v, want ErrInvalidLink", err)
	}

	if err := graph.Run(ctx); err != nil {
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

// boundedTickSource is the direct-graph variant of tickSource: it emits a fixed
// number of ticks and returns, so a direct Run terminates. Same control seam.
type boundedTickSource struct {
	name   string
	count  int
	seekTo atomic.Int64
}

func newBoundedTickSource(name string, count int) *boundedTickSource {
	s := &boundedTickSource{name: name, count: count}
	s.seekTo.Store(-1)
	return s
}

func (s *boundedTickSource) Name() string { return s.name }

func (s *boundedTickSource) Start(ctx context.Context, emitter Emitter) error {
	pos := int64(0)
	for i := 0; i < s.count; i++ {
		if target := s.seekTo.Swap(-1); target >= 0 {
			pos = target
			disc := &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, Reason: "seek"}}
			if err := emitter.Emit(ctx, disc); err != nil {
				return err
			}
		}
		msg := &Message{Kind: MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			return err
		}
		pos++
	}
	return nil
}

func (s *boundedTickSource) Control(_ context.Context, msg *Message) error {
	if msg == nil || msg.Kind != MessageEvent || msg.Event == nil || msg.Event.Type != av.EventSeek {
		return errors.New("boundedTickSource: unsupported control")
	}
	target, ok := msg.Event.Timestamp.Rescale(av.TimeBase{Num: 1, Den: int64(time.Second)})
	if !ok || target.Value < 0 {
		return errors.New("boundedTickSource: bad seek position")
	}
	s.seekTo.Store(target.Value)
	return nil
}

func (s *boundedTickSource) Close() error { return nil }
