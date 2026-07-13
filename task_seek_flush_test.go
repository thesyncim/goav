package goav

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
)

// flushTickSource is a pipeline.ControllableSource whose Start loop emits
// frames with nanosecond-tick PTS. Control records a seek or a segment in one
// atomic the loop swaps between emits; the loop repositions, emits
// av.EventDiscontinuity, and (for a segment) ends with av.EventEndOfStream at
// the exclusive bound — the same contract shape as format's demux pump.
type flushTickSource struct {
	name    string
	pending atomic.Pointer[flushTickRequest]
}

type flushTickRequest struct {
	position int64
	end      int64 // exclusive segment end; 0 means plain seek
}

func (s *flushTickSource) Name() string { return s.name }

func (s *flushTickSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	pos := int64(0)
	end := int64(-1)
	for {
		if ctx.Err() != nil {
			return nil
		}
		if req := s.pending.Swap(nil); req != nil {
			pos = req.position
			end = -1
			reason := "seek"
			if req.end > 0 {
				end = req.end
				reason = "segment"
			}
			disc := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventDiscontinuity, Reason: reason}}
			if err := emitter.Emit(ctx, disc); err != nil {
				return nil
			}
		}
		if end >= 0 && pos >= end {
			eos := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream}}
			_ = emitter.Emit(ctx, eos)
			return nil
		}
		msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &av.Frame{
			PTS: av.Timestamp{Value: pos, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}
		if err := emitter.Emit(ctx, msg); err != nil {
			return nil
		}
		pos++
	}
}

func (s *flushTickSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return errors.New("flushTickSource: unsupported control")
	}
	switch msg.Event.Type {
	case av.EventSeek:
		pos, ok := msg.Event.Timestamp.ToDuration()
		if !ok || pos < 0 {
			return errors.New("flushTickSource: bad seek position")
		}
		s.pending.Store(&flushTickRequest{position: int64(pos)})
		return nil
	case av.EventSegment:
		start, ok := msg.Event.Timestamp.ToDuration()
		if !ok || start < 0 {
			return errors.New("flushTickSource: bad segment start")
		}
		end, ok := av.EventSegmentEnd(msg.Event)
		if !ok || end <= start {
			return errors.New("flushTickSource: bad segment end")
		}
		s.pending.Store(&flushTickRequest{position: int64(start), end: int64(end)})
		return nil
	default:
		return errors.New("flushTickSource: unsupported control")
	}
}

func (s *flushTickSource) Close() error { return nil }

// flushGateSink signals its first delivery and holds every Handle until the
// gate opens; afterwards it records tick PTS values in arrival order, with -1
// for a discontinuity and -2 for end of stream.
type flushGateSink struct {
	name    string
	started chan struct{}
	gate    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	log     []int64
}

func (s *flushGateSink) Name() string { return s.name }

func (s *flushGateSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
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
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventEndOfStream:
		s.log = append(s.log, -2)
	}
	return nil
}

func (s *flushGateSink) Close() error { return nil }

func (s *flushGateSink) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.log...)
}

func (s *flushGateSink) waitForTick(ctx context.Context, min int64) error {
	for {
		for _, tick := range s.snapshot() {
			if tick >= min {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("tick >= %d never arrived (log %v): %w", min, s.snapshot(), ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

// flushHeldTask builds a buffered ticker->sink task whose sink holds its first
// delivery, runs it, and waits until the queue is full and the pump is blocked:
// the sink holds tick 0, ticks 1..4 fill the capacity-4 queue, and tick 5 is
// blocked in the pump's emit. Every later flush count is deterministic.
func flushHeldTask(t *testing.T, ctx context.Context) (*task, *flushGateSink, chan error) {
	t.Helper()
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "seek-flush",
		Buffer: pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropBlock},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker := &flushTickSource{name: "ticker"}
	sink := &flushGateSink{name: "sink", started: make(chan struct{}), gate: make(chan struct{})}
	if _, err := graph.AddSource(ticker, pipeline.BufferPolicy{}); err != nil {
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
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	select {
	case <-sink.started:
	case <-ctx.Done():
		t.Fatalf("sink never received its first frame: %v", ctx.Err())
	}
	// OutFrames counts emits at emit time, so 6 means: tick 0 held in the
	// sink, ticks 1..4 queued, tick 5 blocked in the pump's emit.
	for {
		if task.Stats().Nodes["ticker"].OutFrames >= 6 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pump never filled the queue: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	return task, sink, runErr
}

// assertNoPreSeekAfterDisc pins the accurate-seek contract at the sink: the
// discontinuity was observed, and no pre-seek tick follows it.
func assertNoPreSeekAfterDisc(t *testing.T, log []int64, target int64) {
	t.Helper()
	disc := -1
	for i, tick := range log {
		if tick == -1 {
			disc = i
			break
		}
	}
	if disc < 0 {
		t.Fatalf("no discontinuity in sink log %v", log)
	}
	for _, tick := range log[disc+1:] {
		if tick >= 0 && tick < target {
			t.Fatalf("pre-seek tick %d delivered after the discontinuity (log %v)", tick, log)
		}
	}
}

// TestSeekFlushesInFlightMedia pins accurate seek on a buffered task: with a
// slow sink holding delivery and the queue full of pre-seek media, a seek
// flushes the queued backlog (counted under pipeline.DropFlush in stats and
// snapshot), the discontinuity arrives, and no pre-seek PTS follows it.
func TestSeekFlushesInFlightMedia(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, sink, runErr := flushHeldTask(t, ctx)

	const target = int64(time.Hour)
	if err := task.Control(ctx, control.Seek(time.Hour).At("ticker")); err != nil {
		t.Fatalf("Seek err = %v", err)
	}

	// The flush is synchronous with Control: the four queued pre-seek ticks
	// are already accounted when it returns, in stats and in the snapshot.
	stats := task.Stats()
	if got := stats.DropReasons[pipeline.DropFlush]; got != 4 {
		t.Fatalf("DropReasons[DropFlush] = %d, want 4 (the queued backlog)", got)
	}
	if got := stats.Nodes["sink"].DropReasons[pipeline.DropFlush]; got != 4 {
		t.Fatalf("sink DropReasons[DropFlush] = %d, want 4", got)
	}
	if got := task.Snapshot().Stats.DropReasons[pipeline.DropFlush]; got != 4 {
		t.Fatalf("snapshot DropReasons[DropFlush] = %d, want 4", got)
	}

	close(sink.gate)
	if err := sink.waitForTick(ctx, target); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
	assertNoPreSeekAfterDisc(t, sink.snapshot(), target)
}

// TestSegmentFlushesLikeSeek pins that a segment shares the seek flush
// semantics: the queued backlog is flushed, the window plays after the
// discontinuity, and the source ends the task at the window bound.
func TestSegmentFlushesLikeSeek(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, sink, runErr := flushHeldTask(t, ctx)

	const start = time.Hour
	segment := control.Must(control.Segment(start, start+3*time.Nanosecond))
	if err := task.Control(ctx, segment.At("ticker")); err != nil {
		t.Fatalf("Segment err = %v", err)
	}
	if got := task.Stats().DropReasons[pipeline.DropFlush]; got != 4 {
		t.Fatalf("DropReasons[DropFlush] = %d, want 4 (the queued backlog)", got)
	}

	close(sink.gate)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v (segment should end the source at its bound)", err)
	}
	log := sink.snapshot()
	assertNoPreSeekAfterDisc(t, log, int64(start))
	last := log[len(log)-1]
	if last != -2 {
		t.Fatalf("sink log = %v, want it to end with the segment's end of stream", log)
	}
	window := 0
	for _, tick := range log {
		if tick >= int64(start) && tick < int64(start)+3 {
			window++
		}
	}
	if window != 3 {
		t.Fatalf("sink log = %v, want the 3 ticks of the [start, start+3ns) window", log)
	}
}

// TestSeekWithoutFlushableNodesStillSeeks pins the direct-runner path: no
// queues means no flush capability, and a seek behaves exactly as before —
// discontinuity then repositioned media, no DropFlush accounting, no error.
func TestSeekWithoutFlushableNodesStillSeeks(t *testing.T) {
	ctx := context.Background()
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "seek-direct-flushless"})
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

	if err := task.Control(ctx, control.Seek(100*time.Nanosecond).At("ticker")); err != nil {
		t.Fatalf("Seek on direct graph err = %v", err)
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
	if got := task.Stats().DropReasons[pipeline.DropFlush]; got != 0 {
		t.Fatalf("DropReasons[DropFlush] = %d, want 0 on a direct graph", got)
	}
}

// flushMaxSink records only the highest frame tick it has delivered (one
// atomic per frame), so a free-running pump cannot grow an unbounded log
// during the race pin.
type flushMaxSink struct {
	name    string
	started chan struct{}
	once    sync.Once
	max     atomic.Int64
}

func (s *flushMaxSink) Name() string { return s.name }

func (s *flushMaxSink) Handle(_ context.Context, msg *pipeline.Message) error {
	s.once.Do(func() { close(s.started) })
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	tick := msg.Frame.PTS.Value
	for {
		seen := s.max.Load()
		if tick <= seen || s.max.CompareAndSwap(seen, tick) {
			return nil
		}
	}
}

func (s *flushMaxSink) Close() error { return nil }

func (s *flushMaxSink) waitForTick(ctx context.Context, min int64) error {
	for {
		if s.max.Load() >= min {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("tick >= %d never arrived (max %d): %w", min, s.max.Load(), ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

// TestSeekFlushUnderLiveTraffic exercises the flush concurrently with a
// free-running pump, deliveries, stats, and snapshots — the -race pin for the
// seek flush path. The last seek's media must still arrive.
func TestSeekFlushUnderLiveTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "seek-flush-race",
		Buffer: pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	ticker := &flushTickSource{name: "ticker"}
	sink := &flushMaxSink{name: "sink", started: make(chan struct{})}
	if _, err := graph.AddSource(ticker, pipeline.BufferPolicy{}); err != nil {
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
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()
	select {
	case <-sink.started:
	case <-ctx.Done():
		t.Fatalf("sink never received media: %v", ctx.Err())
	}

	var stats sync.WaitGroup
	statsDone := make(chan struct{})
	stats.Add(1)
	go func() {
		defer stats.Done()
		for {
			select {
			case <-statsDone:
				return
			default:
				_ = task.Stats()
				_ = task.Snapshot()
			}
		}
	}()

	const seeks = 50
	final := int64(time.Hour) + int64(seeks)*int64(time.Minute)
	for i := 1; i <= seeks; i++ {
		position := time.Hour + time.Duration(i)*time.Minute
		if err := task.Control(ctx, control.Seek(position).At("ticker")); err != nil {
			t.Fatalf("Seek #%d err = %v", i, err)
		}
	}
	if err := sink.waitForTick(ctx, final); err != nil {
		t.Fatal(err)
	}
	close(statsDone)
	stats.Wait()
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}
