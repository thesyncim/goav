package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// flushScriptSource emits a fixed message sequence, signals completion, then
// parks until the context ends so the graph stays running while a test
// flushes the queues it filled.
type flushScriptSource struct {
	name    string
	script  []Message
	emitted chan struct{}
}

func (s *flushScriptSource) Name() string { return s.name }

func (s *flushScriptSource) Start(ctx context.Context, emitter Emitter) error {
	for i := range s.script {
		if err := emitter.Emit(ctx, &s.script[i]); err != nil {
			return nil
		}
	}
	close(s.emitted)
	<-ctx.Done()
	return nil
}

func (s *flushScriptSource) Close() error { return nil }

// flushGatedSink signals its first delivery and then holds every Handle until
// the gate opens, so the upstream queue fills deterministically; afterwards it
// records deliveries in arrival order.
type flushGatedSink struct {
	name    string
	started chan struct{}
	gate    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	log     []string
}

func (s *flushGatedSink) Name() string { return s.name }

func (s *flushGatedSink) Handle(ctx context.Context, msg *Message) error {
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
	case msg.Kind == MessageFrame && msg.Frame != nil:
		s.log = append(s.log, fmt.Sprintf("frame:%d", msg.Frame.PTS.Value))
	case msg.Kind == MessageEvent && msg.Event != nil:
		s.log = append(s.log, "event:"+string(msg.Event.Type))
	}
	return nil
}

func (s *flushGatedSink) Close() error { return nil }

func (s *flushGatedSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

func (s *flushGatedSink) waitForLen(ctx context.Context, want int) ([]string, error) {
	for {
		log := s.snapshot()
		if len(log) >= want {
			return log, nil
		}
		select {
		case <-ctx.Done():
			return log, fmt.Errorf("sink log stuck at %v: %w", log, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func flushFrameMessage(tick int64) Message {
	return Message{Kind: MessageFrame, Frame: &av.Frame{
		PTS: av.Timestamp{Value: tick, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
	}}
}

func flushEventMessage(kind av.EventType) Message {
	return Message{Kind: MessageEvent, Event: &av.Event{Type: kind}}
}

type nodeFlusher interface {
	FlushDownstream(NodeRef) error
}

// flushTestGraph wires one script source into one gated sink on a buffered
// DropBlock graph, runs it, and waits until the script is fully emitted with
// the first message held in the sink — so the queue content is exactly the
// script tail, deterministically.
func flushTestGraph(t *testing.T, ctx context.Context, name string, script []Message) (Graph, *flushGatedSink, chan error) {
	t.Helper()
	graph, err := NewGraph(GraphConfig{
		Name:   name,
		Buffer: BufferPolicy{Capacity: 8, Drop: DropBlock},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = graph.Close() })
	source := &flushScriptSource{name: "src", script: script, emitted: make(chan struct{})}
	sink := &flushGatedSink{name: "sink", started: make(chan struct{}), gate: make(chan struct{})}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("src", "sink")); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	select {
	case <-sink.started:
	case <-ctx.Done():
		t.Fatalf("sink never received the first message: %v", ctx.Err())
	}
	select {
	case <-source.emitted:
	case <-ctx.Done():
		t.Fatalf("source never finished its script: %v", ctx.Err())
	}
	return graph, sink, runErr
}

// TestFlushDownstreamPreservesQueuedEvents pins the flush semantics on one
// queue: queued media is shed and counted under DropFlush (graph and node
// stats), queued events (here an EndOfStream) survive in order, and an unknown
// node is refused.
func TestFlushDownstreamPreservesQueuedEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	script := []Message{
		flushFrameMessage(0), // held in the sink's Handle
		flushFrameMessage(1),
		flushFrameMessage(2),
		flushEventMessage(av.EventEndOfStream),
	}
	graph, sink, runErr := flushTestGraph(t, ctx, "flush-events", script)

	flusher, ok := graph.(nodeFlusher)
	if !ok {
		t.Fatalf("buffered graph %T does not expose the structural flush capability", graph)
	}
	if err := flusher.FlushDownstream("nope"); !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("FlushDownstream(unknown) err = %v, want ErrUnknownNode", err)
	}
	if err := flusher.FlushDownstream("src"); err != nil {
		t.Fatalf("FlushDownstream err = %v", err)
	}

	close(sink.gate)
	log, err := sink.waitForLen(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"frame:0", "event:" + string(av.EventEndOfStream)}
	if len(log) != len(want) || log[0] != want[0] || log[1] != want[1] {
		t.Fatalf("sink log = %v, want %v (media flushed, event preserved)", log, want)
	}

	stats := graph.Stats()
	if got := stats.DropReasons[DropFlush]; got != 2 {
		t.Fatalf("graph DropReasons[DropFlush] = %d, want 2", got)
	}
	node := stats.Nodes["sink"]
	if got := node.DropReasons[DropFlush]; got != 2 {
		t.Fatalf("sink DropReasons[DropFlush] = %d, want 2", got)
	}
	if node.Dropped < 2 {
		t.Fatalf("sink Dropped = %d, want >= 2", node.Dropped)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// TestFlushDownstreamStopsAtDiscontinuity pins the reposition marker: a queued
// av.EventDiscontinuity ends the flush for that queue — the discontinuity and
// everything after it (post-seek media included) is preserved.
func TestFlushDownstreamStopsAtDiscontinuity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	script := []Message{
		flushFrameMessage(0), // held in the sink's Handle
		flushFrameMessage(1),
		flushEventMessage(av.EventDiscontinuity),
		flushFrameMessage(100),
	}
	graph, sink, runErr := flushTestGraph(t, ctx, "flush-disc", script)

	if err := graph.(nodeFlusher).FlushDownstream("src"); err != nil {
		t.Fatalf("FlushDownstream err = %v", err)
	}

	close(sink.gate)
	log, err := sink.waitForLen(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"frame:0", "event:" + string(av.EventDiscontinuity), "frame:100"}
	if len(log) != len(want) || log[0] != want[0] || log[1] != want[1] || log[2] != want[2] {
		t.Fatalf("sink log = %v, want %v (flush must stop at the discontinuity)", log, want)
	}
	if got := graph.Stats().DropReasons[DropFlush]; got != 1 {
		t.Fatalf("graph DropReasons[DropFlush] = %d, want 1 (only the pre-discontinuity frame)", got)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// TestFlushDownstreamFlushesOnlyDownstreamClosure pins the scope: flushing one
// source's closure leaves sibling sources' queues untouched.
func TestFlushDownstreamFlushesOnlyDownstreamClosure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	graph, err := NewGraph(GraphConfig{
		Name:   "flush-scope",
		Buffer: BufferPolicy{Capacity: 8, Drop: DropBlock},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = graph.Close() })

	scriptA := []Message{flushFrameMessage(0), flushFrameMessage(1), flushFrameMessage(2)}
	scriptB := []Message{flushFrameMessage(10), flushFrameMessage(11), flushFrameMessage(12)}
	sourceA := &flushScriptSource{name: "srcA", script: scriptA, emitted: make(chan struct{})}
	sourceB := &flushScriptSource{name: "srcB", script: scriptB, emitted: make(chan struct{})}
	sinkA := &flushGatedSink{name: "sinkA", started: make(chan struct{}), gate: make(chan struct{})}
	sinkB := &flushGatedSink{name: "sinkB", started: make(chan struct{}), gate: make(chan struct{})}
	for _, source := range []*flushScriptSource{sourceA, sourceB} {
		if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, sink := range []*flushGatedSink{sinkA, sinkB} {
		if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.Connect(route("srcA", "sinkA")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("srcB", "sinkB")); err != nil {
		t.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	for _, wait := range []chan struct{}{sinkA.started, sinkB.started, sourceA.emitted, sourceB.emitted} {
		select {
		case <-wait:
		case <-ctx.Done():
			t.Fatalf("graph never reached the held state: %v", ctx.Err())
		}
	}

	if err := graph.(nodeFlusher).FlushDownstream("srcA"); err != nil {
		t.Fatalf("FlushDownstream err = %v", err)
	}

	close(sinkA.gate)
	close(sinkB.gate)
	logB, err := sinkB.waitForLen(ctx, len(scriptB))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"frame:10", "frame:11", "frame:12"} {
		if logB[i] != want {
			t.Fatalf("sinkB log = %v, want all of srcB's media intact", logB)
		}
	}
	logA, err := sinkA.waitForLen(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(logA) != 1 || logA[0] != "frame:0" {
		t.Fatalf("sinkA log = %v, want only the held first frame", logA)
	}
	stats := graph.Stats()
	if got := stats.DropReasons[DropFlush]; got != 2 {
		t.Fatalf("graph DropReasons[DropFlush] = %d, want 2 (srcA's queued tail only)", got)
	}
	if got := stats.Nodes["sinkB"].DropReasons[DropFlush]; got != 0 {
		t.Fatalf("sinkB DropReasons[DropFlush] = %d, want 0", got)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}
