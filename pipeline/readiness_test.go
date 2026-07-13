package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func readinessPacket(stream av.StreamID) *Message {
	return &Message{Kind: MessagePacket, Packet: &av.Packet{
		StreamID: stream,
		Payload:  av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable},
	}}
}

func readinessEvent(stream av.StreamID) *Message {
	return &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventStats, StreamID: stream}}
}

// drainTaskReady returns how many av.EventTaskReady events currently sit on
// the graph's event stream without blocking; other event types pass by.
func drainTaskReady(events <-chan av.Event) int {
	count := 0
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return count
			}
			if event.Type == av.EventTaskReady {
				count++
			}
		default:
			return count
		}
	}
}

// waitTaskReady blocks until one av.EventTaskReady arrives, skipping other
// event types.
func waitTaskReady(t *testing.T, events <-chan av.Event) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("event stream closed before task readiness")
			}
			if event.Type == av.EventTaskReady {
				return
			}
		case <-deadline:
			t.Fatal("no readiness event arrived")
		}
	}
}

// readinessGraph builds a graph with one held source fanning out by stream id
// to sinks named after the ids, runs it, and hands back the captured emitter.
func readinessGraph(t *testing.T, buffer BufferPolicy, streams ...string) (Graph, *holdSource, chan error) {
	t.Helper()
	graph, err := NewGraph(GraphConfig{Name: "ready", Buffer: buffer})
	if err != nil {
		t.Fatal(err)
	}
	source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	if _, err := graph.AddSource(source, buffer); err != nil {
		t.Fatal(err)
	}
	for _, stream := range streams {
		if _, err := graph.AddSink(&benchSink{name: stream}, buffer); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(Route{From: "src", To: []string{stream}, Policy: RouteByStream, Label: stream}); err != nil {
			t.Fatal(err)
		}
	}
	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(context.Background()) }()
	<-source.ready
	return graph, source, runDone
}

// waitSinkDelivered polls stats until the named sink has received the wanted
// number of messages, so buffered-worker delivery is observable.
func waitSinkDelivered(t *testing.T, graph Graph, sink string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if stats := graph.Stats(); stats.Nodes[sink].InMessages >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink %q never received %d messages", sink, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestGraphReadinessReportsOnceAfterEverySinkFed pins the readiness signal on
// both runners: av.EventTaskReady is published exactly once, only after every
// sink received its first media message, and events do not count as media.
func TestGraphReadinessReportsOnceAfterEverySinkFed(t *testing.T) {
	policies := map[string]BufferPolicy{
		"direct":   {},
		"buffered": {Capacity: 8},
	}
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			graph, source, runDone := readinessGraph(t, policy, "a", "b")

			// An event is not media: it must not count a sink out.
			if err := source.emitter.Emit(ctx, readinessEvent("a")); err != nil {
				t.Fatal(err)
			}
			waitSinkDelivered(t, graph, "a", 1)
			if got := drainTaskReady(graph.Events()); got != 0 {
				t.Fatalf("readiness fired on an event delivery, count %d", got)
			}

			// One fed sink of two: not ready yet.
			if err := source.emitter.Emit(ctx, readinessPacket("a")); err != nil {
				t.Fatal(err)
			}
			waitSinkDelivered(t, graph, "a", 2)
			if got := drainTaskReady(graph.Events()); got != 0 {
				t.Fatalf("readiness fired before every sink was fed, count %d", got)
			}

			// The second sink's first media message completes readiness.
			if err := source.emitter.Emit(ctx, readinessPacket("b")); err != nil {
				t.Fatal(err)
			}
			waitTaskReady(t, graph.Events())

			// Later media never re-fires.
			if err := source.emitter.Emit(ctx, readinessPacket("a")); err != nil {
				t.Fatal(err)
			}
			if err := source.emitter.Emit(ctx, readinessPacket("b")); err != nil {
				t.Fatal(err)
			}
			waitSinkDelivered(t, graph, "a", 3)
			waitSinkDelivered(t, graph, "b", 2)
			if got := drainTaskReady(graph.Events()); got != 0 {
				t.Fatalf("readiness fired again after the first report, count %d", got)
			}

			close(source.release)
			if err := <-runDone; err != nil {
				t.Fatal(err)
			}
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestGraphReadinessNeverReportsWithoutSinks pins the no-sink scope: a graph
// with nothing to feed never claims readiness.
func TestGraphReadinessNeverReportsWithoutSinks(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "ready"})
	if err != nil {
		t.Fatal(err)
	}
	source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(context.Background()) }()
	<-source.ready
	if got := drainTaskReady(graph.Events()); got != 0 {
		t.Fatalf("sink-less graph reported readiness, count %d", got)
	}
	close(source.release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGraphReadinessRemovedSinkStopsGating pins the runtime-mutation edge: a
// sink removed before its first media message stops gating readiness, so the
// remaining fed sinks complete the signal.
func TestGraphReadinessRemovedSinkStopsGating(t *testing.T) {
	ctx := context.Background()
	graph, source, runDone := readinessGraph(t, BufferPolicy{Capacity: 8}, "a", "b")

	if err := source.emitter.Emit(ctx, readinessPacket("a")); err != nil {
		t.Fatal(err)
	}
	waitSinkDelivered(t, graph, "a", 1)
	if got := drainTaskReady(graph.Events()); got != 0 {
		t.Fatalf("readiness fired before every sink was fed, count %d", got)
	}

	mutable, ok := graph.(interface{ Remove(NodeRef) error })
	if !ok {
		t.Fatal("graph does not support Remove")
	}
	if err := mutable.Remove("b"); err != nil {
		t.Fatal(err)
	}
	waitTaskReady(t, graph.Events())

	close(source.release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}
