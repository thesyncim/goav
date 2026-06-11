package goav

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// watchTestGraph is a minimal pipeline.Graph whose event stream the test feeds
// directly, so fan-out behavior is fully deterministic.
type watchTestGraph struct {
	events    chan av.Event
	closeOnce sync.Once
}

func newWatchTestGraph(capacity int) *watchTestGraph {
	return &watchTestGraph{events: make(chan av.Event, capacity)}
}

func (g *watchTestGraph) AddSource(pipeline.Source, pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	return "", nil
}

func (g *watchTestGraph) AddStage(pipeline.Stage, pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	return "", nil
}

func (g *watchTestGraph) AddSink(pipeline.Sink, pipeline.BufferPolicy) (pipeline.NodeRef, error) {
	return "", nil
}

func (g *watchTestGraph) Connect(pipeline.Route) error    { return nil }
func (g *watchTestGraph) Disconnect(pipeline.Route) error { return nil }
func (g *watchTestGraph) Remove(pipeline.NodeRef) error   { return nil }
func (g *watchTestGraph) Spec() pipeline.Spec             { return pipeline.Spec{} }
func (g *watchTestGraph) Run(context.Context) error       { return nil }
func (g *watchTestGraph) Events() <-chan av.Event         { return g.events }
func (g *watchTestGraph) Stats() pipeline.GraphStats      { return pipeline.GraphStats{} }

func (g *watchTestGraph) Close() error {
	g.closeOnce.Do(func() { close(g.events) })
	return nil
}

func recvWatchEvent(t *testing.T, watch <-chan av.Event) av.Event {
	t.Helper()
	select {
	case event, ok := <-watch:
		if !ok {
			t.Fatal("watch channel closed early")
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
	return av.Event{}
}

// collectUntilClosed drains a watcher until its channel closes, failing the
// test if closure never happens — the goroutine-leak guard for the distributor.
func collectUntilClosed(t *testing.T, watch <-chan av.Event) []av.Event {
	t.Helper()
	var events []av.Event
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-watch:
			if !ok {
				return events
			}
			events = append(events, event)
		case <-deadline:
			t.Fatal("timed out waiting for watch channel to close")
		}
	}
}

func TestWatchFiltersByType(t *testing.T) {
	graph := newWatchTestGraph(8)
	task := newTask(graph, nil)
	loss := task.Watch(WatchTypes(av.EventPacketLoss))

	graph.events <- av.Event{Type: av.EventStats}
	graph.events <- av.Event{Type: av.EventPacketLoss, StreamID: "v0"}
	graph.events <- av.Event{Type: av.EventEndOfStream}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	got := collectUntilClosed(t, loss)
	if len(got) != 1 || got[0].Type != av.EventPacketLoss || got[0].StreamID != "v0" {
		t.Fatalf("events = %+v, want one packet_loss for v0", got)
	}
}

func TestWatchFiltersByStream(t *testing.T) {
	graph := newWatchTestGraph(8)
	task := newTask(graph, nil)
	audio := task.Watch(WatchStream("a0"))

	graph.events <- av.Event{Type: av.EventStats, StreamID: "v0"}
	graph.events <- av.Event{Type: av.EventPacketLoss, StreamID: "a0"}
	graph.events <- av.Event{Type: av.EventEndOfStream, StreamID: "a0"}
	graph.events <- av.Event{Type: av.EventEndOfStream}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	got := collectUntilClosed(t, audio)
	if len(got) != 2 || got[0].Type != av.EventPacketLoss || got[1].Type != av.EventEndOfStream {
		t.Fatalf("events = %+v, want a0 packet_loss then a0 end_of_stream", got)
	}
	for i := range got {
		if got[i].StreamID != "a0" {
			t.Fatalf("events[%d].StreamID = %q, want a0", i, got[i].StreamID)
		}
	}
}

func TestWatchFiltersANDTogether(t *testing.T) {
	graph := newWatchTestGraph(8)
	task := newTask(graph, nil)
	watch := task.Watch(WatchTypes(av.EventPacketLoss), WatchStream("v0"))

	graph.events <- av.Event{Type: av.EventPacketLoss, StreamID: "a0"}
	graph.events <- av.Event{Type: av.EventStats, StreamID: "v0"}
	graph.events <- av.Event{Type: av.EventPacketLoss, StreamID: "v0"}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	got := collectUntilClosed(t, watch)
	if len(got) != 1 || got[0].Type != av.EventPacketLoss || got[0].StreamID != "v0" {
		t.Fatalf("events = %+v, want only the v0 packet_loss", got)
	}
}

func TestWatchSlowWatcherShedsForItselfOnly(t *testing.T) {
	const capacity = 4
	const total = 10
	graph := newWatchTestGraph(1)
	task := newTask(graph, &runtime{eventCapacity: capacity})
	fast := task.Watch()
	slow := task.Watch()

	var fastGot []av.Event
	for i := 0; i < total; i++ {
		graph.events <- av.Event{Type: av.EventStats, Reason: strconv.Itoa(i)}
		// Drain the fast watcher in lockstep so its buffer never fills; the
		// slow watcher is never read, so it keeps only its buffered prefix.
		fastGot = append(fastGot, recvWatchEvent(t, fast))
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	fastGot = append(fastGot, collectUntilClosed(t, fast)...)
	slowGot := collectUntilClosed(t, slow)

	if len(fastGot) != total {
		t.Fatalf("fast watcher events = %d, want %d", len(fastGot), total)
	}
	for i := range fastGot {
		if fastGot[i].Reason != strconv.Itoa(i) {
			t.Fatalf("fast[%d].Reason = %q, want %d", i, fastGot[i].Reason, i)
		}
	}
	if len(slowGot) != capacity {
		t.Fatalf("slow watcher events = %d, want buffer capacity %d", len(slowGot), capacity)
	}
	for i := range slowGot {
		if slowGot[i].Reason != strconv.Itoa(i) {
			t.Fatalf("slow[%d].Reason = %q, want %d (oldest events kept, newest shed)", i, slowGot[i].Reason, i)
		}
	}
	if got := task.watch.dropped.Load(); got != total-capacity {
		t.Fatalf("dropped = %d, want %d", got, total-capacity)
	}
}

func TestWatchDeliversBufferedEventsAfterClose(t *testing.T) {
	graph := newWatchTestGraph(4)
	task := newTask(graph, nil)
	graph.events <- av.Event{Type: av.EventStats}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	got := collectUntilClosed(t, task.Watch())
	if len(got) != 1 || got[0].Type != av.EventStats {
		t.Fatalf("events = %+v, want the buffered stats event then closure", got)
	}
}

func TestWatchAfterDistributorDoneIsClosed(t *testing.T) {
	graph := newWatchTestGraph(1)
	task := newTask(graph, nil)
	first := task.Watch()
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	collectUntilClosed(t, first)

	late := task.Watch()
	if _, ok := <-late; ok {
		t.Fatal("watcher subscribed after shutdown should be closed")
	}
}

// TestWatchSubscribePublishDistributeConcurrently stresses the distributor's
// three concurrent entry points — graph events flowing through distribute,
// task-originated publish, and live Watch subscription — against each other.
// Bounded iterations, run under -race in the gate. Every watcher channel must
// still close on shutdown (no torn watcher list, no leaked goroutine).
func TestWatchSubscribePublishDistributeConcurrently(t *testing.T) {
	graph := newWatchTestGraph(64)
	task := newTask(graph, nil)

	const events = 100
	const watchers = 20
	channels := make(chan (<-chan av.Event), watchers+1)
	channels <- task.Watch() // start the distributor before the stress

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < watchers; i++ {
			channels <- task.Watch(WatchTypes(av.EventStats))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < events; i++ {
			graph.events <- av.Event{Type: av.EventStats, Reason: strconv.Itoa(i)}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < events; i++ {
			task.watch.publish(av.Event{Type: av.EventEndOfStream, Reason: strconv.Itoa(i)})
		}
	}()
	wg.Wait()
	close(channels)
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	for watch := range channels {
		collectUntilClosed(t, watch)
	}
}

func TestEventsReturnsUnderlyingGraphChannel(t *testing.T) {
	graph := newWatchTestGraph(1)
	task := newTask(graph, nil)
	if task.Events() != (<-chan av.Event)(graph.events) {
		t.Fatal("Events must expose the graph event channel unchanged")
	}
}

func TestWatchEndToEndDeliversAndClosesOnTaskClose(t *testing.T) {
	source := &runtimeEventBurstSource{name: "events", count: 3}
	task, err := newTestBuilder(t).
		Source(source).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	watch := task.Watch(WatchTypes(av.EventStats))
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}

	got := collectUntilClosed(t, watch)
	if len(got) != 3 {
		t.Fatalf("events = %+v, want 3 stats events", got)
	}
	for i := range got {
		if got[i].Type != av.EventStats {
			t.Fatalf("events[%d].Type = %q, want stats", i, got[i].Type)
		}
	}
}
