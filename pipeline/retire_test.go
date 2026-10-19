package pipeline

import (
	"context"
	"strconv"
	"testing"
)

// emitterKeepingStage forwards everything and keeps the emitter it was handed,
// so a test can emit through it after the stage was removed.
type emitterKeepingStage struct {
	name    string
	emitter Emitter
}

func (s *emitterKeepingStage) Name() string { return s.name }
func (s *emitterKeepingStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	s.emitter = emitter
	return emitter.Emit(ctx, msg)
}
func (s *emitterKeepingStage) Close() error { return nil }

// TestRemovedNodesAreReclaimed pins that removal frees a node for good in
// both runners: a graph that adds and removes a branch 100 times keeps a
// bounded node table, Stats lists exactly the nodes Spec does, and the graph
// totals still count what the removed branches received.
func TestRemovedNodesAreReclaimed(t *testing.T) {
	for _, mode := range []struct {
		name   string
		buffer BufferPolicy
	}{
		{name: "direct"},
		{name: "buffered", buffer: BufferPolicy{Capacity: 4, Drop: DropBlock}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			graph, err := NewGraph(GraphConfig{Name: "churn", Buffer: mode.buffer})
			if err != nil {
				t.Fatal(err)
			}
			source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
			if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- graph.Run(ctx) }()
			<-source.ready

			const cycles = 100
			for i := range cycles {
				stage := "stage-" + strconv.Itoa(i)
				sink := "sink-" + strconv.Itoa(i)
				if _, err := graph.AddStage(&passStage{name: stage}, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				if _, err := graph.AddSink(&countingSink{name: sink}, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				if err := graph.Connect(route("src", stage)); err != nil {
					t.Fatal(err)
				}
				if err := graph.Connect(route(stage, sink)); err != nil {
					t.Fatal(err)
				}
				packet := immutablePacket(byte(i))
				if err := source.emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &packet}); err != nil {
					t.Fatal(err)
				}
				// Upstream first, so the stage hands its tail to the sink.
				if err := graph.Remove(NodeRef(stage)); err != nil {
					t.Fatal(err)
				}
				if err := graph.Remove(NodeRef(sink)); err != nil {
					t.Fatal(err)
				}
			}

			if n := nodeTableLen(graph); n > 3 {
				t.Errorf("node table holds %d entries after %d add/remove cycles, want at most 3", n, cycles)
			}
			stats := graph.Stats()
			if len(stats.Nodes) != len(graph.Spec().Nodes) {
				t.Errorf("Stats lists %d nodes, Spec %d", len(stats.Nodes), len(graph.Spec().Nodes))
			}
			// Each cycle delivers one packet to its stage and one to its sink.
			if stats.Delivered != 2*cycles {
				t.Errorf("Delivered = %d, want %d: removed nodes must keep counting in the totals", stats.Delivered, 2*cycles)
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

// TestRemovedStageEmitterCannotSpeakForReusedIndex pins the generation check
// that makes index reuse safe: a removed stage that kept its emitter emits
// into nothing, even after another stage took over its index.
func TestRemovedStageEmitterCannotSpeakForReusedIndex(t *testing.T) {
	for _, mode := range []struct {
		name   string
		buffer BufferPolicy
	}{
		{name: "direct"},
		{name: "buffered", buffer: BufferPolicy{Capacity: 4, Drop: DropBlock}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			graph, err := NewGraph(GraphConfig{Name: "stale-emitter", Buffer: mode.buffer})
			if err != nil {
				t.Fatal(err)
			}
			source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
			old := &emitterKeepingStage{name: "old"}
			oldSink := &countingSink{name: "old-sink"}
			if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if _, err := graph.AddStage(old, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if _, err := graph.AddSink(oldSink, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(route("src", "old")); err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(route("old", "old-sink")); err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- graph.Run(ctx) }()
			<-source.ready
			packet := immutablePacket(1)
			if err := source.emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &packet}); err != nil {
				t.Fatal(err)
			}
			if err := graph.Remove("old"); err != nil {
				t.Fatal(err)
			}
			if old.emitter == nil {
				t.Fatal("removed stage never saw a message")
			}
			// The new stage takes the old stage's index and feeds its own sink.
			if _, err := graph.AddStage(&passStage{name: "new"}, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			newSink := &countingSink{name: "new-sink"}
			if _, err := graph.AddSink(newSink, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(route("new", "new-sink")); err != nil {
				t.Fatal(err)
			}
			if err := old.emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &packet}); err != nil {
				t.Fatal(err)
			}
			close(source.release)
			if err := <-runDone; err != nil {
				t.Fatal(err)
			}
			if got := newSink.total(); got != 0 {
				t.Fatalf("the removed stage's emitter reached the new stage's sink %d times, want 0", got)
			}
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// nodeTableLen reports how many entries the graph's node table holds,
// live and retired.
func nodeTableLen(graph Graph) int {
	switch g := graph.(type) {
	case *directRunner:
		g.mu.RLock()
		defer g.mu.RUnlock()
		return len(g.nodes)
	case *bufferedRunner:
		g.mu.RLock()
		defer g.mu.RUnlock()
		return len(g.nodes)
	default:
		return -1
	}
}
