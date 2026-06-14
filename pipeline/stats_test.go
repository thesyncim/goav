package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/goav/av"
)

type dropReportingStage struct {
	name    string
	dropped atomic.Uint64
}

func (s *dropReportingStage) Name() string { return s.name }

func (s *dropReportingStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	return emitter.Emit(ctx, msg)
}

func (s *dropReportingStage) Close() error { return nil }

func (s *dropReportingStage) DroppedMessages() uint64 { return s.dropped.Load() }

type statsTestSink struct{ name string }

func (s *statsTestSink) Name() string                           { return s.name }
func (s *statsTestSink) Handle(context.Context, *Message) error { return nil }
func (s *statsTestSink) Close() error                           { return nil }

// TestStatsFoldsReportedDrops pins the DropReporter capability on both
// runners: a stage that sheds messages internally reports its running total,
// and Stats folds it into the node's Dropped under DropSync plus the graph
// totals — no silent loss.
func TestStatsFoldsReportedDrops(t *testing.T) {
	configs := map[string]GraphConfig{
		"direct":   {Name: "direct"},
		"buffered": {Name: "buffered", Buffer: BufferPolicy{Capacity: 2, Drop: DropOldest}},
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			graph, err := NewGraph(config)
			if err != nil {
				t.Fatal(err)
			}
			defer graph.Close()
			stage := &dropReportingStage{name: "join"}
			stage.dropped.Store(3)
			stageRef, err := graph.AddStage(stage, config.Buffer)
			if err != nil {
				t.Fatal(err)
			}
			sinkRef, err := graph.AddSink(&statsTestSink{name: "out"}, config.Buffer)
			if err != nil {
				t.Fatal(err)
			}
			if err := graph.Connect(Route{From: stageRef.String(), To: []string{sinkRef.String()}, Policy: RouteAll}); err != nil {
				t.Fatal(err)
			}

			stats := graph.Stats()
			node, ok := stats.Nodes["join"]
			if !ok {
				t.Fatalf("stats.Nodes = %+v, want a join entry", stats.Nodes)
			}
			if node.Dropped != 3 || node.DropReasons[DropSync] != 3 {
				t.Fatalf("node dropped=%d reasons=%+v, want 3 under DropSync", node.Dropped, node.DropReasons)
			}
			if stats.Dropped != 3 || stats.DropReasons[DropSync] != 3 {
				t.Fatalf("graph dropped=%d reasons=%+v, want 3 under DropSync", stats.Dropped, stats.DropReasons)
			}
			if out := stats.Nodes["out"]; out.Dropped != 0 {
				t.Fatalf("sink dropped=%d, want 0 (no reporter)", out.Dropped)
			}
		})
	}
}

func TestStatsSumsShardedParallelCounters(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "stats-shards"})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	source := &benchCaptureSource{name: "src"}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(&statsTestSink{name: "out"}, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"out"}}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	const producers = 8
	const perProducer = 100
	var wg sync.WaitGroup
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			msg := &Message{
				Kind: MessagePacket,
				Packet: &av.Packet{
					Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable},
				},
			}
			for j := 0; j < perProducer; j++ {
				if err := source.emitter.Emit(context.Background(), msg); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	stats := graph.Stats()
	want := uint64(producers * perProducer)
	if stats.Packets != want || stats.Messages != want || stats.Delivered != want {
		t.Fatalf("stats packets=%d messages=%d delivered=%d, want all %d",
			stats.Packets, stats.Messages, stats.Delivered, want)
	}
	if got := stats.Nodes["src"].OutPackets; got != want {
		t.Fatalf("source out packets = %d, want %d", got, want)
	}
	if got := stats.Nodes["out"].InPackets; got != want {
		t.Fatalf("sink in packets = %d, want %d", got, want)
	}
}
