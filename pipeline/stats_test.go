package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
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
