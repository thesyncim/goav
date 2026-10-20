package pipeline

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// flushCountingStage acknowledges that all scripted media has reached the next
// queue, without depending on worker scheduling or sleeps.
type flushCountingStage struct {
	name string
	want int
	seen int
	done chan struct{}
}

func (s *flushCountingStage) Name() string { return s.name }
func (s *flushCountingStage) Close() error { return nil }
func (s *flushCountingStage) Handle(ctx context.Context, msg *Message, emit Emitter) error {
	if err := emit.Emit(ctx, msg); err != nil {
		return err
	}
	s.seen++
	if s.seen == s.want {
		close(s.done)
	}
	return nil
}

func TestFlushDownstreamPreservesSharedQueues(t *testing.T) {
	for _, test := range []struct {
		name   string
		stage  bool
		bypass bool
	}{
		{name: "shared-input"},
		{name: "shared-downstream", stage: true},
		{name: "shared-downstream-reached-directly", stage: true, bypass: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			graph, err := NewGraph(GraphConfig{Buffer: BufferPolicy{Capacity: 16, Drop: DropBlock}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = graph.Close() })
			a := &flushScriptSource{name: "a", script: []Message{flushFrameMessage(0)}, emitted: make(chan struct{})}
			b := &flushScriptSource{name: "b", script: []Message{flushFrameMessage(10), flushFrameMessage(11), flushFrameMessage(12)}, emitted: make(chan struct{})}
			sink := &flushGatedSink{name: "sink", started: make(chan struct{}), gate: make(chan struct{})}
			for _, source := range []*flushScriptSource{a, b} {
				if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			waits := []chan struct{}{a.emitted, b.emitted, sink.started}
			target := "sink"
			if test.stage {
				stage := &flushCountingStage{name: "shared", want: 4, done: make(chan struct{})}
				if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
				if err := graph.Connect(route("shared", "sink")); err != nil {
					t.Fatal(err)
				}
				target = "shared"
				waits = append(waits, stage.done)
			}
			for _, name := range []string{"a", "b"} {
				if err := graph.Connect(route(name, target)); err != nil {
					t.Fatal(err)
				}
			}
			if test.bypass {
				// This path reaches sink before the traversal can discover the
				// unrelated arm at shared. Ownership must still propagate to it.
				if err := graph.Connect(route("a", "sink")); err != nil {
					t.Fatal(err)
				}
			}
			var release sync.Once
			runErr := make(chan error, 1)
			go func() { runErr <- graph.Run(ctx) }()
			defer func() {
				cancel()
				release.Do(func() { close(sink.gate) })
				<-runErr
			}()
			for _, wait := range waits {
				select {
				case <-wait:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if err := graph.(nodeFlusher).FlushDownstream("a"); err != nil {
				t.Fatal(err)
			}
			if got := graph.Stats().DropReasons[DropFlush]; got != 0 {
				t.Fatalf("shared media flushed: %d messages", got)
			}
			release.Do(func() { close(sink.gate) })
			want := []string{"frame:0", "frame:10", "frame:11", "frame:12"}
			if test.bypass {
				want = append(want, "frame:0")
			}
			got, err := sink.waitForLen(ctx, len(want))
			if err != nil {
				t.Fatal(err)
			}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("deliveries = %v, want all admitted media %v", got, want)
			}
		})
	}
}

func TestFlushDownstreamFlushesSameSourceRejoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	graph, err := NewGraph(GraphConfig{Buffer: BufferPolicy{Capacity: 16, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = graph.Close() })
	source := &flushScriptSource{name: "source", script: []Message{flushFrameMessage(0), flushFrameMessage(1), flushFrameMessage(2)}, emitted: make(chan struct{})}
	sink := &flushGatedSink{name: "sink", started: make(chan struct{}), gate: make(chan struct{})}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	waits := []chan struct{}{source.emitted, sink.started}
	for _, name := range []string{"left", "right"} {
		stage := &flushCountingStage{name: name, want: 3, done: make(chan struct{})}
		if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route("source", name)); err != nil {
			t.Fatal(err)
		}
		if err := graph.Connect(route(name, "sink")); err != nil {
			t.Fatal(err)
		}
		waits = append(waits, stage.done)
	}
	var release sync.Once
	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	defer func() {
		cancel()
		release.Do(func() { close(sink.gate) })
		<-runErr
	}()
	for _, wait := range waits {
		select {
		case <-wait:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := graph.(nodeFlusher).FlushDownstream("source"); err != nil {
		t.Fatal(err)
	}
	if got := graph.Stats().DropReasons[DropFlush]; got != 5 {
		t.Fatalf("same-source rejoin flushed %d messages, want all 5 queued messages", got)
	}
	release.Do(func() { close(sink.gate) })
	got, err := sink.waitForLen(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"frame:0"}) {
		t.Fatalf("deliveries = %v, want only the in-flight frame", got)
	}
}
