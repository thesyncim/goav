package goav_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
)

// churnSink counts the frames it receives and signals once it has seen want.
type churnSink struct {
	seen atomic.Int64
	want int64
	done chan struct{}
}

func (s *churnSink) handle(_ context.Context, msg component.Message) error {
	if msg.Frame != nil && s.seen.Add(1) == s.want {
		close(s.done)
	}
	return nil
}

// TestAttachDetachChurnPlateaus pins retired-node reclamation: a live
// buffered task attaches a resizing branch, lets it carry frames, and
// detaches it, 200 times. The heap after the last cycle stays within a fixed
// budget of the heap after warmup, and Stats reports exactly the nodes
// Describe shows: a detached branch leaves no queue, slots, or payload memory
// behind.
func TestAttachDetachChurnPlateaus(t *testing.T) {
	if testing.Short() {
		t.Skip("churn probe")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := goavtest.Runtime(runconfig.WithBufferPolicy(flow.DropOldest(16).PipelinePolicy()))
	task, err := rt.Build(ctx, goavtest.LiveVideo("cam", 320, 240).
		Video().
		Tap("cam.frames").
		To(benchSink("main")))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	cycle := func(i int) {
		sink := &churnSink{want: 8, done: make(chan struct{})}
		attachment, err := task.Attach(ctx, task.Tap("cam.frames").
			As("monitor-"+strconv.Itoa(i)).
			Resize(160, 120).
			To(goav.Sink(component.SinkFunc("monitor-sink-"+strconv.Itoa(i), sink.handle))))
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-sink.done:
		case <-time.After(10 * time.Second):
			t.Fatalf("cycle %d: branch saw %d frames, want %d", i, sink.seen.Load(), sink.want)
		}
		if err := task.Detach(ctx, attachment); err != nil {
			t.Fatal(err)
		}
	}
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.HeapAlloc
	}

	const warmup, cycles = 50, 200
	for i := range warmup {
		cycle(i)
	}
	before := heap()
	for i := warmup; i < cycles; i++ {
		cycle(i)
	}
	after := heap()
	t.Logf("heap after %d cycles: %d B; after %d cycles: %d B", warmup, before, cycles, after)

	stats := task.Stats()
	spec := task.Describe()
	if len(stats.Nodes) != len(spec.Nodes) {
		t.Errorf("Stats reports %d nodes, Describe shows %d: retired nodes still counted", len(stats.Nodes), len(spec.Nodes))
	}
	const budget = 1 << 20
	if after > before+budget {
		t.Errorf("heap grew %d B over %d attach/detach cycles, want at most %d: retired branches keep memory", after-before, cycles-warmup, budget)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
