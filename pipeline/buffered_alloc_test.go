package pipeline

import (
	"context"
	"strconv"
	"testing"
)

// holdSource captures its emitter at Start and then blocks until released, so
// a test can drive the buffered producer path from outside Run while the
// worker goroutines drain concurrently.
type holdSource struct {
	name    string
	emitter Emitter
	ready   chan struct{}
	release chan struct{}
}

func (s *holdSource) Name() string { return s.name }

func (s *holdSource) Start(_ context.Context, e Emitter) error {
	s.emitter = e
	close(s.ready)
	<-s.release
	return nil
}

func (s *holdSource) Close() error { return nil }

// TestGraphBufferedSteadyEmitAllocs pins the buffered fanout steady path at
// zero allocations: emit reads the atomic routing snapshot, binds the message
// into a preallocated node slot (immutable payload, so no copy), and each
// sink's worker delivers and releases the slot — producer and worker sides
// both allocation-free per message. Regressions on the lock-free buffered
// data plane fail here loudly.
func TestGraphBufferedSteadyEmitAllocs(t *testing.T) {
	ctx := context.Background()
	graph, err := NewGraph(GraphConfig{Name: "alloc", Buffer: BufferPolicy{Capacity: 1024, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sink-0", "sink-1"} {
		if _, err := graph.AddSink(&benchSink{name: name}, BufferPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.Connect(Route{From: "src", To: []string{"sink-0", "sink-1"}}); err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(ctx) }()
	<-source.ready

	msg := benchPacketMessage(fanoutPayload, true) // immutable: queued by reference
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := source.emitter.Emit(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("buffered steady emit allocs = %v, want 0", allocs)
	}

	close(source.release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

// gateSink blocks every delivery until its gate closes, so an allocation
// measurement can observe the producer enqueue path alone: with all sink
// workers parked inside Handle, no worker-side activity (wakeups, releases,
// lazy stat inits) can leak into testing.AllocsPerRun's global malloc count.
// Shared CI runners made that leak real — see TestBufferedFanoutRefcountAllocs.
type gateSink struct {
	name string
	gate chan struct{}
}

func (s *gateSink) Name() string { return s.name }

func (s *gateSink) Handle(context.Context, *Message) error {
	<-s.gate
	return nil
}

func (s *gateSink) Close() error { return nil }

// TestBufferedFanoutRefcountAllocs pins the refcounted borrowed-packet enqueue
// path at zero allocations across the SFU-width sweep. The first target copies
// the producer-borrowed payload into graph-owned backing once; sibling targets
// bind refcounted views into their own preallocated slots. Sinks are gated
// shut during the measurement so the pinned window is purely producer-driven:
// the warmup emit plus the measured emits stay within the 64-slot queues, so
// no drop or delivery runs concurrently. The pin takes the minimum over a few
// measurement windows because one-time runtime costs — each of the N parked
// worker goroutines allocates its first sudog when it blocks on the gate, and
// on a slow runner those parks straggle past the warmup — land only in early
// windows, while a real per-emit allocation repeats in every window.
// Steady-state recycle allocs are covered by BenchmarkBufferedFanout/refcount
// and the committed baseline ratchet.
func TestBufferedFanoutRefcountAllocs(t *testing.T) {
	ctx := context.Background()
	for _, n := range fanoutSizes {
		t.Run("N="+strconv.Itoa(n), func(t *testing.T) {
			graph, err := NewGraph(GraphConfig{
				Name: "borrowed-fanout-alloc",
				Buffer: BufferPolicy{
					Capacity:        64,
					Drop:            DropOldest,
					CopyPacketBytes: fanoutPayload,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			source := &holdSource{name: "src", ready: make(chan struct{}), release: make(chan struct{})}
			if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			names := make([]string, n)
			for i := 0; i < n; i++ {
				names[i] = "sink-" + strconv.Itoa(i)
				if _, err := graph.AddSink(&gateSink{name: names[i], gate: gate}, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := graph.Connect(Route{From: "src", To: names}); err != nil {
				t.Fatal(err)
			}

			runDone := make(chan error, 1)
			go func() { runDone <- graph.Run(ctx) }()
			<-source.ready

			msg := benchPacketMessage(fanoutPayload, false)
			emit := func() {
				if err := source.emitter.Emit(ctx, msg); err != nil {
					t.Fatal(err)
				}
			}
			allocs := testing.AllocsPerRun(10, emit)
			for attempt := 0; allocs != 0 && attempt < 3; attempt++ {
				allocs = testing.AllocsPerRun(10, emit)
			}
			if allocs != 0 {
				t.Fatalf("borrowed buffered fanout emit allocs = %v in every window, want 0", allocs)
			}

			close(gate)
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
