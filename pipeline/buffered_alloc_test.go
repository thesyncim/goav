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

// TestBufferedFanoutRefcountAllocs pins the refcounted borrowed-packet fanout
// path at zero allocations across the SFU-width sweep. The first target copies
// the producer-borrowed payload into graph-owned backing once; sibling targets
// bind refcounted views into their own preallocated slots.
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
			names := make([]string, n)
			for i := 0; i < n; i++ {
				names[i] = "sink-" + strconv.Itoa(i)
				if _, err := graph.AddSink(&benchSink{name: names[i]}, BufferPolicy{}); err != nil {
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
			if allocs := testing.AllocsPerRun(50, func() {
				if err := source.emitter.Emit(ctx, msg); err != nil {
					t.Fatal(err)
				}
			}); allocs != 0 {
				t.Fatalf("borrowed buffered fanout emit allocs = %v, want 0", allocs)
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
