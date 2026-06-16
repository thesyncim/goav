package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// fanoutPacketSource emits count immutable packets through the plain Emit path.
// When releaseGate is non-nil it closes it after the loop, so a gated sibling
// can drain and the graph can shut down; done closes when the loop finishes,
// which a blocking downstream target prevents — that stall is what the
// backpressure test observes without a wall clock.
type fanoutPacketSource struct {
	name        string
	count       int
	releaseGate chan struct{}
	done        chan struct{}
	emitted     atomic.Int64
}

func (s *fanoutPacketSource) Name() string { return s.name }

func (s *fanoutPacketSource) Start(ctx context.Context, emitter Emitter) error {
	defer close(s.done)
	for i := 0; i < s.count; i++ {
		packet := immutablePacket(byte(i))
		msg := &Message{Kind: MessagePacket, Packet: &packet}
		if err := emitter.Emit(ctx, msg); err != nil {
			return err
		}
		s.emitted.Add(1)
	}
	if s.releaseGate != nil {
		close(s.releaseGate)
	}
	return nil
}

func (s *fanoutPacketSource) Close() error { return nil }

// closedGate returns an already-closed channel, so a gatedSink built with it
// never blocks — a fast sibling.
func closedGate() chan struct{} {
	g := make(chan struct{})
	close(g)
	return g
}

// TestBufferedFanoutDropOldestIsolatesSlowTarget pins the property the gio
// showcase relies on: on a fan-out, a slow target with a dropping (DropOldest)
// buffer sheds for itself and never paces the producer, so a fast sibling
// receives every message. This is the realtime-branch isolation the showcase's
// flow.DropOldest output branches depend on, tested without any wall clock.
func TestBufferedFanoutDropOldestIsolatesSlowTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const messages = 64
	slowGate := make(chan struct{})
	graph, err := NewGraph(GraphConfig{Name: "fanout-drop", Buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	// The source closes slowGate once it has emitted every message — which it can
	// only reach if the slow target never blocked it.
	source := &fanoutPacketSource{name: "src", count: messages, releaseGate: slowGate, done: make(chan struct{})}
	fast := &gatedSink{name: "fast", gate: closedGate()}
	slow := &gatedSink{name: "slow", gate: slowGate}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(fast, BufferPolicy{Capacity: messages, Drop: DropOldest}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(slow, BufferPolicy{Capacity: 1, Drop: DropOldest}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"fast", "slow"}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v (the source blocked behind the gated slow target)", err)
	}

	if got := fast.seen.Load(); got != messages {
		t.Fatalf("fast sibling saw %d/%d messages; a dropping slow target must not starve it", got, messages)
	}
	if got := slow.seen.Load(); got >= messages {
		t.Fatalf("slow target saw %d messages; expected it to shed behind its gate", got)
	}
	if dropped := graph.Stats().Nodes["slow"].Dropped; dropped == 0 {
		t.Fatal("slow target dropped 0; expected DropOldest sheds behind the gate")
	}
}

// TestBufferedFanoutDropBlockBackpressuresSource pins the contrast: a target
// with the blocking policy (the realtime default) does pace the producer on a
// fan-out, so a gated slow target stalls the whole source. This is exactly why
// the showcase's video branches must NOT use the default blocking buffer — it
// would drag the decode tap and every sibling down to the slow encoder's rate.
func TestBufferedFanoutDropBlockBackpressuresSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	release := make(chan struct{})
	graph, err := NewGraph(GraphConfig{Name: "fanout-block", Buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	source := &fanoutPacketSource{name: "src", count: 1000, done: make(chan struct{})}
	fast := &gatedSink{name: "fast", gate: closedGate()}
	slow := &gatedSink{name: "slow", gate: release}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(fast, BufferPolicy{Capacity: 8, Drop: DropOldest}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(slow, BufferPolicy{Capacity: 1, Drop: DropBlock}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"fast", "slow"}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- graph.Run(ctx) }()

	// A working blocking policy must hold the source: with the slow target gated
	// it cannot finish emitting. A broken (non-blocking) fan-out would race to
	// done near-instantly, so this margin reliably catches a regression without
	// being timing-sensitive about the passing case.
	select {
	case <-source.done:
		t.Fatal("source finished despite a gated DropBlock target; fan-out backpressure is missing")
	case <-time.After(250 * time.Millisecond):
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("Run() error after release = %v", err)
	}
	if got := source.emitted.Load(); got != 1000 {
		t.Fatalf("source emitted %d/1000 after release; expected it to drain once unblocked", got)
	}
}
