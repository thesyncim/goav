package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// deliverySource emits count events through the DeliveryEmitter capability and
// tallies the per-message outcomes, releasing the sink gate when done.
type deliverySource struct {
	name      string
	count     int
	gate      chan struct{}
	delivered atomic.Int64
	shed      atomic.Int64
}

func (s *deliverySource) Name() string { return s.name }

func (s *deliverySource) Start(ctx context.Context, emitter Emitter) error {
	de, ok := emitter.(DeliveryEmitter)
	if !ok {
		return ErrDynamicGraphUnsupported
	}
	for i := 0; i < s.count; i++ {
		msg := &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventStats, StreamID: "d"}}
		delivery, err := de.EmitDelivery(ctx, msg)
		if err != nil {
			return err
		}
		s.delivered.Add(int64(delivery.Delivered))
		s.shed.Add(int64(delivery.Shed))
	}
	close(s.gate)
	return nil
}

func (s *deliverySource) Close() error { return nil }

// gatedSink blocks its node worker until the gate closes, so the upstream
// queue fills and the drop policy has to shed.
type gatedSink struct {
	name string
	gate chan struct{}
	seen atomic.Int64
}

func (s *gatedSink) Name() string { return s.name }

func (s *gatedSink) Handle(ctx context.Context, _ *Message) error {
	select {
	case <-s.gate:
	case <-ctx.Done():
	}
	s.seen.Add(1)
	return nil
}

func (s *gatedSink) Close() error { return nil }

// TestEmitDeliveryReportsShedsPerMessage pins the DeliveryEmitter contract: a
// full queue under a dropping policy sheds deliberately with a nil error, and
// EmitDelivery is what makes that visible per message (plain Emit stays silent
// by design — sheds are normal realtime behavior).
func TestEmitDeliveryReportsShedsPerMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	gate := make(chan struct{})
	graph, err := NewGraph(GraphConfig{
		Name:   "delivery",
		Buffer: BufferPolicy{Capacity: 1, Drop: DropNewest},
	})
	if err != nil {
		t.Fatal(err)
	}
	const pushes = 16
	source := &deliverySource{name: "src", count: pushes, gate: gate}
	sink := &gatedSink{name: "sink", gate: gate}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{Capacity: 1, Drop: DropNewest}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"sink"}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}

	delivered, shed := source.delivered.Load(), source.shed.Load()
	if delivered+shed != pushes {
		t.Fatalf("delivered(%d)+shed(%d) = %d, want %d (every push accounted)", delivered, shed, delivered+shed, pushes)
	}
	if delivered == 0 {
		t.Fatal("no message was delivered")
	}
	if shed == 0 {
		t.Fatal("no shed was reported despite a gated sink behind a capacity-1 DropNewest queue")
	}
	if sink.seen.Load() != delivered {
		t.Fatalf("sink saw %d messages, want exactly the %d delivered", sink.seen.Load(), delivered)
	}
}

// TestDirectEmitterImplementsDelivery pins that the direct runner offers the
// same capability and reports accepted fanout targets, not just successful
// emissions.
func TestDirectEmitterImplementsDelivery(t *testing.T) {
	gate := make(chan struct{})
	close(gate) // direct execution must not block
	graph, err := NewGraph(GraphConfig{Name: "delivery-direct"})
	if err != nil {
		t.Fatal(err)
	}
	source := &deliverySource{name: "src", count: 4, gate: make(chan struct{})}
	sink := &gatedSink{name: "sink", gate: gate}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	left := sink
	right := &gatedSink{name: "right", gate: gate}
	if _, err := graph.AddSink(left, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(right, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"sink", "right"}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := source.delivered.Load(); got != 8 {
		t.Fatalf("direct delivered = %d, want 8", got)
	}
	if got := source.shed.Load(); got != 0 {
		t.Fatalf("direct shed = %d, want 0", got)
	}
	if got := left.seen.Load(); got != 4 {
		t.Fatalf("left sink saw %d, want 4", got)
	}
	if got := right.seen.Load(); got != 4 {
		t.Fatalf("right sink saw %d, want 4", got)
	}
}

func TestDirectEmitterDeliveryReportsPausedTargetsAsShed(t *testing.T) {
	gate := make(chan struct{})
	close(gate)
	graph, err := NewGraph(GraphConfig{Name: "delivery-direct-paused"})
	if err != nil {
		t.Fatal(err)
	}
	const pushes = 4
	source := &deliverySource{name: "src", count: pushes, gate: make(chan struct{})}
	left := &gatedSink{name: "left", gate: gate}
	right := &gatedSink{name: "right", gate: gate}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(left, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(right, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(Route{From: "src", To: []string{"left", "right"}, Policy: RouteAll}); err != nil {
		t.Fatal(err)
	}
	pauser := graph.(NodePauser)
	if err := pauser.SetNodePaused("right", true); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := source.delivered.Load(); got != pushes {
		t.Fatalf("direct delivered = %d, want %d", got, pushes)
	}
	if got := source.shed.Load(); got != pushes {
		t.Fatalf("direct shed = %d, want %d", got, pushes)
	}
	if got := left.seen.Load(); got != pushes {
		t.Fatalf("left sink saw %d, want %d", got, pushes)
	}
	if got := right.seen.Load(); got != 0 {
		t.Fatalf("paused right sink saw %d, want 0", got)
	}
}
