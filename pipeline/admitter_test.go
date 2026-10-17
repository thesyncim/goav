package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// holdUntilStage is a converging Admitter: it holds stream "a" back until it
// has handled a message from stream "b".
type holdUntilStage struct {
	sawB    chan struct{}
	closed  atomic.Bool
	handled atomic.Int64
}

func (s *holdUntilStage) Name() string { return "join" }

func (s *holdUntilStage) Admit(ctx context.Context, msg *Message) error {
	if msg.Packet == nil || msg.Packet.StreamID != "a" {
		return nil
	}
	select {
	case <-s.sawB:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *holdUntilStage) Handle(_ context.Context, msg *Message, _ Emitter) error {
	s.handled.Add(1)
	if msg.Packet != nil && msg.Packet.StreamID == "b" && !s.closed.Swap(true) {
		close(s.sawB)
	}
	return nil
}

func (s *holdUntilStage) Close() error { return nil }

// TestAdmitterHoldsOneUpstreamWithoutStallingSiblings pins the Admitter
// contract on both runners: Admit runs on the producer's goroutine before the
// delivery takes the target's lock or a queue slot, so a stage holding one
// upstream back still receives the sibling it is waiting for. A runner that
// admitted under the node's lock would deadlock here.
func TestAdmitterHoldsOneUpstreamWithoutStallingSiblings(t *testing.T) {
	for name, buffer := range map[string]BufferPolicy{
		"direct":   {},
		"buffered": {Capacity: 4, Drop: DropBlock},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			graph, err := NewGraph(GraphConfig{Name: "admit", Buffer: buffer})
			if err != nil {
				t.Fatal(err)
			}
			defer graph.Close()
			stage := &holdUntilStage{sawB: make(chan struct{})}
			sources := map[string]*directTestSource{
				"a": {name: "a", msg: &Message{Kind: MessagePacket, Packet: &av.Packet{StreamID: "a"}}},
				"b": {name: "b", msg: &Message{Kind: MessagePacket, Packet: &av.Packet{StreamID: "b"}}},
			}
			for _, source := range sources {
				if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			for name := range sources {
				if err := graph.Connect(Route{From: name, To: []string{"join"}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := graph.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := stage.handled.Load(); got != 2 {
				t.Fatalf("handled = %d, want both upstreams delivered", got)
			}
		})
	}
}
