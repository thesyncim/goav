package pipeline

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// countThenIdleSource emits packets 1..n, then idles like a live input with
// nothing new to send until its context ends.
type countThenIdleSource struct {
	n    int
	sent chan struct{}
}

func (s *countThenIdleSource) Name() string { return "src" }
func (s *countThenIdleSource) Close() error { return nil }
func (s *countThenIdleSource) Start(ctx context.Context, emitter Emitter) error {
	for i := 1; i <= s.n; i++ {
		packet := immutablePacket(byte(i))
		if err := emitter.Emit(ctx, &Message{Kind: MessagePacket, Packet: &packet}); err != nil {
			return err
		}
	}
	close(s.sent)
	<-ctx.Done()
	return ctx.Err()
}

type recordingSink struct {
	mu     sync.Mutex
	got    []byte
	closed bool
}

func (s *recordingSink) Name() string { return "sink" }
func (s *recordingSink) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *recordingSink) Handle(_ context.Context, msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("delivery after close")
	}
	s.got = append(s.got, msg.Packet.Payload.Bytes[0])
	return nil
}

// TestGraphBufferedRemoveDrainsQueue pins that removing a running branch
// upstream first delivers its tail: the stage's queued packets reach the
// sink while the stage drains, then the sink's own queue drains before it
// closes. Nothing already queued is lost.
func TestGraphBufferedRemoveDrainsQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	graph, err := NewGraph(GraphConfig{Name: "remove-drain", Buffer: BufferPolicy{Capacity: 8, Drop: DropBlock}})
	if err != nil {
		t.Fatal(err)
	}
	const n = 6
	source := &countThenIdleSource{n: n, sent: make(chan struct{})}
	stage := &blockUntilStage{name: "stage", entered: make(chan struct{}), release: make(chan struct{})}
	sink := &recordingSink{}
	for _, add := range []func() (NodeRef, error){
		func() (NodeRef, error) { return graph.AddSource(source, BufferPolicy{}) },
		func() (NodeRef, error) { return graph.AddStage(stage, BufferPolicy{}) },
		func() (NodeRef, error) { return graph.AddSink(sink, BufferPolicy{}) },
	} {
		if _, err := add(); err != nil {
			t.Fatal(err)
		}
	}
	for _, route := range []Route{{From: "src", To: []string{"stage"}}, {From: "stage", To: []string{"sink"}}} {
		if err := graph.Connect(route); err != nil {
			t.Fatal(err)
		}
	}
	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()
	<-stage.entered
	<-source.sent // one packet in the stage's Handle, the rest queued behind it

	removed := make(chan error, 1)
	go func() { removed <- graph.Remove("stage") }()
	select {
	case err := <-removed:
		t.Fatalf("Remove returned %v before the stage drained", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(stage.release)
	if err := <-removed; err != nil {
		t.Fatalf("remove stage: %v", err)
	}
	if err := graph.Remove("sink"); err != nil {
		t.Fatalf("remove sink: %v", err)
	}
	sink.mu.Lock()
	got, closed := slices.Clone(sink.got), sink.closed
	sink.mu.Unlock()
	if want := []byte{1, 2, 3, 4, 5, 6}; !slices.Equal(got, want) || !closed {
		t.Fatalf("sink got %v (closed %v), want %v then closed", got, closed, want)
	}
	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
}
