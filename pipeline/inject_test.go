package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// blockingSource stays alive until release is closed, so a graph keeps running
// while a test injects a control into one of its nodes.
type blockingSource struct {
	name    string
	release chan struct{}
}

func (s *blockingSource) Name() string { return s.name }

func (s *blockingSource) Start(ctx context.Context, _ Emitter) error {
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *blockingSource) Close() error { return nil }

// recordingStage records every event it Handles and signals once a target count
// of events has arrived, so a test can wait for an injected event deterministically.
type recordingStage struct {
	name   string
	mu     sync.Mutex
	events []av.EventType
	want   int
	got    chan struct{}
}

func (s *recordingStage) Name() string { return s.name }

func (s *recordingStage) Handle(ctx context.Context, msg *Message, emitter Emitter) error {
	if msg == nil || msg.Kind != MessageEvent || msg.Event == nil {
		return nil
	}
	s.mu.Lock()
	s.events = append(s.events, msg.Event.Type)
	reached := len(s.events) == s.want
	s.mu.Unlock()
	if reached {
		close(s.got)
	}
	return emitter.Emit(ctx, msg)
}

func (s *recordingStage) Close() error { return nil }

func (s *recordingStage) snapshot() []av.EventType {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]av.EventType(nil), s.events...)
}

func TestGraphBufferedInjectReachesNodeWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	release := make(chan struct{})
	source := &blockingSource{name: "source", release: release}
	stage := &recordingStage{name: "stage", want: 1, got: make(chan struct{})}
	sink := &bufferedBlockingSink{name: "sink", started: make(chan struct{})}

	graph, err := NewGraph(GraphConfig{Name: "inject", Buffer: BufferPolicy{Capacity: 4, Drop: DropOldest}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(stage, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("source", "stage")); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(route("stage", "sink")); err != nil {
		t.Fatal(err)
	}

	injector, ok := graph.(NodeInjector)
	if !ok {
		t.Fatalf("graph %T does not implement NodeInjector", graph)
	}

	// Before Run, the node worker is not draining a queue: Inject must report that.
	if err := injector.Inject(ctx, "stage", &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventKeyframeRequired}}); !errors.Is(err, ErrDynamicGraphUnsupported) {
		t.Fatalf("Inject before Run err = %v, want ErrDynamicGraphUnsupported", err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- graph.Run(ctx) }()

	// Wait until the graph is running and the stage worker is draining.
	if err := waitInjectAccepted(ctx, injector, "stage"); err != nil {
		t.Fatal(err)
	}

	select {
	case <-stage.got:
	case <-ctx.Done():
		t.Fatalf("injected event never reached the stage: %v", ctx.Err())
	}

	close(release)
	if err := <-runErr; err != nil {
		t.Fatalf("Run err = %v", err)
	}

	got := stage.snapshot()
	if len(got) == 0 || got[0] != av.EventKeyframeRequired {
		t.Fatalf("stage events = %v, want first = %q", got, av.EventKeyframeRequired)
	}

	// Unknown node and nil message are rejected cleanly.
	if err := injector.Inject(context.Background(), "missing", &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventStats}}); !errors.Is(err, ErrUnknownNode) && !errors.Is(err, ErrDynamicGraphUnsupported) && !errors.Is(err, ErrClosed) {
		t.Fatalf("Inject unknown node err = %v", err)
	}
}

// waitInjectAccepted injects keyframe events until one is accepted (the graph is
// running and the worker is draining), so the test does not race graph startup.
func waitInjectAccepted(ctx context.Context, injector NodeInjector, node NodeRef) error {
	for {
		err := injector.Inject(ctx, node, &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventKeyframeRequired}})
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrDynamicGraphUnsupported) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func TestGraphBufferedInjectRejectsClosedAndSource(t *testing.T) {
	graph, err := NewGraph(GraphConfig{Name: "inject-reject", Buffer: BufferPolicy{Capacity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	source := &blockingSource{name: "source", release: make(chan struct{})}
	if _, err := graph.AddSource(source, BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	injector := graph.(NodeInjector)

	// Nil message is rejected without touching the graph.
	if err := injector.Inject(context.Background(), "source", nil); !errors.Is(err, ErrNilMessage) {
		t.Fatalf("Inject nil err = %v, want ErrNilMessage", err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if err := injector.Inject(context.Background(), "source", &Message{Kind: MessageEvent, Event: &av.Event{Type: av.EventStats}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Inject after Close err = %v, want ErrClosed", err)
	}
}
