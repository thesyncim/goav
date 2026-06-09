package goav

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// controlSelectorSwitch is the reason marker an injected ControlEvent carries to
// drive a selectorStage's SetActive mid-run. The wrapper below interprets an event
// with this reason as "switch the active input to Event.StreamID"; every other
// message is delegated verbatim to the real selectorStage.
const controlSelectorSwitch = "select.switch"

// controlSelector wraps the real selectorStage so an out-of-band ControlEvent can
// drive SetActive on the node's own serial worker — proving Task.Control reaches a
// running node and the genuine selector forwarding switches. The data-plane
// behaviour (frame forwarding, EOS bookkeeping) is the real selectorStage; only
// the switch event is intercepted.
type controlSelector struct {
	*selectorStage
}

func (s *controlSelector) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg != nil && msg.Kind == pipeline.MessageEvent && msg.Event != nil &&
		msg.Event.Type == av.EventStats && msg.Event.Reason == controlSelectorSwitch {
		return s.SetActive(msg.Event.StreamID)
	}
	return s.selectorStage.Handle(ctx, msg, emitter)
}

// selectControl builds the Control that switches a selector node to stream id.
func selectControl(node pipeline.NodeRef, id av.StreamID) Control {
	return Deliver(av.Event{Type: av.EventStats, StreamID: id, Reason: controlSelectorSwitch}).At(node)
}

// controlFrameSource emits frames for two interleaved input streams until its
// context is cancelled, so the selector always has live traffic on both arms while
// a test flips the active input.
type controlFrameSource struct {
	name string
	a    av.StreamID
	b    av.StreamID
}

func (s *controlFrameSource) Name() string { return s.name }

func (s *controlFrameSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		for _, id := range []av.StreamID{s.a, s.b} {
			msg := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame(id, sampleFor(id))}
			if err := emitter.Emit(ctx, msg); err != nil {
				if errors.Is(err, pipeline.ErrBackpressure) {
					continue
				}
				return nil
			}
		}
	}
}

func (s *controlFrameSource) Close() error { return nil }

func sampleFor(id av.StreamID) int16 {
	if id == "a" {
		return 100
	}
	return 200
}

// controlSink records the StreamID-tagged sample of every forwarded frame and lets
// a test wait until a frame from a wanted stream arrives.
type controlSink struct {
	name string
	mu   sync.Mutex
	last int16
	seen map[int16]struct{}
}

func newControlSink(name string) *controlSink {
	return &controlSink{name: name, seen: make(map[int16]struct{})}
}

func (s *controlSink) Name() string { return s.name }

func (s *controlSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil {
		return nil
	}
	got := mixTestReadS16(msg.Frame)
	if len(got) == 0 {
		return nil
	}
	s.mu.Lock()
	s.last = got[0]
	s.seen[got[0]] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *controlSink) Close() error { return nil }

func (s *controlSink) waitFor(ctx context.Context, sample int16) error {
	for {
		s.mu.Lock()
		_, ok := s.seen[sample]
		s.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (s *controlSink) resetSeen() {
	s.mu.Lock()
	s.seen = make(map[int16]struct{})
	s.mu.Unlock()
}

func TestTaskControlSwitchesSelectorMidRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "control",
		Buffer: pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}

	source := &controlFrameSource{name: "source", a: "a", b: "b"}
	selector := &controlSelector{selectorStage: newSelectorStage("select", []av.StreamID{"a", "b"}, "out")}
	sink := newControlSink("sink")

	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	// The control-target node must be on a non-lossy buffer: an injected control
	// shares the node's input queue with the flooding source, so a DropOldest queue
	// can evict the control before the worker applies it. DropBlock backpressures
	// instead, so the switch is delivered reliably.
	if _, err := graph.AddStage(selector, pipeline.BufferPolicy{Capacity: 32, Drop: pipeline.DropBlock}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "source", To: []string{"select"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "select", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })

	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Default active input is "a" (sample 100): it must reach the sink first.
	if err := sink.waitFor(ctx, 100); err != nil {
		t.Fatalf("active 'a' frame never forwarded: %v", err)
	}

	// Switch the live selector to "b" via the control plane; "b" (sample 200) must
	// start arriving. Retry until accepted so the test does not race graph startup.
	sink.resetSeen()
	if err := controlUntilAccepted(ctx, task, selectControl("select", "b")); err != nil {
		t.Fatalf("Control switch to b: %v", err)
	}
	if err := sink.waitFor(ctx, 200); err != nil {
		t.Fatalf("after switch, active 'b' frame never forwarded: %v", err)
	}
	if got := selector.activeID(); got != "b" {
		t.Fatalf("selector active = %q, want b", got)
	}

	// Switch back to "a" to prove the control plane drives the switch both ways.
	sink.resetSeen()
	if err := task.Control(ctx, selectControl("select", "a")); err != nil {
		t.Fatalf("Control switch to a: %v", err)
	}
	if err := sink.waitFor(ctx, 100); err != nil {
		t.Fatalf("after switch back, active 'a' frame never forwarded: %v", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

// controlUntilAccepted retries Control until the graph is running and accepts it.
func controlUntilAccepted(ctx context.Context, task *task, control Control) error {
	for {
		err := task.Control(ctx, control)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrControlNotRunning) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func TestTaskControlRejectsUnknownAndDirectGraph(t *testing.T) {
	ctx := context.Background()

	// Unknown target on a buffered graph: surfaced as pipeline.ErrUnknownNode once
	// the graph is running, ErrControlNotRunning before that. Both are clean errors.
	bufGraph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "control-reject",
		Buffer: pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	bufTask := newTask(bufGraph, nil)
	t.Cleanup(func() { _ = bufTask.Close() })

	// Empty node target is rejected without touching the graph.
	if err := bufTask.Control(ctx, Keyframe("video")); !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("Control empty node err = %v, want ErrUnknownNode", err)
	}

	// A direct (non-buffered) graph has no per-node worker, so Control is unsupported.
	directGraph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "direct"})
	if err != nil {
		t.Fatal(err)
	}
	directTask := newTask(directGraph, nil)
	t.Cleanup(func() { _ = directTask.Close() })
	if err := directTask.Control(ctx, Keyframe("video").At("encode")); !errors.Is(err, ErrControlUnsupported) {
		t.Fatalf("Control on direct graph err = %v, want ErrControlUnsupported", err)
	}
}
