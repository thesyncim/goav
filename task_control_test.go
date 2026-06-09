package goav

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

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
	selector := newSelectorStage("select", []av.StreamID{"a", "b"}, "out")
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
	if err := controlUntilAccepted(ctx, task, SelectActive("select", "b")); err != nil {
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
	if err := task.Control(ctx, SelectActive("select", "a")); err != nil {
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

// controlEventCapture is a stage that records every event it sees and forwards
// all traffic, so a test can observe an injected control arriving mid-graph.
type controlEventCapture struct {
	name   string
	mu     sync.Mutex
	events []av.Event
}

func (c *controlEventCapture) Name() string { return c.name }

func (c *controlEventCapture) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if msg != nil && msg.Kind == pipeline.MessageEvent && msg.Event != nil {
		c.mu.Lock()
		c.events = append(c.events, *msg.Event)
		c.mu.Unlock()
	}
	return emitter.Emit(ctx, msg)
}

func (c *controlEventCapture) Close() error { return nil }

func (c *controlEventCapture) waitForEvent(ctx context.Context, eventType av.EventType) (av.Event, error) {
	for {
		c.mu.Lock()
		for _, event := range c.events {
			if event.Type == eventType {
				c.mu.Unlock()
				return event, nil
			}
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return av.Event{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func TestTaskControlKeyframeBroadcastsWithoutTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "control-broadcast",
		Buffer: pipeline.BufferPolicy{Capacity: 8, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &controlFrameSource{name: "source", a: "a", b: "b"}
	capture := &controlEventCapture{name: "first"}
	sink := newControlSink("sink")
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(capture, pipeline.BufferPolicy{Capacity: 32, Drop: pipeline.DropBlock}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "source", To: []string{"first"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "first", To: []string{"sink"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// No At / AtTap: the keyframe request enters at the graph's entry row (the
	// node fed by the source) and rides the data path downstream.
	if err := controlUntilAccepted(ctx, task, Keyframe("a")); err != nil {
		t.Fatalf("untargeted Keyframe: %v", err)
	}
	event, err := capture.waitForEvent(ctx, av.EventKeyframeRequired)
	if err != nil {
		t.Fatalf("keyframe request never reached the entry row: %v", err)
	}
	if event.StreamID != "a" {
		t.Fatalf("keyframe stream = %q, want a", event.StreamID)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}

func TestTaskControlUntargetedDeliverNeedsTarget(t *testing.T) {
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{
		Name:   "control-needs-target",
		Buffer: pipeline.BufferPolicy{Capacity: 1, Drop: pipeline.DropOldest},
	})
	if err != nil {
		t.Fatal(err)
	}
	task := newTask(graph, nil)
	t.Cleanup(func() { _ = task.Close() })
	err = task.Control(context.Background(), Deliver(av.Event{Type: av.EventStats}))
	if !errors.Is(err, pipeline.ErrUnknownNode) {
		t.Fatalf("untargeted Deliver err = %v, want ErrUnknownNode", err)
	}
	if err == nil || !strings.Contains(err.Error(), "needs a target") {
		t.Fatalf("err = %v, want a 'needs a target' explanation", err)
	}
}

// controlLiveAudioSource is a recipe-level frame source that emits audio frames
// until the task stops, so a control can land while the graph is live.
func controlLiveAudioSource(id av.StreamID) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(ctx context.Context, push SourcePush) error {
			payload := []byte{1, 0}
			for {
				if ctx.Err() != nil {
					return nil
				}
				frame := &av.Frame{
					StreamID: id, Type: av.MediaAudio,
					Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 1},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}},
				}
				if err := push.Frame(frame); err != nil {
					if errors.Is(err, ErrBackpressure) {
						continue
					}
					return nil
				}
			}
		})
}

func TestTaskControlAtTapTargetsByTapName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rt := New(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 32, Drop: pipeline.DropBlock}))
	var mu sync.Mutex
	var reasons []string
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageEvent && m.Event != nil && m.Event.Type == av.EventStats {
			mu.Lock()
			reasons = append(reasons, m.Event.Reason)
			mu.Unlock()
		}
		return nil
	}))

	built, err := From(controlLiveAudioSource("live")).
		Audio().
		Tap(FrameTap("obs")).
		To(sink).
		UseRuntime(rt).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := built.(*task)
	t.Cleanup(func() { _ = task.Close() })
	runErr := make(chan error, 1)
	go func() { runErr <- task.Run(ctx) }()

	// Target the control by TAP NAME — grammar vocabulary, no graph node names.
	deliver := Deliver(av.Event{Type: av.EventStats, Reason: "from-tap-control"}).AtTap("obs")
	if err := controlUntilAccepted(ctx, task, deliver); err != nil {
		t.Fatalf("Control AtTap: %v", err)
	}
	for {
		mu.Lock()
		n := len(reasons)
		var hit bool
		for _, r := range reasons {
			if r == "from-tap-control" {
				hit = true
			}
		}
		mu.Unlock()
		if hit {
			break
		}
		_ = n
		select {
		case <-ctx.Done():
			t.Fatalf("event delivered AtTap never reached the sink: %v", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}

	// Unknown tap names fail with a clear, typed error.
	err = task.Control(ctx, Deliver(av.Event{Type: av.EventStats}).AtTap("nope"))
	if !errors.Is(err, pipeline.ErrUnknownNode) || !strings.Contains(err.Error(), `unknown tap "nope"`) {
		t.Fatalf("AtTap(nope) err = %v, want unknown-tap ErrUnknownNode", err)
	}

	cancel()
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v", err)
	}
}
