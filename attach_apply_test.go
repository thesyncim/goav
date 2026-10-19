package goav

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// cancelOnCloseSink cancels a context when the graph closes it.
type cancelOnCloseSink struct {
	name   string
	cancel context.CancelFunc
}

func (s *cancelOnCloseSink) Name() string                           { return s.name }
func (s *cancelOnCloseSink) Handle(context.Context, *Message) error { return nil }
func (s *cancelOnCloseSink) Close() error {
	s.cancel()
	return nil
}

// TestDetachReturnsWhenCanceledWhileStoppingChildren pins the progress rule
// of a recursive detach: a parent with two dependent children, its context
// canceled while the first child closes, returns the cancellation instead of
// reselecting the child it can no longer stop, and releases the task.
func TestDetachReturnsWhenCanceledWhileStoppingChildren(t *testing.T) {
	task, err := mustNew().Build(context.Background(), mixTestAudioSource("src", 1).Audio().Tap("base").To(Sink(SinkFunc("main", func(context.Context, Message) error { return nil }))))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	parent, err := task.Attach(context.Background(), task.Tap("base").Do(FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit Emit) error { return emit.Frame(frame) })).Tap("parent.frames").As("parent").To(Sink(SinkFunc("parent", func(context.Context, Message) error { return nil }))))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, name := range []string{"child-a", "child-b"} {
		if _, err := task.Attach(context.Background(), task.Tap("parent.frames").As(name).To(Sink(&cancelOnCloseSink{name: name, cancel: cancel}))); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- task.Detach(ctx, parent) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Detach = nil, want the cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Detach did not return after its context was canceled")
	}
	// The task is released: a later detach with a live context finishes the job.
	if err := task.Detach(context.Background(), parent); err != nil {
		t.Fatalf("second Detach: %v", err)
	}
	if got := len(task.Snapshot().Branches); got != 0 {
		t.Fatalf("branches after detach = %d, want 0", got)
	}
}

// ingressHookGraph runs hook after each route it connects.
type ingressHookGraph struct {
	pipeline.Graph
	hook func(pipeline.Route)
}

func (g *ingressHookGraph) Connect(route pipeline.Route) error {
	if err := g.Graph.Connect(route); err != nil {
		return err
	}
	if g.hook != nil {
		g.hook(route)
	}
	return nil
}

// TestRebranchAtKeyframeDeliversTheSwitchingKeyframe pins that a
// replacement is whole before its anchor reaches it: the keyframe that opens
// a SwitchAt(NextKeyframe) gate is the replacement's first packet, even when
// it arrives the moment the anchor's edge connects.
func TestRebranchAtKeyframeDeliversTheSwitchingKeyframe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	packets := make(chan av.Packet)
	pushed := make(chan struct{})
	input := Source("cam", shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(sctx context.Context, push SourcePush) error {
			for {
				select {
				case <-sctx.Done():
					return nil
				case packet, ok := <-packets:
					if !ok {
						return push.EOS()
					}
					if _, err := push.Packet(&packet); err != nil {
						return err
					}
					pushed <- struct{}{}
				}
			}
		})
	var mu sync.Mutex
	var got []byte
	replacement := Sink(SinkFunc("b-sink", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessagePacket && msg.Packet != nil {
			mu.Lock()
			got = append(got, msg.Packet.Payload.Bytes[0])
			mu.Unlock()
		}
		return nil
	}))
	ignore := func(name string) Destination {
		return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
	}
	live, err := mustNew().Build(ctx, input.Audio().Tap("pkts").To(ignore("main")))
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	built := live.(*task)
	hooked := &ingressHookGraph{Graph: built.graph}
	built.graph = hooked
	runErr := make(chan error, 1)
	go func() { runErr <- live.Run(ctx) }()
	send := func(id byte, key bool) {
		t.Helper()
		select {
		case packets <- av.Packet{StreamID: "cam", Type: av.MediaAudio, Keyframe: key, Payload: av.Buffer{Bytes: []byte{id}}}:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		<-pushed
	}
	send(1, false)
	old, err := live.Attach(ctx, live.Tap("pkts").As("a").To(ignore("a-sink")))
	if err != nil {
		t.Fatal(err)
	}
	send(2, false)
	anchor := ""
	for _, tap := range live.Taps() {
		if tap.Name == "pkts" {
			anchor = tap.Node.String()
		}
	}
	// The switching keyframe arrives the moment the anchor first reaches the
	// replacement.
	hooked.hook = func(route pipeline.Route) {
		if route.From == anchor && slices.Contains(route.To, "b/switch-gate") {
			hooked.hook = nil
			send(3, true)
		}
	}
	if _, err := old.Rebranch(ctx, live.Tap("pkts").As("b").To(replacement), SwitchAt(NextKeyframe())); err != nil {
		t.Fatal(err)
	}
	send(4, false)
	close(packets)
	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 || got[0] != 3 {
		t.Fatalf("replacement packets = %v, want the switching keyframe 3 first", got)
	}
}
