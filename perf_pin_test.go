// Performance pins for the goav-level hot paths the docs claim: SourcePush
// delivery, component.SinkFunc (collector-free) delivery, and the join steps. Each
// pin states exactly what is enforced — zero where the path is allocation-free
// today, an explicit ceiling where it is not — so docs/PERFORMANCE.md cites
// tests instead of intentions and a regression fails loudly.

package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	sourcepkg "github.com/thesyncim/goav/source"
)

// pinCaptureSource captures the graph emitter at Start so the pins can drive
// the data plane directly.
type pinCaptureSource struct {
	name    string
	emitter pipeline.Emitter
}

func (s *pinCaptureSource) Name() string { return s.name }

func (s *pinCaptureSource) Start(_ context.Context, e pipeline.Emitter) error {
	s.emitter = e
	return nil
}

func (s *pinCaptureSource) Close() error { return nil }

type pinNoopSink struct{ name string }

func (s *pinNoopSink) Name() string                                    { return s.name }
func (s *pinNoopSink) Handle(context.Context, *pipeline.Message) error { return nil }
func (s *pinNoopSink) Close() error                                    { return nil }

// pinDirectGraph builds a running direct source→sink graph and returns the
// captured emitter.
func pinDirectGraph(t *testing.T, sink pipeline.Sink) pipeline.Emitter {
	t.Helper()
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "pin"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = graph.Close() })
	source := &pinCaptureSource{name: "src"}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "src", To: []string{sink.Name()}}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Run(context.Background()); err != nil { // captures the emitter
		t.Fatal(err)
	}
	return source.emitter
}

func pinPacket() *av.Packet {
	return &av.Packet{
		StreamID: "s",
		Type:     av.MediaAudio,
		Payload:  av.Buffer{Bytes: make([]byte, 1200), Ownership: av.BufferImmutable},
	}
}

func pinFrame() *av.Frame {
	return &av.Frame{
		StreamID: "s",
		Type:     av.MediaAudio,
		Audio:    &av.AudioFrame{SampleRate: 48_000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 960},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 1920), Ownership: av.BufferImmutable}}},
	}
}

// TestSourcePushDeliveryAllocs pins SourcePush delivery at one allocation per
// push: Emit gives each downstream delivery independent message ownership, so
// buffered or retaining emitters cannot observe later emits mutating earlier
// messages. The ceiling keeps that safety cost from growing.
func TestSourcePushDeliveryAllocs(t *testing.T) {
	ctx := context.Background()
	emitter := pinDirectGraph(t, &pinNoopSink{name: "out"})
	push := sourcepkg.NewPush(ctx, emitter, "s")
	packet := pinPacket()
	frame := pinFrame()

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Packet(packet); err != nil {
			t.Fatal(err)
		}
	}); allocs > 1 {
		t.Fatalf("SourcePush.Packet allocs = %v, want <= 1", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Frame(frame); err != nil {
			t.Fatal(err)
		}
	}); allocs > 1 {
		t.Fatalf("SourcePush.Frame allocs = %v, want <= 1", allocs)
	}
}

// TestSinkFuncDeliveryAllocs pins SourcePush -> component.SinkFunc delivery at
// the same one-allocation ceiling: the sink adapter adds no extra wrapper on
// top of the source emitter's independent per-delivery message.
func TestSinkFuncDeliveryAllocs(t *testing.T) {
	ctx := context.Background()
	delivered := 0
	sink := SinkFunc("out", func(_ context.Context, msg Message) error {
		if msg.Packet != nil || msg.Frame != nil {
			delivered++
		}
		return nil
	})
	emitter := pinDirectGraph(t, sink)
	push := sourcepkg.NewPush(ctx, emitter, "s")
	packet := pinPacket()

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Packet(packet); err != nil {
			t.Fatal(err)
		}
	}); allocs > 1 {
		t.Fatalf("SinkFunc delivery allocs = %v, want <= 1", allocs)
	}
	if delivered == 0 {
		t.Fatal("sink saw no messages")
	}
}

// TestAudioMixStepAllocs pins one audio mix step (two and eight arms, arrival sync) at
// zero allocations after warm-up: arm frames copy into reusable stage-owned
// slots, the join sync state reuses participant scratch, and the mixed output
// rides a borrowed stage-owned buffer that buffered graphs copy before queueing.
func TestAudioMixStepAllocs(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		ids  []av.StreamID
	}{
		{name: "two-arms", ids: []av.StreamID{"a", "b"}},
		{name: "eight-arms", ids: []av.StreamID{"a", "b", "c", "d", "e", "f", "g", "h"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mix := newAudioMixStage("mix", tt.ids, "mix", joinSyncArrival)
			emit := discardPinEmitter{}
			messages := make([]*pipeline.Message, len(tt.ids))
			for i := range tt.ids {
				messages[i] = &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame(tt.ids[i], int16(100+i), int16(200-i))}
			}
			allocs := testing.AllocsPerRun(1000, func() {
				for i := range messages {
					if err := mix.Handle(ctx, messages[i], emit); err != nil {
						t.Fatal(err)
					}
				}
			})
			if allocs != 0 {
				t.Fatalf("audio mix step allocs = %v, want 0", allocs)
			}
		})
	}
}

// TestVideoCompositeStepAllocs pins one video composite step (two I420 arms,
// arrival sync) at zero allocations after warm-up: arm frames copy into
// reusable stage-owned slots, output planes are reused, and the emitted canvas
// is borrowed for the downstream lifetime contract.
func TestVideoCompositeStepAllocs(t *testing.T) {
	ctx := context.Background()
	composite := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncArrival)
	emit := discardPinEmitter{}
	frameA := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("a", 4, 4, 100, 10, 20)}
	frameB := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: compositeTestI420Frame("b", 4, 4, 200, 30, 40)}

	allocs := testing.AllocsPerRun(1000, func() {
		// One full step: a frame from each arm, drained by the second Handle.
		if err := composite.Handle(ctx, frameA, emit); err != nil {
			t.Fatal(err)
		}
		if err := composite.Handle(ctx, frameB, emit); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("video composite step allocs = %v, want 0", allocs)
	}
}

// TestSelectorPassthroughAllocs pins Select's active-arm data path at zero
// allocations: the selector rewrites StreamID on reusable stage-owned
// frame/packet headers instead of allocating a shallow clone for each forward.
func TestSelectorPassthroughAllocs(t *testing.T) {
	ctx := context.Background()
	selectStage := newSelectorStage("select", []av.StreamID{"a", "b"}, "out")
	emit := discardPinEmitter{}
	frame := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: selectorTestFrame("a", 100)}
	packet := &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{
		StreamID: "a",
		Type:     av.MediaAudio,
		Payload:  av.Buffer{Bytes: []byte{1, 2}, Ownership: av.BufferImmutable},
	}}

	if allocs := testing.AllocsPerRun(1000, func() {
		if err := selectStage.Handle(ctx, frame, emit); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("selector frame passthrough allocs = %v, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := selectStage.Handle(ctx, packet, emit); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("selector packet passthrough allocs = %v, want 0", allocs)
	}
}

type discardPinEmitter struct{}

func (discardPinEmitter) Emit(context.Context, *pipeline.Message) error { return nil }
