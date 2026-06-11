// Performance pins for the goav-level hot paths the docs claim: SourcePush
// delivery, SinkFunc (collector-free) delivery, and the audio mix step. Each
// pin states exactly what is enforced — zero where the path is allocation-free
// today, an explicit ceiling where it is not — so docs/PERFORMANCE.md cites
// tests instead of intentions and a regression fails loudly.

package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
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

// TestSourcePushDeliveryAllocs pins SourcePush delivery at zero allocations
// per push: SourcePush.Packet and SourcePush.Frame route through Emit's reused
// message and the graph's pass-through without allocating, so a custom source
// pushing media in a loop adds no per-message garbage.
func TestSourcePushDeliveryAllocs(t *testing.T) {
	ctx := context.Background()
	emitter := pinDirectGraph(t, &pinNoopSink{name: "out"})
	push := SourcePush{emit: Emit{ctx: ctx, emitter: emitter}, stream: "s"}
	packet := pinPacket()
	frame := pinFrame()

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Packet(packet); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("SourcePush.Packet allocs = %v, want 0", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Frame(frame); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("SourcePush.Frame allocs = %v, want 0", allocs)
	}
}

// TestSinkFuncDeliveryAllocs pins collector-free sink delivery at zero
// allocations per message: a goav.SinkFunc destination receives each delivered
// message by value with no per-message wrapping.
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
	push := SourcePush{emit: Emit{ctx: ctx, emitter: emitter}, stream: "s"}
	packet := pinPacket()

	if allocs := testing.AllocsPerRun(1000, func() {
		if _, err := push.Packet(packet); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("SinkFunc delivery allocs = %v, want 0", allocs)
	}
	if delivered == 0 {
		t.Fatal("sink saw no messages")
	}
}

// audioMixStepAllocCeiling is the measured allocation count of one audio mix
// step with two arms TODAY: the stage deep-clones each buffered arm frame
// (frame header, audio header, plane slice, payload bytes), builds one
// freshly-allocated mixed output frame, and steps through the join sync
// state's per-step scratch. The mix step is NOT allocation-free; this pin is
// a ceiling so the cost cannot silently grow, not a zero-alloc claim.
// Reducing it (slot reuse in joinSyncState and the mix output) lowers the
// ceiling.
const audioMixStepAllocCeiling = 16

// TestAudioMixStepAllocCeiling pins the per-step allocation cost of the audio
// mix join (two arms, arrival sync) at its current measured value.
func TestAudioMixStepAllocCeiling(t *testing.T) {
	ctx := context.Background()
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncArrival)
	emit := discardPinEmitter{}
	frameA := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("a", 100, 200)}
	frameB := &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("b", 50, -50)}

	allocs := testing.AllocsPerRun(1000, func() {
		// One full step: a frame from each arm, drained by the second Handle.
		if err := mix.Handle(ctx, frameA, emit); err != nil {
			t.Fatal(err)
		}
		if err := mix.Handle(ctx, frameB, emit); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > audioMixStepAllocCeiling {
		t.Fatalf("audio mix step allocs = %v, above the pinned ceiling %d", allocs, audioMixStepAllocCeiling)
	}
}

type discardPinEmitter struct{}

func (discardPinEmitter) Emit(context.Context, *pipeline.Message) error { return nil }
