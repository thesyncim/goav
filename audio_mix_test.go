package goav

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func mixTestS16Frame(id av.StreamID, samples ...int16) *av.Frame {
	b := make([]byte, len(samples)*2)
	for i := range samples {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(samples[i]))
	}
	return &av.Frame{
		StreamID: id,
		Type:     av.MediaAudio,
		Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
	}
}

func mixTestReadS16(frame *av.Frame) []int16 {
	b := frame.Planes[0].Buffer.Bytes
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}

type mixTestEmitter struct {
	frames []*av.Frame
	events []*av.Event
}

func (c *mixTestEmitter) Emit(_ context.Context, m *pipeline.Message) error {
	switch m.Kind {
	case pipeline.MessageFrame:
		c.frames = append(c.frames, m.Frame)
	case pipeline.MessageEvent:
		c.events = append(c.events, m.Event)
	}
	return nil
}

func TestAudioMixStageSumsAlignedS16(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix")
	emit := &mixTestEmitter{}
	ctx := context.Background()

	// One arm ready is not enough — the mix only advances when every arm has a frame.
	if err := mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("a", 100, 200)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 0 {
		t.Fatalf("emitted %d before both inputs ready, want 0", len(emit.frames))
	}
	if err := mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("b", 50, -50)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(emit.frames))
	}
	if got, want := mixTestReadS16(emit.frames[0]), []int16{150, 150}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed=%v, want %v", got, want)
	}
	if emit.frames[0].StreamID != "mix" {
		t.Fatalf("output stream=%s, want mix", emit.frames[0].StreamID)
	}
}

func TestAudioMixStageClampsOverflow(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix")
	emit := &mixTestEmitter{}
	ctx := context.Background()
	_ = mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("a", 30000, -30000)}, emit)
	_ = mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("b", 30000, -30000)}, emit)
	if got, want := mixTestReadS16(emit.frames[0]), []int16{32767, -32768}; !reflect.DeepEqual(got, want) {
		t.Fatalf("clamped=%v, want %v", got, want)
	}
}

func TestAudioMixStageEmitsEOSWhenAllInputsEnd(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix")
	emit := &mixTestEmitter{}
	ctx := context.Background()
	eos := func(id av.StreamID) *pipeline.Message {
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream, StreamID: id}}
	}
	_ = mix.Handle(ctx, eos("a"), emit)
	if len(emit.events) != 0 {
		t.Fatal("emitted EOS before all inputs ended")
	}
	_ = mix.Handle(ctx, eos("b"), emit)
	if len(emit.events) != 1 || emit.events[0].Type != av.EventEndOfStream || emit.events[0].StreamID != "mix" {
		t.Fatalf("events=%+v, want one mix EOS", emit.events)
	}
}

func mixTestAudioSource(id av.StreamID, samples ...int16) InputSpec {
	return Source(string(id),
		FrameShape(av.MediaAudio, ShapeAudio(48000, Mono, av.SampleFormatS16), ShapeStream(id)),
		func(_ context.Context, push SourcePush) error {
			b := make([]byte, len(samples)*2)
			for i := range samples {
				binary.LittleEndian.PutUint16(b[i*2:], uint16(samples[i]))
			}
			frame := &av.Frame{
				StreamID: id, Type: av.MediaAudio,
				Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
			}
			if err := push.Frame(frame); err != nil {
				return err
			}
			return push.EOS()
		})
}

func TestMixRunsTwoAudioSourcesIntoSink(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			got = append(got, mixTestReadS16(m.Frame))
		}
		return nil
	}))

	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], []int16{150, 150}) {
		t.Fatalf("mixed=%v, want [[150 150]]", got)
	}
}

func TestMixDescribeShowsConvergentJoin(t *testing.T) {
	spec, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer spec.Close()
	graph := spec.Describe()
	var into int
	var mixNode string
	for _, n := range graph.Nodes {
		if n.Kind == pipeline.NodeStage {
			mixNode = string(n.Name)
		}
	}
	for _, e := range graph.Edges {
		if string(e.To) == mixNode {
			into++
		}
	}
	if into != 2 {
		t.Fatalf("edges into mix node = %d, want 2 (convergence); spec=%+v", into, graph)
	}
}

func TestMixRequiresTwoArms(t *testing.T) {
	_, err := Mix(From(mixTestAudioSource("a", 1)).Audio()).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errorsAsMix(err, &buildErr) || buildErr.Code != "mix_inputs" {
		t.Fatalf("err = %v, want mix_inputs", err)
	}
}
func errorsAsMix(err error, target **BuildError) bool { return errors.As(err, target) }
