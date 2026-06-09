package goav

import (
	"context"
	"encoding/binary"
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
