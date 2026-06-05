package codec

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type fakeDecoder struct {
	events     int
	closed     bool
	pendingPLC int
	flushes    int
	request    bool
}

func (d *fakeDecoder) Descriptor() Descriptor {
	return Descriptor{ID: av.CodecOpus}
}

func (d *fakeDecoder) Open(context.Context, DecodeConfig) error {
	return nil
}

func (d *fakeDecoder) DecodeInto(_ context.Context, pkt *av.Packet, out *DecodeResult) error {
	if pkt == nil {
		if d.pendingPLC == 0 {
			return nil
		}
		d.pendingPLC--
	}
	if len(out.Frames) == cap(out.Frames) {
		return ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.Type = av.MediaAudio
	if pkt != nil {
		frame.StreamID = pkt.StreamID
	}
	if d.request {
		if len(out.Requests) == cap(out.Requests) {
			return ErrResultFull
		}
		index := len(out.Requests)
		out.Requests = out.Requests[:index+1]
		request := &out.Requests[index]
		request.Type = ControlRequestKeyframe
		request.StreamID = "video"
		request.Reason = "lost reference"
	}
	return nil
}

func (d *fakeDecoder) FlushInto(_ context.Context, out *DecodeResult) error {
	d.flushes++
	if len(out.Frames) == cap(out.Frames) {
		return ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.Type = av.MediaAudio
	frame.StreamID = "flushed"
	return nil
}

func (d *fakeDecoder) HandleEvent(_ context.Context, event *av.Event) error {
	d.events++
	if event != nil && event.Type == av.EventPacketLoss {
		d.pendingPLC++
	}
	return nil
}

func (d *fakeDecoder) Close() error {
	d.closed = true
	return nil
}

type stageEmitter struct {
	frames     int
	events     int
	lastEvent  av.EventType
	lastStream av.StreamID
	order      [2]pipeline.MessageKind
	orderLen   int
}

func (e *stageEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessageFrame:
		e.frames++
	case pipeline.MessageEvent:
		e.events++
		if msg.Event != nil {
			e.lastEvent = msg.Event.Type
			e.lastStream = msg.Event.StreamID
		}
	}
	if e.orderLen < len(e.order) {
		e.order[e.orderLen] = msg.Kind
		e.orderLen++
	}
	return nil
}

func (e *stageEmitter) Reset() {
	e.frames = 0
	e.events = 0
	e.lastEvent = ""
	e.lastStream = ""
	e.order = [2]pipeline.MessageKind{}
	e.orderLen = 0
}

func TestDecoderStageEmitsFrames(t *testing.T) {
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.frames != 1 || emitter.events != 0 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
}

func TestDecoderStageHandlesEventsAndPLC(t *testing.T) {
	decoder := &fakeDecoder{}
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: decoder,
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventPacketLoss, StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if decoder.events != 1 {
		t.Fatalf("decoder events = %d, want 1", decoder.events)
	}
	if emitter.frames != 1 || emitter.events != 1 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
}

func TestDecoderStageCanDropInputEvents(t *testing.T) {
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder:         &fakeDecoder{},
		Result:          DecodeResult{Frames: make([]av.Frame, 0, 1)},
		DropInputEvents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventPacketLoss, StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.frames != 1 || emitter.events != 0 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
}

func TestDecoderStageFlushesBeforeEOS(t *testing.T) {
	decoder := &fakeDecoder{}
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: decoder,
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventEndOfStream, StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if decoder.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", decoder.flushes)
	}
	if emitter.frames != 1 || emitter.events != 1 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
	if emitter.order != [2]pipeline.MessageKind{pipeline.MessageFrame, pipeline.MessageEvent} {
		t.Fatalf("order = %+v", emitter.order)
	}
}

func TestDecoderStagePassesFramesThrough(t *testing.T) {
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := av.Frame{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.frames != 1 || emitter.events != 0 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
}

func TestDecoderStageEmitsControlRequestsAsEvents(t *testing.T) {
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{request: true},
		Result: DecodeResult{
			Frames:   make([]av.Frame, 0, 1),
			Requests: make([]ControlRequest, 0, 1),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "video"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.frames != 1 || emitter.events != 1 {
		t.Fatalf("frames=%d events=%d", emitter.frames, emitter.events)
	}
	if emitter.lastEvent != av.EventKeyframeRequired || emitter.lastStream != "video" {
		t.Fatalf("event=%s stream=%s", emitter.lastEvent, emitter.lastStream)
	}
}

func TestDecoderStageControlRequestEvent(t *testing.T) {
	request := ControlRequest{Type: ControlRequestKeyframe, StreamID: "video", Reason: "loss"}
	var event av.Event
	if !controlRequestEvent(&request, &event) {
		t.Fatal("request did not convert")
	}
	if event.Type != av.EventKeyframeRequired || event.StreamID != "video" || event.Reason != "loss" {
		t.Fatalf("event = %+v", event)
	}
}

func TestDecoderStageAllocs(t *testing.T) {
	stage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &stageEmitter{}

	if allocs := testing.AllocsPerRun(1000, func() {
		emitter.Reset()
		if err := stage.Handle(context.Background(), &message, emitter); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("decoder stage allocs = %v, want 0", allocs)
	}
}
