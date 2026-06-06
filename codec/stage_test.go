package codec

import (
	"context"
	"errors"
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

type fakeEncoder struct {
	events    int
	closed    bool
	flushes   int
	withEvent bool
}

func (d *fakeDecoder) Descriptor() Descriptor {
	return Descriptor{ID: av.CodecOpus}
}

func (e *fakeEncoder) Descriptor() Descriptor {
	return Descriptor{ID: av.CodecOpus}
}

func (d *fakeDecoder) Open(context.Context, DecodeConfig) error {
	return nil
}

func (e *fakeEncoder) Open(context.Context, EncodeConfig) error {
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

func (e *fakeEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *EncodeResult) error {
	if len(out.Packets) == cap(out.Packets) {
		return ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	if frame != nil {
		packet.StreamID = frame.StreamID
		packet.PTS = frame.PTS
		packet.Duration = frame.Duration
	}
	if e.withEvent {
		if len(out.Events) == cap(out.Events) {
			return ErrResultFull
		}
		index := len(out.Events)
		out.Events = out.Events[:index+1]
		event := &out.Events[index]
		event.Type = av.EventStats
		event.StreamID = packet.StreamID
		event.Reason = "encoded"
	}
	return nil
}

func (e *fakeEncoder) FlushInto(_ context.Context, out *EncodeResult) error {
	e.flushes++
	if len(out.Packets) == cap(out.Packets) {
		return ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	packet.StreamID = "flushed"
	return nil
}

func (d *fakeDecoder) HandleEvent(_ context.Context, event *av.Event) error {
	d.events++
	if event != nil && event.Type == av.EventPacketLoss {
		d.pendingPLC++
	}
	return nil
}

func (e *fakeEncoder) HandleEvent(context.Context, *av.Event) error {
	e.events++
	return nil
}

func (d *fakeDecoder) Close() error {
	d.closed = true
	return nil
}

func (e *fakeEncoder) Close() error {
	e.closed = true
	return nil
}

type stageEmitter struct {
	packets    int
	frames     int
	events     int
	lastEvent  av.EventType
	lastStream av.StreamID
	lastPacket av.StreamID
	lastEpoch  av.Epoch
	order      [2]pipeline.MessageKind
	orderLen   int
}

func (e *stageEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		e.packets++
		if msg.Packet != nil {
			e.lastPacket = msg.Packet.StreamID
			e.lastEpoch = msg.Packet.CodecEpoch
		}
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
	e.packets = 0
	e.frames = 0
	e.events = 0
	e.lastEvent = ""
	e.lastStream = ""
	e.lastPacket = ""
	e.lastEpoch = 0
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

func TestDecoderStageTracksSameCodecReplacementStream(t *testing.T) {
	decoder := &fakeDecoder{}
	stream := av.Stream{
		ID:    "video-main",
		Type:  av.MediaVideo,
		Epoch: 1,
		Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo},
	}
	updated := stream
	updated.ID = "video-replaced"
	updated.Epoch = 2
	stage, err := NewDecoderStage(DecoderStageConfig{
		InputStream: stream,
		Decoder:     decoder,
		Result:      DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{
		Type:     av.EventCodecChanged,
		StreamID: updated.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}

	if err := stage.Handle(context.Background(), &message, &stageEmitter{}); err != nil {
		t.Fatal(err)
	}
	if decoder.events != 1 || stage.inputStream.ID != updated.ID || stage.inputStream.Epoch != updated.Epoch {
		t.Fatalf("events=%d input=%+v", decoder.events, stage.inputStream)
	}
}

func TestDecoderStageRejectsDifferentCodecReplacementStream(t *testing.T) {
	decoder := &fakeDecoder{}
	stream := av.Stream{
		ID:    "video",
		Type:  av.MediaVideo,
		Epoch: 1,
		Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo},
	}
	updated := stream
	updated.Epoch = 2
	updated.Codec = av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}
	stage, err := NewDecoderStage(DecoderStageConfig{
		InputStream: stream,
		Decoder:     decoder,
		Result:      DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{
		Type:     av.EventCodecChanged,
		StreamID: stream.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}

	err = stage.Handle(context.Background(), &message, &stageEmitter{})
	if !errors.Is(err, ErrUnsupportedCodecSwitch) {
		t.Fatalf("err = %v, want ErrUnsupportedCodecSwitch", err)
	}
	if decoder.events != 0 {
		t.Fatalf("decoder events = %d, want 0", decoder.events)
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

func TestEncoderStageEmitsPackets(t *testing.T) {
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
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
	if emitter.packets != 1 || emitter.events != 0 || emitter.frames != 0 {
		t.Fatalf("packets=%d frames=%d events=%d", emitter.packets, emitter.frames, emitter.events)
	}
	if emitter.lastPacket != "audio" {
		t.Fatalf("last packet stream = %s", emitter.lastPacket)
	}
}

func TestEncoderStageCanStampOutputStream(t *testing.T) {
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder:           &fakeEncoder{},
		Result:            EncodeResult{Packets: make([]av.Packet, 0, 1)},
		OutputStreamID:    "audio-low",
		OutputCodecEpoch:  3,
		StampOutputStream: true,
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
	if emitter.lastPacket != "audio-low" || emitter.lastEpoch != 3 {
		t.Fatalf("last packet stream=%s epoch=%d", emitter.lastPacket, emitter.lastEpoch)
	}
}

func TestEncoderStageEmitsResultEventsBeforePackets(t *testing.T) {
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{withEvent: true},
		Result: EncodeResult{
			Packets: make([]av.Packet, 0, 1),
			Events:  make([]av.Event, 0, 1),
		},
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
	if emitter.packets != 1 || emitter.events != 1 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
	if emitter.order != [2]pipeline.MessageKind{pipeline.MessageEvent, pipeline.MessagePacket} {
		t.Fatalf("order = %+v", emitter.order)
	}
}

func TestEncoderStageHandlesEvents(t *testing.T) {
	encoder := &fakeEncoder{}
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: encoder,
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventKeyframeRequired, StreamID: "video"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if encoder.events != 1 {
		t.Fatalf("encoder events = %d, want 1", encoder.events)
	}
	if emitter.events != 1 || emitter.packets != 0 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
}

func TestEncoderStageCanDropInputEvents(t *testing.T) {
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder:         &fakeEncoder{},
		Result:          EncodeResult{Packets: make([]av.Packet, 0, 1)},
		DropInputEvents: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventKeyframeRequired, StreamID: "video"}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &stageEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 0 || emitter.packets != 0 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
}

func TestEncoderStageFlushesBeforeEOS(t *testing.T) {
	encoder := &fakeEncoder{}
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: encoder,
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
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
	if encoder.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", encoder.flushes)
	}
	if emitter.packets != 1 || emitter.events != 1 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
	if emitter.order != [2]pipeline.MessageKind{pipeline.MessagePacket, pipeline.MessageEvent} {
		t.Fatalf("order = %+v", emitter.order)
	}
}

func TestEncoderStagePassesPacketsThrough(t *testing.T) {
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
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
	if emitter.packets != 1 || emitter.frames != 0 || emitter.events != 0 {
		t.Fatalf("packets=%d frames=%d events=%d", emitter.packets, emitter.frames, emitter.events)
	}
}

func TestEncoderStageClose(t *testing.T) {
	encoder := &fakeEncoder{}
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: encoder,
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if !encoder.closed {
		t.Fatal("encoder not closed")
	}
	frame := av.Frame{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	if err := stage.Handle(context.Background(), &message, &stageEmitter{}); err != pipeline.ErrClosed {
		t.Fatalf("handle after close = %v, want %v", err, pipeline.ErrClosed)
	}
}

func TestEncoderStageAllocs(t *testing.T) {
	stage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := av.Frame{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	emitter := &stageEmitter{}

	if allocs := testing.AllocsPerRun(1000, func() {
		emitter.Reset()
		if err := stage.Handle(context.Background(), &message, emitter); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("encoder stage allocs = %v, want 0", allocs)
	}
}
