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
	decodeErr  error
	flushErr   error
	eventErr   error
	closeErr   error
}

type fakeEncoder struct {
	events    int
	closed    bool
	flushes   int
	withEvent bool
	encodeErr error
	flushErr  error
	eventErr  error
	closeErr  error
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
	if d.decodeErr != nil {
		return d.decodeErr
	}
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
	if d.flushErr != nil {
		return d.flushErr
	}
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
	if e.encodeErr != nil {
		return e.encodeErr
	}
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
	if e.flushErr != nil {
		return e.flushErr
	}
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
	if d.eventErr != nil {
		return d.eventErr
	}
	if event != nil && event.Type == av.EventPacketLoss {
		d.pendingPLC++
	}
	return nil
}

func (e *fakeEncoder) HandleEvent(context.Context, *av.Event) error {
	e.events++
	if e.eventErr != nil {
		return e.eventErr
	}
	return nil
}

func (d *fakeDecoder) Close() error {
	d.closed = true
	return d.closeErr
}

func (e *fakeEncoder) Close() error {
	e.closed = true
	return e.closeErr
}

type stageEmitter struct {
	packets    int
	frames     int
	events     int
	err        error
	lastEvent  av.EventType
	lastStream av.StreamID
	lastPacket av.StreamID
	lastEpoch  av.Epoch
	order      [2]pipeline.MessageKind
	orderLen   int
}

func (e *stageEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	if e.err != nil {
		return e.err
	}
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
	e.err = nil
	e.lastEvent = ""
	e.lastStream = ""
	e.lastPacket = ""
	e.lastEpoch = 0
	e.order = [2]pipeline.MessageKind{}
	e.orderLen = 0
}

func TestStageConstructorsNamesDescribeAndNil(t *testing.T) {
	if _, err := NewDecoderStage(DecoderStageConfig{}); !errors.Is(err, ErrNilDecoder) {
		t.Fatalf("nil decoder err = %v, want ErrNilDecoder", err)
	}
	if _, err := NewEncoderStage(EncoderStageConfig{}); !errors.Is(err, ErrNilEncoder) {
		t.Fatalf("nil encoder err = %v, want ErrNilEncoder", err)
	}

	decoder := &fakeDecoder{}
	decoderStage, err := NewDecoderStage(DecoderStageConfig{
		Name:    "decode-audio",
		Detail:  "opus -> pcm",
		Decoder: decoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decoderStage.Name() != "decode-audio" {
		t.Fatalf("decoder Name() = %q", decoderStage.Name())
	}
	if got := decoderStage.DescribeNode(); got.Name != "decode-audio" || got.Kind != pipeline.NodeStage || got.Detail != "opus -> pcm" {
		t.Fatalf("decoder DescribeNode() = %+v", got)
	}
	defaultDecoderStage, err := NewDecoderStage(DecoderStageConfig{Decoder: decoder})
	if err != nil {
		t.Fatal(err)
	}
	if defaultDecoderStage.Name() != "decode" {
		t.Fatalf("default decoder name = %q", defaultDecoderStage.Name())
	}

	encoder := &fakeEncoder{}
	encoderStage, err := NewEncoderStage(EncoderStageConfig{
		Name:    "encode-web",
		Detail:  "pcm -> opus",
		Encoder: encoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if encoderStage.Name() != "encode-web" {
		t.Fatalf("encoder Name() = %q", encoderStage.Name())
	}
	if got := encoderStage.DescribeNode(); got.Name != "encode-web" || got.Kind != pipeline.NodeStage || got.Detail != "pcm -> opus" {
		t.Fatalf("encoder DescribeNode() = %+v", got)
	}
	defaultEncoderStage, err := NewEncoderStage(EncoderStageConfig{Encoder: encoder})
	if err != nil {
		t.Fatal(err)
	}
	if defaultEncoderStage.Name() != "encode" {
		t.Fatalf("default encoder name = %q", defaultEncoderStage.Name())
	}
}

func TestDecoderStageNoopContextAndClose(t *testing.T) {
	decoder := &fakeDecoder{}
	stage, err := NewDecoderStage(DecoderStageConfig{Decoder: decoder})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &stageEmitter{}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	if err := stage.Handle(cancelled, &message, emitter); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Handle() = %v, want context.Canceled", err)
	}
	if err := stage.Handle(context.Background(), nil, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket}, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent}, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: "unknown"}, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.packets != 0 || emitter.frames != 0 || emitter.events != 0 {
		t.Fatalf("noop emits packets=%d frames=%d events=%d", emitter.packets, emitter.frames, emitter.events)
	}

	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if !decoder.closed {
		t.Fatal("decoder not closed")
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
	if err := stage.Handle(context.Background(), &message, emitter); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("handle after close = %v, want ErrClosed", err)
	}
}

func TestEncoderStageNoopContextAndClose(t *testing.T) {
	encoder := &fakeEncoder{}
	stage, err := NewEncoderStage(EncoderStageConfig{Encoder: encoder})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &stageEmitter{}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	frame := av.Frame{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}
	if err := stage.Handle(cancelled, &message, emitter); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Handle() = %v, want context.Canceled", err)
	}
	if err := stage.Handle(context.Background(), nil, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame}, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent}, emitter); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(context.Background(), &pipeline.Message{Kind: "unknown"}, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.packets != 0 || emitter.frames != 0 || emitter.events != 0 {
		t.Fatalf("noop emits packets=%d frames=%d events=%d", emitter.packets, emitter.frames, emitter.events)
	}

	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if !encoder.closed {
		t.Fatal("encoder not closed")
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
	if err := stage.Handle(context.Background(), &message, emitter); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("handle after close = %v, want ErrClosed", err)
	}
}

func TestStagePropagatesCodecAndEmitterErrors(t *testing.T) {
	boom := errors.New("boom")
	packet := av.Packet{StreamID: "audio"}
	frame := av.Frame{StreamID: "audio"}
	eos := av.Event{Type: av.EventEndOfStream, StreamID: "audio"}
	stats := av.Event{Type: av.EventStats, StreamID: "audio"}

	decoderStage, err := NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{decodeErr: boom},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}, &stageEmitter{}); !errors.Is(err, boom) {
		t.Fatalf("decoder decode err = %v, want boom", err)
	}

	decoderStage, err = NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{eventErr: boom},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &stats}, &stageEmitter{}); !errors.Is(err, boom) {
		t.Fatalf("decoder event err = %v, want boom", err)
	}

	decoderStage, err = NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{flushErr: boom},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &eos}, &stageEmitter{}); !errors.Is(err, boom) {
		t.Fatalf("decoder flush err = %v, want boom", err)
	}

	decoderStage, err = NewDecoderStage(DecoderStageConfig{
		Decoder: &fakeDecoder{},
		Result:  DecodeResult{Frames: make([]av.Frame, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := decoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}, &stageEmitter{err: boom}); !errors.Is(err, boom) {
		t.Fatalf("decoder emit err = %v, want boom", err)
	}

	encoderStage, err := NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{encodeErr: boom},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}, &stageEmitter{}); !errors.Is(err, boom) {
		t.Fatalf("encoder encode err = %v, want boom", err)
	}

	encoderStage, err = NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{eventErr: boom},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &stats}, &stageEmitter{}); !errors.Is(err, boom) {
		t.Fatalf("encoder event err = %v, want boom", err)
	}

	encoderStage, err = NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{flushErr: boom},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &eos}, &stageEmitter{}); !errors.Is(err, boom) {
		t.Fatalf("encoder flush err = %v, want boom", err)
	}

	encoderStage, err = NewEncoderStage(EncoderStageConfig{
		Encoder: &fakeEncoder{},
		Result:  EncodeResult{Packets: make([]av.Packet, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoderStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame}, &stageEmitter{err: boom}); !errors.Is(err, boom) {
		t.Fatalf("encoder emit err = %v, want boom", err)
	}

	closeDecoder, err := NewDecoderStage(DecoderStageConfig{Decoder: &fakeDecoder{closeErr: boom}})
	if err != nil {
		t.Fatal(err)
	}
	if err := closeDecoder.Close(); !errors.Is(err, boom) {
		t.Fatalf("decoder close err = %v, want boom", err)
	}
	closeEncoder, err := NewEncoderStage(EncoderStageConfig{Encoder: &fakeEncoder{closeErr: boom}})
	if err != nil {
		t.Fatal(err)
	}
	if err := closeEncoder.Close(); !errors.Is(err, boom) {
		t.Fatalf("encoder close err = %v, want boom", err)
	}
}

func TestCodecChangedStreamEdgeCases(t *testing.T) {
	current := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec:    av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo},
		Epoch:    1,
	}
	if _, ok := codecChangedStream(current, nil); ok {
		t.Fatal("nil event matched")
	}
	if _, ok := codecChangedStream(current, &av.Event{Type: av.EventStats}); ok {
		t.Fatal("non-codec event matched")
	}
	if _, ok := codecChangedStream(current, &av.Event{Type: av.EventCodecChanged, StreamID: "other"}); ok {
		t.Fatal("mismatched stream without payload matched")
	}
	if _, ok := codecChangedStream(current, &av.Event{
		Type:     av.EventCodecChanged,
		StreamID: "other",
		Stream:   &av.Stream{ID: "other", Type: av.MediaAudio},
	}); ok {
		t.Fatal("mismatched replacement media matched")
	}

	next, ok := codecChangedStream(current, &av.Event{
		Type:     av.EventCodecChanged,
		StreamID: "replacement",
		Epoch:    7,
		Stream:   &av.Stream{},
	})
	if !ok {
		t.Fatal("replacement stream did not match")
	}
	if next.ID != "replacement" ||
		next.Type != current.Type ||
		next.Codec.ID != current.Codec.ID ||
		next.TimeBase != current.TimeBase ||
		next.Epoch != 7 {
		t.Fatalf("replacement stream = %+v", next)
	}

	codecParameters := av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, ClockRate: 90000}
	next, ok = codecChangedStream(current, &av.Event{
		Type:  av.EventCodecChanged,
		Codec: &codecParameters,
	})
	if !ok {
		t.Fatal("codec-only event did not match")
	}
	if next.Codec.ID != codecParameters.ID || next.Codec.ClockRate != codecParameters.ClockRate || next.Type != av.MediaVideo {
		t.Fatalf("codec-only replacement = %+v", next)
	}
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

func TestEncoderStageConsumesInputEvents(t *testing.T) {
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
	if emitter.packets != 1 || emitter.events != 0 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
	if emitter.order[0] != pipeline.MessagePacket {
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
