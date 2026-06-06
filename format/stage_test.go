package format

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type demuxStep struct {
	packet bool
	stream av.StreamID
	event  av.EventType
	err    error
}

type fakeDemuxer struct {
	streams []av.Stream
	steps   []demuxStep
	index   int
	closed  bool
}

func (d *fakeDemuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (d *fakeDemuxer) Open(context.Context, Input, OpenOptions) error {
	return nil
}

func (d *fakeDemuxer) Streams() []av.Stream {
	return d.streams
}

func (d *fakeDemuxer) ReadInto(_ context.Context, out *ReadResult) error {
	if d.index >= len(d.steps) {
		return io.EOF
	}
	step := &d.steps[d.index]
	d.index++
	if step.err != nil {
		return step.err
	}
	if step.event != "" {
		if err := out.AddEvent(av.Event{Type: step.event, StreamID: step.stream}); err != nil {
			return err
		}
	}
	if step.packet {
		out.PacketReady = true
		out.Packet.StreamID = step.stream
	}
	return nil
}

func (d *fakeDemuxer) Close() error {
	d.closed = true
	return nil
}

type fakeMuxer struct {
	writes     int
	closed     bool
	withEvent  bool
	lastPacket *av.Packet
}

func (m *fakeMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *fakeMuxer) Open(context.Context, Output, []av.Stream, OpenOptions) error {
	return nil
}

func (m *fakeMuxer) Write(_ context.Context, packet *av.Packet, out *WriteResult) error {
	m.writes++
	m.lastPacket = packet
	if m.withEvent {
		return out.AddEvent(av.Event{Type: av.EventStats, StreamID: packet.StreamID})
	}
	return nil
}

func (m *fakeMuxer) Close() error {
	m.closed = true
	return nil
}

type formatEmitter struct {
	packets    int
	events     int
	frames     int
	lastPacket av.StreamID
	lastEvent  av.EventType
	lastStream av.StreamID
	order      [4]pipeline.MessageKind
	orderLen   int
}

func (e *formatEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		e.packets++
		if msg.Packet != nil {
			e.lastPacket = msg.Packet.StreamID
		}
	case pipeline.MessageEvent:
		e.events++
		if msg.Event != nil {
			e.lastEvent = msg.Event.Type
			e.lastStream = msg.Event.StreamID
		}
	case pipeline.MessageFrame:
		e.frames++
	}
	if e.orderLen < len(e.order) {
		e.order[e.orderLen] = msg.Kind
		e.orderLen++
	}
	return nil
}

func (e *formatEmitter) Reset() {
	e.packets = 0
	e.events = 0
	e.frames = 0
	e.lastPacket = ""
	e.lastEvent = ""
	e.lastStream = ""
	e.order = [4]pipeline.MessageKind{}
	e.orderLen = 0
}

func TestDemuxSourceEmitsStreamsEventsPacketsAndEOS(t *testing.T) {
	demuxer := &fakeDemuxer{
		streams: []av.Stream{{ID: "audio", Type: av.MediaAudio, Epoch: 3}},
		steps: []demuxStep{
			{event: av.EventPacketLoss, stream: "audio"},
			{packet: true, stream: "audio"},
		},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet, Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &formatEmitter{}

	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.packets != 1 || emitter.events != 3 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
	if emitter.order != [4]pipeline.MessageKind{
		pipeline.MessageEvent,
		pipeline.MessageEvent,
		pipeline.MessagePacket,
		pipeline.MessageEvent,
	} {
		t.Fatalf("order = %+v", emitter.order)
	}
	if emitter.lastPacket != "audio" || emitter.lastEvent != av.EventEndOfStream {
		t.Fatalf("packet=%s event=%s", emitter.lastPacket, emitter.lastEvent)
	}
}

func TestDemuxSourceEOSUsesSingleStream(t *testing.T) {
	demuxer := &fakeDemuxer{streams: []av.Stream{{ID: "audio", Epoch: 7}}}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &formatEmitter{}

	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 2 || emitter.lastEvent != av.EventEndOfStream || emitter.lastStream != "audio" {
		t.Fatalf("events=%d event=%s stream=%s", emitter.events, emitter.lastEvent, emitter.lastStream)
	}
}

func TestDemuxSourceEventOnlyReadDoesNotEmitPacket(t *testing.T) {
	demuxer := &fakeDemuxer{
		steps: []demuxStep{{event: av.EventStats, stream: "audio"}},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet, Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &formatEmitter{}

	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.packets != 0 || emitter.events != 2 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
}

func TestDemuxSourceClose(t *testing.T) {
	demuxer := &fakeDemuxer{}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed {
		t.Fatal("demuxer not closed")
	}
	if err := source.Start(context.Background(), &formatEmitter{}); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestNewDemuxSourceValidation(t *testing.T) {
	if _, err := NewDemuxSource(DemuxSourceConfig{}); !errors.Is(err, ErrNilDemuxer) {
		t.Fatalf("err = %v, want ErrNilDemuxer", err)
	}
	if _, err := NewDemuxSource(DemuxSourceConfig{Demuxer: &fakeDemuxer{}}); !errors.Is(err, ErrNilPacket) {
		t.Fatalf("err = %v, want ErrNilPacket", err)
	}
}

func TestDemuxSourceAllocs(t *testing.T) {
	demuxer := &fakeDemuxer{
		steps: []demuxStep{{packet: true, stream: "audio"}},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &formatEmitter{}

	if allocs := testing.AllocsPerRun(1000, func() {
		demuxer.index = 0
		emitter.Reset()
		if err := source.Start(context.Background(), emitter); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("demux source allocs = %v, want 0", allocs)
	}
}

func TestMuxStageWritesPacketAndEmitsEvents(t *testing.T) {
	muxer := &fakeMuxer{withEvent: true}
	stage, err := NewMuxStage(MuxStageConfig{
		Muxer:  muxer,
		Result: WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &formatEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if muxer.writes != 1 || muxer.lastPacket != &packet {
		t.Fatalf("writes=%d packet=%p", muxer.writes, muxer.lastPacket)
	}
	if emitter.events != 1 || emitter.packets != 0 {
		t.Fatalf("packets=%d events=%d", emitter.packets, emitter.events)
	}
}

func TestMuxStageConsumesInputEvents(t *testing.T) {
	stage, err := NewMuxStage(MuxStageConfig{Muxer: &fakeMuxer{}})
	if err != nil {
		t.Fatal(err)
	}
	event := av.Event{Type: av.EventStats}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &formatEmitter{}
	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 0 {
		t.Fatalf("events=%d, want 0", emitter.events)
	}
}

func TestMuxStageClose(t *testing.T) {
	muxer := &fakeMuxer{}
	stage, err := NewMuxStage(MuxStageConfig{Muxer: muxer})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	if !muxer.closed {
		t.Fatal("muxer not closed")
	}
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	if err := stage.Handle(context.Background(), &message, &formatEmitter{}); !errors.Is(err, pipeline.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestNewMuxStageValidation(t *testing.T) {
	if _, err := NewMuxStage(MuxStageConfig{}); !errors.Is(err, ErrNilMuxer) {
		t.Fatalf("err = %v, want ErrNilMuxer", err)
	}
}

func TestMuxStageAllocs(t *testing.T) {
	stage, err := NewMuxStage(MuxStageConfig{Muxer: &fakeMuxer{}})
	if err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &formatEmitter{}

	if allocs := testing.AllocsPerRun(1000, func() {
		emitter.Reset()
		if err := stage.Handle(context.Background(), &message, emitter); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("mux stage allocs = %v, want 0", allocs)
	}
}
