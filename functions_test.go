package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type funcEmitter struct {
	packets   int
	events    int
	lastEvent av.Event
}

func (e *funcEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		e.packets++
	case pipeline.MessageEvent:
		e.events++
		if msg.Event != nil {
			e.lastEvent = *msg.Event
		}
	}
	return nil
}

func TestPacketFuncCanEmit(t *testing.T) {
	stage := PacketFunc("meter", func(_ context.Context, packet *av.Packet, emit Emit) error {
		if packet == nil {
			t.Fatal("nil packet")
		}
		if err := emit.Event(av.Event{Type: av.EventStats, StreamID: packet.StreamID}); err != nil {
			return err
		}
		return emit.Packet(packet)
	})
	packet := av.Packet{StreamID: "audio"}
	message := pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}
	emitter := &funcEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 1 || emitter.packets != 1 {
		t.Fatalf("events=%d packets=%d", emitter.events, emitter.packets)
	}
}

func TestFunctionAdaptersRejectNilCallbacks(t *testing.T) {
	packetStage := PacketFunc("packets", nil)
	if packetStage == nil {
		t.Fatal("PacketFunc with nil callback should return a named invalid stage")
	}
	if packetStage.Name() != "packets" {
		t.Fatalf("PacketFunc invalid name = %q, want packets", packetStage.Name())
	}
	if err := packetStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessagePacket}, &funcEmitter{}); !errors.Is(err, ErrNilStage) {
		t.Fatalf("PacketFunc invalid Handle err = %v, want ErrNilStage", err)
	}

	frameStage := FrameFunc("frames", nil)
	if frameStage == nil {
		t.Fatal("FrameFunc with nil callback should return a named invalid stage")
	}
	if err := frameStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageFrame}, &funcEmitter{}); !errors.Is(err, ErrNilStage) {
		t.Fatalf("FrameFunc invalid Handle err = %v, want ErrNilStage", err)
	}

	eventStage := EventFunc("events", nil)
	if eventStage == nil {
		t.Fatal("EventFunc with nil callback should return a named invalid stage")
	}
	if err := eventStage.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent}, &funcEmitter{}); !errors.Is(err, ErrNilStage) {
		t.Fatalf("EventFunc invalid Handle err = %v, want ErrNilStage", err)
	}

	sink := SinkFunc("sink", nil)
	if sink == nil {
		t.Fatal("SinkFunc with nil callback should return a named invalid sink")
	}
	if sink.Name() != "sink" {
		t.Fatalf("SinkFunc invalid name = %q, want sink", sink.Name())
	}
	if err := sink.Handle(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent}); !errors.Is(err, ErrNilSink) {
		t.Fatalf("SinkFunc invalid Handle err = %v, want ErrNilSink", err)
	}
}

func TestSinkFuncReceivesMessage(t *testing.T) {
	var got Message
	sink := SinkFunc("collect", func(_ context.Context, msg Message) error {
		got = msg
		return nil
	})
	event := av.Event{Type: av.EventStats}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}

	if err := sink.Handle(context.Background(), &message); err != nil {
		t.Fatal(err)
	}
	if got.Kind != pipeline.MessageEvent || got.Event == nil || got.Event.Type != av.EventStats {
		t.Fatalf("got=%+v", got)
	}
}

func TestEventFuncObservesAndPreservesEvents(t *testing.T) {
	observed := 0
	stage := EventFunc("events", func(_ context.Context, event av.Event) error {
		if event.Type == av.EventStats {
			observed++
		}
		return nil
	})
	event := av.Event{Type: av.EventStats}
	message := pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}
	emitter := &funcEmitter{}

	if err := stage.Handle(context.Background(), &message, emitter); err != nil {
		t.Fatal(err)
	}
	if observed != 1 || emitter.events != 1 {
		t.Fatalf("observed=%d emitted=%d", observed, emitter.events)
	}
}

func TestEmitEOSCanEmitStreamEnd(t *testing.T) {
	emitter := &funcEmitter{}
	emit := Emit{ctx: context.Background(), emitter: emitter}

	if err := emit.EOS("audio"); err != nil {
		t.Fatal(err)
	}
	if emitter.events != 1 ||
		emitter.lastEvent.Type != av.EventEndOfStream ||
		emitter.lastEvent.StreamID != "audio" {
		t.Fatalf("events=%d last=%+v, want audio EOS", emitter.events, emitter.lastEvent)
	}
}
