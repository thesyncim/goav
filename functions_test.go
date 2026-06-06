package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type funcEmitter struct {
	packets int
	events  int
}

func (e *funcEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch msg.Kind {
	case pipeline.MessagePacket:
		e.packets++
	case pipeline.MessageEvent:
		e.events++
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
	if PacketFunc("packets", nil) != nil {
		t.Fatal("PacketFunc with nil callback should return nil")
	}
	if FrameFunc("frames", nil) != nil {
		t.Fatal("FrameFunc with nil callback should return nil")
	}
	if EventFunc("events", nil) != nil {
		t.Fatal("EventFunc with nil callback should return nil")
	}
	if SinkFunc("sink", nil) != nil {
		t.Fatal("SinkFunc with nil callback should return nil")
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
