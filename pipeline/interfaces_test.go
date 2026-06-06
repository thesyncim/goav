package pipeline

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestMessageAndScratchResetAllocs(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	message := Message{Kind: MessagePacket, Packet: &packet}
	events := make([]av.Event, 2)
	scratch := Scratch{Message: message, Events: events}

	if allocs := testing.AllocsPerRun(1000, func() {
		message.Reset()
		message.Kind = MessagePacket
		message.Packet = &packet

		scratch.Reset()
		scratch.Events = events
	}); allocs != 0 {
		t.Fatalf("message reset allocs = %v, want 0", allocs)
	}
}

func route(from string, to ...string) Route {
	return Route{
		From:   from,
		To:     append([]string(nil), to...),
		Policy: RouteAll,
	}
}

func TestRouteModifiers(t *testing.T) {
	base := route("source", "decode", "stats")
	if base.From != "source" || len(base.To) != 2 || base.To[0] != "decode" || base.To[1] != "stats" ||
		base.Policy != RouteAll {
		t.Fatalf("route = %+v", base)
	}

	stream := route("decode", "record").ByStream("video")
	if stream.Policy != RouteByStream || stream.Label != "video" || len(stream.To) != 1 || stream.To[0] != "record" {
		t.Fatalf("stream route = %+v", stream)
	}

	event := route("source", "feedback").ByEvent(av.EventPacketLoss)
	if event.Policy != RouteByEvent || event.Label != string(av.EventPacketLoss) || len(event.To) != 1 ||
		event.To[0] != "feedback" {
		t.Fatalf("event route = %+v", event)
	}
}
