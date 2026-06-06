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

func TestConnectRouteHelpers(t *testing.T) {
	route := Connect("source", "decode", "stats")
	if route.From != "source" || len(route.To) != 2 || route.To[0] != "decode" || route.To[1] != "stats" ||
		route.Policy != RouteAll {
		t.Fatalf("route = %+v", route)
	}

	stream := Connect("decode", "record").ByStream("video")
	if stream.Policy != RouteByStream || stream.Label != "video" || len(stream.To) != 1 || stream.To[0] != "record" {
		t.Fatalf("stream route = %+v", stream)
	}

	event := Connect("source", "feedback").ByEvent(av.EventPacketLoss)
	if event.Policy != RouteByEvent || event.Label != string(av.EventPacketLoss) || len(event.To) != 1 ||
		event.To[0] != "feedback" {
		t.Fatalf("event route = %+v", event)
	}
}
