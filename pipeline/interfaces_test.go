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
