package codec

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestResultResetReusesStorage(t *testing.T) {
	frames := make([]av.Frame, 2)
	events := make([]av.Event, 1)
	requests := make([]ControlRequest, 1)
	decode := DecodeResult{Frames: frames, Events: events, Requests: requests}

	packets := make([]av.Packet, 2)
	encodeEvents := make([]av.Event, 1)
	encode := EncodeResult{Packets: packets, Events: encodeEvents}

	if allocs := testing.AllocsPerRun(1000, func() {
		decode.Reset()
		decode.Frames = frames
		decode.Events = events
		decode.Requests = requests

		encode.Reset()
		encode.Packets = packets
		encode.Events = encodeEvents
	}); allocs != 0 {
		t.Fatalf("result reset allocs = %v, want 0", allocs)
	}
}
