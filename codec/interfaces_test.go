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

func TestDecodeBoundsWithDefaults(t *testing.T) {
	defaults := DecodeBounds{
		MaxFramesPerInput:   1,
		MaxEventsPerInput:   2,
		MaxRequestsPerInput: 3,
		MaxPayloadBytes:     1200,
		MaxRetainedBytes:    4096,
		MaxWidth:            1280,
		MaxHeight:           720,
	}
	bounds := DecodeBounds{
		MaxFramesPerInput: 4,
		MaxPayloadBytes:   1500,
		MaxWidth:          1920,
	}

	got := bounds.WithDefaults(defaults)
	if got.MaxFramesPerInput != 4 ||
		got.MaxEventsPerInput != 2 ||
		got.MaxRequestsPerInput != 3 ||
		got.MaxPayloadBytes != 1500 ||
		got.MaxRetainedBytes != 4096 ||
		got.MaxWidth != 1920 ||
		got.MaxHeight != 720 {
		t.Fatalf("bounds with defaults = %+v", got)
	}
}

func TestDecodeBoundsWithDefaultsAllocFree(t *testing.T) {
	defaults := DecodeBounds{
		MaxFramesPerInput:   1,
		MaxEventsPerInput:   2,
		MaxRequestsPerInput: 3,
		MaxPayloadBytes:     1200,
		MaxRetainedBytes:    4096,
		MaxWidth:            1280,
		MaxHeight:           720,
	}
	bounds := DecodeBounds{MaxFramesPerInput: 4}

	if allocs := testing.AllocsPerRun(1000, func() {
		_ = bounds.WithDefaults(defaults)
	}); allocs != 0 {
		t.Fatalf("decode bounds defaulting allocs = %v, want 0", allocs)
	}
}
