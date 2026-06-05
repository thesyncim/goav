package rtpav

import (
	"testing"

	"github.com/pion/rtp"
)

func TestSequenceDetectorDetectsGap(t *testing.T) {
	var detector SequenceDetector
	missing := make([]uint16, 0, 4)

	first := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	state := detector.Push(&first, missing)
	if state.LossBefore || state.Expected != 11 {
		t.Fatalf("first state = %+v", state)
	}

	next := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 13}}
	state = detector.Push(&next, missing)
	if !state.LossBefore || state.Expected != 14 {
		t.Fatalf("gap state = %+v", state)
	}
	if len(state.Missing) != 2 || state.Missing[0] != 11 || state.Missing[1] != 12 {
		t.Fatalf("missing = %+v", state.Missing)
	}
}

func TestSequenceDetectorAllocs(t *testing.T) {
	var detector SequenceDetector
	missing := make([]uint16, 0, 4)
	first := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 10}}
	next := rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 13}}

	if allocs := testing.AllocsPerRun(1000, func() {
		detector.Reset()
		detector.Push(&first, missing)
		state := detector.Push(&next, missing)
		if len(state.Missing) != 2 {
			t.Fatalf("missing = %+v", state.Missing)
		}
	}); allocs != 0 {
		t.Fatalf("sequence detector allocs = %v, want 0", allocs)
	}
}

func TestStaticPayloadMap(t *testing.T) {
	codecs := []PayloadCodec{{PayloadType: 111, MIMEType: "audio/opus", ClockRate: 48000}}
	payloads := NewStaticPayloadMap(7, codecs)

	codec, ok := payloads.Lookup(111)
	if !ok || codec.MIMEType != "audio/opus" {
		t.Fatalf("payload lookup = %+v %v", codec, ok)
	}
	if payloads.Epoch() != 7 {
		t.Fatalf("epoch = %d, want 7", payloads.Epoch())
	}
}
