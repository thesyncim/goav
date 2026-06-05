package rtpav

import (
	"errors"
	"testing"

	"github.com/pion/rtcp"
)

func TestFeedbackResultAddNACK(t *testing.T) {
	result := FeedbackResult{
		Packets:   make([]rtcp.Packet, 0, 1),
		NACKPairs: make([]rtcp.NackPair, 0, 2),
	}

	if err := result.AddNACK(1, 2, []uint16{10, 11, 12, 30}); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(result.Packets))
	}
	nack, ok := result.Packets[0].(*rtcp.TransportLayerNack)
	if !ok {
		t.Fatalf("packet = %T", result.Packets[0])
	}
	if nack.SenderSSRC != 1 || nack.MediaSSRC != 2 {
		t.Fatalf("nack = %+v", nack)
	}
	if len(nack.Nacks) != 2 {
		t.Fatalf("nacks = %+v", nack.Nacks)
	}
	if nack.Nacks[0].PacketID != 10 || uint16(nack.Nacks[0].LostPackets) != 0b11 {
		t.Fatalf("first nack pair = %+v", nack.Nacks[0])
	}
	if nack.Nacks[1].PacketID != 30 || nack.Nacks[1].LostPackets != 0 {
		t.Fatalf("second nack pair = %+v", nack.Nacks[1])
	}
}

func TestFeedbackResultAddPLIAndFIR(t *testing.T) {
	result := FeedbackResult{
		Packets:    make([]rtcp.Packet, 0, 2),
		FIREntries: make([]rtcp.FIREntry, 0, 1),
	}

	if err := result.AddPLI(10, 20); err != nil {
		t.Fatal(err)
	}
	if err := result.AddFIR(10, 20, 7); err != nil {
		t.Fatal(err)
	}
	if _, ok := result.Packets[0].(*rtcp.PictureLossIndication); !ok {
		t.Fatalf("packet[0] = %T", result.Packets[0])
	}
	fir, ok := result.Packets[1].(*rtcp.FullIntraRequest)
	if !ok {
		t.Fatalf("packet[1] = %T", result.Packets[1])
	}
	if len(fir.FIR) != 1 || fir.FIR[0].SSRC != 20 || fir.FIR[0].SequenceNumber != 7 {
		t.Fatalf("fir = %+v", fir)
	}
}

func TestFeedbackResultCapacity(t *testing.T) {
	result := FeedbackResult{
		Packets:   make([]rtcp.Packet, 0, 1),
		NACKPairs: make([]rtcp.NackPair, 0, 1),
	}

	if err := result.AddNACK(1, 2, []uint16{10, 30}); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
	result.Reset()
	if err := result.AddPLI(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := result.AddPLI(1, 2); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestFeedbackResultAllocs(t *testing.T) {
	result := FeedbackResult{
		Packets:    make([]rtcp.Packet, 0, 3),
		NACKPairs:  make([]rtcp.NackPair, 0, 2),
		FIREntries: make([]rtcp.FIREntry, 0, 1),
	}
	missing := []uint16{10, 11, 12, 30}

	if allocs := testing.AllocsPerRun(1000, func() {
		result.Reset()
		if err := result.AddNACK(1, 2, missing); err != nil {
			t.Fatal(err)
		}
		if err := result.AddPLI(1, 2); err != nil {
			t.Fatal(err)
		}
		if err := result.AddFIR(1, 2, 1); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("feedback allocs = %v, want 0", allocs)
	}
}
