package rtpav

import (
	"context"
	"errors"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

func TestSequenceDetectorWraparoundAfterGapAt0xFFFF(t *testing.T) {
	var d SequenceDetector
	missing := make([]uint16, 0, 8)
	d.Push(&rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 0xfffd}}, missing)
	// Gap skips 0xfffe, lands on 0xffff: cycle must increment for the wrap to 0x0000.
	state := d.Push(&rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 0xffff}}, missing)
	if state.Cycles != 1 || state.Expected != 0 || !state.LossBefore {
		t.Fatalf("after 0xffff gap: state = %+v, want Cycles=1 Expected=0 LossBefore=true", state)
	}
	state = d.Push(&rtp.Packet{Header: rtp.Header{SSRC: 1, SequenceNumber: 0x0000}}, missing)
	if state.Cycles != 1 || state.Expected != 1 || state.LossBefore {
		t.Fatalf("after 0x0000: state = %+v, want Cycles=1 Expected=1 LossBefore=false", state)
	}
}

func TestFeedbackResultNoPointerAliasing(t *testing.T) {
	res := FeedbackResult{
		Packets:    make([]rtcp.Packet, 0, 6),
		NACKPairs:  make([]rtcp.NackPair, 0, 4),
		FIREntries: make([]rtcp.FIREntry, 0, 4),
	}
	if err := res.AddNACK(1, 100, []uint16{10}); err != nil {
		t.Fatal(err)
	}
	if err := res.AddNACK(1, 200, []uint16{20}); err != nil {
		t.Fatal(err)
	}
	if err := res.AddPLI(1, 100); err != nil {
		t.Fatal(err)
	}
	if err := res.AddPLI(1, 200); err != nil {
		t.Fatal(err)
	}
	if err := res.AddFIR(1, 100, 1); err != nil {
		t.Fatal(err)
	}
	if err := res.AddFIR(1, 200, 2); err != nil {
		t.Fatal(err)
	}
	if res.Packets[0].(*rtcp.TransportLayerNack).MediaSSRC != 100 || res.Packets[1].(*rtcp.TransportLayerNack).MediaSSRC != 200 {
		t.Fatalf("NACK aliasing: %+v %+v", res.Packets[0], res.Packets[1])
	}
	if res.Packets[2].(*rtcp.PictureLossIndication).MediaSSRC != 100 || res.Packets[3].(*rtcp.PictureLossIndication).MediaSSRC != 200 {
		t.Fatalf("PLI aliasing: %+v %+v", res.Packets[2], res.Packets[3])
	}
	if res.Packets[4].(*rtcp.FullIntraRequest).MediaSSRC != 100 || res.Packets[5].(*rtcp.FullIntraRequest).MediaSSRC != 200 {
		t.Fatalf("FIR aliasing: %+v %+v", res.Packets[4], res.Packets[5])
	}
}

func TestH264DepacketizerPreservesSPSPPSDuringLossRecovery(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, ClockRate: 90000}}
	d := NewH264Depacketizer(stream)
	pc := PayloadCodec{PayloadType: 96, Parameters: av.CodecParameters{ID: av.CodecH264}, ClockRate: 90000}
	out := DepacketizeResult{
		Packets:  make([]av.Packet, 0, 2),
		Events:   make([]av.Event, 0, 2),
		Feedback: make([]rtcp.Packet, 0, 2),
	}

	// Trigger loss so depacketizer enters dropping mode.
	if err := d.HandleEvent(ctx, &av.Event{Type: av.EventPacketLoss, StreamID: "v"}); err != nil {
		t.Fatal(err)
	}

	// STAP-A containing SPS (0x67, 0x42, 0xe0, 0x1f) and PPS (0x68, 0xce, 0x3c, 0x80), Marker=false.
	stapa := []byte{
		24,
		0, 4, 0x67, 0x42, 0xe0, 0x1f,
		0, 4, 0x68, 0xce, 0x3c, 0x80,
	}
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{Header: rtp.Header{Timestamp: 9000, Marker: false}, Payload: stapa}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 {
		t.Fatalf("unexpected packet emitted before IDR: %d", len(out.Packets))
	}

	// Single IDR NALU with same timestamp and Marker=true.
	idr := []byte{0x65, 0x88, 0x84, 0x21}
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{Header: rtp.Header{Timestamp: 9000, Marker: true}, Payload: idr}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !out.Packets[0].Keyframe {
		t.Fatalf("expected 1 keyframe packet, got %d", len(out.Packets))
	}
	want := []byte{
		0, 0, 0, 1, 0x67, 0x42, 0xe0, 0x1f,
		0, 0, 0, 1, 0x68, 0xce, 0x3c, 0x80,
		0, 0, 0, 1, 0x65, 0x88, 0x84, 0x21,
	}
	got := out.Packets[0].Payload.Bytes
	if string(got) != string(want) {
		t.Fatalf("payload = %x, want %x", got, want)
	}
}

func TestAV1DepacketizerRejectsMissingFinalElementWhenWSet(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAV1, ClockRate: 90000}}
	d := NewAV1Depacketizer(stream)
	pc := PayloadCodec{PayloadType: 96, Parameters: av.CodecParameters{ID: av.CodecAV1}, ClockRate: 90000}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}

	// W=2 (0b0010_0000), N=1 (0b0000_1000) -> claims 2 elements, but payload ends right after element 0.
	payload := []byte{0x28, 0x02, 0x0a, 0x00}
	err := d.PushInto(ctx, &rtp.Packet{Header: rtp.Header{Timestamp: 9000, Marker: true}, Payload: payload}, pc, &out)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("err = %v, want ErrInvalidPayload", err)
	}
}

func TestVideoFrameAssemblerSinglePacketFastPathResetsStartedAndChecksMaxSize(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, ClockRate: 90000}}
	d := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(4))
	pc := PayloadCodec{PayloadType: 96, Parameters: av.CodecParameters{ID: av.CodecVP8}, ClockRate: 90000}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 2), Events: make([]av.Event, 0, 2)}

	// Single-packet VP8 keyframe larger than 4 bytes must fail with ErrFrameTooLarge.
	big := []byte{0x10, 0x00, 1, 2, 3, 4} // 1-byte VP8 descriptor (S=1) + 5-byte frame
	err := d.PushInto(ctx, &rtp.Packet{Header: rtp.Header{Timestamp: 1000, Marker: true}, Payload: big}, pc, &out)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized single packet err = %v, want ErrFrameTooLarge", err)
	}

	// Valid 2-byte VP8 keyframe must leave assembler.state == asmIdle after emit.
	small := []byte{0x10, 0x00, 0x01}
	out.Reset()
	if err := d.PushInto(ctx, &rtp.Packet{Header: rtp.Header{Timestamp: 2000, Marker: true}, Payload: small}, pc, &out); err != nil {
		t.Fatal(err)
	}
	if d.assembler.state != asmIdle {
		t.Fatalf("assembler.state = %v after single-packet fast-path emit, want asmIdle", d.assembler.state)
	}
}
