package rtpav

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

func TestAV1DepacketizerSinglePacket(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 90},
		Payload: []byte{0x18, 0x30, 0xaa, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	got := out.Packets[0]
	if got.StreamID != stream.ID || got.CodecEpoch != stream.Epoch || !got.Keyframe {
		t.Fatalf("packet = %+v", got)
	}
	if got.PTS.Value != 90 || !bytes.Equal(got.Payload.Bytes, []byte{0x32, 0x02, 0xaa, 0xbb}) {
		t.Fatalf("packet = %+v payload=%v", got, got.Payload.Bytes)
	}
}

func TestAV1DepacketizerMultipleOBUs(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(32))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9},
		Payload: []byte{0x28, 0x02, 0x30, 0xaa, 0x30, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	want := []byte{0x32, 0x01, 0xaa, 0x32, 0x01, 0xbb}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %v, want %v", out.Packets[0].Payload.Bytes, want)
	}
}

func TestAV1DepacketizerRewritesOBUSizeField(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9},
		Payload: []byte{0x18, 0x32, 0x01, 0xaa, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	want := []byte{0x32, 0x02, 0xaa, 0xbb}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %v, want %v", out.Packets[0].Payload.Bytes, want)
	}
}

func TestAV1DepacketizerIgnoresNonFrameOBUs(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9},
		Payload: []byte{0x38, 0x01, 0x10, 0x01, 0x40, 0x30, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	want := []byte{0x32, 0x01, 0xaa}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %v, want %v", out.Packets[0].Payload.Bytes, want)
	}
}

func TestAV1DepacketizerAssemblesFragments(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 7},
		Payload: []byte{0x58, 0x30, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 {
		t.Fatalf("packets = %d, want 0", len(out.Packets))
	}
	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 7},
		Payload: []byte{0x90, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	if !out.Packets[0].Keyframe || !bytes.Equal(out.Packets[0].Payload.Bytes, []byte{0x32, 0x02, 0xaa, 0xbb}) {
		t.Fatalf("packet = %+v payload=%v", out.Packets[0], out.Packets[0].Payload.Bytes)
	}
}

func TestAV1DepacketizerLossRequestsKeyframeAndDropsUntilSync(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{
		Packets: make([]av.Packet, 0, 1),
		Events:  make([]av.Event, 0, 1),
	}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1},
		Payload: []byte{0x58, 0x30, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if err := depacketizer.HandleEvent(context.Background(), &av.Event{Type: av.EventPacketLoss, StreamID: stream.ID}); err != nil {
		t.Fatal(err)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x90, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 || len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("out = %+v", out)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 2},
		Payload: []byte{0x10, 0x30, 0xcc},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 || len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("out = %+v", out)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 3},
		Payload: []byte{0x18, 0x30, 0xdd},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !out.Packets[0].LossBefore || !out.Packets[0].Keyframe {
		t.Fatalf("packets = %+v", out.Packets)
	}
}

func TestAV1DepacketizerCodecChangedAdoptsOldIDReplacementStream(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{
		Packets: make([]av.Packet, 0, 1),
		Events:  make([]av.Event, 0, 1),
	}
	updated := stream
	updated.ID = "video-replaced"
	updated.Epoch = 5

	if err := depacketizer.HandleEvent(context.Background(), &av.Event{
		Type:     av.EventCodecChanged,
		StreamID: stream.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}); err != nil {
		t.Fatal(err)
	}
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 3},
		Payload: []byte{0x18, 0x30, 0xdd},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 ||
		out.Packets[0].StreamID != updated.ID ||
		out.Packets[0].CodecEpoch != updated.Epoch ||
		!out.Packets[0].LossBefore ||
		!out.Packets[0].Discontinuous ||
		!out.Packets[0].Keyframe {
		t.Fatalf("packets = %+v", out.Packets)
	}
}

func TestAV1DepacketizerFrameTooLarge(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(2))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x18, 0x30, 0xaa, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestAV1DepacketizerRejectsInvalidPayload(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x00, 0xff},
	}, PayloadCodec{ClockRate: 90000}, &out)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("err = %v, want ErrInvalidPayload", err)
	}
}

func TestAV1DepacketizerAllocs(t *testing.T) {
	stream := videoTestStream(av.CodecAV1)
	depacketizer := NewAV1Depacketizer(stream, WithMaxVideoFrameSize(16))
	packet := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x18, 0x30, 0xaa, 0xbb},
	}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	allocs := testing.AllocsPerRun(1000, func() {
		out.Reset()
		if err := depacketizer.PushInto(context.Background(), packet, PayloadCodec{ClockRate: 90000}, &out); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}
