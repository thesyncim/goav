package rtpav

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

func TestVP8DepacketizerSinglePacket(t *testing.T) {
	stream := videoTestStream(av.CodecVP8)
	depacketizer := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	packet := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 90},
		Payload: []byte{0x10, 0x00, 0x11, 0x22},
	}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), packet, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	got := out.Packets[0]
	if got.StreamID != stream.ID || got.CodecEpoch != stream.Epoch || !got.Keyframe {
		t.Fatalf("packet = %+v", got)
	}
	if got.PTS.Value != 90 || !bytes.Equal(got.Payload.Bytes, []byte{0x00, 0x11, 0x22}) {
		t.Fatalf("packet = %+v", got)
	}
}

func TestVP8DepacketizerAssemblesFragments(t *testing.T) {
	stream := videoTestStream(av.CodecVP8)
	depacketizer := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 7},
		Payload: []byte{0x10, 0xaa},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 {
		t.Fatalf("packets = %d, want 0", len(out.Packets))
	}
	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 7},
		Payload: []byte{0x00, 0xbb},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, []byte{0xaa, 0xbb}) {
		t.Fatalf("payload = %v", out.Packets[0].Payload.Bytes)
	}
}

func TestVP9DepacketizerSinglePacket(t *testing.T) {
	stream := videoTestStream(av.CodecVP9)
	depacketizer := NewVP9Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 99},
		Payload: []byte{0x0c, 0x81, 0x82},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	if !out.Packets[0].Keyframe || !bytes.Equal(out.Packets[0].Payload.Bytes, []byte{0x81, 0x82}) {
		t.Fatalf("packet = %+v", out.Packets[0])
	}
}

func TestVP9DepacketizerAssemblesFragments(t *testing.T) {
	stream := videoTestStream(av.CodecVP9)
	depacketizer := NewVP9Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 9},
		Payload: []byte{0x08, 0xaa},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 9},
		Payload: []byte{0x04, 0xbb},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !bytes.Equal(out.Packets[0].Payload.Bytes, []byte{0xaa, 0xbb}) {
		t.Fatalf("packets = %+v", out.Packets)
	}
}

func TestVideoDepacketizerLossRequestsKeyframeAndDropsUntilSync(t *testing.T) {
	stream := videoTestStream(av.CodecVP8)
	depacketizer := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	out := DepacketizeResult{
		Packets: make([]av.Packet, 0, 1),
		Events:  make([]av.Event, 0, 1),
	}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1},
		Payload: []byte{0x10, 0xaa},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if err := depacketizer.HandleEvent(context.Background(), &av.Event{Type: av.EventPacketLoss, StreamID: stream.ID}); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x00, 0xbb},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 || len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("out = %+v", out)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 2},
		Payload: []byte{0x10, 0x01, 0xcc},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 || len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("out = %+v", out)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 3},
		Payload: []byte{0x10, 0x00, 0xcc},
	}, payload, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(out.Packets))
	}
	if !out.Packets[0].LossBefore || !out.Packets[0].Keyframe {
		t.Fatalf("packet = %+v", out.Packets[0])
	}
}

func TestVideoDepacketizerFrameTooLarge(t *testing.T) {
	stream := videoTestStream(av.CodecVP8)
	depacketizer := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(1))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1},
		Payload: []byte{0x10, 0xaa, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestVP8DepacketizerAllocs(t *testing.T) {
	stream := videoTestStream(av.CodecVP8)
	depacketizer := NewVP8Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	packet := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x10, 0x00, 0x11},
	}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	allocs := testing.AllocsPerRun(1000, func() {
		out.Reset()
		if err := depacketizer.PushInto(context.Background(), packet, payload, &out); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestVP9DepacketizerAllocs(t *testing.T) {
	stream := videoTestStream(av.CodecVP9)
	depacketizer := NewVP9Depacketizer(stream, WithMaxVideoFrameSize(16))
	payload := PayloadCodec{ClockRate: 90000}
	packet := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x0c, 0x11},
	}
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	allocs := testing.AllocsPerRun(1000, func() {
		out.Reset()
		if err := depacketizer.PushInto(context.Background(), packet, payload, &out); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func videoTestStream(codec av.CodecID) av.Stream {
	return av.Stream{
		ID:       av.StreamID(codec),
		Epoch:    3,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        codec,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
}
