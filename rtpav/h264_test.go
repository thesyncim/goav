package rtpav

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

func TestH264DepacketizerSingleNALU(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 90},
		Payload: []byte{0x65, 0xaa, 0xbb},
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
	want := []byte{0, 0, 0, 1, 0x65, 0xaa, 0xbb}
	if got.PTS.Value != 90 || !bytes.Equal(got.Payload.Bytes, want) {
		t.Fatalf("packet = %+v payload=%v", got, got.Payload.Bytes)
	}
}

func TestH264DepacketizerSTAPA(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(32))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header: rtp.Header{Marker: true, Timestamp: 9},
		Payload: []byte{
			0x78,
			0x00, 0x02, 0x67, 0x01,
			0x00, 0x02, 0x68, 0x02,
			0x00, 0x02, 0x65, 0x03,
		},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !out.Packets[0].Keyframe {
		t.Fatalf("packets = %+v", out.Packets)
	}
	want := []byte{
		0, 0, 0, 1, 0x67, 0x01,
		0, 0, 0, 1, 0x68, 0x02,
		0, 0, 0, 1, 0x65, 0x03,
	}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %v, want %v", out.Packets[0].Payload.Bytes, want)
	}
}

func TestH264DepacketizerAssemblesFUA(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	for _, packet := range []*rtp.Packet{
		{Header: rtp.Header{Timestamp: 7}, Payload: []byte{0x7c, 0x85, 0xaa}},
		{Header: rtp.Header{Timestamp: 7}, Payload: []byte{0x7c, 0x05, 0xbb}},
		{Header: rtp.Header{Marker: true, Timestamp: 7}, Payload: []byte{0x7c, 0x45, 0xcc}},
	} {
		out.Reset()
		if err := depacketizer.PushInto(context.Background(), packet, PayloadCodec{ClockRate: 90000}, &out); err != nil {
			t.Fatal(err)
		}
	}
	if len(out.Packets) != 1 || !out.Packets[0].Keyframe {
		t.Fatalf("packets = %+v", out.Packets)
	}
	want := []byte{0, 0, 0, 1, 0x65, 0xaa, 0xbb, 0xcc}
	if !bytes.Equal(out.Packets[0].Payload.Bytes, want) {
		t.Fatalf("payload = %v, want %v", out.Packets[0].Payload.Bytes, want)
	}
}

func TestH264DepacketizerLossRequestsKeyframeAndDropsUntilSync(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{
		Packets: make([]av.Packet, 0, 1),
		Events:  make([]av.Event, 0, 1),
	}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1},
		Payload: []byte{0x7c, 0x85, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if err := depacketizer.HandleEvent(context.Background(), &av.Event{Type: av.EventPacketLoss, StreamID: stream.ID}); err != nil {
		t.Fatal(err)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x7c, 0x45, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 || len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("out = %+v", out)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 2},
		Payload: []byte{0x61, 0xcc},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 0 || len(out.Events) != 1 || out.Events[0].Type != av.EventKeyframeRequired {
		t.Fatalf("out = %+v", out)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 3},
		Payload: []byte{0x65, 0xdd},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || !out.Packets[0].LossBefore || !out.Packets[0].Keyframe {
		t.Fatalf("packets = %+v", out.Packets)
	}
}

func TestH264DepacketizerCodecChangedUpdatesEpochAndDropsUntilSync(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
	out := DepacketizeResult{
		Packets: make([]av.Packet, 0, 1),
		Events:  make([]av.Event, 0, 1),
	}

	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1},
		Payload: []byte{0x61, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	updated := stream
	updated.Epoch = 5
	if err := depacketizer.HandleEvent(context.Background(), &av.Event{
		Type:     av.EventCodecChanged,
		StreamID: updated.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}); err != nil {
		t.Fatal(err)
	}

	out.Reset()
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 2},
		Payload: []byte{0x65, 0xbb},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) != 1 || out.Packets[0].CodecEpoch != updated.Epoch ||
		!out.Packets[0].LossBefore || !out.Packets[0].Discontinuous {
		t.Fatalf("packets = %+v", out.Packets)
	}
}

func TestH264DepacketizerFrameTooLarge(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(4))
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x65, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestH264DepacketizerRejectsInvalidPayload(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	out := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	cases := [][]byte{
		{},
		{0x78, 0x00, 0x03, 0x65},
	}
	for _, payload := range cases {
		depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
		out.Reset()
		err := depacketizer.PushInto(context.Background(), &rtp.Packet{
			Header:  rtp.Header{Marker: true, Timestamp: 1},
			Payload: payload,
		}, PayloadCodec{ClockRate: 90000}, &out)
		if !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("payload %v err = %v, want ErrInvalidPayload", payload, err)
		}
	}

	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
	if err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Timestamp: 1},
		Payload: []byte{0x61, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out); err != nil {
		t.Fatal(err)
	}
	err := depacketizer.PushInto(context.Background(), &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x7c, 0x05, 0xaa},
	}, PayloadCodec{ClockRate: 90000}, &out)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("fu continuation err = %v, want ErrInvalidPayload", err)
	}
}

func TestH264DepacketizerAllocs(t *testing.T) {
	stream := videoTestStream(av.CodecH264)
	depacketizer := NewH264Depacketizer(stream, WithMaxVideoFrameSize(16))
	packet := &rtp.Packet{
		Header:  rtp.Header{Marker: true, Timestamp: 1},
		Payload: []byte{0x65, 0xaa},
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
