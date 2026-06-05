package rtpav

import (
	"context"
	"errors"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

func TestOpusDepacketizerProducesBorrowedPacket(t *testing.T) {
	stream := av.Stream{ID: "audio", Epoch: 3, Codec: av.CodecParameters{ClockRate: 48000}}
	depacketizer := NewOpusDepacketizer(stream)
	payload := []byte{0xf8, 0xff, 0xfe}
	packet := rtp.Packet{
		Header:  rtp.Header{Timestamp: 960},
		Payload: payload,
	}
	result := DepacketizeResult{Packets: make([]av.Packet, 0, 1)}

	if err := depacketizer.PushInto(context.Background(), &packet, PayloadCodec{ClockRate: 48000}, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 {
		t.Fatalf("packets = %d, want 1", len(result.Packets))
	}
	out := result.Packets[0]
	if out.StreamID != "audio" || out.CodecEpoch != 3 {
		t.Fatalf("packet stream/epoch = %+v", out)
	}
	if out.Payload.Ownership != av.BufferBorrowed {
		t.Fatalf("ownership = %q", out.Payload.Ownership)
	}
	if &out.Payload.Bytes[0] != &payload[0] {
		t.Fatal("payload was copied")
	}
	if out.PTS.Value != 960 || out.PTS.Base.Den != 48000 {
		t.Fatalf("pts = %+v", out.PTS)
	}
}

func TestOpusDepacketizerResultCapacity(t *testing.T) {
	depacketizer := NewOpusDepacketizer(av.Stream{ID: "audio"})
	packet := rtp.Packet{Payload: []byte{1}}
	result := DepacketizeResult{}

	if err := depacketizer.PushInto(context.Background(), &packet, PayloadCodec{}, &result); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestOpusDepacketizerAllocs(t *testing.T) {
	ctx := context.Background()
	depacketizer := NewOpusDepacketizer(av.Stream{ID: "audio", Codec: av.CodecParameters{ClockRate: 48000}})
	packet := rtp.Packet{Header: rtp.Header{Timestamp: 960}, Payload: []byte{1, 2, 3}}
	result := DepacketizeResult{Packets: make([]av.Packet, 0, 1), Events: make([]av.Event, 0, 1)}
	payload := PayloadCodec{ClockRate: 48000}

	if allocs := testing.AllocsPerRun(1000, func() {
		result.Reset()
		if err := depacketizer.PushInto(ctx, &packet, payload, &result); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("opus depacketizer allocs = %v, want 0", allocs)
	}
}
