package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/pion/rtp"
	gopusadapter "github.com/thesyncim/goav/adapters/gopus"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
)

func gopusTestStream() av.Stream {
	return av.Stream{
		ID:    "audio",
		Type:  av.MediaAudio,
		Epoch: 1,
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			ClockRate:    48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

func gopusTestDecoder(t *testing.T) (codec.Decoder, codec.DecodeResult) {
	t.Helper()
	ctx := context.Background()
	decoder, err := gopusadapter.NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: gopusTestStream()})
	if err != nil {
		t.Fatal(err)
	}
	frames := make([]av.Frame, 1)
	frames[0].Planes = make([]av.Plane, 1)
	frames[0].Planes[0].Buffer.Bytes = make([]byte, 5760*2)
	result := codec.DecodeResult{Frames: frames}
	result.Reset()
	return decoder, result
}

// TestDecodeRTPDepacketizedOpusIntoPreallocatedFrame proves the rtpav
// depacketizer output feeds the gopus decoder directly: one RTP packet in,
// one decoded PCM frame out, no intermediate copies.
func TestDecodeRTPDepacketizedOpusIntoPreallocatedFrame(t *testing.T) {
	ctx := context.Background()
	decoder, result := gopusTestDecoder(t)
	depacketizer := rtpav.NewOpusDepacketizer(gopusTestStream())
	depacketized := rtpav.DepacketizeResult{Packets: make([]av.Packet, 0, 1)}
	rtpPacket := rtp.Packet{
		Header:  rtp.Header{Timestamp: 960},
		Payload: gopusCELTPacket(),
	}

	if err := depacketizer.PushInto(ctx, &rtpPacket, rtpav.PayloadCodec{ClockRate: 48000}, &depacketized); err != nil {
		t.Fatal(err)
	}
	if err := decoder.DecodeInto(ctx, &depacketized.Packets[0], &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 || result.Frames[0].Audio == nil {
		t.Fatalf("frames = %+v", result.Frames)
	}
	if result.Frames[0].Audio.Samples != 960 {
		t.Fatalf("samples = %d, want 960", result.Frames[0].Audio.Samples)
	}
	if result.Frames[0].PTS.Value != 960 {
		t.Fatalf("pts = %+v", result.Frames[0].PTS)
	}
}

// Example_rtpOpusDecode depacketizes one Opus RTP packet and decodes it
// through the gopus adapter.
func Example_rtpOpusDecode() {
	ctx := context.Background()
	stream := av.Stream{
		ID:    "audio",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, SampleRate: 48000, ClockRate: 48000, Channels: 1},
	}

	depacketizer := rtpav.NewOpusDepacketizer(stream)
	packet := rtp.Packet{
		Header:  rtp.Header{Timestamp: 960},
		Payload: gopusCELTPacket(),
	}
	depacketized := rtpav.DepacketizeResult{Packets: make([]av.Packet, 0, 1)}
	if err := depacketizer.PushInto(ctx, &packet, rtpav.PayloadCodec{ClockRate: 48000}, &depacketized); err != nil {
		fmt.Println(err)
		return
	}

	decoder, err := gopusadapter.NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: stream})
	if err != nil {
		fmt.Println(err)
		return
	}
	frames := make([]av.Frame, 1)
	frames[0].Planes = make([]av.Plane, 1)
	frames[0].Planes[0].Buffer.Bytes = make([]byte, 5760*2)
	decoded := codec.DecodeResult{Frames: frames}
	decoded.Reset()

	if err := decoder.DecodeInto(ctx, &depacketized.Packets[0], &decoded); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("frames=%d samples=%d\n", len(decoded.Frames), decoded.Frames[0].Audio.Samples)
	// Output:
	// frames=1 samples=960
}

// gopusCELTPacket returns a self-delimiting CELT packet the Opus decoder
// accepts.
func gopusCELTPacket() []byte {
	data := make([]byte, 50)
	data[0] = 0xf8
	for i := 1; i < len(data); i++ {
		data[i] = byte(i * 7)
	}
	return data
}
