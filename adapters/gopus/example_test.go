package gopus_test

import (
	"context"
	"fmt"

	"github.com/pion/rtp"
	adapter "github.com/thesyncim/goav/adapters/gopus"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
)

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
		Payload: exampleCELTPacket(),
	}
	depacketized := rtpav.DepacketizeResult{Packets: make([]av.Packet, 0, 1)}
	if err := depacketizer.PushInto(ctx, &packet, rtpav.PayloadCodec{ClockRate: 48000}, &depacketized); err != nil {
		fmt.Println(err)
		return
	}

	decoder, err := adapter.NewDecoderFactory().NewDecoder(ctx, codec.DecodeConfig{Stream: stream})
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

func exampleCELTPacket() []byte {
	data := make([]byte, 50)
	data[0] = 0xf8
	for i := 1; i < len(data); i++ {
		data[i] = byte(i * 7)
	}
	return data
}
