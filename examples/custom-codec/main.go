package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

const examplePCM av.CodecID = "example/pcm"

func main() {
	decoded, err := runCustomCodec(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("decoded:", decoded)
}

func runCustomCodec(ctx context.Context) ([][]int16, error) {
	rt := customCodecRuntime()
	packets, err := encodeCustomPCM(ctx, rt)
	if err != nil {
		return nil, err
	}
	if len(packets) == 0 {
		return nil, fmt.Errorf("custom codec encode produced no packets")
	}

	decoded := goavtest.NewCollector()
	if err := goav.From(packetSource("encoded", packets)).
		Audio().
		Decode().
		To(decoded.Sink()).
		UseRuntime(rt).
		Run(ctx); err != nil {
		return nil, err
	}
	return decoded.S16(), nil
}

func encodeCustomPCM(ctx context.Context, rt *goav.Runtime) ([]*av.Packet, error) {
	encoded := goavtest.NewCollector()
	if err := goav.From(goavtest.Audio(8000, 1, []int16{5, 6})).
		Audio().
		Encode(codec.Codec(examplePCM, av.MediaAudio, codec.SampleRate(8000), codec.Channels(codec.Mono))).
		To(encoded.Sink()).
		UseRuntime(rt).
		Run(ctx); err != nil {
		return nil, err
	}
	return encoded.Packets(), nil
}

func customCodecRuntime() *goav.Runtime {
	desc := customCodecDescriptor()
	factory := customCodecFactory{}
	return goavtest.Runtime(
		goavruntime.WithEncoder(desc, factory),
		goavruntime.WithDecoder(desc, factory),
	)
}

func customCodecDescriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:   examplePCM,
		Name: "example PCM",
		Type: av.MediaAudio,
		Modes: []codec.Mode{
			codec.ModeEncode,
			codec.ModeDecode,
		},
		Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}},
		Backend: codec.Backend{
			Name:    "example",
			Module:  "github.com/thesyncim/goav/examples/custom-codec",
			Package: "main",
			Status:  "example",
		},
	}
}

type customCodecFactory struct{}

func (customCodecFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	encoder := &pcmEncoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

func (customCodecFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	decoder := &pcmDecoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type pcmEncoder struct {
	stream av.Stream
}

func (e *pcmEncoder) Descriptor() codec.Descriptor { return customCodecDescriptor() }

func (e *pcmEncoder) Open(_ context.Context, config codec.EncodeConfig) error {
	e.stream = config.Stream
	return nil
}

func (e *pcmEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil || len(frame.Planes) == 0 {
		return nil
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	payload := frame.Planes[0].Buffer.Bytes
	packet.Payload.Bytes = append([]byte(nil), payload...)
	packet.Payload.Ownership = av.BufferOwned
	packet.StreamID = frame.StreamID
	packet.Type = av.MediaAudio
	packet.PTS = frame.PTS
	packet.DTS = frame.PTS
	packet.Duration = frame.Duration
	packet.Keyframe = true
	return nil
}

func (e *pcmEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }

func (e *pcmEncoder) HandleEvent(context.Context, *av.Event) error { return nil }

func (e *pcmEncoder) Close() error { return nil }

type pcmDecoder struct {
	stream av.Stream
	audio  av.AudioFrame
}

func (d *pcmDecoder) Descriptor() codec.Descriptor { return customCodecDescriptor() }

func (d *pcmDecoder) Open(_ context.Context, config codec.DecodeConfig) error {
	d.stream = config.Stream
	rate := config.Stream.Codec.SampleRate
	if rate == 0 {
		rate = 8000
	}
	channels := config.Stream.Codec.Channels
	if channels == 0 {
		channels = 1
	}
	d.audio = av.AudioFrame{SampleRate: rate, Channels: channels, SampleFormat: av.SampleFormatS16}
	return nil
}

func (d *pcmDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	payload := packet.Payload.Bytes
	frame.Planes = []av.Plane{{
		Buffer: av.Buffer{Bytes: append([]byte(nil), payload...), Ownership: av.BufferOwned},
		Stride: d.audio.Channels * 2,
	}}
	d.audio.Samples = len(payload) / (d.audio.Channels * 2)
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	frame.PTS = packet.PTS
	frame.Duration = packet.Duration
	frame.Audio = &d.audio
	return nil
}

func (d *pcmDecoder) FlushInto(context.Context, *codec.DecodeResult) error { return nil }

func (d *pcmDecoder) HandleEvent(context.Context, *av.Event) error { return nil }

func (d *pcmDecoder) Close() error { return nil }

func packetSource(name string, packets []*av.Packet) goav.InputSpec {
	return goav.Source(name,
		shape.Packet(av.MediaAudio, examplePCM, shape.Audio(8000, 1, av.SampleFormatS16)),
		func(_ context.Context, push source.Push) error {
			for _, packet := range packets {
				clone := *packet
				clone.StreamID = av.StreamID(name)
				clone.Payload.Bytes = append([]byte(nil), packet.Payload.Bytes...)
				clone.Payload.Ownership = av.BufferImmutable
				if _, err := push.Packet(&clone); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func samplesToBytes(samples ...int16) []byte {
	out := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
	}
	return out
}
