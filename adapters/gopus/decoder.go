package gopus

import (
	"context"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
	gopuslib "github.com/thesyncim/gopus"
)

const backendName = "gopus"

type DecoderFactory struct{}

func NewDecoderFactory() DecoderFactory {
	return DecoderFactory{}
}

func Descriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:       av.CodecOpus,
		Name:     "Opus",
		Type:     av.MediaAudio,
		Modes:    []codec.Mode{codec.ModeDecode},
		Realtime: true,
		Capabilities: codec.Capabilities{
			CodecID:       av.CodecOpus,
			Type:          av.MediaAudio,
			Decode:        true,
			Realtime:      true,
			SampleFormats: []string{av.SampleFormatS16},
			RTPPayloads:   []string{rtpav.MIMEOpus},
		},
		Backend: codec.Backend{
			Name:    backendName,
			Module:  "github.com/thesyncim/gopus",
			Package: "github.com/thesyncim/goav/adapters/gopus",
			Status:  "active",
		},
	}
}

func Register(registry *codec.SimpleRegistry) {
	registry.RegisterDecoder(Descriptor(), NewDecoderFactory())
}

func (DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	decoder := &Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type Decoder struct {
	decoder    *gopuslib.Decoder
	stream     av.Stream
	sampleRate int
	channels   int
	audio      av.AudioFrame
	pendingPLC int
}

func (d *Decoder) Descriptor() codec.Descriptor {
	return Descriptor()
}

func (d *Decoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream := config.Stream
	sampleRate := stream.Codec.SampleRate
	if sampleRate == 0 {
		sampleRate = int(stream.Codec.ClockRate)
	}
	if sampleRate == 0 {
		sampleRate = 48000
	}
	channels := stream.Codec.Channels
	if channels == 0 {
		channels = 1
	}

	decoder, err := gopuslib.NewDecoder(gopuslib.DefaultDecoderConfig(sampleRate, channels))
	if err != nil {
		return err
	}

	stream.Type = av.MediaAudio
	stream.Codec.ID = av.CodecOpus
	stream.Codec.Type = av.MediaAudio
	stream.Codec.SampleRate = sampleRate
	stream.Codec.ClockRate = uint32(sampleRate)
	stream.Codec.Channels = channels
	stream.Codec.SampleFormat = av.SampleFormatS16
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(sampleRate)}
	}

	d.decoder = decoder
	d.stream = stream
	d.sampleRate = sampleRate
	d.channels = channels
	d.audio = av.AudioFrame{
		SampleRate:    sampleRate,
		Channels:      channels,
		ChannelLayout: stream.Codec.ChannelLayout,
		SampleFormat:  av.SampleFormatS16,
	}
	d.pendingPLC = 0
	return nil
}

func (d *Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	for d.pendingPLC > 0 {
		if err := d.decodeOne(nil, av.Timestamp{}, out); err != nil {
			return err
		}
		d.pendingPLC--
	}
	if pkt == nil {
		return nil
	}
	return d.decodeOne(pkt.Payload.Bytes, pkt.PTS, out)
}

func (d *Decoder) FlushInto(context.Context, *codec.DecodeResult) error {
	return nil
}

func (d *Decoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	switch event.Type {
	case av.EventPacketLoss:
		d.pendingPLC++
	case av.EventCodecChanged, av.EventDiscontinuity:
		if d.decoder != nil {
			d.decoder.Reset()
		}
		d.pendingPLC = 0
	}
	return nil
}

func (d *Decoder) Close() error {
	d.decoder = nil
	return nil
}

func (d *Decoder) decodeOne(data []byte, pts av.Timestamp, out *codec.DecodeResult) error {
	if d.decoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	if cap(frame.Planes) < 1 {
		out.Frames = out.Frames[:index]
		return codec.ErrOutputBufferTooSmall
	}
	frame.Planes = frame.Planes[:1]
	plane := &frame.Planes[0]
	if cap(plane.Buffer.Bytes) < 2 {
		out.Frames = out.Frames[:index]
		return codec.ErrOutputBufferTooSmall
	}

	pcmBytes := plane.Buffer.Bytes[:cap(plane.Buffer.Bytes)]
	if len(pcmBytes)%2 != 0 {
		pcmBytes = pcmBytes[:len(pcmBytes)-1]
	}
	pcm := bytesAsInt16(pcmBytes)
	samples, err := d.decoder.DecodeInt16(data, pcm)
	if err != nil {
		out.Frames = out.Frames[:index]
		return err
	}

	used := samples * d.channels * 2
	plane.Buffer.Bytes = pcmBytes[:used]
	plane.Buffer.Ownership = av.BufferOwned
	plane.Stride = d.channels * 2

	d.audio.Samples = samples
	frame.StreamID = d.stream.ID
	frame.CodecEpoch = d.stream.Epoch
	frame.Type = av.MediaAudio
	frame.PTS = pts
	frame.Duration = av.SamplesDuration(samples, d.sampleRate)
	frame.Audio = &d.audio
	return nil
}

func bytesAsInt16(data []byte) []int16 {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(data))), len(data)/2)
}
