package gopus

import (
	"context"
	"errors"
	"unsafe"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	gopuslib "github.com/thesyncim/gopus"
)

type EncoderFactory struct{}

func NewEncoderFactory() EncoderFactory {
	return EncoderFactory{}
}

func (EncoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	encoder := &Encoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type Encoder struct {
	encoder    *gopuslib.Encoder
	stream     av.Stream
	sampleRate int
	channels   int
	closed     bool
}

func (e *Encoder) Descriptor() codec.Descriptor {
	return Descriptor()
}

func (e *Encoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, sampleRate, channels, err := normalizeEncodeStream(config)
	if err != nil {
		return err
	}
	encoder, err := gopuslib.NewEncoder(gopuslib.EncoderConfig{
		SampleRate:  sampleRate,
		Channels:    channels,
		Application: gopuslib.ApplicationAudio,
	})
	if err != nil {
		return mapEncodeError(err)
	}
	if config.Settings.Bitrate > 0 {
		if err := encoder.SetBitrate(config.Settings.Bitrate); err != nil {
			return mapEncodeError(err)
		}
	}
	e.encoder = encoder
	e.stream = stream
	e.sampleRate = sampleRate
	e.channels = channels
	e.closed = false
	return nil
}

func (e *Encoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed {
		return codec.ErrClosed
	}
	if frame == nil {
		return nil
	}
	if e.encoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if len(out.Packets) == cap(out.Packets) {
		return codec.ErrResultFull
	}
	pcm, samples, err := e.pcmFromFrame(frame)
	if err != nil {
		return err
	}
	if samples != e.encoder.FrameSize() {
		if err := e.encoder.SetFrameSize(samples); err != nil {
			return mapEncodeError(err)
		}
	}

	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	if cap(packet.Payload.Bytes) == 0 {
		out.Packets = out.Packets[:index]
		return codec.ErrOutputBufferTooSmall
	}

	dst := packet.Payload.Bytes[:cap(packet.Payload.Bytes)]
	n, err := e.encoder.EncodeInt16(pcm, dst)
	if err != nil {
		out.Packets = out.Packets[:index]
		return mapEncodeError(err)
	}
	if n == 0 {
		out.Packets = out.Packets[:index]
		return nil
	}

	packet.StreamID = e.stream.ID
	packet.CodecEpoch = e.stream.Epoch
	packet.Payload.Bytes = dst[:n]
	packet.Payload.Ownership = av.BufferOwned
	packet.PTS = frame.PTS
	packet.Duration = frame.Duration
	if packet.Duration.Value == 0 {
		packet.Duration = av.SamplesDuration(samples, e.sampleRate)
	}
	packet.Metadata = frame.Metadata
	return nil
}

func (e *Encoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if e.closed {
		return codec.ErrClosed
	}
	return nil
}

func (e *Encoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	if e.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventCodecChanged, av.EventDiscontinuity:
		e.encoder.Reset()
	}
	return nil
}

func (e *Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.encoder = nil
	return nil
}

func (e *Encoder) pcmFromFrame(frame *av.Frame) ([]int16, int, error) {
	if frame.Type != "" && frame.Type != av.MediaAudio {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio == nil || len(frame.Planes) == 0 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio.SampleFormat != "" && frame.Audio.SampleFormat != av.SampleFormatS16 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	channels := frame.Audio.Channels
	if channels == 0 {
		channels = e.channels
	}
	if channels != e.channels {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	data := frame.Planes[0].Buffer.Bytes
	if len(data)%2 != 0 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	pcm := unsafe.Slice((*int16)(unsafe.Pointer(unsafe.SliceData(data))), len(data)/2)
	samples := frame.Audio.Samples
	if samples == 0 {
		if channels <= 0 || len(pcm)%channels != 0 {
			return nil, 0, codec.ErrUnsupportedFormat
		}
		samples = len(pcm) / channels
	}
	if len(pcm) != samples*channels {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	return pcm, samples, nil
}

func normalizeEncodeStream(config codec.EncodeConfig) (av.Stream, int, int, error) {
	stream := config.Stream
	parameters := config.Parameters
	if stream.Type == "" {
		stream.Type = av.MediaAudio
	}
	if parameters.Type == "" {
		parameters.Type = av.MediaAudio
	}
	if stream.Codec.ID != "" && stream.Codec.ID != av.CodecOpus {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if parameters.ID != "" && parameters.ID != av.CodecOpus {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	sampleRate := firstPositive(parameters.SampleRate, int(parameters.ClockRate), stream.Codec.SampleRate, int(stream.Codec.ClockRate), 48000)
	channels := firstPositive(parameters.Channels, stream.Codec.Channels, 2)
	if channels != 1 && channels != 2 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	parameters.ID = av.CodecOpus
	parameters.Type = av.MediaAudio
	parameters.SampleRate = sampleRate
	parameters.ClockRate = uint32(sampleRate)
	parameters.Channels = channels
	if parameters.ChannelLayout == "" {
		if channels == 1 {
			parameters.ChannelLayout = "mono"
		} else {
			parameters.ChannelLayout = "stereo"
		}
	}
	parameters.SampleFormat = av.SampleFormatS16
	stream.Type = av.MediaAudio
	stream.Codec = parameters
	stream.TimeBase = av.TimeBase{Num: 1, Den: int64(sampleRate)}
	return stream, sampleRate, channels, nil
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func mapEncodeError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, gopuslib.ErrBufferTooSmall):
		return codec.ErrOutputBufferTooSmall
	case errors.Is(err, gopuslib.ErrInvalidSampleRate),
		errors.Is(err, gopuslib.ErrInvalidChannels),
		errors.Is(err, gopuslib.ErrInvalidSampleFormat),
		errors.Is(err, gopuslib.ErrInvalidFrameSize),
		errors.Is(err, gopuslib.ErrInvalidBitrate),
		errors.Is(err, gopuslib.ErrInvalidApplication),
		errors.Is(err, gopuslib.ErrInvalidArgument):
		return codec.ErrUnsupportedFormat
	default:
		return err
	}
}
