package gopus

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	gopuslib "github.com/thesyncim/gopus"
)

type EncoderFactory struct{}

func NewEncoderFactory() EncoderFactory {
	return EncoderFactory{}
}

type OpusEncoderControl func(*gopuslib.Encoder) error

func (EncoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	if config.Parameters.ID != "" && config.Parameters.ID != av.CodecOpus {
		return nil, codec.ErrUnsupportedFormat
	}
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
	stream, sampleRate, channels, err := normalizeOpusEncodeStream(config)
	if err != nil {
		return err
	}
	encoderConfig, err := opusEncoderConfig(config, sampleRate, channels)
	if err != nil {
		return err
	}
	encoder, err := gopuslib.NewEncoder(encoderConfig)
	if err != nil {
		return err
	}
	if config.Bitrate > 0 {
		if err := encoder.SetBitrate(config.Bitrate); err != nil {
			return err
		}
	}
	if err := applyOpusEncoderControls(encoder, config.Controls); err != nil {
		return err
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
	pcm, samples, err := opusPCMFromFrame(frame, e.sampleRate, e.channels)
	if err != nil {
		return err
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
		return err
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
	if packet.Duration == (av.Duration{}) {
		packet.Duration = av.SamplesDuration(samples, e.sampleRate)
	}
	packet.Keyframe = true
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
		if e.encoder != nil {
			e.encoder.Reset()
		}
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

func normalizeOpusEncodeStream(config codec.EncodeConfig) (av.Stream, int, int, error) {
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
	sampleRate := pickAudioSampleRate(parameters, stream.Codec)
	channels := parameters.Channels
	if channels <= 0 {
		channels = stream.Codec.Channels
	}
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	if channels <= 0 {
		channels = 1
	}
	if parameters.SampleFormat != "" && parameters.SampleFormat != av.SampleFormatS16 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	parameters.ID = av.CodecOpus
	parameters.Type = av.MediaAudio
	parameters.SampleRate = sampleRate
	parameters.ClockRate = uint32(sampleRate)
	parameters.Channels = channels
	parameters.SampleFormat = av.SampleFormatS16
	if parameters.ChannelLayout == "" {
		parameters.ChannelLayout = channelLayout(channels)
	}
	stream.Type = av.MediaAudio
	stream.Codec = parameters
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(sampleRate)}
	}
	return stream, sampleRate, channels, nil
}

func opusEncoderConfig(config codec.EncodeConfig, sampleRate int, channels int) (gopuslib.EncoderConfig, error) {
	if config.Config == nil {
		return gopuslib.EncoderConfig{
			SampleRate:  sampleRate,
			Channels:    channels,
			Application: gopuslib.ApplicationAudio,
		}, nil
	}
	native, ok := config.Config.(gopuslib.EncoderConfig)
	if !ok {
		return gopuslib.EncoderConfig{}, codec.ErrUnsupportedFormat
	}
	native.SampleRate = sampleRate
	native.Channels = channels
	return native, nil
}

func applyOpusEncoderControls(encoder *gopuslib.Encoder, controls []any) error {
	for i := range controls {
		control := controls[i]
		switch c := control.(type) {
		case nil:
		case OpusEncoderControl:
			if err := c(encoder); err != nil {
				return err
			}
		case func(*gopuslib.Encoder) error:
			if err := c(encoder); err != nil {
				return err
			}
		case interface{ ApplyOpusEncoder(*gopuslib.Encoder) error }:
			if err := c.ApplyOpusEncoder(encoder); err != nil {
				return err
			}
		default:
			return codec.ErrUnsupportedFormat
		}
	}
	return nil
}

func opusPCMFromFrame(frame *av.Frame, sampleRate int, channels int) ([]int16, int, error) {
	if frame.Type != "" && frame.Type != av.MediaAudio {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio == nil {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio.SampleRate != 0 && frame.Audio.SampleRate != sampleRate {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio.Channels != 0 && frame.Audio.Channels != channels {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if frame.Audio.SampleFormat != "" && frame.Audio.SampleFormat != av.SampleFormatS16 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	if len(frame.Planes) < 1 {
		return nil, 0, codec.ErrOutputBufferTooSmall
	}
	pcm := bytesAsInt16(frame.Planes[0].Buffer.Bytes)
	if len(pcm) == 0 || channels <= 0 || len(pcm)%channels != 0 {
		return nil, 0, codec.ErrUnsupportedFormat
	}
	return pcm, len(pcm) / channels, nil
}

func pickAudioSampleRate(parameters av.CodecParameters, stream av.CodecParameters) int {
	if parameters.SampleRate > 0 {
		return parameters.SampleRate
	}
	if parameters.ClockRate > 0 {
		return int(parameters.ClockRate)
	}
	if stream.SampleRate > 0 {
		return stream.SampleRate
	}
	if stream.ClockRate > 0 {
		return int(stream.ClockRate)
	}
	return 0
}

func channelLayout(channels int) string {
	switch channels {
	case 1:
		return "mono"
	case 2:
		return "stereo"
	default:
		return ""
	}
}
