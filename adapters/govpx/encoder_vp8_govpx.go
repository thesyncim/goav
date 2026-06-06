//go:build goav_govpx

package govpx

import (
	"context"
	"errors"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

type VP8EncoderFactory struct{}

func NewVP8EncoderFactory() VP8EncoderFactory {
	return VP8EncoderFactory{}
}

func (VP8EncoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	if config.Parameters.ID != "" && config.Parameters.ID != av.CodecVP8 {
		return nil, codec.ErrUnsupportedFormat
	}
	encoder := &VP8Encoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type VP8Encoder struct {
	encoder         *govpxlib.VP8Encoder
	stream          av.Stream
	width           int
	height          int
	defaultDuration uint64
	closed          bool
}

func (e *VP8Encoder) Descriptor() codec.Descriptor {
	return activeVP8Descriptor(descriptor(av.CodecVP8, "VP8", []string{av.PixelFormatI420}, []string{"video/vp8"}))
}

func (e *VP8Encoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, width, height, err := normalizeVP8EncodeStream(config)
	if err != nil {
		return err
	}
	e.stream = stream
	e.width = width
	e.height = height
	e.defaultDuration = defaultEncodeDuration(config)
	e.closed = false
	return e.resetEncoder(config)
}

func (e *VP8Encoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
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

	src, err := govpxImageFromFrame(frame, e.width, e.height)
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
	result, err := e.encoder.EncodeInto(dst, src, timestampValue(frame.PTS), e.frameDuration(frame), 0)
	if err != nil {
		out.Packets = out.Packets[:index]
		return mapGovpxEncodeError(err)
	}
	if result.Dropped || len(result.Data) == 0 {
		out.Packets = out.Packets[:index]
		return nil
	}

	packet.StreamID = e.stream.ID
	packet.CodecEpoch = e.stream.Epoch
	packet.Payload.Bytes = result.Data
	packet.Payload.Ownership = av.BufferOwned
	packet.PTS = frame.PTS
	packet.Duration = frame.Duration
	packet.Keyframe = result.KeyFrame
	packet.Metadata = frame.Metadata
	return nil
}

func (e *VP8Encoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
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

func (e *VP8Encoder) HandleEvent(ctx context.Context, event *av.Event) error {
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
	case av.EventKeyframeRequired:
		e.encoder.ForceKeyFrame()
	case av.EventCodecChanged, av.EventDiscontinuity:
		e.encoder.Reset()
		e.encoder.ForceKeyFrame()
	}
	return nil
}

func (e *VP8Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	if e.encoder == nil {
		return nil
	}
	if err := e.encoder.Close(); err != nil && !errors.Is(err, govpxlib.ErrClosed) {
		return err
	}
	e.encoder = nil
	return nil
}

func (e *VP8Encoder) resetEncoder(config codec.EncodeConfig) error {
	options := govpxlib.EncoderOptions{
		Width:             e.width,
		Height:            e.height,
		FPS:               encodeFPS(config),
		TargetBitrateKbps: encodeBitrateKbps(config, e.width, e.height),
	}
	encoder, err := govpxlib.NewVP8Encoder(options)
	if err != nil {
		return mapGovpxEncodeError(err)
	}
	e.encoder = encoder
	return nil
}

func (e *VP8Encoder) frameDuration(frame *av.Frame) uint64 {
	if frame.Duration.Value > 0 {
		return uint64(frame.Duration.Value)
	}
	if e.defaultDuration > 0 {
		return e.defaultDuration
	}
	return 1
}

func normalizeVP8EncodeStream(config codec.EncodeConfig) (av.Stream, int, int, error) {
	stream := config.Stream
	parameters := config.Parameters
	if stream.Type == "" {
		stream.Type = av.MediaVideo
	}
	if parameters.Type == "" {
		parameters.Type = av.MediaVideo
	}
	if stream.Codec.ID != "" && stream.Codec.ID != av.CodecVP8 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if parameters.ID != "" && parameters.ID != av.CodecVP8 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	width := pickPositive(parameters.Width, stream.Codec.Width)
	height := pickPositive(parameters.Height, stream.Codec.Height)
	if width <= 0 || height <= 0 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	parameters.ID = av.CodecVP8
	parameters.Type = av.MediaVideo
	parameters.Width = width
	parameters.Height = height
	if parameters.ClockRate == 0 {
		parameters.ClockRate = pickClockRate(stream.Codec.ClockRate, 90000)
	}
	if parameters.PixelFormat == "" {
		parameters.PixelFormat = av.PixelFormatI420
	}
	stream.Type = av.MediaVideo
	stream.Codec = parameters
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(parameters.ClockRate)
	}
	return stream, width, height, nil
}

func govpxImageFromFrame(frame *av.Frame, width, height int) (govpxlib.Image, error) {
	if frame.Type != "" && frame.Type != av.MediaVideo {
		return govpxlib.Image{}, codec.ErrUnsupportedFormat
	}
	if frame.Video == nil {
		return govpxlib.Image{}, codec.ErrUnsupportedFormat
	}
	if frame.Video.Width != width || frame.Video.Height != height {
		return govpxlib.Image{}, codec.ErrUnsupportedFormat
	}
	if frame.Video.PixelFormat != "" &&
		frame.Video.PixelFormat != av.PixelFormatI420 &&
		frame.Video.PixelFormat != av.PixelFormatYUV420P {
		return govpxlib.Image{}, codec.ErrUnsupportedFormat
	}
	if len(frame.Planes) < 3 {
		return govpxlib.Image{}, codec.ErrOutputBufferTooSmall
	}
	return govpxlib.Image{
		Width:   width,
		Height:  height,
		Y:       frame.Planes[0].Buffer.Bytes,
		U:       frame.Planes[1].Buffer.Bytes,
		V:       frame.Planes[2].Buffer.Bytes,
		YStride: frame.Planes[0].Stride,
		UStride: frame.Planes[1].Stride,
		VStride: frame.Planes[2].Stride,
	}, nil
}

func encodeFPS(config codec.EncodeConfig) int {
	if fps := fpsFromDuration(config.Framerate); fps > 0 {
		return fps
	}
	return 30
}

func fpsFromDuration(duration av.Duration) int {
	std, ok := duration.ToDuration()
	if !ok || std <= 0 {
		return 0
	}
	fps := int64(time.Second) / int64(std)
	if fps <= 0 {
		return 0
	}
	if fps > int64(^uint(0)>>1) {
		return 0
	}
	return int(fps)
}

func defaultEncodeDuration(config codec.EncodeConfig) uint64 {
	if config.Framerate.Value > 0 {
		return uint64(config.Framerate.Value)
	}
	return 1
}

func encodeBitrateKbps(config codec.EncodeConfig, width, height int) int {
	if config.Bitrate > 0 {
		if config.Bitrate > 10000 {
			return (config.Bitrate + 999) / 1000
		}
		return config.Bitrate
	}
	pixels := width * height
	if pixels <= 0 {
		return 1200
	}
	kbps := pixels * 30 * 8 / 1000
	if kbps < 300 {
		return 300
	}
	return kbps
}

func pickPositive(a, b int) int {
	if a > 0 {
		return a
	}
	return b
}

func pickClockRate(a, b uint32) uint32 {
	if a != 0 {
		return a
	}
	return b
}

func mapGovpxEncodeError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, govpxlib.ErrClosed):
		return codec.ErrClosed
	case errors.Is(err, govpxlib.ErrBufferTooSmall):
		return codec.ErrOutputBufferTooSmall
	case errors.Is(err, govpxlib.ErrFrameNotReady):
		return nil
	case errors.Is(err, govpxlib.ErrInvalidConfig),
		errors.Is(err, govpxlib.ErrInvalidBitrate),
		errors.Is(err, govpxlib.ErrInvalidQuantizer):
		return codec.ErrUnsupportedFormat
	default:
		return err
	}
}
