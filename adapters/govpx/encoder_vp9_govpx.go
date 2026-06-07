package govpx

import (
	"context"
	"errors"
	"image"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

type VP9EncoderFactory struct{}

func NewVP9EncoderFactory() VP9EncoderFactory {
	return VP9EncoderFactory{}
}

func (VP9EncoderFactory) NewEncoder(ctx context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	if config.Parameters.ID != "" && config.Parameters.ID != av.CodecVP9 {
		return nil, codec.ErrUnsupportedFormat
	}
	encoder := &VP9Encoder{}
	if err := encoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return encoder, nil
}

type VP9Encoder struct {
	encoder *govpxlib.VP9Encoder
	config  codec.EncodeConfig
	stream  av.Stream
	width   int
	height  int
	closed  bool
}

func (e *VP9Encoder) Descriptor() codec.Descriptor {
	return activeVP9Descriptor(descriptor(av.CodecVP9, "VP9", []string{av.PixelFormatI420}, []string{"video/vp9"}))
}

func (e *VP9Encoder) Open(ctx context.Context, config codec.EncodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, width, height, err := normalizeVP9EncodeStream(config)
	if err != nil {
		return err
	}
	e.config = config
	e.stream = stream
	e.width = width
	e.height = height
	e.closed = false
	return e.resetEncoder()
}

func (e *VP9Encoder) EncodeInto(ctx context.Context, frame *av.Frame, out *codec.EncodeResult) error {
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

	src, err := govpxYCbCrFromFrame(frame, e.width, e.height)
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
	result, err := e.encoder.EncodeIntoWithResult(&src, dst)
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

func (e *VP9Encoder) FlushInto(ctx context.Context, out *codec.EncodeResult) error {
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

func (e *VP9Encoder) HandleEvent(ctx context.Context, event *av.Event) error {
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
		if err := e.resetEncoder(); err != nil {
			return err
		}
		e.encoder.ForceKeyFrame()
	}
	return nil
}

func (e *VP9Encoder) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	return e.closeEncoder()
}

func (e *VP9Encoder) resetEncoder() error {
	if err := e.closeEncoder(); err != nil {
		return err
	}
	options := govpxlib.VP9EncoderOptions{
		Width:              e.width,
		Height:             e.height,
		FPS:                encodeFPS(e.config),
		TargetBitrateKbps:  encodeBitrateKbps(e.config, e.width, e.height),
		RateControlModeSet: true,
		RateControlMode:    govpxlib.RateControlCBR,
	}
	encoder, err := govpxlib.NewVP9Encoder(options)
	if err != nil {
		return mapGovpxEncodeError(err)
	}
	e.encoder = encoder
	return nil
}

func (e *VP9Encoder) closeEncoder() error {
	if e.encoder == nil {
		return nil
	}
	if err := e.encoder.Close(); err != nil && !errors.Is(err, govpxlib.ErrClosed) {
		return err
	}
	e.encoder = nil
	return nil
}

func normalizeVP9EncodeStream(config codec.EncodeConfig) (av.Stream, int, int, error) {
	stream := config.Stream
	parameters := config.Parameters
	if stream.Type == "" {
		stream.Type = av.MediaVideo
	}
	if parameters.Type == "" {
		parameters.Type = av.MediaVideo
	}
	if stream.Codec.ID != "" && stream.Codec.ID != av.CodecVP9 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	if parameters.ID != "" && parameters.ID != av.CodecVP9 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	width := pickPositive(parameters.Width, stream.Codec.Width)
	height := pickPositive(parameters.Height, stream.Codec.Height)
	if width <= 0 || height <= 0 {
		return av.Stream{}, 0, 0, codec.ErrUnsupportedFormat
	}
	parameters.ID = av.CodecVP9
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

func govpxYCbCrFromFrame(frame *av.Frame, width, height int) (image.YCbCr, error) {
	if frame.Type != "" && frame.Type != av.MediaVideo {
		return image.YCbCr{}, codec.ErrUnsupportedFormat
	}
	if frame.Video == nil {
		return image.YCbCr{}, codec.ErrUnsupportedFormat
	}
	if frame.Video.Width != width || frame.Video.Height != height {
		return image.YCbCr{}, codec.ErrUnsupportedFormat
	}
	if frame.Video.PixelFormat != "" &&
		frame.Video.PixelFormat != av.PixelFormatI420 &&
		frame.Video.PixelFormat != av.PixelFormatYUV420P {
		return image.YCbCr{}, codec.ErrUnsupportedFormat
	}
	if len(frame.Planes) < 3 {
		return image.YCbCr{}, codec.ErrOutputBufferTooSmall
	}
	if frame.Planes[1].Stride != frame.Planes[2].Stride {
		return image.YCbCr{}, codec.ErrUnsupportedFormat
	}
	yStride := frame.Planes[0].Stride
	cStride := frame.Planes[1].Stride
	if yStride <= 0 || cStride <= 0 {
		return image.YCbCr{}, codec.ErrUnsupportedFormat
	}
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	if !planeHasExtent(frame.Planes[0].Buffer.Bytes, yStride, width, height) ||
		!planeHasExtent(frame.Planes[1].Buffer.Bytes, cStride, chromaWidth, chromaHeight) ||
		!planeHasExtent(frame.Planes[2].Buffer.Bytes, cStride, chromaWidth, chromaHeight) {
		return image.YCbCr{}, codec.ErrOutputBufferTooSmall
	}
	return image.YCbCr{
		Y:              frame.Planes[0].Buffer.Bytes,
		Cb:             frame.Planes[1].Buffer.Bytes,
		Cr:             frame.Planes[2].Buffer.Bytes,
		YStride:        yStride,
		CStride:        cStride,
		SubsampleRatio: image.YCbCrSubsampleRatio420,
		Rect:           image.Rect(0, 0, width, height),
	}, nil
}

func planeHasExtent(data []byte, stride, width, height int) bool {
	if width <= 0 || height <= 0 || stride < width {
		return false
	}
	required := (height-1)*stride + width
	return required <= len(data)
}
