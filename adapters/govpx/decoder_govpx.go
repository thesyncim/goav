package govpx

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

type DecoderFactory struct{}

func NewDecoderFactory() DecoderFactory {
	return DecoderFactory{}
}

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	descriptors := Descriptors()
	for i := range descriptors {
		switch descriptors[i].ID {
		case av.CodecVP8:
			desc := activeVP8Descriptor(descriptors[i])
			registry.RegisterDecoder(desc, NewDecoderFactory())
			registry.RegisterEncoder(desc, NewVP8EncoderFactory())
		case av.CodecVP9:
			desc := activeVP9Descriptor(descriptors[i])
			registry.RegisterDecoder(desc, NewVP9DecoderFactory())
			registry.RegisterEncoder(desc, NewVP9EncoderFactory())
		default:
			registry.RegisterDescriptor(descriptors[i])
		}
	}
}

func activeVP8Descriptor(desc codec.Descriptor) codec.Descriptor {
	desc.Backend.Status = "active"
	return desc
}

func activeVP9Descriptor(desc codec.Descriptor) codec.Descriptor {
	desc.Backend.Status = "active"
	return desc
}

func (DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	if config.Stream.Codec.ID != "" && config.Stream.Codec.ID != av.CodecVP8 {
		return nil, codec.ErrUnsupportedFormat
	}
	decoder := &Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type Decoder struct {
	decoder          *govpxlib.VP8Decoder
	stream           av.Stream
	video            av.VideoFrame
	requestKeyframes bool
	dropDamagedVideo bool
	dropUntilSync    bool
	closed           bool
}

func (d *Decoder) Descriptor() codec.Descriptor {
	return activeVP8Descriptor(descriptor(av.CodecVP8, "VP8", []string{av.PixelFormatI420}, []string{"video/vp8"}))
}

func (d *Decoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.stream = normalizeStream(config.Stream)
	d.video = av.VideoFrame{
		Width:       d.stream.Codec.Width,
		Height:      d.stream.Codec.Height,
		PixelFormat: d.stream.Codec.PixelFormat,
	}
	d.requestKeyframes = config.Resilience.RequestKeyframes
	d.dropDamagedVideo = config.Resilience.DropDamagedVideo
	d.dropUntilSync = true
	d.closed = false
	return d.resetDecoder(config.Resilience.AcceptLoss)
}

func (d *Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	if d.decoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		d.dropUntilSync = true
		return d.appendKeyframeRequest(out, "vp8 packet loss")
	}
	requestedKeyframe := false
	if pkt.LossBefore || pkt.Discontinuous {
		d.dropUntilSync = true
		if err := d.appendKeyframeRequest(out, "vp8 discontinuity"); err != nil {
			return err
		}
		requestedKeyframe = true
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		d.dropUntilSync = true
		return d.appendKeyframeRequest(out, "vp8 corrupt packet")
	}

	streamInfo, err := govpxlib.PeekVP8StreamInfo(pkt.Payload.Bytes)
	if err != nil {
		return mapGovpxError(err)
	}
	if d.dropUntilSync && !streamInfo.KeyFrame {
		if requestedKeyframe {
			return nil
		}
		return d.appendKeyframeRequest(out, "vp8 waiting for keyframe")
	}
	if streamInfo.KeyFrame {
		d.dropUntilSync = false
	}

	return d.decodeVisiblePacket(pkt, streamInfo, out)
}

func (d *Decoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	return nil
}

func (d *Decoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	if d.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventPacketLoss:
		d.dropUntilSync = true
	case av.EventCodecChanged, av.EventDiscontinuity:
		d.applyEventStream(event)
		d.dropUntilSync = true
		d.decoder.Reset()
	}
	return nil
}

func (d *Decoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	if d.decoder == nil {
		return nil
	}
	if err := d.decoder.Close(); err != nil && !errors.Is(err, govpxlib.ErrClosed) {
		return err
	}
	d.decoder = nil
	return nil
}

func (d *Decoder) resetDecoder(acceptLoss bool) error {
	decoder, err := govpxlib.NewVP8Decoder(govpxlib.DecoderOptions{
		ErrorConcealment: acceptLoss,
	})
	if err != nil {
		return mapGovpxError(err)
	}
	d.decoder = decoder
	return nil
}

func (d *Decoder) decodeVisiblePacket(pkt *av.Packet, streamInfo govpxlib.StreamInfo, out *codec.DecodeResult) error {
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()

	width, height := d.outputDimensions(streamInfo)
	img, err := prepareGovpxImageFrame(frame, width, height)
	if err != nil {
		out.Frames = out.Frames[:index]
		return err
	}

	frameInfo, err := d.decoder.DecodeIntoWithPTS(pkt.Payload.Bytes, &img, timestampValue(pkt.PTS))
	if err != nil {
		out.Frames = out.Frames[:index]
		return mapGovpxError(err)
	}
	if !frameInfo.ShowFrame {
		out.Frames = out.Frames[:index]
		return nil
	}

	stampDecodedVideoFrame(frame, &d.video, &d.stream, pkt, frameInfo.Width, frameInfo.Height)
	return nil
}

func (d *Decoder) outputDimensions(info govpxlib.StreamInfo) (int, int) {
	if info.Width > 0 && info.Height > 0 {
		return info.Width, info.Height
	}
	if d.video.Width > 0 && d.video.Height > 0 {
		return d.video.Width, d.video.Height
	}
	return d.stream.Codec.Width, d.stream.Codec.Height
}

func prepareGovpxImageFrame(frame *av.Frame, width, height int) (govpxlib.Image, error) {
	if err := prepareI420Frame(frame, width, height); err != nil {
		return govpxlib.Image{}, err
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

func prepareI420Frame(frame *av.Frame, width, height int) error {
	if width <= 0 || height <= 0 {
		return codec.ErrUnsupportedFormat
	}
	if cap(frame.Planes) < 3 {
		return codec.ErrOutputBufferTooSmall
	}
	planes := frame.Planes[:3]
	chromaWidth := (width + 1) / 2
	chromaHeight := (height + 1) / 2
	ySize := width * height
	chromaSize := chromaWidth * chromaHeight
	if cap(planes[0].Buffer.Bytes) < ySize ||
		cap(planes[1].Buffer.Bytes) < chromaSize ||
		cap(planes[2].Buffer.Bytes) < chromaSize {
		return codec.ErrOutputBufferTooSmall
	}

	frame.Planes = planes
	frame.Planes[0] = av.Plane{
		Buffer: av.Buffer{Bytes: planes[0].Buffer.Bytes[:ySize], Ownership: av.BufferOwned},
		Stride: width,
	}
	frame.Planes[1] = av.Plane{
		Buffer: av.Buffer{Bytes: planes[1].Buffer.Bytes[:chromaSize], Ownership: av.BufferOwned},
		Stride: chromaWidth,
	}
	frame.Planes[2] = av.Plane{
		Buffer: av.Buffer{Bytes: planes[2].Buffer.Bytes[:chromaSize], Ownership: av.BufferOwned},
		Stride: chromaWidth,
	}
	return nil
}

func stampDecodedVideoFrame(frame *av.Frame, video *av.VideoFrame, stream *av.Stream, pkt *av.Packet, width, height int) {
	video.Width = width
	video.Height = height
	video.PixelFormat = av.PixelFormatI420
	stream.Codec.Width = width
	stream.Codec.Height = height
	stream.Codec.PixelFormat = av.PixelFormatI420

	frame.StreamID = stream.ID
	frame.CodecEpoch = stream.Epoch
	if pkt.StreamID != "" {
		frame.StreamID = pkt.StreamID
	}
	if pkt.CodecEpoch != 0 {
		frame.CodecEpoch = pkt.CodecEpoch
	}
	frame.Type = av.MediaVideo
	frame.PTS = pkt.PTS
	frame.Duration = pkt.Duration
	frame.Video = video
	frame.Metadata = pkt.Metadata
}

func (d *Decoder) applyEventStream(event *av.Event) {
	if event.Stream != nil {
		d.stream = normalizeStream(*event.Stream)
		if event.Codec == nil {
			return
		}
	}
	if event.StreamID != "" {
		d.stream.ID = event.StreamID
	}
	if event.Epoch != 0 {
		d.stream.Epoch = event.Epoch
	}
	if event.Codec != nil {
		d.stream.Codec = *event.Codec
	}
	d.stream = normalizeStream(d.stream)
	d.video.Width = d.stream.Codec.Width
	d.video.Height = d.stream.Codec.Height
	d.video.PixelFormat = d.stream.Codec.PixelFormat
}

func normalizeStream(stream av.Stream) av.Stream {
	stream.Type = av.MediaVideo
	stream.Codec.ID = av.CodecVP8
	stream.Codec.Type = av.MediaVideo
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = 90000
	}
	if stream.Codec.PixelFormat == "" {
		stream.Codec.PixelFormat = av.PixelFormatI420
	}
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(stream.Codec.ClockRate)
	}
	return stream
}

func (d *Decoder) appendKeyframeRequest(out *codec.DecodeResult, reason string) error {
	if !d.requestKeyframes {
		return nil
	}
	if len(out.Requests) == cap(out.Requests) {
		return codec.ErrResultFull
	}
	out.Requests = append(out.Requests, codec.ControlRequest{
		Type:     codec.ControlRequestKeyframe,
		StreamID: d.stream.ID,
		Reason:   reason,
	})
	return nil
}

func timestampValue(ts av.Timestamp) uint64 {
	if ts.Value <= 0 {
		return 0
	}
	return uint64(ts.Value)
}

func mapGovpxError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, govpxlib.ErrClosed):
		return codec.ErrClosed
	case errors.Is(err, govpxlib.ErrInvalidConfig):
		return codec.ErrUnsupportedFormat
	default:
		return err
	}
}
