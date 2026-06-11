package govpx

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	govpxlib "github.com/thesyncim/govpx"
)

type VP9DecoderFactory struct{}

func NewVP9DecoderFactory() VP9DecoderFactory {
	return VP9DecoderFactory{}
}

func (VP9DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	if config.Stream.Codec.ID != "" && config.Stream.Codec.ID != av.CodecVP9 {
		return nil, codec.ErrUnsupportedFormat
	}
	decoder := &VP9Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type VP9Decoder struct {
	decoder          *govpxlib.VP9Decoder
	stream           av.Stream
	video            av.VideoFrame
	requestKeyframes bool
	dropDamagedVideo bool
	dropUntilSync    bool
	closed           bool
}

func (d *VP9Decoder) Descriptor() codec.Descriptor {
	return activeVP9Descriptor(descriptor(av.CodecVP9, "VP9", []string{av.PixelFormatI420}, []string{av.MIMEVP9}))
}

func (d *VP9Decoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.stream = normalizeVP9Stream(config.Stream)
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

func (d *VP9Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
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
		return d.appendKeyframeRequest(out, "vp9 packet loss")
	}
	requestedKeyframe := false
	if pkt.LossBefore || pkt.Discontinuous {
		d.dropUntilSync = true
		if err := d.appendKeyframeRequest(out, "vp9 discontinuity"); err != nil {
			return err
		}
		requestedKeyframe = true
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		d.dropUntilSync = true
		return d.appendKeyframeRequest(out, "vp9 corrupt packet")
	}

	streamInfo, err := govpxlib.PeekVP9StreamInfo(pkt.Payload.Bytes)
	if err != nil {
		return mapGovpxError(err)
	}
	if d.dropUntilSync && !streamInfo.KeyFrame {
		if requestedKeyframe {
			return nil
		}
		return d.appendKeyframeRequest(out, "vp9 waiting for keyframe")
	}
	if streamInfo.KeyFrame {
		d.dropUntilSync = false
	}

	return d.decodeVisiblePacket(pkt, streamInfo, out)
}

func (d *VP9Decoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
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

func (d *VP9Decoder) HandleEvent(ctx context.Context, event *av.Event) error {
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

func (d *VP9Decoder) Close() error {
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

func (d *VP9Decoder) resetDecoder(acceptLoss bool) error {
	decoder, err := govpxlib.NewVP9Decoder(govpxlib.VP9DecoderOptions{
		ErrorConcealment: acceptLoss,
	})
	if err != nil {
		return mapGovpxError(err)
	}
	d.decoder = decoder
	return nil
}

func (d *VP9Decoder) decodeVisiblePacket(pkt *av.Packet, streamInfo govpxlib.VP9StreamInfo, out *codec.DecodeResult) error {
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

func (d *VP9Decoder) outputDimensions(info govpxlib.VP9StreamInfo) (int, int) {
	if info.Width > 0 && info.Height > 0 {
		return info.Width, info.Height
	}
	if d.video.Width > 0 && d.video.Height > 0 {
		return d.video.Width, d.video.Height
	}
	return d.stream.Codec.Width, d.stream.Codec.Height
}

func (d *VP9Decoder) applyEventStream(event *av.Event) {
	if event.Stream != nil {
		d.stream = normalizeVP9Stream(*event.Stream)
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
	d.stream = normalizeVP9Stream(d.stream)
	d.video.Width = d.stream.Codec.Width
	d.video.Height = d.stream.Codec.Height
	d.video.PixelFormat = d.stream.Codec.PixelFormat
}

func normalizeVP9Stream(stream av.Stream) av.Stream {
	stream.Type = av.MediaVideo
	stream.Codec.ID = av.CodecVP9
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

func (d *VP9Decoder) appendKeyframeRequest(out *codec.DecodeResult, reason string) error {
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
