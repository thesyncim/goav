//go:build goav_goh264

package goh264

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	goh264lib "github.com/thesyncim/goh264"
)

type DecoderFactory struct{}

func NewDecoderFactory() DecoderFactory {
	return DecoderFactory{}
}

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDecoder(activeDescriptor(), NewDecoderFactory())
}

func activeDescriptor() codec.Descriptor {
	desc := Descriptor()
	desc.Backend.Status = "active-build-tagged"
	return desc
}

func (DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	decoder := &Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

type Decoder struct {
	decoder          *goh264lib.Decoder
	stream           av.Stream
	video            av.VideoFrame
	requestKeyframes bool
	dropDamagedVideo bool
	dropUntilSync    bool
}

func (d *Decoder) Descriptor() codec.Descriptor {
	return activeDescriptor()
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
	d.dropUntilSync = false
	return d.resetDecoder(d.stream.Codec.ExtraData.Bytes)
}

func (d *Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.decoder == nil {
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		return d.appendKeyframeRequest(out, "h264 packet loss")
	}
	if pkt.LossBefore || pkt.Discontinuous {
		d.dropUntilSync = true
		if err := d.appendKeyframeRequest(out, "h264 discontinuity"); err != nil {
			return err
		}
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		d.dropUntilSync = true
		return d.appendKeyframeRequest(out, "h264 corrupt packet")
	}
	if d.dropUntilSync && !pkt.Keyframe {
		return nil
	}
	if pkt.Keyframe {
		d.dropUntilSync = false
	}

	frames, err := d.decoder.DecodeFrames(pkt.Payload.Bytes)
	if err != nil {
		return err
	}
	return d.appendDecodedFrames(pkt, frames, out)
}

func (d *Decoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.decoder == nil {
		return nil
	}
	frames, err := d.decoder.FlushDelayedFrames()
	if err != nil {
		return err
	}
	return d.appendDecodedFrames(nil, frames, out)
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
		d.dropUntilSync = true
	case av.EventCodecChanged, av.EventDiscontinuity:
		d.applyEventStream(event)
		d.dropUntilSync = true
		return d.resetDecoder(eventExtraData(event))
	}
	return nil
}

func (d *Decoder) Close() error {
	d.decoder = nil
	return nil
}

func (d *Decoder) resetDecoder(extra []byte) error {
	d.decoder = goh264lib.NewDecoder()
	if len(extra) == 0 {
		return nil
	}
	_, err := d.decoder.DecodeFrames(extra)
	return err
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
}

func eventExtraData(event *av.Event) []byte {
	if event == nil {
		return nil
	}
	if event.Codec != nil && len(event.Codec.ExtraData.Bytes) != 0 {
		return event.Codec.ExtraData.Bytes
	}
	if event.Stream != nil {
		return event.Stream.Codec.ExtraData.Bytes
	}
	return nil
}

func normalizeStream(stream av.Stream) av.Stream {
	stream.Type = av.MediaVideo
	stream.Codec.ID = av.CodecH264
	stream.Codec.Type = av.MediaVideo
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = 90000
	}
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(stream.Codec.ClockRate)
	}
	return stream
}

func (d *Decoder) appendDecodedFrames(pkt *av.Packet, frames []*goh264lib.Frame, out *codec.DecodeResult) error {
	needed := 0
	for i := range frames {
		if frames[i] != nil {
			needed++
		}
	}
	if len(out.Frames)+needed > cap(out.Frames) {
		return codec.ErrResultFull
	}
	for i := range frames {
		if frames[i] == nil {
			continue
		}
		if err := d.appendDecodedFrame(pkt, frames[i], out); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) appendDecodedFrame(pkt *av.Packet, src *goh264lib.Frame, out *codec.DecodeResult) error {
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	if err := d.fillDecodedFrame(pkt, src, frame); err != nil {
		out.Frames = out.Frames[:index]
		return err
	}
	return nil
}

func (d *Decoder) fillDecodedFrame(pkt *av.Packet, src *goh264lib.Frame, frame *av.Frame) error {
	if src.BitDepthLuma != 8 || src.BitDepthChroma != 8 {
		return codec.ErrUnsupportedFormat
	}
	if len(src.Y) == 0 || len(src.Cb) == 0 || len(src.Cr) == 0 {
		return codec.ErrUnsupportedFormat
	}
	if cap(frame.Planes) < 3 {
		return codec.ErrOutputBufferTooSmall
	}

	pixelFormat, err := src.RawPixelFormat()
	if err != nil {
		return err
	}

	frame.Planes = frame.Planes[:3]
	frame.Planes[0] = av.Plane{
		Buffer: av.Buffer{Bytes: src.Y, Ownership: av.BufferBorrowed},
		Stride: src.YStride,
	}
	frame.Planes[1] = av.Plane{
		Buffer: av.Buffer{Bytes: src.Cb, Ownership: av.BufferBorrowed},
		Stride: src.CStride,
	}
	frame.Planes[2] = av.Plane{
		Buffer: av.Buffer{Bytes: src.Cr, Ownership: av.BufferBorrowed},
		Stride: src.CStride,
	}

	d.video.Width = src.Width
	d.video.Height = src.Height
	d.video.PixelFormat = pixelFormat

	frame.StreamID = d.stream.ID
	frame.CodecEpoch = d.stream.Epoch
	frame.Type = av.MediaVideo
	frame.Video = &d.video
	if pkt != nil {
		if pkt.StreamID != "" {
			frame.StreamID = pkt.StreamID
		}
		if pkt.CodecEpoch != 0 {
			frame.CodecEpoch = pkt.CodecEpoch
		}
		frame.PTS = pkt.PTS
		frame.Duration = pkt.Duration
	}
	return nil
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
