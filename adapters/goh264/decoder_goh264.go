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
	closed           bool
	// pending holds the timing of each packet fed to the backend whose
	// picture has not come out yet, in arrival order. See popTiming.
	pending []frameTiming
}

// frameTiming is the identity a packet hands to the picture it carries.
type frameTiming struct {
	pts      av.Timestamp
	duration av.Duration
	streamID av.StreamID
	epoch    av.Epoch
}

// maxPendingTimings bounds pending: the H.264 DPB delays at most 16
// pictures, and the margin absorbs packets that carry no picture.
const maxPendingTimings = 32

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
	d.closed = false
	if cap(d.pending) == 0 {
		d.pending = make([]frameTiming, 0, maxPendingTimings)
	}
	return d.resetDecoder(d.stream.Codec.ExtraData.Bytes)
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

	// An empty packet carries no picture (the backend reads it as a flush).
	pushed := len(pkt.Payload.Bytes) != 0 && d.pushTiming(pkt)
	frames, err := d.decoder.DecodeFrames(pkt.Payload.Bytes)
	if err != nil {
		if pushed {
			d.pending = d.pending[:len(d.pending)-1]
		}
		return err
	}
	return d.appendDecodedFrames(frames, out)
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
	if d.decoder == nil {
		return nil
	}
	frames, err := d.decoder.FlushDelayedFrames()
	if err != nil {
		return err
	}
	err = d.appendDecodedFrames(frames, out)
	d.pending = d.pending[:0]
	return err
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
		return d.resetDecoder(eventExtraData(event))
	}
	return nil
}

func (d *Decoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	d.decoder = nil
	return nil
}

func (d *Decoder) resetDecoder(extra []byte) error {
	d.decoder = goh264lib.NewDecoder()
	d.pending = d.pending[:0]
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

func (d *Decoder) appendDecodedFrames(frames []*goh264lib.Frame, out *codec.DecodeResult) error {
	needed := 0
	for i := range frames {
		if frames[i] != nil {
			needed++
		}
	}
	if len(out.Frames)+needed > cap(out.Frames) {
		// The pictures are lost; so are their timings.
		for range needed {
			d.popTiming()
		}
		return codec.ErrResultFull
	}
	for i := range frames {
		if frames[i] == nil {
			continue
		}
		timing, ok := d.popTiming()
		var timed *frameTiming
		if ok {
			timed = &timing
		}
		if err := d.appendDecodedFrame(timed, frames[i], out); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) appendDecodedFrame(timing *frameTiming, src *goh264lib.Frame, out *codec.DecodeResult) error {
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	if err := d.fillDecodedFrame(timing, src, frame); err != nil {
		out.Frames = out.Frames[:index]
		return err
	}
	return nil
}

func (d *Decoder) fillDecodedFrame(timing *frameTiming, src *goh264lib.Frame, frame *av.Frame) error {
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
	if timing != nil {
		if timing.streamID != "" {
			frame.StreamID = timing.streamID
		}
		if timing.epoch != 0 {
			frame.CodecEpoch = timing.epoch
		}
		frame.PTS = timing.pts
		frame.Duration = timing.duration
	}
	return nil
}

// The backend carries no timestamps: it returns pictures in presentation
// order, held back by the stream's reorder delay, with the tail only at
// flush. So a picture's timing is not the packet being decoded when it comes
// out. Each packet fed in queues its timing, and each picture out takes the
// earliest pending presentation timestamp — presentation order is output
// order. Packets with a timestamp already pending (a parameter-set packet
// stamped like its picture, or the second field of a pair) queue nothing.
func (d *Decoder) pushTiming(pkt *av.Packet) bool {
	if pkt.PTS.Base.Valid() {
		for i := range d.pending {
			if gap, ok := pkt.PTS.Sub(d.pending[i].pts); ok && gap.Value == 0 && d.pending[i].pts.Base.Valid() {
				return false
			}
		}
	}
	if len(d.pending) == cap(d.pending) {
		// The oldest entry belongs to a packet whose picture never came.
		copy(d.pending, d.pending[1:])
		d.pending = d.pending[:len(d.pending)-1]
	}
	d.pending = append(d.pending, frameTiming{
		pts:      pkt.PTS,
		duration: pkt.Duration,
		streamID: pkt.StreamID,
		epoch:    pkt.CodecEpoch,
	})
	return true
}

// popTiming removes and returns the pending timing with the earliest
// presentation timestamp; untimed entries leave in arrival order.
func (d *Decoder) popTiming() (frameTiming, bool) {
	if len(d.pending) == 0 {
		return frameTiming{}, false
	}
	best := 0
	for i := 1; i < len(d.pending); i++ {
		if gap, ok := d.pending[i].pts.Sub(d.pending[best].pts); ok && gap.Value < 0 && d.pending[i].pts.Base.Valid() {
			best = i
		}
	}
	timing := d.pending[best]
	d.pending = append(d.pending[:best], d.pending[best+1:]...)
	return timing, true
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
