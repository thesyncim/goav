package goav1

import (
	"context"
	"errors"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

type DecoderFactory struct{}

var _ codec.DecodeStateFactory = DecoderFactory{}

func NewDecoderFactory() DecoderFactory {
	return DecoderFactory{}
}

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDecoder(activeDescriptor(), NewDecoderFactory())
	registry.RegisterEncoder(activeEncoderDescriptor(), NewEncoderFactory())
}

func activeDescriptor() codec.Descriptor {
	desc := Descriptor()
	desc.Backend.Status = "active"
	return desc
}

func activeEncoderDescriptor() codec.Descriptor {
	desc := EncoderDescriptor()
	desc.Backend.Status = "active"
	return desc
}

func (DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	if config.Stream.Codec.ID != "" && config.Stream.Codec.ID != av.CodecAV1 {
		return nil, codec.ErrUnsupportedFormat
	}
	decoder := &Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

func (DecoderFactory) NewDecodeState(ctx context.Context, config codec.DecodeConfig) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.Stream.Codec.ID != "" && config.Stream.Codec.ID != av.CodecAV1 {
		return nil, codec.ErrUnsupportedFormat
	}
	bounds := config.Bounds
	if bounds.MaxWidth <= 0 {
		bounds.MaxWidth = config.Stream.Codec.Width
	}
	if bounds.MaxHeight <= 0 {
		bounds.MaxHeight = config.Stream.Codec.Height
	}
	format, err := DecoderFrameFormatFromStream(config.Stream, bounds)
	if err != nil {
		return nil, err
	}
	return NewDecoderState(DecoderStateConfig{
		Bounds:  bounds,
		Format:  format,
		Workers: defaultDecoderWorker,
	})
}

type goav1DecoderState uint8

const (
	goav1DecoderUnopened goav1DecoderState = iota
	goav1DecoderAwaitingSeqHeader
	goav1DecoderDecoding
	goav1DecoderDropping
	goav1DecoderClosed
)

// Decoder consumes depacketized AV1 low-overhead OBU buffers through the
// generic codec.Decoder path. Low-level RTP callers that intentionally own raw
// AV1 RTP payload bytes can use DecodeRTPPayloadInto on this concrete type.
type Decoder struct {
	state            *DecoderState
	stream           av.Stream
	video            av.VideoFrame
	requestKeyframes bool
	dropDamagedVideo bool
	lifecycle        goav1DecoderState
}

func (d *Decoder) Descriptor() codec.Descriptor {
	return activeDescriptor()
}

func (d *Decoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, ok := config.OpaqueState.(*DecoderState)
	if !ok || state == nil {
		return codec.ErrUnsupportedFormat
	}
	pixelFormat, _, err := backendFramePixelFormat(state.Format())
	if err != nil {
		return err
	}
	if requested := normalizeDecoderPixelFormat(config.Stream.Codec.PixelFormat); requested != "" && requested != pixelFormat {
		return codec.ErrUnsupportedFormat
	}

	state.Reset()
	d.state = state
	d.stream = normalizeDecoderStream(config.Stream, state.Format(), pixelFormat)
	d.video = av.VideoFrame{
		Width:       d.stream.Codec.Width,
		Height:      d.stream.Codec.Height,
		PixelFormat: d.stream.Codec.PixelFormat,
	}
	d.requestKeyframes = config.Resilience.RequestKeyframes
	d.dropDamagedVideo = config.Resilience.DropDamagedVideo
	if state.HasSequenceHeader() {
		d.lifecycle = goav1DecoderDecoding
	} else {
		d.lifecycle = goav1DecoderAwaitingSeqHeader
	}
	return nil
}

func (d *Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	switch d.lifecycle {
	case goav1DecoderClosed:
		return codec.ErrClosed
	case goav1DecoderUnopened:
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		d.resetAfterLoss()
		return d.appendKeyframeRequest(out, "av1 packet loss")
	}
	if pkt.LossBefore || pkt.Discontinuous {
		d.resetAfterLoss()
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		d.resetAfterLoss()
		return d.appendKeyframeRequest(out, "av1 corrupt packet")
	}
	if len(pkt.Payload.Bytes) == 0 {
		return nil
	}

	dropping := d.lifecycle == goav1DecoderDropping
	hasSeq := lowOverheadHasSequenceHeader(pkt.Payload.Bytes)
	sync := pkt.Keyframe || hasSeq
	if dropping && !sync {
		return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
	}
	if len(out.Frames) >= cap(out.Frames) {
		return codec.ErrResultFull
	}

	rtpPayload, err := d.state.packetizeLowOverhead(pkt.Payload.Bytes, sync)
	if err != nil {
		return d.decodeFailure(out, dropping, err)
	}
	if len(rtpPayload) == 0 {
		return nil
	}

	if d.state.dec == nil {
		if err := d.state.ensureDecoder([][]byte{rtpPayload}); err != nil {
			return d.decodeFailure(out, dropping, err)
		}
	}

	frames, err := d.state.dec.DecodeRTPPayload(rtpPayload)
	if err != nil && hasSeq {
		// Re-initialize on sequence/format changes carrying a new sequence header.
		if initErr := d.state.ensureDecoder([][]byte{rtpPayload}); initErr == nil {
			frames, err = d.state.dec.DecodeRTPPayload(rtpPayload)
		}
	}
	if err != nil {
		return d.decodeFailure(out, dropping, err)
	}
	d.lifecycle = goav1DecoderDecoding
	return d.appendDecodedFrames(pkt, frames, out)
}

func (d *Decoder) decodeFailure(out *codec.DecodeResult, dropping bool, err error) error {
	if dropping {
		return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
	}
	return mapGoav1Error(err)
}

func (d *Decoder) DecodeRTPPayloadInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	switch d.lifecycle {
	case goav1DecoderClosed:
		return codec.ErrClosed
	case goav1DecoderUnopened:
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		d.resetRTPAfterLoss()
		return d.appendKeyframeRequest(out, "av1 rtp packet loss")
	}

	afterLoss := pkt.LossBefore || pkt.Discontinuous
	if afterLoss {
		d.resetRTPAfterLoss()
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		d.resetRTPAfterLoss()
		return d.appendKeyframeRequest(out, "av1 corrupt rtp payload")
	}
	if len(pkt.Payload.Bytes) == 0 {
		return nil
	}

	dropping := d.lifecycle == goav1DecoderDropping
	hdr, _, err := backend.ParseRTPAggregationHeader(pkt.Payload.Bytes)
	if err != nil {
		return d.decodeFailure(out, dropping, err)
	}
	sync := pkt.Keyframe || hdr.StartsNewCodedVideoSequence
	if dropping && !sync {
		return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
	}
	if len(out.Frames) >= cap(out.Frames) && !hdr.ContinuesNext {
		return codec.ErrResultFull
	}

	if d.state.dec == nil {
		return d.initDecoderFromRTP(pkt, hdr, afterLoss, sync, dropping, out)
	}

	var frames []*backend.Frame
	if afterLoss {
		frames, err = d.state.dec.DecodeRTPPayloadAfterLoss(pkt.Payload.Bytes)
	} else {
		frames, err = d.state.dec.DecodeRTPPayload(pkt.Payload.Bytes)
	}
	if err != nil && hdr.StartsNewCodedVideoSequence && !hdr.ContinuesNext {
		if initErr := d.state.ensureDecoder([][]byte{pkt.Payload.Bytes}); initErr == nil {
			frames, err = d.state.dec.DecodeRTPPayload(pkt.Payload.Bytes)
		}
	}
	if err != nil {
		return d.decodeFailure(out, dropping, err)
	}
	d.lifecycle = goav1DecoderDecoding
	return d.appendDecodedFrames(pkt, frames, out)
}

// initDecoderFromRTP accumulates initial RTP fragments until a sequence header
// and completing tile group allow creating and priming the underlying decoder.
// Kept out-of-line so the steady-state DecodeRTPPayloadInto path stays compact.
func (d *Decoder) initDecoderFromRTP(
	pkt *av.Packet,
	hdr backend.RTPAggregationHeader,
	afterLoss, sync, dropping bool,
	out *codec.DecodeResult,
) error {
	if afterLoss {
		d.state.resetInitFragments()
	}
	if !d.state.appendInitFragment(pkt.Payload.Bytes) {
		d.lifecycle = goav1DecoderDropping
		if dropping {
			return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
		}
		return codec.ErrUnsupportedFormat
	}
	if hdr.ContinuesNext {
		if sync {
			d.lifecycle = goav1DecoderAwaitingSeqHeader
		}
		return nil
	}
	if err := d.state.ensureDecoder(d.state.initPayloads); err != nil {
		if strings.Contains(err.Error(), "did not identify a bind event") {
			if sync {
				d.lifecycle = goav1DecoderAwaitingSeqHeader
			}
			return nil
		}
		d.state.resetInitFragments()
		return d.decodeFailure(out, dropping, err)
	}
	var lastFrames []*backend.Frame
	for i, frag := range d.state.initPayloads {
		var (
			frames []*backend.Frame
			err    error
		)
		if i == 0 && afterLoss {
			frames, err = d.state.dec.DecodeRTPPayloadAfterLoss(frag)
		} else {
			frames, err = d.state.dec.DecodeRTPPayload(frag)
		}
		if err != nil {
			d.state.resetInitFragments()
			return d.decodeFailure(out, dropping, err)
		}
		if len(frames) > 0 {
			lastFrames = frames
		}
	}
	if len(lastFrames) == 0 {
		// Sequence/frame headers arrived without a completing tile group yet;
		// keep accumulating initPayloads so the decoder arena is sized with
		// at least one completed frame output slot.
		d.state.dec.Close()
		d.state.dec = nil
		if sync {
			d.lifecycle = goav1DecoderAwaitingSeqHeader
		}
		return nil
	}
	d.state.resetInitFragments()
	d.lifecycle = goav1DecoderDecoding
	return d.appendDecodedFrames(pkt, lastFrames, out)
}

func (d *Decoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.lifecycle == goav1DecoderClosed {
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
	switch d.lifecycle {
	case goav1DecoderClosed:
		return codec.ErrClosed
	case goav1DecoderUnopened:
		return codec.ErrUnsupportedFormat
	}
	switch event.Type {
	case av.EventPacketLoss:
		d.resetAfterLoss()
	case av.EventCodecChanged, av.EventDiscontinuity:
		d.applyEventStream(event)
		d.resetAfterLoss()
	}
	return nil
}

func (d *Decoder) Close() error {
	if d.lifecycle == goav1DecoderClosed {
		return nil
	}
	d.lifecycle = goav1DecoderClosed
	if d.state != nil {
		d.state.Close()
	}
	return nil
}

func (d *Decoder) resetAfterLoss() {
	if d.state != nil {
		d.state.Reset()
	}
	d.lifecycle = goav1DecoderDropping
}

func (d *Decoder) resetRTPAfterLoss() {
	if d.state != nil {
		d.state.resetInitFragments()
	}
	d.lifecycle = goav1DecoderDropping
}

func (d *Decoder) applyEventStream(event *av.Event) {
	if event.Stream != nil {
		d.stream = normalizeDecoderStream(*event.Stream, d.state.Format(), d.video.PixelFormat)
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
	d.stream = normalizeDecoderStream(d.stream, d.state.Format(), d.video.PixelFormat)
	d.video.Width = d.stream.Codec.Width
	d.video.Height = d.stream.Codec.Height
	d.video.PixelFormat = d.stream.Codec.PixelFormat
}

func normalizeDecoderStream(stream av.Stream, format backend.FrameFormat, pixelFormat string) av.Stream {
	stream.Type = av.MediaVideo
	stream.Codec.ID = av.CodecAV1
	stream.Codec.Type = av.MediaVideo
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = 90000
	}
	if stream.Codec.Width <= 0 {
		stream.Codec.Width = format.Width
	}
	if stream.Codec.Height <= 0 {
		stream.Codec.Height = format.Height
	}
	stream.Codec.PixelFormat = normalizeDecoderPixelFormat(stream.Codec.PixelFormat)
	if stream.Codec.PixelFormat == "" {
		stream.Codec.PixelFormat = pixelFormat
	}
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(stream.Codec.ClockRate)
	}
	return stream
}

func normalizeDecoderPixelFormat(pixelFormat string) string {
	switch pixelFormat {
	case av.PixelFormatYUV420P:
		return av.PixelFormatI420
	case av.PixelFormatYUV422P:
		return av.PixelFormatI422
	case av.PixelFormatYUV444P:
		return av.PixelFormatI444
	default:
		return pixelFormat
	}
}

func (d *Decoder) appendDecodedFrames(pkt *av.Packet, outputs []*backend.Frame, out *codec.DecodeResult) error {
	if len(outputs) == 0 {
		return nil
	}
	needed := 0
	for i := range outputs {
		if outputs[i] != nil {
			needed++
		}
	}
	if len(out.Frames)+needed > cap(out.Frames) {
		return codec.ErrResultFull
	}
	for i := range outputs {
		if outputs[i] == nil {
			continue
		}
		if err := d.appendDecodedFrame(pkt, outputs[i], out); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) appendDecodedFrame(pkt *av.Packet, src *backend.Frame, out *codec.DecodeResult) error {
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

func (d *Decoder) fillDecodedFrame(pkt *av.Packet, src *backend.Frame, frame *av.Frame) error {
	pixelFormat, planeCount, err := backendFramePixelFormat(src.Format)
	if err != nil {
		return err
	}
	if cap(frame.Planes) < planeCount {
		return codec.ErrOutputBufferTooSmall
	}

	frame.Planes = frame.Planes[:planeCount]
	frame.Planes[0] = av.Plane{
		Buffer: av.Buffer{Bytes: src.Y.Pix},
		Stride: src.Y.Stride,
	}
	if planeCount == 3 {
		frame.Planes[1] = av.Plane{
			Buffer: av.Buffer{Bytes: src.U.Pix},
			Stride: src.U.Stride,
		}
		frame.Planes[2] = av.Plane{
			Buffer: av.Buffer{Bytes: src.V.Pix},
			Stride: src.V.Stride,
		}
	}

	width, height := src.Format.Width, src.Format.Height
	if src.Y.Width > 0 {
		width = src.Y.Width
	}
	if src.Y.Height > 0 {
		height = src.Y.Height
	}
	d.video.Width = width
	d.video.Height = height
	d.video.PixelFormat = pixelFormat
	d.stream.Codec.Width = width
	d.stream.Codec.Height = height
	d.stream.Codec.PixelFormat = pixelFormat

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
		frame.Metadata = pkt.Metadata
	}
	return nil
}

func backendFramePixelFormat(format backend.FrameFormat) (string, int, error) {
	if format.Width <= 0 || format.Height <= 0 || format.BitDepth != 8 {
		return "", 0, codec.ErrUnsupportedFormat
	}
	if format.MonoChrome {
		return av.PixelFormatGray8, 1, nil
	}
	if format.SubsamplingX && format.SubsamplingY {
		return av.PixelFormatI420, 3, nil
	}
	if format.SubsamplingX && !format.SubsamplingY {
		return av.PixelFormatI422, 3, nil
	}
	if !format.SubsamplingX && !format.SubsamplingY {
		return av.PixelFormatI444, 3, nil
	}
	return "", 0, codec.ErrUnsupportedFormat
}

// lowOverheadHasSequenceHeader scans a low-overhead OBU temporal unit without
// allocating and reports whether it contains an OBU_SEQUENCE_HEADER (type 1).
func lowOverheadHasSequenceHeader(data []byte) bool {
	i := 0
	for i < len(data) {
		hdr := data[i]
		i++
		if hdr&0x80 != 0 {
			return false
		}
		obuType := (hdr >> 3) & 0x0f
		hasExt := hdr&0x04 != 0
		hasSize := hdr&0x02 != 0
		if hasExt {
			if i >= len(data) {
				return false
			}
			i++
		}
		if obuType == 1 {
			return true
		}
		if !hasSize {
			return false
		}
		var size uint64
		var shift uint
		for {
			if i >= len(data) || shift >= 56 {
				return false
			}
			b := data[i]
			i++
			size |= uint64(b&0x7f) << shift
			if b&0x80 == 0 {
				break
			}
			shift += 7
		}
		if size > uint64(len(data)-i) {
			return false
		}
		i += int(size)
	}
	return false
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

func mapGoav1Error(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, backend.ErrDecoderFrameSizeLimit):
		return codec.ErrUnsupportedFormat
	default:
		return err
	}
}
