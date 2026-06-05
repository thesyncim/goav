package rtpav

import (
	"context"

	"github.com/pion/rtp"
	pioncodecs "github.com/pion/rtp/codecs"
	"github.com/thesyncim/goav/av"
)

const defaultVideoClockRate = 90000
const defaultMaxVideoFrameSize = 4 << 20

type VideoDepacketizerOption func(*videoDepacketizerConfig)

type videoDepacketizerConfig struct {
	maxFrameSize int
}

type VP8Depacketizer struct {
	packet    pioncodecs.VP8Packet
	assembler videoFrameAssembler
}

type VP9Depacketizer struct {
	packet    pioncodecs.VP9Packet
	assembler videoFrameAssembler
}

var _ Depacketizer = (*VP8Depacketizer)(nil)
var _ Depacketizer = (*VP9Depacketizer)(nil)

type videoFrameAssembler struct {
	stream        av.Stream
	buffer        []byte
	started       bool
	dropping      bool
	lossBefore    bool
	discontinuous bool
	requestSync   bool
	timestamp     uint32
}

func WithMaxVideoFrameSize(size int) VideoDepacketizerOption {
	return func(config *videoDepacketizerConfig) {
		config.maxFrameSize = size
	}
}

func NewVP8Depacketizer(stream av.Stream, options ...VideoDepacketizerOption) *VP8Depacketizer {
	return &VP8Depacketizer{
		assembler: newVideoFrameAssembler(stream, av.CodecVP8, options...),
	}
}

func NewVP9Depacketizer(stream av.Stream, options ...VideoDepacketizerOption) *VP9Depacketizer {
	return &VP9Depacketizer{
		assembler: newVideoFrameAssembler(stream, av.CodecVP9, options...),
	}
}

func (d *VP8Depacketizer) Codec() av.CodecID {
	return av.CodecVP8
}

func (d *VP8Depacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	frame, err := d.packet.Unmarshal(pkt.Payload)
	if err != nil {
		return err
	}
	start := d.packet.S == 1 && d.packet.PID == 0
	keyframe := start && len(frame) > 0 && frame[0]&0x01 == 0
	return d.assembler.push(ctx, pkt, payload, frame, start, pkt.Marker, keyframe, out)
}

func (d *VP8Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	return nil
}

func (d *VP8Depacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
	return d.assembler.handleEvent(ctx, event)
}

func (d *VP9Depacketizer) Codec() av.CodecID {
	return av.CodecVP9
}

func (d *VP9Depacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	d.resetPacket()
	frame, err := d.packet.Unmarshal(pkt.Payload)
	if err != nil {
		return err
	}
	keyframe := d.packet.B && !d.packet.P
	end := pkt.Marker || d.packet.E
	return d.assembler.push(ctx, pkt, payload, frame, d.packet.B, end, keyframe, out)
}

func (d *VP9Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	return nil
}

func (d *VP9Depacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
	return d.assembler.handleEvent(ctx, event)
}

func (d *VP9Depacketizer) resetPacket() {
	d.packet.PDiff = d.packet.PDiff[:0]
	d.packet.PGTID = d.packet.PGTID[:0]
	d.packet.PGU = d.packet.PGU[:0]
	d.packet.PGPDiff = d.packet.PGPDiff[:0]
	d.packet.Payload = nil
}

func newVideoFrameAssembler(stream av.Stream, codec av.CodecID, options ...VideoDepacketizerOption) videoFrameAssembler {
	config := videoDepacketizerConfig{maxFrameSize: defaultMaxVideoFrameSize}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	if config.maxFrameSize <= 0 {
		config.maxFrameSize = defaultMaxVideoFrameSize
	}
	stream.Type = av.MediaVideo
	stream.Codec.ID = codec
	stream.Codec.Type = av.MediaVideo
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = defaultVideoClockRate
	}
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(stream.Codec.ClockRate)
	}
	return videoFrameAssembler{
		stream: stream,
		buffer: make([]byte, 0, config.maxFrameSize),
	}
}

func (a *videoFrameAssembler) push(
	ctx context.Context,
	pkt *rtp.Packet,
	payload PayloadCodec,
	frame []byte,
	start bool,
	end bool,
	keyframe bool,
	out *DepacketizeResult,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !start && (!a.started || pkt.Timestamp != a.timestamp) {
		a.dropUntilSync()
		return a.emitPendingSync(out)
	}
	if start {
		if a.dropping && !keyframe {
			a.requestSync = true
			a.resetPartial()
			return a.emitPendingSync(out)
		}
		a.requestSync = false
		a.started = true
		a.dropping = false
		a.timestamp = pkt.Timestamp
		a.buffer = a.buffer[:0]
	}
	if a.dropping {
		return nil
	}
	if end && len(a.buffer) == 0 {
		return a.emitPacket(pkt.Timestamp, payloadClockRate(payload, a.stream), frame, keyframe, out)
	}
	if len(a.buffer)+len(frame) > cap(a.buffer) {
		a.dropUntilSync()
		return ErrFrameTooLarge
	}
	a.buffer = append(a.buffer, frame...)
	if !end {
		return nil
	}
	packetPayload := a.buffer
	a.started = false
	return a.emitPacket(pkt.Timestamp, payloadClockRate(payload, a.stream), packetPayload, keyframe, out)
}

func (a *videoFrameAssembler) handleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	switch event.Type {
	case av.EventPacketLoss:
		a.markLoss(false)
	case av.EventDiscontinuity, av.EventCodecChanged:
		a.markLoss(true)
	}
	return nil
}

func (a *videoFrameAssembler) markLoss(discontinuous bool) {
	a.resetPartial()
	a.lossBefore = true
	if discontinuous {
		a.discontinuous = true
	}
	a.requestSync = true
	a.dropping = true
}

func (a *videoFrameAssembler) dropUntilSync() {
	if !a.dropping {
		a.requestSync = true
	}
	a.lossBefore = true
	a.dropping = true
	a.resetPartial()
}

func (a *videoFrameAssembler) resetPartial() {
	a.started = false
	a.buffer = a.buffer[:0]
	a.timestamp = 0
}

func (a *videoFrameAssembler) emitPendingSync(out *DepacketizeResult) error {
	if !a.requestSync {
		return nil
	}
	if len(out.Events) == cap(out.Events) {
		return ErrResultFull
	}
	out.Events = append(out.Events, av.Event{
		Type:     av.EventKeyframeRequired,
		StreamID: a.stream.ID,
		Epoch:    a.stream.Epoch,
		Reason:   "video depacketizer needs sync",
	})
	a.requestSync = false
	return nil
}

func (a *videoFrameAssembler) emitPacket(timestamp uint32, clockRate uint32, payload []byte, keyframe bool, out *DepacketizeResult) error {
	if len(out.Packets) == cap(out.Packets) {
		return ErrResultFull
	}
	packet := av.Packet{
		StreamID:   a.stream.ID,
		CodecEpoch: a.stream.Epoch,
		Payload: av.Buffer{
			Bytes:     payload,
			Ownership: av.BufferBorrowed,
		},
		PTS:           av.RTPToTimestamp(timestamp, clockRate),
		Keyframe:      keyframe,
		LossBefore:    a.lossBefore,
		Discontinuous: a.discontinuous,
	}
	out.Packets = append(out.Packets, packet)
	a.lossBefore = false
	a.discontinuous = false
	return nil
}

func payloadClockRate(payload PayloadCodec, stream av.Stream) uint32 {
	if payload.ClockRate != 0 {
		return payload.ClockRate
	}
	if stream.Codec.ClockRate != 0 {
		return stream.Codec.ClockRate
	}
	return defaultVideoClockRate
}
