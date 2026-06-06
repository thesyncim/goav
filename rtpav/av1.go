package rtpav

import (
	"context"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

const (
	av1ZMask = byte(0b10000000)
	av1YMask = byte(0b01000000)
	av1WMask = byte(0b00110000)
	av1NMask = byte(0b00001000)

	av1OBUTemporalDelimiter = 2
	av1OBUTileList          = 8
)

// AV1Depacketizer converts AV1 RTP aggregation payloads into bounded packets.
type AV1Depacketizer struct {
	assembler videoFrameAssembler
	fragment  []byte
	keyframe  bool
}

var _ Depacketizer = (*AV1Depacketizer)(nil)

// NewAV1Depacketizer assembles AV1 RTP payloads into AV1 low-overhead bitstream packets.
func NewAV1Depacketizer(stream av.Stream, options ...VideoDepacketizerOption) *AV1Depacketizer {
	assembler := newVideoFrameAssembler(stream, av.CodecAV1, options...)
	return &AV1Depacketizer{
		assembler: assembler,
		fragment:  make([]byte, 0, cap(assembler.buffer)),
	}
}

func (d *AV1Depacketizer) Codec() av.CodecID {
	return av.CodecAV1
}

func (d *AV1Depacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(pkt.Payload) <= 1 {
		return ErrInvalidPayload
	}

	header := pkt.Payload[0]
	z := header&av1ZMask != 0
	y := header&av1YMask != 0
	n := header&av1NMask != 0
	count := int((header & av1WMask) >> 4)

	if n || !z {
		d.fragment = d.fragment[:0]
	}
	if d.assembler.dropping {
		if !n || z {
			d.assembler.requestSync = true
			d.assembler.resetPartial()
			return d.assembler.emitPendingSync(out)
		}
		d.begin(pkt.Timestamp)
		d.keyframe = n
		d.assembler.dropping = false
		d.assembler.requestSync = false
	} else if !d.assembler.started {
		if z {
			d.assembler.dropUntilSync()
			return d.assembler.emitPendingSync(out)
		}
		d.begin(pkt.Timestamp)
		d.keyframe = n
	} else if pkt.Timestamp != d.assembler.timestamp {
		d.fragment = d.fragment[:0]
		d.keyframe = false
		d.assembler.dropUntilSync()
		return d.assembler.emitPendingSync(out)
	}

	if err := d.appendPayload(pkt.Payload, z, y, count); err != nil {
		if err == ErrFrameTooLarge {
			d.assembler.dropUntilSync()
		}
		return err
	}
	if !pkt.Marker || y || len(d.fragment) != 0 {
		return nil
	}
	if len(d.assembler.buffer) == 0 {
		d.assembler.started = false
		return nil
	}

	frame := d.assembler.buffer
	d.assembler.started = false
	return d.assembler.emitPacket(pkt.Timestamp, payloadClockRate(payload, d.assembler.stream), frame, d.keyframe, out)
}

func (d *AV1Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	d.fragment = d.fragment[:0]
	d.keyframe = false
	return nil
}

func (d *AV1Depacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := d.assembler.handleEvent(ctx, event); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	switch event.Type {
	case av.EventPacketLoss, av.EventDiscontinuity, av.EventCodecChanged:
		if eventMatchesStream(d.assembler.stream, event) {
			d.fragment = d.fragment[:0]
			d.keyframe = false
		}
	}
	return nil
}

func (d *AV1Depacketizer) begin(timestamp uint32) {
	d.assembler.started = true
	d.assembler.timestamp = timestamp
	d.assembler.buffer = d.assembler.buffer[:0]
	d.keyframe = false
}

func (d *AV1Depacketizer) appendPayload(payload []byte, z bool, y bool, count int) error {
	offset := 1
	elementIndex := 0
	for offset < len(payload) {
		isFirst := elementIndex == 0
		isLast := count != 0 && elementIndex == count-1
		length := 0
		if count == 0 || !isLast {
			value, read, ok := readLEB128(payload[offset:])
			if !ok {
				return ErrInvalidPayload
			}
			length = value
			offset += read
			if count == 0 && offset+length == len(payload) {
				isLast = true
			}
		} else {
			length = len(payload) - offset
		}
		if length < 0 || offset+length > len(payload) {
			return ErrInvalidPayload
		}
		element := payload[offset : offset+length]
		offset += length

		if isFirst && z {
			if len(d.fragment) == 0 {
				if isLast {
					break
				}
				elementIndex++
				continue
			}
			if err := d.appendFragment(element); err != nil {
				return err
			}
			if isLast && y {
				return nil
			}
			if err := d.appendOBU(d.fragment); err != nil {
				return err
			}
			d.fragment = d.fragment[:0]
		} else if isLast && y {
			return d.appendFragment(element)
		} else if err := d.appendOBU(element); err != nil {
			return err
		}

		if isLast {
			break
		}
		elementIndex++
	}
	if count != 0 && elementIndex != count-1 {
		return ErrInvalidPayload
	}
	return nil
}

func (d *AV1Depacketizer) appendFragment(element []byte) error {
	if len(d.fragment)+len(element) > cap(d.fragment) {
		return ErrFrameTooLarge
	}
	d.fragment = append(d.fragment, element...)
	return nil
}

func (d *AV1Depacketizer) appendOBU(element []byte) error {
	if len(element) == 0 {
		return nil
	}
	headerLen, obuType, payloadOffset, ok := parseOBUElement(element)
	if !ok {
		return ErrInvalidPayload
	}
	if obuType == av1OBUTemporalDelimiter || obuType == av1OBUTileList {
		return nil
	}
	bodySize := len(element) - payloadOffset
	needed := headerLen + leb128Size(bodySize) + bodySize
	if len(d.assembler.buffer)+needed > cap(d.assembler.buffer) {
		return ErrFrameTooLarge
	}

	header := element[0] | 0x02
	d.assembler.buffer = append(d.assembler.buffer, header)
	if headerLen == 2 {
		d.assembler.buffer = append(d.assembler.buffer, element[1])
	}
	d.assembler.buffer = appendLEB128(d.assembler.buffer, bodySize)
	d.assembler.buffer = append(d.assembler.buffer, element[payloadOffset:]...)
	return nil
}

func parseOBUElement(element []byte) (headerLen int, obuType int, payloadOffset int, ok bool) {
	if len(element) < 1 || element[0]&0x80 != 0 {
		return 0, 0, 0, false
	}
	headerLen = 1
	if element[0]&0x04 != 0 {
		headerLen = 2
	}
	if len(element) < headerLen {
		return 0, 0, 0, false
	}
	obuType = int((element[0] & 0x78) >> 3)
	payloadOffset = headerLen
	if element[0]&0x02 != 0 {
		_, read, ok := readLEB128(element[headerLen:])
		if !ok {
			return 0, 0, 0, false
		}
		payloadOffset += read
		if payloadOffset > len(element) {
			return 0, 0, 0, false
		}
	}
	return headerLen, obuType, payloadOffset, true
}

func readLEB128(in []byte) (int, int, bool) {
	var out uint64
	for i := range in {
		if i == 10 {
			return 0, 0, false
		}
		out |= uint64(in[i]&0x7f) << (7 * i)
		if in[i]&0x80 == 0 {
			if out > uint64(^uint(0)>>1) {
				return 0, 0, false
			}
			return int(out), i + 1, true
		}
	}
	return 0, 0, false
}

func appendLEB128(out []byte, value int) []byte {
	v := uint(value)
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v == 0 {
			return append(out, b)
		}
		out = append(out, b|0x80)
	}
}

func leb128Size(value int) int {
	size := 1
	v := uint(value)
	for v >>= 7; v != 0; v >>= 7 {
		size++
	}
	return size
}
