package rtpav

import (
	"context"
	"encoding/binary"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

const (
	h264NALUTypeMask = byte(0x1f)
	h264NALUNRIMask  = byte(0x60)

	h264NALUIDR   = byte(5)
	h264NALUSTAPA = byte(24)
	h264NALUFUA   = byte(28)

	h264FUStartMask = byte(0x80)
	h264FUEndMask   = byte(0x40)
)

// H264Depacketizer converts H264 RTP payloads into Annex B access-unit packets.
type H264Depacketizer struct {
	assembler videoFrameAssembler
	keyframe  bool
	fragment  bool
}

var _ Depacketizer = (*H264Depacketizer)(nil)

// NewH264Depacketizer assembles H264 RTP packets into bounded Annex B payloads.
func NewH264Depacketizer(stream av.Stream, options ...VideoDepacketizerOption) *H264Depacketizer {
	return &H264Depacketizer{
		assembler: newVideoFrameAssembler(stream, av.CodecH264, options...),
	}
}

// Codec reports the codec this depacketizer reassembles.
func (d *H264Depacketizer) Codec() av.CodecID {
	return av.CodecH264
}

// PushInto consumes one RTP packet (single NALU, STAP-A, or FU-A), appending
// a completed Annex B access unit to out when the packet finishes one.
func (d *H264Depacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(pkt.Payload) == 0 {
		return ErrInvalidPayload
	}

	start := h264StartsNALU(pkt.Payload)
	sync := start && h264HasIDR(pkt.Payload)
	if d.assembler.dropping {
		if !sync {
			d.assembler.requestSync = true
			d.assembler.resetPartial()
			d.fragment = false
			d.keyframe = false
			return d.assembler.emitPendingSync(out)
		}
		d.begin(pkt.Timestamp)
		d.assembler.dropping = false
		d.assembler.requestSync = false
	} else if !d.assembler.started {
		if !start {
			d.assembler.dropUntilSync()
			d.fragment = false
			d.keyframe = false
			return d.assembler.emitPendingSync(out)
		}
		d.begin(pkt.Timestamp)
	} else if pkt.Timestamp != d.assembler.timestamp {
		d.fragment = false
		d.keyframe = false
		d.assembler.dropUntilSync()
		return d.assembler.emitPendingSync(out)
	}

	if err := d.appendPayload(pkt.Payload); err != nil {
		if err == ErrFrameTooLarge {
			d.assembler.dropUntilSync()
		}
		return err
	}
	if !pkt.Marker || d.fragment {
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

// FlushInto discards any partial access unit at end of stream.
func (d *H264Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	d.keyframe = false
	d.fragment = false
	return nil
}

// HandleEvent resets reassembly state on discontinuities.
func (d *H264Depacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
	affected := eventAffectsCodecStream(d.assembler.stream, d.Codec(), event)
	if err := d.assembler.handleEvent(ctx, event); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	switch event.Type {
	case av.EventPacketLoss, av.EventDiscontinuity, av.EventCodecChanged:
		if affected {
			d.keyframe = false
			d.fragment = false
		}
	}
	return nil
}

func (d *H264Depacketizer) begin(timestamp uint32) {
	d.assembler.started = true
	d.assembler.timestamp = timestamp
	d.assembler.buffer = d.assembler.buffer[:0]
	d.keyframe = false
	d.fragment = false
}

func (d *H264Depacketizer) appendPayload(payload []byte) error {
	naluType := payload[0] & h264NALUTypeMask
	switch {
	case naluType > 0 && naluType < h264NALUSTAPA:
		return d.appendNALU(payload)
	case naluType == h264NALUSTAPA:
		return d.appendSTAPA(payload)
	case naluType == h264NALUFUA:
		return d.appendFUA(payload)
	default:
		return ErrInvalidPayload
	}
}

func (d *H264Depacketizer) appendSTAPA(payload []byte) error {
	offset := 1
	for offset < len(payload) {
		if offset+2 > len(payload) {
			return ErrInvalidPayload
		}
		size := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if size == 0 {
			continue
		}
		if offset+size > len(payload) {
			return ErrInvalidPayload
		}
		if err := d.appendNALU(payload[offset : offset+size]); err != nil {
			return err
		}
		offset += size
	}
	return nil
}

func (d *H264Depacketizer) appendFUA(payload []byte) error {
	if len(payload) < 2 {
		return ErrInvalidPayload
	}
	start := payload[1]&h264FUStartMask != 0
	end := payload[1]&h264FUEndMask != 0
	naluHeader := (payload[0] & h264NALUNRIMask) | (payload[1] & h264NALUTypeMask)
	if start {
		d.fragment = !end
		if naluHeader&h264NALUTypeMask == h264NALUIDR {
			d.keyframe = true
		}
		if err := d.appendStartCode(); err != nil {
			return err
		}
		if err := d.appendByte(naluHeader); err != nil {
			return err
		}
		return d.appendRaw(payload[2:])
	}
	if !d.fragment {
		return ErrInvalidPayload
	}
	if err := d.appendRaw(payload[2:]); err != nil {
		return err
	}
	if end {
		d.fragment = false
	}
	return nil
}

func (d *H264Depacketizer) appendNALU(nalu []byte) error {
	if len(nalu) == 0 {
		return nil
	}
	if nalu[0]&h264NALUTypeMask == h264NALUIDR {
		d.keyframe = true
	}
	if err := d.appendStartCode(); err != nil {
		return err
	}
	return d.appendRaw(nalu)
}

func (d *H264Depacketizer) appendStartCode() error {
	if len(d.assembler.buffer)+4 > cap(d.assembler.buffer) {
		return ErrFrameTooLarge
	}
	d.assembler.buffer = append(d.assembler.buffer, 0x00, 0x00, 0x00, 0x01)
	return nil
}

func (d *H264Depacketizer) appendRaw(payload []byte) error {
	if len(d.assembler.buffer)+len(payload) > cap(d.assembler.buffer) {
		return ErrFrameTooLarge
	}
	d.assembler.buffer = append(d.assembler.buffer, payload...)
	return nil
}

func (d *H264Depacketizer) appendByte(value byte) error {
	if len(d.assembler.buffer)+1 > cap(d.assembler.buffer) {
		return ErrFrameTooLarge
	}
	d.assembler.buffer = append(d.assembler.buffer, value)
	return nil
}

func h264StartsNALU(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	naluType := payload[0] & h264NALUTypeMask
	if naluType == h264NALUFUA {
		return len(payload) >= 2 && payload[1]&h264FUStartMask != 0
	}
	return naluType > 0 && naluType <= h264NALUSTAPA
}

func h264HasIDR(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	naluType := payload[0] & h264NALUTypeMask
	switch {
	case naluType == h264NALUIDR:
		return true
	case naluType == h264NALUSTAPA:
		offset := 1
		for offset+2 <= len(payload) {
			size := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
			offset += 2
			if size == 0 {
				continue
			}
			if offset+size > len(payload) {
				return false
			}
			if payload[offset]&h264NALUTypeMask == h264NALUIDR {
				return true
			}
			offset += size
		}
	case naluType == h264NALUFUA:
		return len(payload) >= 2 &&
			payload[1]&h264FUStartMask != 0 &&
			payload[1]&h264NALUTypeMask == h264NALUIDR
	}
	return false
}
