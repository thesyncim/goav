package rtpav

import (
	"bytes"
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

const (
	h264NALUSPS    = byte(7)
	h264NALUPPS    = byte(8)
	h264NALUAUD    = byte(9)
	h264NALUFiller = byte(12)
)

// h264Packetizer sends Annex B access units in packetization mode 1 (RFC
// 6184): NAL units that fit share STAP-A packets, larger ones split into
// FU-A fragments, and an IDR missing its parameter sets gets the last ones
// seen (in band, or the stream's extradata) ahead of its first slice.
type h264Packetizer struct {
	budget int
	clock  uint32
	sps    []byte
	pps    []byte
	// nalus and pending are reused scratch: the access unit's NAL units, and
	// the run waiting to share a packet.
	nalus   [][]byte
	pending [][]byte
}

func newH264Packetizer(stream av.Stream, budget int) *h264Packetizer {
	p := &h264Packetizer{budget: budget, clock: clockOr(stream.Codec.ClockRate, defaultVideoClockRate)}
	extra := stream.Codec.ExtraData.Bytes
	if len(extra) > 0 && extra[0] == 1 {
		p.learnAVCC(extra)
	} else {
		h264EachNALU(extra, func(nalu []byte) { p.learn(nalu) })
	}
	return p
}

func (p *h264Packetizer) Codec() av.CodecID { return av.CodecH264 }

func (p *h264Packetizer) PacketizeInto(ctx context.Context, packet *av.Packet, payload PayloadCodec, out *PacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ts, err := rtpTimestamp(packet, payload, p.clock)
	if err != nil {
		return err
	}
	p.nalus = p.nalus[:0]
	idr, sps, pps := false, false, false
	invalid := false
	h264EachNALU(packet.Payload.Bytes, func(nalu []byte) {
		switch nalType := nalu[0] & h264NALUTypeMask; {
		case nalu[0]&0x80 != 0 || nalType == 0 || nalType >= h264NALUSTAPA:
			invalid = true
		case nalType == h264NALUAUD || nalType == h264NALUFiller:
			// Delimiters and filler carry nothing a WebRTC receiver uses.
		default:
			idr = idr || nalType == h264NALUIDR
			sps = sps || nalType == h264NALUSPS
			pps = pps || nalType == h264NALUPPS
			p.learn(nalu)
			p.nalus = append(p.nalus, nalu)
		}
	})
	if invalid || len(p.nalus) == 0 {
		return ErrInvalidPayload
	}
	for i, nalu := range p.nalus {
		if idr && (!sps || !pps) && isH264Slice(nalu[0]&h264NALUTypeMask) {
			p.nalus = p.withParameterSets(i, !sps, !pps)
			break
		}
	}
	for _, nalu := range p.nalus {
		switch {
		case len(nalu) > p.budget:
			p.flush(out, payload.PayloadType, ts)
			p.fragment(out, payload.PayloadType, ts, nalu)
		case h264AggregateSize(p.pending, nalu) > p.budget:
			p.flush(out, payload.PayloadType, ts)
			p.pending = append(p.pending, nalu)
		default:
			p.pending = append(p.pending, nalu)
		}
	}
	p.flush(out, payload.PayloadType, ts)
	out.seal(true)
	return nil
}

// withParameterSets inserts the missing cached parameter sets before the
// NAL unit at i, the access unit's first slice.
func (p *h264Packetizer) withParameterSets(i int, sps, pps bool) [][]byte {
	inserted := 0
	if sps && len(p.sps) != 0 {
		inserted++
	}
	if pps && len(p.pps) != 0 {
		inserted++
	}
	if inserted == 0 {
		return p.nalus
	}
	nalus := p.nalus
	for range inserted {
		nalus = append(nalus, nil)
	}
	copy(nalus[i+inserted:], nalus[i:len(nalus)-inserted])
	if sps && len(p.sps) != 0 {
		nalus[i] = p.sps
		i++
	}
	if pps && len(p.pps) != 0 {
		nalus[i] = p.pps
	}
	return nalus
}

// flush sends the pending run: one NAL unit alone, several as a STAP-A.
func (p *h264Packetizer) flush(out *PacketizeResult, payloadType uint8, ts uint32) {
	switch len(p.pending) {
	case 0:
		return
	case 1:
		out.open(payloadType, ts)
		out.write(p.pending[0]...)
		out.close()
	default:
		var f, nri byte
		for _, nalu := range p.pending {
			f |= nalu[0] & 0x80
			nri = max(nri, nalu[0]&h264NALUNRIMask)
		}
		out.open(payloadType, ts)
		out.write(f | nri | h264NALUSTAPA)
		for _, nalu := range p.pending {
			out.write(byte(len(nalu)>>8), byte(len(nalu)))
			out.write(nalu...)
		}
		out.close()
	}
	clear(p.pending)
	p.pending = p.pending[:0]
}

// fragment sends one NAL unit too large for a packet as FU-A fragments.
func (p *h264Packetizer) fragment(out *PacketizeResult, payloadType uint8, ts uint32, nalu []byte) {
	indicator := nalu[0]&(0x80|h264NALUNRIMask) | h264NALUFUA
	nalType := nalu[0] & h264NALUTypeMask
	data := nalu[1:]
	chunk := evenChunk(len(data), p.budget-2)
	for offset := 0; offset < len(data); offset += chunk {
		header := nalType
		if offset == 0 {
			header |= h264FUStartMask
		}
		end := min(offset+chunk, len(data))
		if end == len(data) {
			header |= h264FUEndMask
		}
		out.open(payloadType, ts)
		out.write(indicator, header)
		out.write(data[offset:end]...)
		out.close()
	}
}

// learn keeps a copy of an SPS or PPS for later IDRs that lack one.
func (p *h264Packetizer) learn(nalu []byte) {
	switch nalu[0] & h264NALUTypeMask {
	case h264NALUSPS:
		p.sps = append(p.sps[:0], nalu...)
	case h264NALUPPS:
		p.pps = append(p.pps[:0], nalu...)
	}
}

// learnAVCC reads the parameter sets of an avcC decoder configuration
// record (ISO/IEC 14496-15); a malformed record seeds nothing.
func (p *h264Packetizer) learnAVCC(record []byte) {
	if len(record) < 6 {
		return
	}
	pos := 5
	read := func(count int) bool {
		for range count {
			if pos+2 > len(record) {
				return false
			}
			size := int(binary.BigEndian.Uint16(record[pos:]))
			pos += 2
			if size == 0 || pos+size > len(record) {
				return false
			}
			p.learn(record[pos : pos+size])
			pos += size
		}
		return true
	}
	count := int(record[pos] & 0x1f)
	pos++
	if !read(count) || pos >= len(record) {
		return
	}
	count = int(record[pos])
	pos++
	read(count)
}

// h264AggregateSize is the STAP-A size of pending plus next.
func h264AggregateSize(pending [][]byte, next []byte) int {
	size := 1 + 2 + len(next)
	for _, nalu := range pending {
		size += 2 + len(nalu)
	}
	return size
}

func isH264Slice(nalType byte) bool { return nalType >= 1 && nalType <= h264NALUIDR }

// h264EachNALU calls fn with each NAL unit of an Annex B access unit, start
// codes removed; data without a leading start code is one bare NAL unit.
func h264EachNALU(data []byte, fn func([]byte)) {
	start, width := h264StartCode(data, 0)
	if start < 0 {
		start, width = len(data), 0
	}
	if lead := h264TrimZeros(data[:start]); len(lead) != 0 {
		fn(lead)
	}
	for start < len(data) {
		begin := start + width
		next, nextWidth := h264StartCode(data, begin)
		if next < 0 {
			next, nextWidth = len(data), 0
		}
		if nalu := h264TrimZeros(data[begin:next]); len(nalu) != 0 {
			fn(nalu)
		}
		start, width = next, nextWidth
	}
}

// h264TrimZeros drops trailing zero bytes, which belong to the next start
// code (or the byte stream), not the NAL unit.
func h264TrimZeros(data []byte) []byte {
	end := len(data)
	for end > 0 && data[end-1] == 0 {
		end--
	}
	return data[:end]
}

// h264StartCode finds the next 3- or 4-byte start code at or after offset.
func h264StartCode(data []byte, offset int) (int, int) {
	if offset >= len(data) {
		return -1, 0
	}
	i := bytes.Index(data[offset:], h264StartCode3)
	if i < 0 {
		return -1, 0
	}
	i += offset
	if i > offset && data[i-1] == 0 {
		return i - 1, 4
	}
	return i, 3
}

var h264StartCode3 = []byte{0, 0, 1}
