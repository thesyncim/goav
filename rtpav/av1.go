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

// Codec reports the codec this depacketizer reassembles.
func (d *AV1Depacketizer) Codec() av.CodecID {
	return av.CodecAV1
}

// PushInto consumes one RTP packet of AV1 aggregation elements, appending a
// completed temporal-unit packet to out when the packet finishes one.
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

// FlushInto discards any partial temporal unit at end of stream.
func (d *AV1Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	d.fragment = d.fragment[:0]
	d.keyframe = false
	return nil
}

// HandleEvent resets reassembly state on discontinuities.
func (d *AV1Depacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
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
			if count == 0 && length == len(payload)-offset {
				isLast = true
			}
		} else {
			length = len(payload) - offset
		}
		if length < 0 || length > len(payload)-offset { // no offset+length: it overflows
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

const (
	av1OBUSequenceHeader = 1
	av1OBUPadding        = 15
)

// av1Element is one OBU as an RTP aggregation element: its header with
// obu_has_size_field cleared (the element length carries the size) and its
// payload.
type av1Element struct {
	header  [2]byte
	hlen    int
	payload []byte
}

func (e *av1Element) size() int { return e.hlen + len(e.payload) }

// av1Span is one element, or a fragment of one, placed in a packet.
type av1Span struct {
	element, offset, length int
}

// av1Packetizer sends AV1 temporal units in the RTP payload format of the
// AV1 RTP specification (v1.0): OBUs aggregate into packets behind an
// aggregation header (Z, Y, W, N), fragmenting across packets when large;
// temporal delimiters, tile lists, and padding are dropped, and a keyframe
// without a sequence header gets the last one seen (in band, or the
// stream's av1C extradata).
type av1Packetizer struct {
	budget    int
	clock     uint32
	sequence  []byte
	seqHeader av1Element
	elements  []av1Element
	spans     []av1Span
}

func newAV1Packetizer(stream av.Stream, budget int) *av1Packetizer {
	p := &av1Packetizer{budget: budget, clock: clockOr(stream.Codec.ClockRate, defaultVideoClockRate)}
	extra := stream.Codec.ExtraData.Bytes
	if len(extra) >= 4 && extra[0]&0x80 != 0 {
		extra = extra[4:] // av1C: the config OBUs follow its four bytes
	}
	p.eachElement(extra, func(e av1Element, obuType int) bool {
		if obuType == av1OBUSequenceHeader {
			p.learn(e)
		}
		return true
	})
	return p
}

func (p *av1Packetizer) Codec() av.CodecID { return av.CodecAV1 }

func (p *av1Packetizer) PacketizeInto(ctx context.Context, packet *av.Packet, payload PayloadCodec, out *PacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ts, err := rtpTimestamp(packet, payload, p.clock)
	if err != nil {
		return err
	}
	p.elements = p.elements[:0]
	sequence := false
	if !p.eachElement(packet.Payload.Bytes, func(e av1Element, obuType int) bool {
		switch obuType {
		case av1OBUTemporalDelimiter, av1OBUTileList, av1OBUPadding:
			return true
		case av1OBUSequenceHeader:
			sequence = true
			p.learn(e)
		}
		p.elements = append(p.elements, e)
		return true
	}) || len(p.elements) == 0 {
		return ErrInvalidPayload
	}
	if packet.Keyframe && !sequence && len(p.sequence) != 0 {
		p.elements = append(p.elements, av1Element{})
		copy(p.elements[1:], p.elements)
		p.elements[0] = p.seqHeader
	}
	first := true
	element, offset := 0, 0
	for element < len(p.elements) {
		p.spans = p.spans[:0]
		used := 0 // the packet's elements, every one length-prefixed
		for element < len(p.elements) {
			rest := p.elements[element].size() - offset
			space := p.budget - 1 - used
			var n int
			if len(p.spans) < 3 {
				// With W set, the packet's last element carries no length.
				n = min(rest, space)
			} else if rest+leb128Size(rest) <= space {
				n = rest
			} else {
				n = space - leb128Size(space)
			}
			if n <= 0 {
				break
			}
			p.spans = append(p.spans, av1Span{element: element, offset: offset, length: n})
			used += leb128Size(n) + n
			if n < rest {
				offset += n
				break
			}
			element, offset = element+1, 0
		}
		p.writePacket(out, payload.PayloadType, ts, first && packet.Keyframe, element < len(p.elements) && offset > 0)
		first = false
	}
	out.seal(true)
	return nil
}

// writePacket writes the planned spans as one packet: Z when its first span
// continues an element, Y when its last one continues in the next packet.
func (p *av1Packetizer) writePacket(out *PacketizeResult, payloadType uint8, ts uint32, newSequence bool, continues bool) {
	var header byte
	if p.spans[0].offset > 0 {
		header |= av1ZMask
	}
	if continues {
		header |= av1YMask
	}
	count := len(p.spans)
	if count <= 3 {
		header |= byte(count) << 4
	}
	if newSequence {
		header |= av1NMask
	}
	out.open(payloadType, ts)
	out.write(header)
	for i, span := range p.spans {
		if count > 3 || i < count-1 {
			out.arena = appendLEB128(out.arena, span.length)
		}
		e := &p.elements[span.element]
		from, to := span.offset, span.offset+span.length
		if from < e.hlen {
			out.write(e.header[from:min(to, e.hlen)]...)
		}
		if to > e.hlen {
			out.write(e.payload[max(from-e.hlen, 0) : to-e.hlen]...)
		}
	}
	out.close()
}

// learn keeps a copy of a sequence header for later keyframes that lack one.
func (p *av1Packetizer) learn(e av1Element) {
	p.sequence = append(p.sequence[:0], e.payload...)
	p.seqHeader = e
	p.seqHeader.payload = p.sequence
}

// eachElement parses an AV1 low-overhead bitstream into elements, stopping
// at the first malformed OBU (false) or when fn returns false.
func (p *av1Packetizer) eachElement(data []byte, fn func(av1Element, int) bool) bool {
	for len(data) > 0 {
		var e av1Element
		if data[0]&0x80 != 0 {
			return false
		}
		e.hlen = 1
		if data[0]&0x04 != 0 {
			e.hlen = 2
		}
		if len(data) < e.hlen {
			return false
		}
		copy(e.header[:], data[:e.hlen])
		e.header[0] &^= 0x02
		obuType := int(data[0]&0x78) >> 3
		body := data[e.hlen:]
		size := len(body)
		if data[0]&0x02 != 0 {
			value, read, ok := readLEB128(body)
			if !ok || value > len(body)-read {
				return false
			}
			body, size = body[read:], value
		}
		e.payload = body[:size]
		data = body[size:]
		if !fn(e, obuType) {
			return true
		}
	}
	return true
}
