package rtpav

import (
	"context"

	"github.com/pion/rtp"
	pioncodecs "github.com/pion/rtp/codecs"
	"github.com/thesyncim/goav/av"
)

const defaultVideoClockRate = 90000
const defaultMaxVideoFrameSize = 4 << 20

// VideoDepacketizerOption configures a video depacketizer constructor.
type VideoDepacketizerOption func(*videoDepacketizerConfig)

type videoDepacketizerConfig struct {
	maxFrameSize int
}

// VP8Depacketizer reassembles VP8 RTP payloads into whole-frame av.Packets,
// detecting keyframes and dropping fragments interrupted by loss.
type VP8Depacketizer struct {
	packet    pioncodecs.VP8Packet
	assembler videoFrameAssembler
}

// VP9Depacketizer reassembles VP9 RTP payloads into whole-frame av.Packets,
// detecting keyframes and dropping fragments interrupted by loss.
type VP9Depacketizer struct {
	packet    pioncodecs.VP9Packet
	assembler videoFrameAssembler
}

var _ Depacketizer = (*VP8Depacketizer)(nil)
var _ Depacketizer = (*VP9Depacketizer)(nil)

type assemblerState uint8

const (
	asmIdle assemblerState = iota
	asmAssembling
	asmDropping
)

type syncRequestState uint8

const (
	syncReqNone syncRequestState = iota
	syncReqPending
	syncReqSent
)

type videoFrameAssembler struct {
	stream        av.Stream
	buffer        []byte
	state         assemblerState
	lossBefore    bool
	discontinuous bool
	syncReq       syncRequestState
	syncTimestamp uint32
	// keyframe is frame-level: detected on the first fragment, kept through
	// emission.
	keyframe  bool
	timestamp uint32
}

// WithMaxVideoFrameSize caps the bytes one reassembled video frame may
// reach; larger frames are refused with ErrFrameTooLarge.
func WithMaxVideoFrameSize(size int) VideoDepacketizerOption {
	return func(config *videoDepacketizerConfig) {
		config.maxFrameSize = size
	}
}

// NewVP8Depacketizer builds a VP8 depacketizer for the given stream.
func NewVP8Depacketizer(stream av.Stream, options ...VideoDepacketizerOption) *VP8Depacketizer {
	return &VP8Depacketizer{
		assembler: newVideoFrameAssembler(stream, av.CodecVP8, options...),
	}
}

// NewVP9Depacketizer builds a VP9 depacketizer for the given stream.
func NewVP9Depacketizer(stream av.Stream, options ...VideoDepacketizerOption) *VP9Depacketizer {
	return &VP9Depacketizer{
		assembler: newVideoFrameAssembler(stream, av.CodecVP9, options...),
	}
}

// Codec reports the codec this depacketizer reassembles.
func (d *VP8Depacketizer) Codec() av.CodecID {
	return av.CodecVP8
}

// PushInto consumes one RTP packet, appending a completed VP8 frame to out
// when the packet finishes one.
func (d *VP8Depacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	frame, err := d.packet.Unmarshal(pkt.Payload)
	if err != nil {
		return err
	}
	start := d.packet.S == 1 && d.packet.PID == 0
	keyframe := start && len(frame) > 0 && frame[0]&0x01 == 0
	return d.assembler.push(ctx, videoFragment{
		timestamp: pkt.Timestamp,
		clockRate: payloadClockRate(payload, d.assembler.stream),
		data:      frame,
		start:     start,
		end:       pkt.Marker,
		keyframe:  keyframe,
	}, out)
}

// FlushInto discards any partial frame at end of stream.
func (d *VP8Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	return nil
}

// HandleEvent resets reassembly state on discontinuities.
func (d *VP8Depacketizer) HandleEvent(ctx context.Context, event *av.Event) error {
	return d.assembler.handleEvent(ctx, event)
}

// Codec reports the codec this depacketizer reassembles.
func (d *VP9Depacketizer) Codec() av.CodecID {
	return av.CodecVP9
}

// PushInto consumes one RTP packet, appending a completed VP9 frame to out
// when the packet finishes one.
func (d *VP9Depacketizer) PushInto(ctx context.Context, pkt *rtp.Packet, payload PayloadCodec, out *DepacketizeResult) error {
	d.resetPacket()
	frame, err := d.packet.Unmarshal(pkt.Payload)
	if err != nil {
		return err
	}
	keyframe := d.packet.B && !d.packet.P
	end := pkt.Marker || d.packet.E
	return d.assembler.push(ctx, videoFragment{
		timestamp: pkt.Timestamp,
		clockRate: payloadClockRate(payload, d.assembler.stream),
		data:      frame,
		start:     d.packet.B,
		end:       end,
		keyframe:  keyframe,
	}, out)
}

// FlushInto discards any partial frame at end of stream.
func (d *VP9Depacketizer) FlushInto(context.Context, *DepacketizeResult) error {
	d.assembler.resetPartial()
	return nil
}

// HandleEvent resets reassembly state on discontinuities.
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

type videoFragment struct {
	timestamp uint32
	clockRate uint32
	data      []byte
	start     bool
	end       bool
	keyframe  bool
}

func (a *videoFrameAssembler) emitSinglePacketFrame(frag videoFragment, keyframe, lossBefore bool, out *DepacketizeResult) error {
	if len(frag.data) > cap(a.buffer) {
		a.dropUntilSync()
		return ErrFrameTooLarge
	}
	if len(out.Packets) == cap(out.Packets) {
		return ErrResultFull
	}
	if lossBefore {
		a.lossBefore = true
	}
	a.beginFrame(frag.timestamp, keyframe)
	if err := a.emitPacket(frag.timestamp, frag.clockRate, frag.data, a.keyframe, out); err != nil {
		return err
	}
	a.state = asmIdle
	return nil
}

func (a *videoFrameAssembler) push(ctx context.Context, frag videoFragment, out *DepacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch a.state {
	case asmIdle:
		if !frag.start {
			a.dropUntilSync()
			return a.emitPendingSync(frag.timestamp, out)
		}
		if frag.end {
			return a.emitSinglePacketFrame(frag, frag.keyframe, false, out)
		}
		a.beginFrame(frag.timestamp, frag.keyframe)
	case asmAssembling:
		if frag.start {
			// Previous frame never saw its end marker (truncated frame).
			if !frag.keyframe {
				a.dropUntilSync()
				return a.emitPendingSync(frag.timestamp, out)
			}
			if frag.end {
				return a.emitSinglePacketFrame(frag, true, true, out)
			}
			a.lossBefore = true
			a.beginFrame(frag.timestamp, true)
		} else if frag.timestamp != a.timestamp {
			a.dropUntilSync()
			return a.emitPendingSync(frag.timestamp, out)
		}
	case asmDropping:
		if !frag.start {
			return a.emitPendingSync(frag.timestamp, out)
		}
		if !frag.keyframe {
			a.clearPartial()
			return a.requestSyncOnFrame(frag.timestamp, out)
		}
		if frag.end {
			return a.emitSinglePacketFrame(frag, true, false, out)
		}
		a.beginFrame(frag.timestamp, true)
	}

	if len(a.buffer)+len(frag.data) > cap(a.buffer) {
		a.dropUntilSync()
		return ErrFrameTooLarge
	}
	if frag.end {
		if len(out.Packets) == cap(out.Packets) {
			return ErrResultFull
		}
		packetPayload := frag.data
		if len(a.buffer) != 0 {
			a.buffer = append(a.buffer, frag.data...)
			packetPayload = a.buffer
		}
		if err := a.emitPacket(frag.timestamp, frag.clockRate, packetPayload, a.keyframe, out); err != nil {
			return err
		}
		a.state = asmIdle
		return nil
	}
	a.buffer = append(a.buffer, frag.data...)
	return nil
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
		if eventMatchesStream(a.stream, event) {
			a.markLoss(false)
		}
	case av.EventDiscontinuity:
		if eventMatchesStream(a.stream, event) {
			a.markLoss(true)
		}
	case av.EventCodecChanged:
		if applyCodecChangedEvent(&a.stream, a.stream.Codec.ID, event) {
			a.markLoss(true)
		}
	}
	return nil
}

func (a *videoFrameAssembler) beginFrame(timestamp uint32, keyframe bool) {
	a.state = asmAssembling
	a.syncReq = syncReqNone
	a.timestamp = timestamp
	a.keyframe = keyframe
	a.buffer = a.buffer[:0]
}

func (a *videoFrameAssembler) resumeWithBufferedParams(timestamp uint32) {
	a.state = asmAssembling
	a.syncReq = syncReqNone
	a.timestamp = timestamp
	a.keyframe = false
}

func (a *videoFrameAssembler) markLoss(discontinuous bool) {
	a.state = asmDropping
	a.lossBefore = true
	if discontinuous {
		a.discontinuous = true
	}
	a.syncReq = syncReqPending
	a.clearPartial()
}

func (a *videoFrameAssembler) dropUntilSync() {
	if a.state != asmDropping {
		a.syncReq = syncReqPending
	}
	a.state = asmDropping
	a.lossBefore = true
	a.clearPartial()
}

func (a *videoFrameAssembler) clearPartial() {
	a.buffer = a.buffer[:0]
	a.keyframe = false
	a.timestamp = 0
}

func (a *videoFrameAssembler) resetPartial() {
	if a.state == asmAssembling {
		a.state = asmIdle
	}
	a.clearPartial()
}

func (a *videoFrameAssembler) requestSyncOnFrame(timestamp uint32, out *DepacketizeResult) error {
	if a.syncReq != syncReqSent || a.syncTimestamp != timestamp {
		a.syncReq = syncReqPending
	}
	return a.emitPendingSync(timestamp, out)
}

func (a *videoFrameAssembler) emitPendingSync(timestamp uint32, out *DepacketizeResult) error {
	if a.syncReq != syncReqPending {
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
	a.syncReq = syncReqSent
	a.syncTimestamp = timestamp
	return nil
}

func (a *videoFrameAssembler) emitPacket(timestamp uint32, clockRate uint32, payload []byte, keyframe bool, out *DepacketizeResult) error {
	if len(out.Packets) == cap(out.Packets) {
		return ErrResultFull
	}
	packet := av.Packet{
		StreamID:   a.stream.ID,
		Type:       a.stream.Type,
		CodecEpoch: a.stream.Epoch,
		Payload: av.Buffer{
			Bytes: payload,
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

// vp8Packetizer sends a VP8 frame (RFC 7741) behind a four-byte payload
// descriptor carrying a 15-bit picture id, split into even fragments.
type vp8Packetizer struct {
	budget    int
	clock     uint32
	pictureID uint16
}

const vp8DescriptorSize = 4

func (p *vp8Packetizer) Codec() av.CodecID { return av.CodecVP8 }

func (p *vp8Packetizer) PacketizeInto(ctx context.Context, packet *av.Packet, payload PayloadCodec, out *PacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frame := packet.Payload.Bytes
	if len(frame) == 0 {
		return ErrInvalidPayload
	}
	ts, err := rtpTimestamp(packet, payload, p.clock)
	if err != nil {
		return err
	}
	chunk := evenChunk(len(frame), p.budget-vp8DescriptorSize)
	for offset := 0; offset < len(frame); offset += chunk {
		first := byte(0x80) // X: the extension byte follows
		if offset == 0 {
			first |= 0x10 // S: start of partition 0
		}
		out.open(payload.PayloadType, ts)
		out.write(first, 0x80, 0x80|byte(p.pictureID>>8), byte(p.pictureID))
		out.write(frame[offset:min(offset+chunk, len(frame))]...)
		out.close()
	}
	out.seal(true)
	p.pictureID = (p.pictureID + 1) & 0x7fff
	return nil
}

// vp9Packetizer sends a VP9 frame in non-flexible mode (RFC 9628): a 15-bit
// picture id on every packet, P on inter frames, and on a keyframe's first
// packet the scalability structure with its resolution.
type vp9Packetizer struct {
	budget    int
	clock     uint32
	pictureID uint16
	// width and height are the stream's, for keyframes whose header does not
	// parse.
	width, height int
}

const (
	vp9DescriptorSize = 3
	vp9SSSize         = 8
)

func (p *vp9Packetizer) Codec() av.CodecID { return av.CodecVP9 }

func (p *vp9Packetizer) PacketizeInto(ctx context.Context, packet *av.Packet, payload PayloadCodec, out *PacketizeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	frame := packet.Payload.Bytes
	if len(frame) == 0 {
		return ErrInvalidPayload
	}
	ts, err := rtpTimestamp(packet, payload, p.clock)
	if err != nil {
		return err
	}
	keyframe, width, height, ok := vp9FrameInfo(frame)
	if !ok {
		keyframe, width, height = packet.Keyframe, p.width, p.height
	}
	for offset := 0; offset < len(frame); {
		room := p.budget - vp9DescriptorSize
		ss := keyframe && offset == 0 && width > 0 && height > 0
		if ss {
			room -= vp9SSSize
		}
		end := min(offset+evenChunk(len(frame)-offset, room), len(frame))
		first := byte(0x80 | 0x01) // I: picture id; Z: no upper spatial layer
		if !keyframe {
			first |= 0x40 // P: inter-picture predicted
		}
		if offset == 0 {
			first |= 0x08 // B: start of frame
		}
		if end == len(frame) {
			first |= 0x04 // E: end of frame
		}
		if ss {
			first |= 0x02 // V: scalability structure follows
		}
		out.open(payload.PayloadType, ts)
		out.write(first, 0x80|byte(p.pictureID>>8), byte(p.pictureID))
		if ss {
			// One spatial layer with its resolution (N_S=0, Y=1, G=1) and a
			// one-picture group: TID 0, switching up point, one reference
			// one picture back.
			out.write(0x18, byte(width>>8), byte(width), byte(height>>8), byte(height), 0x01, 0x14, 0x01)
		}
		out.write(frame[offset:end]...)
		out.close()
		offset = end
	}
	out.seal(true)
	p.pictureID = (p.pictureID + 1) & 0x7fff
	return nil
}

// vp9FrameInfo reads the uncompressed header of the frame's first frame: a
// keyframe, and a keyframe's resolution. ok is false when it does not parse.
func vp9FrameInfo(frame []byte) (keyframe bool, width int, height int, ok bool) {
	bits := bitReader{data: frame}
	if bits.read(2) != 2 { // frame_marker
		return false, 0, 0, false
	}
	profile := bits.read(1) | bits.read(1)<<1
	if profile == 3 {
		bits.read(1)
	}
	if bits.read(1) == 1 { // show_existing_frame
		return false, 0, 0, !bits.short
	}
	if bits.read(1) == 1 { // frame_type: inter
		return false, 0, 0, !bits.short
	}
	bits.read(2) // show_frame, error_resilient_mode
	if bits.read(24) != 0x498342 {
		return false, 0, 0, false
	}
	if profile >= 2 {
		bits.read(1) // ten_or_twelve_bit
	}
	if bits.read(3) != 7 { // color_space other than sRGB
		bits.read(1) // color_range
		if profile == 1 || profile == 3 {
			bits.read(3) // subsampling_x, subsampling_y, reserved_zero
		}
	} else if profile == 1 || profile == 3 {
		bits.read(1) // reserved_zero
	}
	width = int(bits.read(16)) + 1
	height = int(bits.read(16)) + 1
	return true, width, height, !bits.short
}

// bitReader reads big-endian bit fields; short reports reading past the end.
type bitReader struct {
	data  []byte
	pos   int
	short bool
}

func (b *bitReader) read(n int) uint32 {
	var v uint32
	for range n {
		if b.pos >= len(b.data)*8 {
			b.short = true
			return 0
		}
		v = v<<1 | uint32(b.data[b.pos/8]>>(7-b.pos%8)&1)
		b.pos++
	}
	return v
}
