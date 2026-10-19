package ts

import (
	"context"
	"fmt"
	"io"
	"math/bits"
	"strconv"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// MuxerOptions configure the muxer. The zero value is a variable-bitrate
// stream whose PCR runs 700ms behind its DTS.
type MuxerOptions struct {
	// MuxRate, in bits per second, makes the stream constant-bitrate: the
	// muxer sends packets at that rate on its PCR clock and fills the gaps
	// with null packets. A rate the media outruns delivers it late instead.
	MuxRate int
	// Delay is how far the PCR runs behind the DTS, the time a receiver
	// buffers before decoding (700ms, as ffmpeg, by default). Timestamps are
	// written as they come, lifted by one offset only when the first DTS is
	// below Delay, so the PCR never starts negative.
	Delay time.Duration
}

const (
	defaultDelay = 700 * time.Millisecond
	// maxPCRGap is the longest PCR interval written (ISO/IEC 13818-1 allows
	// 100ms; DVB asks for 40ms).
	maxPCRGap = 40 * clock27M / 1000
	// psiPeriod is the longest gap between PAT/PMT repetitions.
	psiPeriod = 100 * clock27M / 1000
	// pcrJump is the gap past which the muxer re-times instead of filling
	// it with PCR packets: a pause, not media.
	pcrJump = clock27M
)

// FormatMuxer writes one program as a transport stream; it is a
// format.Muxer.
type FormatMuxer struct {
	options MuxerOptions
	w       io.Writer
	streams []muxStream
	pcr     int // index of the stream carrying the PCR
	video   bool
	pat     []byte
	pmt     []byte
	patCC   byte
	pmtCC   byte

	started bool
	shift   int64  // 90 kHz offset added to every timestamp
	delay   int64  // in 27 MHz
	clock   int64  // 27 MHz; at a constant bitrate, the time of packet 0
	sent    uint64 // packets written, the constant-bitrate clock
	spacing int64  // 27 MHz per packet at a constant bitrate
	lastPCR int64
	lastPSI int64
	resync  bool // the next PCR carries discontinuity_indicator
	pcrDue  bool // the clock was re-timed and awaits its PCR

	out     []byte
	pes     [19]byte
	adts    [7]byte
	opus    []byte
	payload gather
	closed  bool
	err     error
}

type muxStream struct {
	id        av.StreamID
	pid       uint16
	codec     av.CodecID
	video     bool
	streamID  byte
	cc        byte
	params    []byte // Annex B parameter sets for keyframes that lack them
	aac       aacConfig
	opusTrim  int // pre-skip, the first packet's start trim
	opusFirst bool
}

// NewFormatMuxer returns a transport stream muxer.
func NewFormatMuxer(options MuxerOptions) *FormatMuxer {
	return &FormatMuxer{options: options}
}

func (m *FormatMuxer) Format() av.FormatID { return av.FormatTS }

// Open binds the output and the program's streams.
func (m *FormatMuxer) Open(ctx context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return fmt.Errorf("%w: no writer", ErrInvalidData)
	}
	if len(streams) == 0 || len(streams) > 64 {
		return fmt.Errorf("%w: a program of %d streams", ErrUnsupported, len(streams))
	}
	if m.options.MuxRate < 0 || m.options.Delay < 0 {
		return fmt.Errorf("%w: negative MuxRate or Delay", ErrInvalidData)
	}
	*m = FormatMuxer{options: m.options, w: output.Writer, pcr: -1, out: m.out[:0]}
	if m.options.MuxRate > 0 {
		m.spacing = max(1, packetSize*8*clock27M/int64(m.options.MuxRate))
	}
	delay := m.options.Delay
	if delay == 0 {
		delay = defaultDelay
	}
	m.delay = delay.Nanoseconds() * clock27M / int64(time.Second)
	es := make([]esInfo, 0, len(streams))
	used := map[uint16]bool{pidPAT: true, pidPMT: true, pidNull: true}
	for i := range streams {
		st, info, err := newMuxStream(streams[i])
		if err != nil {
			return err
		}
		st.pid = streamPID(streams[i].ID, uint16(pidStream+i), used)
		info.pid = st.pid
		m.streams = append(m.streams, st)
		es = append(es, info)
		if st.video && !m.video {
			m.video, m.pcr = true, i
		}
	}
	if m.pcr < 0 {
		m.pcr = 0
	}
	m.pat = patSection(pidPMT)
	m.pmt = pmtSection(m.streams[m.pcr].pid, es)
	if len(m.pmt) > payloadSize-1 {
		return fmt.Errorf("%w: a PMT longer than one packet", ErrUnsupported)
	}
	return nil
}

// streamPID keeps a stream id that is a free elementary PID, as a stream
// demuxed from a transport stream has, and assigns fallback otherwise.
func streamPID(id av.StreamID, fallback uint16, used map[uint16]bool) uint16 {
	pid := fallback
	if v, err := strconv.ParseUint(string(id), 10, 13); err == nil && v >= pidFirstFree && v < pidNull && !used[uint16(v)] {
		pid = uint16(v)
	}
	for used[pid] {
		pid++
	}
	used[pid] = true
	return pid
}

func newMuxStream(stream av.Stream) (muxStream, esInfo, error) {
	params := stream.Codec
	st := muxStream{id: stream.ID, codec: params.ID}
	info := esInfo{}
	switch params.ID {
	case av.CodecH264, av.CodecHEVC:
		st.video, st.streamID = true, streamIDVideo
		info.streamType = streamTypeH264
		if params.ID == av.CodecHEVC {
			info.streamType = streamTypeHEVC
		}
		sets, err := parameterSets(params.ID, params.ExtraData.Bytes)
		if err != nil {
			return st, info, fmt.Errorf("%w: stream %q %s configuration: %w", ErrInvalidData, stream.ID, params.ID, err)
		}
		st.params = sets
	case av.CodecAAC:
		config, err := aacConfigOf(params)
		if err != nil {
			return st, info, fmt.Errorf("stream %q: %w", stream.ID, err)
		}
		st.aac, st.streamID, info.streamType = config, streamIDAudio, streamTypeAAC
	case av.CodecOpus:
		code, preSkip, err := opusChannelCode(params)
		if err != nil {
			return st, info, fmt.Errorf("stream %q: %w", stream.ID, err)
		}
		st.streamID, info.streamType = streamIDPrivate1, streamTypePrivate
		st.opusTrim, st.opusFirst = preSkip, true
		info.descriptors = []byte{descriptorRegistration, 4, 'O', 'p', 'u', 's', descriptorExtension, 2, extensionOpus, code}
	default:
		return st, info, fmt.Errorf("%w: a transport stream carries H.264, HEVC, AAC, and Opus, not %q (stream %q)", ErrUnsupported, params.ID, stream.ID)
	}
	if lang := stream.Language; len(lang) == 3 {
		info.descriptors = append(info.descriptors, descriptorLanguage, 4, lang[0], lang[1], lang[2], 0)
	}
	return st, info, nil
}

// Write muxes one packet as a PES packet, preceded when due by the PAT and
// PMT, PCR packets, and, at a constant bitrate, null packets.
func (m *FormatMuxer) Write(ctx context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.closed {
		return ErrClosed
	}
	if m.err != nil {
		return m.err
	}
	if packet == nil {
		return fmt.Errorf("%w: nil packet", ErrInvalidData)
	}
	index := -1
	for i := range m.streams {
		if m.streams[i].id == packet.StreamID {
			index = i
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("%w: packet for stream %q, which the muxer was not opened with", ErrInvalidData, packet.StreamID)
	}
	st := &m.streams[index]
	pts, ok := ticks90k(packet.PTS)
	if !ok {
		return fmt.Errorf("%w: stream %q packet without a presentation time", ErrInvalidData, st.id)
	}
	dts, ok := ticks90k(packet.DTS)
	if !ok || !packet.DTS.Base.Valid() {
		dts = pts
	}
	if !m.started {
		m.shift = max(0, m.delay/pcrPerPTS-dts)
	}
	pts += m.shift
	dts += m.shift
	send := dts*pcrPerPTS - m.delay
	if packet.Discontinuous && m.started {
		m.resync = true
	}

	keyframe := packet.Keyframe && (st.video || !m.video)
	if err := m.buildPayload(st, packet, keyframe); err != nil {
		return err
	}
	m.out = m.out[:0]
	carriesPCR := index == m.pcr && m.options.MuxRate == 0
	m.advance(send)
	if !m.started || keyframe && st.video || m.psiDue() {
		m.writePSI()
	}
	if m.pcrDue && !carriesPCR {
		m.writePCR(m.now())
	}
	m.writePES(st, pts, dts, keyframe, carriesPCR)
	m.started = true
	if _, err := m.w.Write(m.out); err != nil {
		m.err = err
		return err
	}
	return nil
}

// ticks90k reads a timestamp in 90 kHz ticks.
func ticks90k(t av.Timestamp) (int64, bool) {
	v, ok := t.Rescale(av.TimeBase{Num: 1, Den: clock90k})
	return v.Value, ok
}

// now is the PCR clock, in 27 MHz: at a variable bitrate the send time of
// the PES being written; at a constant bitrate the time of the next packet
// at MuxRate.
func (m *FormatMuxer) now() int64 {
	if m.options.MuxRate == 0 {
		return m.clock
	}
	hi, lo := bits.Mul64(m.sent*packetSize*8, clock27M)
	q, _ := bits.Div64(hi, lo, uint64(m.options.MuxRate))
	return m.clock + int64(q)
}

// psiDue reports a PAT and PMT due: within maxPCRGap of psiPeriod, so the
// next chance, at most maxPCRGap later, does not overrun it.
func (m *FormatMuxer) psiDue() bool {
	return m.now()-m.lastPSI >= psiPeriod-maxPCRGap
}

// advance moves the PCR clock to the send time of the next PES (its DTS less
// Delay). At a variable bitrate, PCR-only packets keep PCRs at most
// maxPCRGap apart across the step, the PAT and PMT repeating among them; at
// a constant bitrate, null packets carry the clock there, and media that
// runs late goes out at once. The first packet, a discontinuity, and a jump
// past pcrJump (a pause) re-time the clock instead: a PCR at the new time
// is due, after the first one with discontinuity_indicator set.
func (m *FormatMuxer) advance(send int64) {
	if step := send - m.now(); !m.started || m.resync || step > pcrJump || step < -pcrJump {
		m.resync = m.started
		m.clock += step
		m.pcrDue = true
		return
	}
	if m.options.MuxRate > 0 {
		for m.now() < send {
			if m.psiDue() {
				m.writePSI()
			} else {
				m.writeNull()
			}
		}
		return
	}
	target := max(m.clock, send)
	for target-m.lastPCR > maxPCRGap {
		m.clock = m.lastPCR + maxPCRGap
		m.writePCR(m.clock)
		if m.psiDue() {
			m.writePSI()
		}
	}
	m.clock = target
}

// buildPayload gathers the PES payload of a packet: AUD and parameter sets
// for video that lacks them, the ADTS header raw AAC needs, Opus's control
// header.
func (m *FormatMuxer) buildPayload(st *muxStream, packet *av.Packet, keyframe bool) error {
	data := packet.Payload.Bytes
	m.payload.reset()
	switch st.codec {
	case av.CodecH264, av.CodecHEVC:
		au := readAccessUnit(st.codec, data)
		if !au.annexB {
			return fmt.Errorf("%w: stream %q %s packet is not Annex B", ErrInvalidData, st.id, st.codec)
		}
		if au.audEnd == 0 {
			if st.codec == av.CodecHEVC {
				m.payload.add(hevcAUDUnit)
			} else {
				m.payload.add(h264AUDUnit)
			}
		} else {
			m.payload.add(data[:au.audEnd])
			data = data[au.audEnd:]
		}
		if keyframe && !au.params {
			m.payload.add(st.params)
		}
	case av.CodecAAC:
		if len(data)+7 > 0x1fff {
			return fmt.Errorf("%w: stream %q AAC frame of %d bytes is longer than ADTS allows", ErrInvalidData, st.id, len(data))
		}
		st.aac.adtsHeader(&m.adts, len(data))
		m.payload.add(m.adts[:])
	case av.CodecOpus:
		trim := 0
		if st.opusFirst {
			trim, st.opusFirst = st.opusTrim, false
		}
		m.opus = opusControlHeader(m.opus[:0], len(data), trim)
		m.payload.add(m.opus)
	}
	m.payload.add(data)
	return nil
}

// writePES packetizes the gathered payload behind its PES header, the PCR
// in the first packet's adaptation field when pcr is set.
func (m *FormatMuxer) writePES(st *muxStream, pts, dts int64, keyframe, pcr bool) {
	h := m.pes[:0]
	h = append(h, 0, 0, 1, st.streamID, 0, 0, 0x84)
	if dts != pts {
		h = append(h, 0xc0, 10)
		h = appendTimestamp(h, 3, pts)
		h = appendTimestamp(h, 1, dts)
	} else {
		h = append(h, 0x80, 5)
		h = appendTimestamp(h, 2, pts)
	}
	if n := len(h) - 6 + m.payload.size; n <= 0xffff {
		h[4], h[5] = byte(n>>8), byte(n)
	} // else 0: unbounded, which only video may be
	m.payload.prepend(h)
	for first := true; m.payload.size > 0; first = false {
		var flags byte
		if first && keyframe {
			flags |= flagRandomAccess
		}
		if first && pcr {
			flags |= flagPCR
		}
		m.writePacket(st.pid, &st.cc, first, flags, &m.payload)
	}
}

// appendTimestamp appends a 33-bit PES timestamp behind its 4-bit prefix.
func appendTimestamp(b []byte, prefix byte, t int64) []byte {
	v := uint64(t) & uint64(wrap33-1)
	return append(b,
		prefix<<4|byte(v>>29)&0x0e|1,
		byte(v>>22),
		byte(v>>14)|1,
		byte(v>>7),
		byte(v<<1)|1,
	)
}

// writePSI writes the PAT and the PMT.
func (m *FormatMuxer) writePSI() {
	m.lastPSI = m.now()
	for _, s := range [2]struct {
		pid     uint16
		cc      *byte
		section []byte
	}{{pidPAT, &m.patCC, m.pat}, {pidPMT, &m.pmtCC, m.pmt}} {
		p := m.dataPacket()
		writeHeader(p, s.pid, true, 0x10|*s.cc)
		*s.cc = (*s.cc + 1) & 0xf
		p[4] = 0 // pointer_field
		n := copy(p[5:], s.section)
		for i := 5 + n; i < packetSize; i++ {
			p[i] = 0xff
		}
	}
}

// writePCR writes a PCR-only packet on the PCR PID; its continuity counter
// stays, as it carries no payload.
func (m *FormatMuxer) writePCR(clock int64) {
	st := &m.streams[m.pcr]
	p := m.packet()
	writeHeader(p, st.pid, false, 0x20|(st.cc+15)&0xf) // the last payload packet's counter
	p[4] = payloadSize - 1
	p[5] = flagPCR
	if m.resync {
		p[5] |= flagDiscontinuity
	}
	putPCR(p[6:], clock)
	for i := 12; i < packetSize; i++ {
		p[i] = 0xff
	}
	m.notePCR(clock)
}

func (m *FormatMuxer) notePCR(clock int64) {
	m.lastPCR = clock
	m.resync, m.pcrDue = false, false
}

func (m *FormatMuxer) writeNull() {
	p := m.dataPacket()
	writeHeader(p, pidNull, false, 0x10)
	for i := 4; i < packetSize; i++ {
		p[i] = 0xff
	}
}

// dataPacket appends a packet that is not a PCR, preceded at a constant
// bitrate by the PCR packet due before it.
func (m *FormatMuxer) dataPacket() []byte {
	if m.options.MuxRate > 0 && !m.pcrDue && m.now()+m.spacing-m.lastPCR > maxPCRGap {
		m.writePCR(m.now())
	}
	return m.packet()
}

// packet appends one packet to the output and returns it.
func (m *FormatMuxer) packet() []byte {
	n := len(m.out)
	m.out = append(m.out, make([]byte, packetSize)...)
	m.sent++
	return m.out[n : n+packetSize]
}

func writeHeader(p []byte, pid uint16, start bool, control byte) {
	p[0] = syncByte
	p[1] = byte(pid>>8) & 0x1f
	if start {
		p[1] |= 0x40
	}
	p[2] = byte(pid)
	p[3] = control
}

// writePacket writes one packet of payload: the adaptation field when flags
// ask for one (with flagPCR, the PCR at the clock's time), stuffing it out
// when the payload ends short.
func (m *FormatMuxer) writePacket(pid uint16, cc *byte, start bool, flags byte, payload *gather) {
	p := m.dataPacket()
	af := 0
	if flags != 0 {
		af = 2
		if flags&flagPCR != 0 {
			af += 6
		}
	}
	if room := payloadSize - af; payload.size < room {
		af += room - payload.size
	}
	control := byte(0x10)
	if af > 0 {
		control = 0x30
	}
	writeHeader(p, pid, start, control|*cc)
	*cc = (*cc + 1) & 0xf
	if af > 0 {
		p[4] = byte(af - 1)
	}
	if af > 1 {
		at := 6
		if flags&flagPCR != 0 {
			if m.resync {
				flags |= flagDiscontinuity
			}
			putPCR(p[6:], m.now())
			m.notePCR(m.now())
			at = 12
		}
		p[5] = flags
		for i := at; i < 4+af; i++ {
			p[i] = 0xff
		}
	}
	payload.read(p[4+af:])
}

// putPCR writes a 27 MHz clock as a 6-byte PCR: a 33-bit 90 kHz base and a
// 9-bit extension.
func putPCR(p []byte, clock int64) {
	clock %= wrap33 * pcrPerPTS
	if clock < 0 {
		clock += wrap33 * pcrPerPTS
	}
	base, ext := uint64(clock/pcrPerPTS), uint64(clock%pcrPerPTS)
	p[0] = byte(base >> 25)
	p[1] = byte(base >> 17)
	p[2] = byte(base >> 9)
	p[3] = byte(base >> 1)
	p[4] = byte(base&1)<<7 | 0x7e | byte(ext>>8)
	p[5] = byte(ext)
}

// Close ends the stream. It writes nothing more and leaves the writer open.
func (m *FormatMuxer) Close() error {
	m.closed = true
	return nil
}

// gather is a payload made of up to five byte slices, read in order
// without copying them together.
type gather struct {
	parts [5][]byte
	n     int
	size  int
}

func (g *gather) reset() { *g = gather{} }

func (g *gather) add(b []byte) {
	if len(b) > 0 {
		g.parts[g.n] = b
		g.n++
		g.size += len(b)
	}
}

func (g *gather) prepend(b []byte) {
	copy(g.parts[1:g.n+1], g.parts[:g.n])
	g.parts[0] = b
	g.n++
	g.size += len(b)
}

// read fills dst from the head of the payload.
func (g *gather) read(dst []byte) {
	for len(dst) > 0 && g.n > 0 {
		n := copy(dst, g.parts[0])
		dst = dst[n:]
		g.size -= n
		if g.parts[0] = g.parts[0][n:]; len(g.parts[0]) == 0 {
			copy(g.parts[:], g.parts[1:g.n])
			g.n--
		}
	}
}
