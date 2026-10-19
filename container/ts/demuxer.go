package ts

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/h264"
)

const (
	// probeWindow bounds how far Open reads for the program and each
	// stream's codec parameters.
	probeWindow = 8 << 20
	// probeSpan bounds it in media time too (5s, as ffmpeg analyzes): a live
	// input whose PMT names a stream that sends nothing opens without its
	// parameters, which arrive with av.EventCodecChanged if it ever does.
	probeSpan = 5 * clock90k
	// maxPES bounds one reassembled PES packet; a longer one is dropped.
	maxPES = 8 << 20
)

var timeBase = av.TimeBase{Num: 1, Den: clock90k}

// FormatDemuxer reads the first program of a transport stream; it is a
// format.Demuxer. Open reads until the PMT and every stream's codec
// parameters are known (at most 8 MiB or 5s of media), keeping the packets
// it read.
type FormatDemuxer struct {
	r      io.Reader
	buf    []byte
	start  int
	end    int
	eof    bool // the reader is exhausted
	done   bool // and every packet read
	synced bool
	read   int64
	open   bool
	closed atomic.Bool // Close may race a ReadInto blocked on the reader

	pat        sectionBuffer
	pmt        sectionBuffer
	program    uint16
	pmtPID     int
	pmtVersion int
	pcrPID     int
	lastPCR    int64
	havePCR    bool

	streams   []*demuxStream
	byPID     [pidNull + 1]uint8 // index+1 into streams
	declared  []av.Stream
	nextIndex int
	skipped   []byte // the stream types of the PMT goav does not read

	queue []unit
	head  int
	pool  [][]byte

	firstDTS int64 // the probe's media span: the first DTS read, and the last
	lastDTS  int64
	spanned  bool
	ref      int64 // the last timestamp, which the next unwraps against
	haveRef  bool
	broken   bool // a time-base discontinuity is due before the next packet
}

type demuxStream struct {
	pid        uint16
	streamType byte
	stream     av.Stream
	opusCode   byte
	announced  bool
	ready      bool

	cc         int
	pes        []byte
	collecting bool
	damaged    bool
	lossNext   bool
	breakNext  bool
	changed    bool // configured after Open announced it: EventCodecChanged is due
	pesLoss    bool // the PES being read follows lost data

	tail     []byte // an ADTS frame the next PES completes
	next     int64  // the time of the next audio frame
	haveNext bool
	last     int64 // the last DTS, for a PES without timestamps
}

// unit is one queued read: a packet of stream, or an event when stream is
// nil.
type unit struct {
	stream   *demuxStream
	event    av.Event
	data     []byte
	pts, dts int64
	duration av.Duration
	keyframe bool
	corrupt  bool
	loss     bool
	broken   bool
	epoch    av.Epoch
}

// NewFormatDemuxer returns a transport stream demuxer.
func NewFormatDemuxer() *FormatDemuxer { return &FormatDemuxer{} }

func (d *FormatDemuxer) Format() av.FormatID { return av.FormatTS }

// Open binds the input and reads until the program's streams are known.
func (d *FormatDemuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r := input.Reader
	if r == nil && input.ReaderAt != nil {
		size := input.Size
		if size <= 0 {
			size = math.MaxInt64
		}
		r = io.NewSectionReader(input.ReaderAt, 0, size)
	}
	if r == nil {
		return fmt.Errorf("%w: no reader", ErrInvalidData)
	}
	*d = FormatDemuxer{r: r, buf: make([]byte, 0, 512*packetSize), pmtPID: -1, pmtVersion: -1, pcrPID: -1}
	for !d.probed() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.step(); err == io.EOF {
			d.finishAll()
			break
		} else if err != nil {
			return readError(ctx, err)
		}
	}
	if d.pmtVersion < 0 {
		return fmt.Errorf("%w: no program in the first %d bytes", ErrInvalidData, d.read)
	}
	if len(d.streams) == 0 {
		return fmt.Errorf("%w: the program carries stream types % #x, none of them H.264, HEVC, AAC, or Opus", ErrUnsupported, d.skipped)
	}
	for _, st := range d.streams {
		st.announced = true
		d.declared = append(d.declared, st.stream)
	}
	d.open = true
	return nil
}

// probed reports whether Open has read enough: the PMT and every stream's
// parameters, or the whole probe window.
func (d *FormatDemuxer) probed() bool {
	if d.read >= probeWindow {
		return true
	}
	if d.pmtVersion < 0 {
		return false
	}
	if d.spanned && d.lastDTS-d.firstDTS >= probeSpan {
		return true
	}
	for _, st := range d.streams {
		if !st.ready {
			return false
		}
	}
	return true
}

// Streams returns the program's streams as Open found them.
func (d *FormatDemuxer) Streams() []av.Stream {
	out := make([]av.Stream, len(d.declared))
	for i, stream := range d.declared {
		out[i] = cloneStream(stream)
	}
	return out
}

func cloneStream(stream av.Stream) av.Stream {
	stream.Codec.ExtraData.Bytes = bytes.Clone(stream.Codec.ExtraData.Bytes)
	return stream
}

// ReadInto returns the next packet or event.
func (d *FormatDemuxer) ReadInto(ctx context.Context, out *format.ReadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil || out.Packet == nil {
		return format.ErrNilPacket
	}
	if d.closed.Load() || d.r == nil {
		return io.EOF
	}
	for d.head == len(d.queue) {
		if d.done {
			return io.EOF
		}
		if err := d.step(); err == io.EOF {
			d.finishAll()
		} else if err != nil {
			return readError(ctx, err)
		}
	}
	u := d.queue[d.head]
	d.queue[d.head] = unit{}
	if d.head++; d.head == len(d.queue) {
		d.queue, d.head = d.queue[:0], 0
	}
	if u.stream == nil {
		return out.AddEvent(u.event)
	}
	p := out.Packet
	payload := p.Payload.Bytes[:0]
	p.Reset()
	if p.Payload.Owner != nil {
		payload = nil
	}
	p.Payload.Bytes = append(payload, u.data...)
	d.pool = append(d.pool, u.data[:0])
	p.StreamID = u.stream.stream.ID
	p.Type = u.stream.stream.Type
	p.CodecEpoch = u.epoch
	p.PTS = av.Timestamp{Value: u.pts, Base: timeBase}
	p.DTS = av.Timestamp{Value: u.dts, Base: timeBase}
	p.Duration = u.duration
	p.Keyframe = u.keyframe
	p.Corrupt = u.corrupt
	p.LossBefore = u.loss
	p.Discontinuous = u.broken
	out.PacketReady = true
	return nil
}

func (d *FormatDemuxer) Close() error {
	d.closed.Store(true)
	return nil
}

// readError is a failed read: the run's end when its context is done (the
// runtime closes a live input's reader then), else the reader's error.
func readError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// step reads and handles one transport packet.
func (d *FormatDemuxer) step() error {
	p, err := d.nextPacket()
	if err != nil {
		return err
	}
	d.handle(p)
	return nil
}

// nextPacket returns the next 188-byte packet, resynchronizing on two sync
// bytes a packet apart after garbage or a lost packet boundary.
func (d *FormatDemuxer) nextPacket() ([]byte, error) {
	for {
		// Synced, a packet is enough; finding sync takes the next sync byte too.
		need := packetSize
		if !d.synced {
			need++
		}
		if d.end-d.start < need && !d.eof {
			if err := d.fill(); err != nil {
				return nil, err
			}
			continue
		}
		b := d.buf[d.start:d.end]
		if len(b) < packetSize {
			d.read += int64(len(b))
			d.start = d.end
			return nil, io.EOF
		}
		if b[0] == syncByte && (d.synced || len(b) == packetSize || b[packetSize] == syncByte) {
			d.synced = true
			d.start += packetSize
			d.read += packetSize
			return b[:packetSize], nil
		}
		if d.synced {
			d.synced = false
			for _, st := range d.streams {
				st.damaged = st.damaged || st.collecting
			}
		}
		skip := bytes.IndexByte(b[1:], syncByte) + 1
		if skip == 0 {
			skip = len(b)
		}
		d.start += skip
		d.read += int64(skip)
	}
}

func (d *FormatDemuxer) fill() error {
	if d.start > 0 {
		d.end = copy(d.buf[:cap(d.buf)], d.buf[d.start:d.end])
		d.start = 0
	}
	n, err := d.r.Read(d.buf[d.end:cap(d.buf)])
	d.end += n
	d.buf = d.buf[:d.end]
	if err == io.EOF {
		d.eof = true
		return nil
	}
	return err
}

// handle routes one packet: the PAT, the program's PMT, or a stream.
func (d *FormatDemuxer) handle(p []byte) {
	pid := uint16(p[1]&0x1f)<<8 | uint16(p[2])
	st := d.stream(pid)
	if p[1]&0x80 != 0 { // transport_error_indicator
		if st != nil {
			st.damaged, st.lossNext = st.damaged || st.collecting, true
		}
		return
	}
	start := p[1]&0x40 != 0
	control := p[3] >> 4 & 3
	payload := p[4:]
	discontinuity := false
	if control&2 != 0 {
		n := int(p[4])
		if n > payloadSize-1 {
			return
		}
		if n > 0 {
			flags := p[5]
			discontinuity = flags&0x80 != 0
			if flags&0x10 != 0 && n >= 7 && int(pid) == d.pcrPID {
				d.notePCR(readPCR(p[6:]), discontinuity)
			}
		}
		payload = p[5+n:]
	}
	if control&1 == 0 || p[3]&0xc0 != 0 { // no payload, or scrambled
		payload = nil
	}
	switch {
	case pid == pidPAT:
		if payload != nil {
			d.pat.add(payload, start, d.onPAT)
		}
	case int(pid) == d.pmtPID:
		if payload != nil {
			d.pmt.add(payload, start, d.onPMT)
		}
	case st != nil && payload != nil:
		d.onPayload(st, start, int(p[3]&0xf), discontinuity, payload)
	}
}

func (d *FormatDemuxer) stream(pid uint16) *demuxStream {
	if i := d.byPID[pid&pidNull]; i != 0 {
		return d.streams[i-1]
	}
	return nil
}

func readPCR(b []byte) int64 {
	base := int64(b[0])<<25 | int64(b[1])<<17 | int64(b[2])<<9 | int64(b[3])<<1 | int64(b[4]>>7)
	return base*pcrPerPTS + int64(b[4]&1)<<8 | int64(b[5])
}

// notePCR follows the program clock: a signalled discontinuity, or a PCR
// that steps back or jumps forward past pcrJump, breaks the time base.
func (d *FormatDemuxer) notePCR(pcr int64, signalled bool) {
	if d.havePCR {
		step := (pcr - d.lastPCR) % (wrap33 * pcrPerPTS)
		if step < 0 {
			step += wrap33 * pcrPerPTS
		}
		if signalled || step > pcrJump {
			d.breakTimeBase()
		}
	}
	d.lastPCR, d.havePCR = pcr, true
}

// breakTimeBase starts a new time base: timestamps no longer unwrap against
// the old one, and an av.EventDiscontinuity precedes the next packet.
func (d *FormatDemuxer) breakTimeBase() {
	d.haveRef = false
	d.broken = true
	for _, st := range d.streams {
		st.breakNext = true
		st.tail, st.haveNext = st.tail[:0], false
	}
}

func (d *FormatDemuxer) onPAT(b []byte) {
	sec, ok := parseSection(b)
	if !ok || sec.table != tablePAT {
		return
	}
	pmtPID := -1
	patPrograms(sec.body, func(number, pid uint16) {
		if number != 0 && (pmtPID < 0 && d.program == 0 || number == d.program) {
			d.program, pmtPID = number, int(pid)
		}
	})
	if pmtPID >= 0 && pmtPID != d.pmtPID {
		d.pmtPID, d.pmtVersion = pmtPID, -1
		d.pmt.reset()
	}
}

func (d *FormatDemuxer) onPMT(b []byte) {
	sec, ok := parseSection(b)
	if !ok || sec.table != tablePMT || sec.extension != d.program || int(sec.version) == d.pmtVersion {
		return
	}
	pcrPID, list, ok := pmtStreams(sec.body)
	if !ok {
		return
	}
	d.pmtVersion, d.pcrPID = int(sec.version), int(pcrPID)
	var streams []*demuxStream
	d.skipped = d.skipped[:0]
	for _, es := range list {
		codec, media := codecOf(es)
		if codec == "" {
			d.skipped = append(d.skipped, es.streamType)
		}
		if codec == "" || streamIndex(streams, es.pid) >= 0 {
			continue
		}
		if old := d.stream(es.pid); old != nil && old.streamType == es.streamType {
			streams = append(streams, old)
			continue
		}
		streams = append(streams, d.newStream(es, codec, media))
	}
	for _, old := range d.streams {
		// A PID that left the PMT, or now carries another stream type.
		if i := streamIndex(streams, old.pid); (i < 0 || streams[i] != old) && old.announced {
			d.push(unit{event: av.Event{Type: av.EventStreamRemoved, StreamID: old.stream.ID}})
		}
	}
	d.streams = streams
	d.byPID = [pidNull + 1]uint8{}
	for i, st := range streams {
		d.byPID[st.pid] = uint8(i + 1)
	}
}

func streamIndex(streams []*demuxStream, pid uint16) int {
	for i, st := range streams {
		if st.pid == pid {
			return i
		}
	}
	return -1
}

// codecOf maps a PMT entry to the codec it carries; "" for one goav does not
// read.
func codecOf(es esInfo) (av.CodecID, av.MediaType) {
	switch es.streamType {
	case streamTypeH264:
		return av.CodecH264, av.MediaVideo
	case streamTypeHEVC:
		return av.CodecHEVC, av.MediaVideo
	case streamTypeAAC:
		return av.CodecAAC, av.MediaAudio
	case streamTypePrivate:
		if id, ok := descriptor(es.descriptors, descriptorRegistration, 0); ok && string(id) == "Opus" {
			return av.CodecOpus, av.MediaAudio
		}
	}
	return "", ""
}

func (d *FormatDemuxer) newStream(es esInfo, codec av.CodecID, media av.MediaType) *demuxStream {
	st := &demuxStream{
		pid:        es.pid,
		streamType: es.streamType,
		cc:         -1,
		stream: av.Stream{
			ID:       av.StreamID(strconv.Itoa(int(es.pid))),
			Index:    d.nextIndex,
			Type:     media,
			Codec:    av.CodecParameters{ID: codec, Type: media},
			TimeBase: timeBase,
		},
	}
	d.nextIndex++
	if lang, ok := descriptor(es.descriptors, descriptorLanguage, 0); ok && len(lang) >= 3 {
		st.stream.Language = string(lang[:3])
	}
	if codec == av.CodecOpus {
		st.opusCode = 2
		if ext, ok := descriptor(es.descriptors, descriptorExtension, extensionOpus); ok && len(ext) >= 2 {
			st.opusCode = ext[1]
		}
		st.stream.Codec.SampleRate = 48000
		if st.opusCode >= 1 && st.opusCode <= 8 {
			st.stream.Codec.Channels = int(st.opusCode)
		}
	}
	return st
}

// onPayload adds one packet's payload to its stream's PES packet, checking
// the continuity counter: a gap damages the PES being read and marks the
// next one LossBefore; a repeated packet is dropped.
func (d *FormatDemuxer) onPayload(st *demuxStream, start bool, cc int, discontinuity bool, payload []byte) {
	if st.cc >= 0 && !discontinuity {
		if cc == st.cc {
			return
		}
		if cc != (st.cc+1)&0xf {
			st.damaged, st.lossNext = st.damaged || st.collecting, true
		}
	}
	st.cc = cc
	if start {
		d.finish(st)
		st.collecting, st.damaged = true, false
		st.pesLoss, st.lossNext = st.lossNext, false
		st.pes = append(st.pes[:0], payload...)
	} else {
		if !st.collecting {
			return
		}
		if len(st.pes)+len(payload) > maxPES {
			st.collecting, st.lossNext = false, true
			return
		}
		st.pes = append(st.pes, payload...)
	}
	if len(st.pes) >= 6 {
		if n := int(binary.BigEndian.Uint16(st.pes[4:])); n > 0 && len(st.pes) >= 6+n {
			d.finish(st)
		}
	}
}

// finishAll ends every PES packet being read, at the end of the input.
func (d *FormatDemuxer) finishAll() {
	for _, st := range d.streams {
		d.finish(st)
	}
	d.done = true
}

// finish parses the PES packet st has read and queues its media.
func (d *FormatDemuxer) finish(st *demuxStream) {
	if !st.collecting {
		return
	}
	st.collecting = false
	data, damaged := st.pes, st.damaged
	if len(data) < 9 || data[0] != 0 || data[1] != 0 || data[2] != 1 || data[6]&0xc0 != 0x80 {
		st.lossNext = st.lossNext || damaged
		return
	}
	if n := int(binary.BigEndian.Uint16(data[4:])); n > 0 {
		if 6+n <= len(data) {
			data = data[:6+n]
		} else {
			damaged = true
		}
	}
	if len(data) < 9 {
		st.lossNext = true
		return
	}
	flags, header := data[7]>>6, int(data[8])
	if 9+header > len(data) || flags == 1 || flags >= 2 && header < 5 || flags == 3 && header < 10 {
		st.lossNext = true
		return
	}
	payload := data[9+header:]
	pts, dts, timed := st.last, st.last, flags >= 2
	if timed {
		pts = d.unwrap(readTimestamp(data[9:]))
		dts = pts
		if flags == 3 {
			dts = d.unwrap(readTimestamp(data[14:]))
		}
		if !d.spanned {
			d.firstDTS, d.spanned = dts, true
		}
		d.lastDTS = dts
	}
	switch st.stream.Codec.ID {
	case av.CodecH264, av.CodecHEVC:
		d.videoUnit(st, payload, pts, dts, damaged)
	case av.CodecAAC:
		d.aacUnits(st, payload, pts, timed, damaged)
	case av.CodecOpus:
		d.opusUnits(st, payload, pts, timed, damaged)
	}
}

func readTimestamp(b []byte) int64 {
	return int64(b[0]>>1&7)<<30 | int64(b[1])<<22 | int64(b[2]>>1)<<15 | int64(b[3])<<7 | int64(b[4]>>1)
}

// unwrap extends a 33-bit timestamp to the one nearest the last, so a
// stream runs on past the wrap every 26.5 hours.
func (d *FormatDemuxer) unwrap(raw int64) int64 {
	if !d.haveRef {
		d.ref, d.haveRef = raw, true
		return raw
	}
	delta := (raw - d.ref) & (wrap33 - 1)
	if delta >= wrap33/2 {
		delta -= wrap33
	}
	d.ref += delta
	return d.ref
}

// videoUnit queues an access unit without its delimiter: goav frames
// access units itself. The first with parameter sets configures the stream.
func (d *FormatDemuxer) videoUnit(st *demuxStream, payload []byte, pts, dts int64, damaged bool) {
	codec := st.stream.Codec.ID
	au := readAccessUnit(codec, payload)
	payload = payload[au.audEnd:]
	if !st.ready && au.params {
		d.configureVideo(st, payload)
	}
	d.emit(st, unit{data: payload, pts: pts, dts: dts, keyframe: au.keyframe, corrupt: damaged})
}

func (d *FormatDemuxer) configureVideo(st *demuxStream, au []byte) {
	params := &st.stream.Codec
	if params.ID == av.CodecHEVC {
		sets, err := parameterSets(av.CodecHEVC, au)
		if err != nil || len(sets) == 0 {
			return
		}
		params.ExtraData.Bytes = sets
		d.markReady(st)
		return
	}
	record, err := h264.ConfigFromAnnexB(au)
	if err != nil {
		return
	}
	if config, err := h264.ParseConfig(record); err == nil && len(config.SPS) > 0 {
		if sps, err := h264.ParseSPS(config.SPS[0]); err == nil {
			params.Width, params.Height = sps.Width, sps.Height
		}
	}
	params.ExtraData.Bytes = record
	d.markReady(st)
}

// aacUnits queues each ADTS frame of a PES packet as a raw access unit,
// timed from the PES timestamp in 1024-sample steps. A frame cut by the PES
// boundary waits in tail for the rest, and keeps the time it started at.
func (d *FormatDemuxer) aacUnits(st *demuxStream, payload []byte, pts int64, timed, damaged bool) {
	data := payload
	carried := len(st.tail) > 0 && st.haveNext
	if len(st.tail) > 0 {
		st.tail = append(st.tail, payload...)
		data = st.tail
	}
	started := int64(0) // frames that start in this PES
	for len(data) > 0 {
		config, header, length, ok := adtsFrame(data)
		if !ok {
			if len(data) < 0x2000 && data[0] == 0xff && (len(data) < 2 || data[1]&0xf6 == 0xf0) {
				break // a frame the next PES completes
			}
			data = data[1:] // resync on the next sync word
			damaged = true
			continue
		}
		if !st.ready {
			d.configureAAC(st, config)
		}
		rate := int64(config.sampleRate())
		at := st.next
		switch {
		case carried:
			carried = false
		case timed:
			at = pts + started*1024*clock90k/rate
			started++
		}
		d.emit(st, unit{
			data: data[header:length], pts: at, dts: at, keyframe: true, corrupt: damaged,
			duration: av.Duration{Value: 1024, Base: av.TimeBase{Num: 1, Den: rate}},
		})
		st.next, st.haveNext = at+1024*clock90k/rate, true
		data = data[length:]
	}
	st.tail = append(st.tail[:0], data...)
}

func (d *FormatDemuxer) configureAAC(st *demuxStream, config aacConfig) {
	params := &st.stream.Codec
	params.SampleRate = config.sampleRate()
	params.Channels = config.channels
	params.ExtraData.Bytes = config.audioSpecificConfig()
	d.markReady(st)
}

// opusUnits queues each Opus packet of a PES packet behind its control
// header, timed by the packets' durations. The first packet's start trim is
// the stream's pre-skip.
func (d *FormatDemuxer) opusUnits(st *demuxStream, payload []byte, pts int64, timed, damaged bool) {
	at := pts
	if !timed {
		at = st.next
	}
	for len(payload) > 0 {
		packet, trim, consumed, ok := opusPacket(payload)
		if !ok {
			st.lossNext = true
			break
		}
		if !st.ready {
			// Dual mono (code 0) and the extended codes have no OpusHead of
			// their own: their packets flow unconfigured.
			if head, channels, ok := opusHead(st.opusCode, trim); ok {
				st.stream.Codec.ExtraData.Bytes, st.stream.Codec.Channels = head, channels
				d.markReady(st)
			}
		}
		samples := opusSamples(packet)
		d.emit(st, unit{
			data: packet, pts: at, dts: at, keyframe: true, corrupt: damaged,
			duration: av.Duration{Value: int64(samples), Base: av.TimeBase{Num: 1, Den: 48000}},
		})
		at += int64(samples) * clock90k / 48000
		payload = payload[consumed:]
	}
	st.next, st.haveNext = at, true
}

// markReady records that st's codec parameters are known. Found after Open
// announced the stream without them, they ride an av.EventCodecChanged
// ahead of its next packet, on a new epoch.
func (d *FormatDemuxer) markReady(st *demuxStream) {
	st.ready = true
	if d.open && st.announced {
		st.stream.Epoch++
		st.changed = true
	}
}

// emit queues a unit of st, preceded by the events it needs: a time-base
// discontinuity, and the announcement of a stream found after Open.
func (d *FormatDemuxer) emit(st *demuxStream, u unit) {
	if d.broken {
		d.broken = false
		d.push(unit{event: av.Event{Type: av.EventDiscontinuity, Reason: "transport stream time base changed"}})
	}
	if d.open && !st.announced {
		st.announced = true
		stream := cloneStream(st.stream)
		d.push(unit{event: av.Event{Type: av.EventStreamAdded, StreamID: stream.ID, Stream: &stream}})
	}
	if st.changed {
		st.changed = false
		params := cloneStream(st.stream).Codec
		d.push(unit{event: av.Event{Type: av.EventCodecChanged, StreamID: st.stream.ID, Epoch: st.stream.Epoch, Codec: &params}})
	}
	u.stream = st
	u.loss, st.pesLoss = st.pesLoss, false
	u.broken, st.breakNext = st.breakNext, false
	st.last = u.dts
	u.epoch = st.stream.Epoch
	buf := d.take(len(u.data))
	u.data = append(buf, u.data...)
	d.push(u)
}

func (d *FormatDemuxer) push(u unit) { d.queue = append(d.queue, u) }

// take returns a recycled buffer for a queued unit.
func (d *FormatDemuxer) take(n int) []byte {
	for i := len(d.pool) - 1; i >= 0; i-- {
		if cap(d.pool[i]) >= n {
			buf := d.pool[i]
			d.pool[i] = d.pool[len(d.pool)-1]
			d.pool = d.pool[:len(d.pool)-1]
			return buf
		}
	}
	return make([]byte, 0, max(n, 4096))
}
