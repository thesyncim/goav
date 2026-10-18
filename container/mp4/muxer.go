package mp4

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// MuxerFactory opens MP4 muxers behind the goav format extension point.
type MuxerFactory struct{}

// NewMuxer returns an MP4 muxer.
func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatMP4 {
		return nil, format.ErrNotFound
	}
	return &FormatMuxer{}, nil
}

// movieTimescale is the timescale of mvhd, tkhd, and edit list durations.
const movieTimescale = 1000

// fragmentSpan is how much media an audio-only fragmented file puts in one
// fragment; a file with video cuts a fragment at each video keyframe.
const fragmentSpan = time.Second

// FormatMuxer writes MP4 (ISO/IEC 14496-12 and -14). To a seekable output it
// writes a progressive file: ftyp, every sample in one mdat, then moov with
// the sample tables, patching the mdat size at Close. To any other output,
// or a realtime one, it writes fragmented MP4: ftyp and a moov without
// samples, then a moof and mdat per fragment, each fragment starting at a
// video keyframe, so the file plays while it grows and survives a crash up
// to its last fragment. Edit lists place each track: a track that starts
// later is delayed by an empty edit, B-frame and Opus pre-roll stay before
// presentation, and the earliest track starts at zero.
type FormatMuxer struct {
	w        io.Writer
	seeker   io.Seeker
	tracks   []*muxTrack
	byStream map[av.StreamID]*muxTrack
	offset   int64 // bytes written
	err      error
	closed   bool

	// Progressive: where the mdat header is, and the track the last sample
	// went to, which a chunk continues.
	mdatAt int64
	last   *muxTrack

	// Fragmented: whether ftyp and moov are written, the fragment sequence,
	// and the track whose keyframes (or spans) cut fragments.
	headerDone bool
	sequence   uint32
	anchor     *muxTrack
	box        boxWriter
	// fragment is the longest fragment the output asked for (0: cut at
	// keyframes only).
	fragment time.Duration
}

type muxTrack struct {
	id        uint32
	stream    av.Stream
	codec     av.CodecID
	media     av.MediaType
	timescale uint32

	config     []byte
	prerollNS  int64
	width      int
	height     int
	channels   int
	sampleRate int
	vp         vpInfo
	vpKnown    bool
	scratch    []byte

	samples []muxSample
	chunks  []muxChunk
	// Fragmented: the samples of the fragment being built, and their bytes.
	data []byte
	// startDTS is the decode time media time zero stands for; startPTS the
	// earliest presentation; endPTS where presentation ends. All in ticks.
	startDTS, startPTS, endPTS int64
	started                    bool
	// based marks startDTS fixed by a fragmented header written before the
	// track's first sample: media time zero is the movie's origin.
	based         bool
	fragmentStart int64
}

type muxSample struct {
	dts      int64
	cts      int64 // presentation minus decode time
	duration int64 // -1 until known
	size     uint32
	sync     bool
	offset   int64 // progressive: file offset; fragmented: offset in data
}

type muxChunk struct {
	offset int64
	count  uint32
}

// Format reports av.FormatMP4.
func (m *FormatMuxer) Format() av.FormatID { return av.FormatMP4 }

// Open binds the output and the streams, one track each.
func (m *FormatMuxer) Open(ctx context.Context, output format.Output, streams []av.Stream, options format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return fmt.Errorf("%w: no writer", ErrInvalidData)
	}
	*m = FormatMuxer{w: output.Writer, byStream: make(map[av.StreamID]*muxTrack, len(streams)), fragment: output.Fragment}
	for i := range streams {
		track, err := newMuxTrack(streams[i], uint32(i+1))
		if err != nil {
			return err
		}
		m.tracks = append(m.tracks, track)
		m.byStream[streams[i].ID] = track
		if m.anchor == nil || track.media == av.MediaVideo && m.anchor.media != av.MediaVideo {
			m.anchor = track
		}
	}
	if seeker, ok := output.Writer.(io.Seeker); ok && !output.Realtime && !options.Realtime && output.Fragment == 0 {
		if at, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			m.seeker, m.offset = seeker, at
		}
	}
	if m.seeker != nil {
		var w boxWriter
		writeFtyp(&w, m.tracks, false)
		m.mdatAt = m.offset + int64(len(w.b))
		// A 64-bit mdat header, sized at Close.
		w.u32(1)
		w.str("mdat")
		w.u64(0)
		return m.write(w.b)
	}
	return nil
}

func newMuxTrack(stream av.Stream, id uint32) (*muxTrack, error) {
	codec := stream.Codec.ID
	if _, ok := muxCodecs[codec]; !ok {
		return nil, fmt.Errorf("%w: mp4 carries %s; write stream %q (%s) to Matroska (.mkv) instead", ErrUnsupported, muxCodecList(), stream.ID, codec)
	}
	media := stream.Type
	if media == "" {
		media = stream.Codec.Type
	}
	t := &muxTrack{
		id: id, stream: stream, codec: codec, media: media,
		width: stream.Codec.Width, height: stream.Codec.Height,
		channels: stream.Codec.Channels, sampleRate: stream.Codec.SampleRate,
	}
	switch {
	case codec == av.CodecOpus:
		t.timescale = 48000
	case media == av.MediaAudio && t.sampleRate > 0:
		t.timescale = uint32(t.sampleRate)
	case stream.TimeBase.Num == 1 && stream.TimeBase.Den >= 1000 && stream.TimeBase.Den <= math.MaxInt32:
		t.timescale = uint32(stream.TimeBase.Den)
	default:
		t.timescale = 90000
	}
	if err := t.configure(); err != nil {
		return nil, err
	}
	return t, nil
}

func muxCodecList() string {
	return "H.264, AV1, VP8, VP9, AAC, Opus, and FLAC"
}

// Write adds one packet as a sample of its stream's track.
func (m *FormatMuxer) Write(ctx context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if packet == nil {
		return ErrNilPacket
	}
	if m.closed {
		return ErrClosed
	}
	if m.err != nil {
		return m.err
	}
	t, ok := m.byStream[packet.StreamID]
	if !ok {
		return fmt.Errorf("%w: packet for stream %q, which the muxer was not opened with", ErrInvalidData, packet.StreamID)
	}
	dts, pts, err := t.times(packet)
	if err != nil {
		return err
	}
	data, err := t.sampleData(packet.Payload.Bytes, packet.Keyframe)
	if err != nil {
		return fmt.Errorf("%w: stream %q: %w", ErrInvalidData, t.stream.ID, err)
	}
	if uint64(len(data)) > math.MaxUint32 {
		return fmt.Errorf("%w: a %d-byte sample", ErrInvalidData, len(data))
	}
	duration := int64(-1)
	if d, ok := packet.Duration.Rescale(av.TimeBase{Num: 1, Den: int64(t.timescale)}); ok && d.Value > 0 {
		duration = d.Value
	}
	sync := packet.Keyframe || t.media != av.MediaVideo
	t.closeLast(dts)
	if m.seeker == nil && t == m.anchor && t.cuts(sync, dts, m.fragment) {
		if err := m.flushFragment(false); err != nil {
			return err
		}
	}
	t.add(dts, pts, duration, sync)
	s := &t.samples[len(t.samples)-1]
	s.size = uint32(len(data))
	if m.seeker == nil {
		s.offset = int64(len(t.data))
		t.data = append(t.data, data...)
		return nil
	}
	s.offset = m.offset
	if m.last == t && len(t.chunks) != 0 {
		t.chunks[len(t.chunks)-1].count++
	} else {
		t.chunks = append(t.chunks, muxChunk{offset: m.offset, count: 1})
	}
	m.last = t
	return m.write(data)
}

// times reads a packet's decode and presentation times in track ticks.
func (t *muxTrack) times(packet *av.Packet) (dts, pts int64, err error) {
	base := av.TimeBase{Num: 1, Den: int64(t.timescale)}
	p, ok := packet.PTS.Rescale(base)
	if !ok {
		return 0, 0, fmt.Errorf("%w: stream %q packet without a presentation time", ErrInvalidData, t.stream.ID)
	}
	d, ok := packet.DTS.Rescale(base)
	if !ok || !packet.DTS.Base.Valid() {
		d = p
	}
	if n := len(t.samples); n != 0 && d.Value < t.samples[n-1].dts {
		return 0, 0, fmt.Errorf("%w: stream %q decode time goes back (%d after %d ticks)", ErrInvalidData, t.stream.ID, d.Value, t.samples[n-1].dts)
	}
	return d.Value, p.Value, nil
}

// closeLast gives the previous sample its duration, now that the next
// decode time is known.
func (t *muxTrack) closeLast(dts int64) {
	if n := len(t.samples); n != 0 && t.samples[n-1].duration < 0 {
		t.samples[n-1].duration = dts - t.samples[n-1].dts
	}
}

// add appends a sample.
func (t *muxTrack) add(dts, pts, duration int64, sync bool) {
	if !t.started {
		t.started = true
		if !t.based {
			t.startDTS = dts
		}
		t.startPTS, t.endPTS = pts, pts
	}
	if len(t.samples) == 0 {
		// The first sample since the last fragment flushed starts the next.
		t.fragmentStart = dts
	}
	t.startPTS = min(t.startPTS, pts)
	end := pts
	if duration > 0 {
		end += duration
	}
	t.endPTS = max(t.endPTS, end)
	t.samples = append(t.samples, muxSample{dts: dts, cts: pts - dts, duration: duration, sync: sync})
}

// cuts reports whether a sample starts a new fragment, once the fragment
// holds something: a video keyframe, a sample that would carry the fragment
// past the length the output asked for (estimating its duration as the
// last one's), or an audio-only file's span.
func (t *muxTrack) cuts(sync bool, dts int64, limit time.Duration) bool {
	n := len(t.samples)
	if n == 0 {
		return false
	}
	if t.media == av.MediaVideo && sync {
		return true
	}
	if limit > 0 {
		return dts-t.fragmentStart+max(t.samples[n-1].duration, 0) > int64(limit.Seconds()*float64(t.timescale))
	}
	return t.media != av.MediaVideo && dts-t.fragmentStart >= int64(fragmentSpan.Seconds()*float64(t.timescale))
}

// finish fills a last sample's unknown duration with the one before it.
func (t *muxTrack) finish() {
	n := len(t.samples)
	if n == 0 || t.samples[n-1].duration >= 0 {
		return
	}
	t.samples[n-1].duration = 0
	if n > 1 {
		t.samples[n-1].duration = t.samples[n-2].duration
	}
	t.endPTS = max(t.endPTS, t.samples[n-1].dts+t.samples[n-1].cts+t.samples[n-1].duration)
}

// Close finalizes the file: the moov of a progressive file, or a fragmented
// file's last fragment. It does not close the writer.
func (m *FormatMuxer) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	if m.err != nil {
		return m.err
	}
	if m.w == nil {
		return nil
	}
	for _, t := range m.tracks {
		t.finish()
	}
	if m.seeker == nil {
		return m.flushFragment(true)
	}
	return m.finishProgressive()
}

func (m *FormatMuxer) write(b []byte) error {
	n, err := m.w.Write(b)
	m.offset += int64(n)
	if err != nil {
		m.err = err
	}
	return err
}

func (m *FormatMuxer) finishProgressive() error {
	for _, t := range m.tracks {
		if err := t.ready(); err != nil {
			m.err = err
			return err
		}
	}
	end := m.offset
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(end-m.mdatAt))
	if _, err := m.seeker.Seek(m.mdatAt+8, io.SeekStart); err != nil {
		return err
	}
	if _, err := m.w.Write(size[:]); err != nil {
		return err
	}
	if _, err := m.seeker.Seek(end, io.SeekStart); err != nil {
		return err
	}
	var w boxWriter
	m.writeMoov(&w, false)
	return m.write(w.b)
}

// flushFragment writes the samples whose durations are known as one
// fragment (at Close, all of them), writing ftyp and moov first.
func (m *FormatMuxer) flushFragment(last bool) error {
	if !m.headerDone {
		for _, t := range m.tracks {
			if err := t.ready(); err != nil {
				m.err = err
				return err
			}
		}
		var w boxWriter
		writeFtyp(&w, m.tracks, true)
		m.writeMoov(&w, true)
		if err := m.write(w.b); err != nil {
			return err
		}
		m.headerDone = true
		originNS := m.originNS()
		for _, t := range m.tracks {
			if !t.started {
				t.startDTS, t.based = nsTicks(originNS, t.timescale), true
			}
		}
	}
	// Each track sends the samples it knows the length of; a trailing sample
	// without a duration waits for the next fragment.
	ready := make([]int, len(m.tracks))
	any := false
	for i, t := range m.tracks {
		n := len(t.samples)
		if !last && n != 0 && t.samples[n-1].duration < 0 {
			n--
		}
		ready[i] = n
		any = any || n != 0
	}
	if !any {
		return nil
	}
	m.sequence++
	w := &m.box
	w.b, w.open = w.b[:0], w.open[:0]
	moofAt := len(w.b)
	w.start("moof")
	w.startFull("mfhd", 0, 0)
	w.u32(m.sequence)
	w.end()
	dataOffsets := make([]int, len(m.tracks))
	for i, t := range m.tracks {
		if ready[i] == 0 {
			continue
		}
		dataOffsets[i] = t.writeTraf(w, ready[i])
	}
	w.end()
	moofSize := len(w.b) - moofAt
	// trun data offsets count from the moof's first byte to the track's
	// first sample in the mdat that follows.
	at := moofSize + 8
	mdatSize := 8
	for i, t := range m.tracks {
		if ready[i] == 0 {
			continue
		}
		binary.BigEndian.PutUint32(w.b[dataOffsets[i]:], uint32(at))
		n := t.fragmentBytes(ready[i])
		at += n
		mdatSize += n
	}
	w.u32(uint32(mdatSize))
	w.str("mdat")
	for i, t := range m.tracks {
		if ready[i] == 0 {
			continue
		}
		w.bytes(t.data[:t.fragmentBytes(ready[i])])
	}
	if err := m.write(w.b); err != nil {
		return err
	}
	for i, t := range m.tracks {
		t.dropFragment(ready[i])
	}
	return nil
}

func (t *muxTrack) fragmentBytes(n int) int {
	if n == 0 {
		return 0
	}
	s := t.samples[n-1]
	return int(s.offset) + int(s.size)
}

// dropFragment forgets the first n samples, which a fragment carried, and
// keeps the rest, rebased into a compacted buffer.
func (t *muxTrack) dropFragment(n int) {
	if n == 0 {
		return
	}
	cut := t.fragmentBytes(n)
	rest := t.samples[n:]
	t.data = t.data[:copy(t.data, t.data[cut:])]
	t.samples = t.samples[:copy(t.samples, rest)]
	for i := range t.samples {
		t.samples[i].offset -= int64(cut)
	}
	if len(t.samples) != 0 {
		t.fragmentStart = t.samples[0].dts
	}
}

// writeTraf writes a track's fragment header and sample run for its first n
// samples, returning where the trun's data offset is to be patched.
func (t *muxTrack) writeTraf(w *boxWriter, n int) int {
	samples := t.samples[:n]
	w.start("traf")
	w.startFull("tfhd", 0, 0x020000) // default-base-is-moof
	w.u32(t.id)
	w.end()
	w.startFull("tfdt", 1, 0)
	w.u64(uint64(max(samples[0].dts-t.startDTS, 0)))
	w.end()
	flags := uint32(0x000001 | 0x000100 | 0x000200 | 0x000400) // offset, durations, sizes, flags
	version := uint8(0)
	for _, s := range samples {
		if s.cts != 0 {
			flags |= 0x000800
		}
		if s.cts < 0 {
			version = 1
		}
	}
	w.startFull("trun", version, flags)
	w.u32(uint32(n))
	patch := len(w.b)
	w.u32(0)
	for _, s := range samples {
		w.u32(uint32(s.duration))
		w.u32(s.size)
		if s.sync {
			w.u32(0x02000000) // depends on no other sample
		} else {
			w.u32(0x01010000) // depends on others, not a sync sample
		}
		if flags&0x000800 != 0 {
			w.u32(uint32(int32(s.cts)))
		}
	}
	w.end()
	w.end()
	return patch
}

func writeFtyp(w *boxWriter, tracks []*muxTrack, fragmented bool) {
	w.start("ftyp")
	brands := []string{"isom", "iso2", "mp41"}
	if fragmented {
		w.str("iso6")
		w.u32(0)
		brands = []string{"iso6", "isom", "mp41"}
	} else {
		w.str("isom")
		w.u32(0x200)
	}
	for _, b := range brands {
		w.str(b)
	}
	for _, t := range tracks {
		switch t.codec {
		case av.CodecH264:
			w.str("avc1")
		case av.CodecAV1:
			w.str("av01")
		}
	}
	w.end()
}

// writeMoov writes the movie: mvhd, a trak per track, and for a fragmented
// file the mvex that declares its tracks' fragments.
func (m *FormatMuxer) writeMoov(w *boxWriter, fragmented bool) {
	origin := m.originNS()
	var movieDuration int64
	edits := make([]trackEditPlan, len(m.tracks))
	for i, t := range m.tracks {
		edits[i] = t.editPlan(origin, fragmented)
		movieDuration = max(movieDuration, edits[i].total)
	}
	w.start("moov")
	w.startFull("mvhd", 1, 0)
	w.u64(0) // creation time
	w.u64(0) // modification time
	w.u32(movieTimescale)
	w.u64(uint64(movieDuration))
	w.u32(0x00010000) // rate
	w.u16(0x0100)     // volume
	w.zeros(10)
	w.matrix()
	w.zeros(24)
	w.u32(uint32(len(m.tracks) + 1)) // next track id
	w.end()
	for i, t := range m.tracks {
		t.writeTrak(w, edits[i], fragmented)
	}
	if fragmented {
		w.start("mvex")
		for _, t := range m.tracks {
			w.startFull("trex", 0, 0)
			w.u32(t.id)
			w.u32(1) // sample description index
			w.u32(0)
			w.u32(0)
			w.u32(0)
			w.end()
		}
		w.end()
	}
	w.end()
}

// originNS is the movie's time zero: the earliest presentation of any
// track, past its pre-roll.
func (m *FormatMuxer) originNS() int64 {
	origin := int64(math.MaxInt64)
	for _, t := range m.tracks {
		if t.started {
			origin = min(origin, ticksNS(t.startPTS, t.timescale)+t.prerollNS)
		}
	}
	if origin == math.MaxInt64 {
		return 0
	}
	return origin
}

// trackEditPlan places a track on the movie timeline: an empty edit of
// delay movie ticks, then its media from mediaTime ticks for duration movie
// ticks (0 while fragments are still to come).
type trackEditPlan struct {
	delay, mediaTime, duration, total int64
}

func (t *muxTrack) editPlan(originNS int64, fragmented bool) trackEditPlan {
	if !t.started {
		return trackEditPlan{}
	}
	presentNS := ticksNS(t.startPTS, t.timescale) + t.prerollNS
	plan := trackEditPlan{
		delay:     nsTicks(presentNS-originNS, movieTimescale),
		mediaTime: t.startPTS + nsTicks(t.prerollNS, t.timescale) - t.startDTS,
	}
	if !fragmented {
		plan.duration = nsTicks(ticksNS(t.endPTS, t.timescale)-presentNS, movieTimescale)
		plan.total = plan.delay + plan.duration
	}
	return plan
}

func (t *muxTrack) writeTrak(w *boxWriter, edit trackEditPlan, fragmented bool) {
	var mediaDuration int64
	if !fragmented {
		for _, s := range t.samples {
			mediaDuration += s.duration
		}
	}
	w.start("trak")
	w.startFull("tkhd", 1, 0x000003) // enabled, in movie
	w.u64(0)
	w.u64(0)
	w.u32(t.id)
	w.u32(0)
	w.u64(uint64(edit.total))
	w.zeros(8)
	w.u16(0) // layer
	w.u16(0) // alternate group
	if t.media == av.MediaAudio {
		w.u16(0x0100)
	} else {
		w.u16(0)
	}
	w.u16(0)
	w.matrix()
	w.u32(uint32(t.width) << 16)
	w.u32(uint32(t.height) << 16)
	w.end()
	if edit.delay != 0 || edit.mediaTime != 0 {
		w.start("edts")
		entries := uint32(1)
		if edit.delay > 0 {
			entries = 2
		}
		w.startFull("elst", 1, 0)
		w.u32(entries)
		if edit.delay > 0 {
			w.u64(uint64(edit.delay))
			w.u64(math.MaxUint64) // media_time -1: empty
			w.u32(0x00010000)
		}
		w.u64(uint64(edit.duration))
		w.u64(uint64(edit.mediaTime))
		w.u32(0x00010000)
		w.end()
		w.end()
	}
	w.start("mdia")
	w.startFull("mdhd", 1, 0)
	w.u64(0)
	w.u64(0)
	w.u32(t.timescale)
	w.u64(uint64(mediaDuration))
	w.u16(0x55c4) // language: und
	w.u16(0)
	w.end()
	w.startFull("hdlr", 0, 0)
	w.u32(0)
	if t.media == av.MediaAudio {
		w.str("soun")
		w.zeros(12)
		w.str("SoundHandler\x00")
	} else {
		w.str("vide")
		w.zeros(12)
		w.str("VideoHandler\x00")
	}
	w.end()
	w.start("minf")
	if t.media == av.MediaAudio {
		w.startFull("smhd", 0, 0)
		w.u32(0)
		w.end()
	} else {
		w.startFull("vmhd", 0, 1)
		w.zeros(8)
		w.end()
	}
	w.start("dinf")
	w.startFull("dref", 0, 0)
	w.u32(1)
	w.startFull("url ", 0, 1) // the media is in this file
	w.end()
	w.end()
	w.end()
	t.writeStbl(w, fragmented)
	w.end() // minf
	w.end() // mdia
	w.end() // trak
}

func (t *muxTrack) writeStbl(w *boxWriter, fragmented bool) {
	samples := t.samples
	if fragmented {
		samples = nil
	}
	w.start("stbl")
	w.startFull("stsd", 0, 0)
	w.u32(1)
	t.writeSampleEntry(w)
	w.end()
	// stts: decode-time deltas, run-length coded.
	w.startFull("stts", 0, 0)
	runs := runLength(len(samples), func(i int) int64 { return samples[i].duration })
	w.u32(uint32(len(runs)))
	for _, r := range runs {
		w.u32(r.count)
		w.u32(uint32(r.value))
	}
	w.end()
	if offsets := runLength(len(samples), func(i int) int64 { return samples[i].cts }); len(offsets) > 1 || len(offsets) == 1 && offsets[0].value != 0 {
		version := uint8(0)
		for _, r := range offsets {
			if r.value < 0 {
				version = 1
			}
		}
		w.startFull("ctts", version, 0)
		w.u32(uint32(len(offsets)))
		for _, r := range offsets {
			w.u32(r.count)
			w.u32(uint32(int32(r.value)))
		}
		w.end()
	}
	if t.media == av.MediaVideo {
		var syncs []uint32
		for i, s := range samples {
			if s.sync {
				syncs = append(syncs, uint32(i+1))
			}
		}
		if len(syncs) != len(samples) {
			w.startFull("stss", 0, 0)
			w.u32(uint32(len(syncs)))
			for _, n := range syncs {
				w.u32(n)
			}
			w.end()
		}
	}
	chunks := t.chunks
	if fragmented {
		chunks = nil
	}
	w.startFull("stsc", 0, 0)
	perChunk := runLength(len(chunks), func(i int) int64 { return int64(chunks[i].count) })
	w.u32(uint32(len(perChunk)))
	first := uint32(1)
	for _, r := range perChunk {
		w.u32(first)
		w.u32(uint32(r.value))
		w.u32(1)
		first += r.count
	}
	w.end()
	w.startFull("stsz", 0, 0)
	uniform := len(samples) > 0
	for _, s := range samples {
		uniform = uniform && s.size == samples[0].size
	}
	if uniform {
		w.u32(samples[0].size)
		w.u32(uint32(len(samples)))
	} else {
		w.u32(0)
		w.u32(uint32(len(samples)))
		for _, s := range samples {
			w.u32(s.size)
		}
	}
	w.end()
	wide := len(chunks) != 0 && chunks[len(chunks)-1].offset > math.MaxUint32
	if wide {
		w.startFull("co64", 0, 0)
	} else {
		w.startFull("stco", 0, 0)
	}
	w.u32(uint32(len(chunks)))
	for _, c := range chunks {
		if wide {
			w.u64(uint64(c.offset))
		} else {
			w.u32(uint32(c.offset))
		}
	}
	w.end()
	w.end()
}

type run struct {
	count uint32
	value int64
}

func runLength(n int, value func(int) int64) []run {
	var runs []run
	for i := range n {
		v := value(i)
		if len(runs) != 0 && runs[len(runs)-1].value == v {
			runs[len(runs)-1].count++
			continue
		}
		runs = append(runs, run{count: 1, value: v})
	}
	return runs
}

func ticksNS(ticks int64, timescale uint32) int64 {
	return int64(float64(ticks) * float64(time.Second) / float64(timescale))
}

func nsTicks(ns int64, timescale uint32) int64 {
	return int64(math.Round(float64(ns) * float64(timescale) / float64(time.Second)))
}
