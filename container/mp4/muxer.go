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

type muxState uint8

const (
	muxUnopened muxState = iota
	muxProgressive
	muxFragmentedPreHeader
	muxFragmentedStreaming
	muxFailed
	muxClosed
)

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
	state    muxState

	// Progressive: where the mdat header is, and the track the last sample
	// went to, which a chunk continues.
	mdatAt int64
	last   *muxTrack

	// Fragmented: the fragment sequence, and the track whose keyframes
	// (or spans) cut fragments.
	sequence   uint32
	anchor     *muxTrack
	box        boxWriter
	readyBuf   []int
	offsetsBuf []int
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
	// earliest presentation; endPTS where presentation ends; lastDTS the
	// most recently added sample's decode time (persisted across fragment
	// flushes so non-monotonic DTS across fragments is caught). All in ticks.
	startDTS, startPTS, endPTS, lastDTS int64
	started                             bool
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
	tracks := make([]*muxTrack, 0, len(streams))
	byStream := make(map[av.StreamID]*muxTrack, len(streams))
	var anchor *muxTrack
	for i := range streams {
		track, err := newMuxTrack(streams[i], uint32(i+1))
		if err != nil {
			*m = FormatMuxer{}
			return err
		}
		tracks = append(tracks, track)
		byStream[streams[i].ID] = track
		if anchor == nil || track.media == av.MediaVideo && anchor.media != av.MediaVideo {
			anchor = track
		}
	}
	*m = FormatMuxer{
		w:        output.Writer,
		tracks:   tracks,
		byStream: byStream,
		anchor:   anchor,
		fragment: output.Fragment,
		state:    muxFragmentedPreHeader,
	}
	if seeker, ok := output.Writer.(io.Seeker); ok && !output.Realtime && !options.Realtime && output.Fragment == 0 {
		if at, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			m.seeker, m.offset = seeker, at
			m.state = muxProgressive
		}
	}
	if m.state == muxProgressive {
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
	switch m.state {
	case muxClosed:
		return ErrClosed
	case muxFailed:
		return m.err
	case muxUnopened:
		return fmt.Errorf("%w: muxer not opened", ErrInvalidData)
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
	if m.state != muxProgressive && t == m.anchor && t.cuts(sync, dts, m.fragment) {
		if err := m.flushFragment(false); err != nil {
			return err
		}
	}
	t.add(dts, pts, duration, sync)
	s := &t.samples[len(t.samples)-1]
	s.size = uint32(len(data))
	if m.state != muxProgressive {
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
	if t.started && d.Value < t.lastDTS {
		return 0, 0, fmt.Errorf("%w: stream %q decode time goes back (%d after %d ticks)", ErrInvalidData, t.stream.ID, d.Value, t.lastDTS)
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
	t.lastDTS = dts
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

func (m *FormatMuxer) fail(err error) error {
	m.err = err
	m.state = muxFailed
	return err
}

// Close finalizes the file: the moov of a progressive file, or a fragmented
// file's last fragment. It does not close the writer.
func (m *FormatMuxer) Close() error {
	switch m.state {
	case muxClosed:
		return nil
	case muxUnopened:
		m.state = muxClosed
		return nil
	case muxFailed:
		return m.err
	}
	mode := m.state
	for _, t := range m.tracks {
		t.finish()
	}
	var err error
	if mode == muxProgressive {
		err = m.finishProgressive()
	} else {
		err = m.flushFragment(true)
	}
	if err != nil {
		return m.fail(err)
	}
	m.state = muxClosed
	return nil
}

func (m *FormatMuxer) write(b []byte) error {
	n, err := m.w.Write(b)
	m.offset += int64(n)
	if err != nil {
		return m.fail(err)
	}
	return nil
}

func (m *FormatMuxer) finishProgressive() error {
	for _, t := range m.tracks {
		if err := t.ready(); err != nil {
			return m.fail(err)
		}
	}
	end := m.offset
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(end-m.mdatAt))
	if _, err := m.seeker.Seek(m.mdatAt+8, io.SeekStart); err != nil {
		return m.fail(err)
	}
	if _, err := m.w.Write(size[:]); err != nil {
		return m.fail(err)
	}
	if _, err := m.seeker.Seek(end, io.SeekStart); err != nil {
		return m.fail(err)
	}
	var w boxWriter
	m.writeMoov(&w, false)
	return m.write(w.b)
}

// flushFragment writes the samples whose durations are known as one
// fragment (at Close, all of them), writing ftyp and moov first.
func (m *FormatMuxer) flushFragment(last bool) error {
	if m.state == muxFragmentedPreHeader {
		for _, t := range m.tracks {
			if err := t.ready(); err != nil {
				return m.fail(err)
			}
		}
		var w boxWriter
		writeFtyp(&w, m.tracks, true)
		m.writeMoov(&w, true)
		if err := m.write(w.b); err != nil {
			return err
		}
		m.state = muxFragmentedStreaming
		originNS := m.originNS()
		for _, t := range m.tracks {
			if !t.started {
				t.startDTS, t.based = nsTicks(originNS, t.timescale), true
			}
		}
	}
	// Each track sends the samples it knows the length of; a trailing sample
	// without a duration waits for the next fragment.
	if cap(m.readyBuf) < len(m.tracks) {
		m.readyBuf = make([]int, len(m.tracks))
	}
	ready := m.readyBuf[:len(m.tracks)]
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
	if cap(m.offsetsBuf) < len(m.tracks) {
		m.offsetsBuf = make([]int, len(m.tracks))
	}
	dataOffsets := m.offsetsBuf[:len(m.tracks)]
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
	if err := m.write(w.b); err != nil {
		return err
	}
	for i, t := range m.tracks {
		if ready[i] == 0 {
			continue
		}
		if err := m.write(t.data[:t.fragmentBytes(ready[i])]); err != nil {
			return err
		}
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
