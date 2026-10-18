// Package hls packages goav output as HTTP Live Streaming (RFC 8216):
// fragmented MP4 segments and a media playlist, for live or on-demand
// delivery.
//
// A packager is a provider.Destination. goav muxes the streams routed to it
// into fragmented MP4, cut at video keyframes, and the packager splits that
// byte stream: the ftyp and moov become the init segment, and fragments
// group into media segments of about the target duration, each starting at
// a keyframe. After every segment it republishes the playlist; at the end it
// marks the playlist complete.
//
//	live := goav.Custom("live", hls.New("live.m3u8", hls.Dir("/srv/hls"), hls.Window(6)))
//	err := bundle.Run(ctx, video.To(live), audio.To(live))
//
// With Window(n), a live playlist lists the last n segments and the
// packager removes older ones once players can no longer be fetching them;
// without it, the playlist keeps every segment, an event that becomes an
// on-demand playlist when the run ends.
//
// A Ladder is an adaptive-bitrate set: each of its variants is a packager,
// and it writes the multivariant playlist that lists them with their
// bandwidth, codecs (RFC 6381, read from each init segment), and
// resolution.
//
// With Parts, a playlist is low-latency HLS: the muxer cuts fragments of at
// most the part duration, each published as an EXT-X-PART with the next one
// hinted (EXT-X-PRELOAD-HINT) and wall-clock times on the segments
// (EXT-X-PROGRAM-DATE-TIME), delta updates for long playlists (EXT-X-SKIP
// past six target durations), and, in a ladder, rendition reports of where
// the other renditions stand. A Server holds what LL-HLS asks a server to
// hold: blocking playlist reloads (_HLS_msn, _HLS_part) and requests for a
// hinted part. Serve it over HTTPS, so players get HTTP/2.
package hls

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/provider"
)

// Store is where a packager writes its files. Create returns a writer whose
// Close publishes the file whole, so a player never reads half a playlist;
// Remove deletes a segment that has left a live playlist.
type Store interface {
	Create(name string) (io.WriteCloser, error)
	Remove(name string) error
}

// Option configures a packager.
type Option func(*config)

type config struct {
	target time.Duration
	window int
	parts  time.Duration
}

// SegmentDuration sets the target segment length (default 4s). A segment
// ends at the first fragment boundary, a video keyframe, at or past it, so
// segments are as long as the stream's keyframe interval allows.
func SegmentDuration(d time.Duration) Option {
	return func(c *config) { c.target = d }
}

// Window makes the playlist live: it lists the last n segments, and the
// packager removes a segment once it has been out of the playlist for
// another n. Zero, the default, keeps every segment.
func Window(n int) Option {
	return func(c *config) { c.window = n }
}

// Parts makes the playlist low-latency (LL-HLS): segments are also published
// in parts of at most d (200ms to 1s is typical), each a CMAF chunk listed
// as an EXT-X-PART with the next one hinted, so a player plays within a few
// parts of the live edge. LL-HLS needs a server that holds blocking playlist
// reloads and hinted-part requests: package into an hls.Server and serve it.
func Parts(d time.Duration) Option {
	return func(c *config) { c.parts = d }
}

// New returns a packager writing the playlist (a name such as "live.m3u8")
// and its segments to store. Segment names derive from the playlist's:
// live-init.mp4, live-00000.m4s, and so on.
func New(playlist string, store Store, options ...Option) provider.Destination {
	c := config{target: 4 * time.Second}
	for _, option := range options {
		if option != nil {
			option(&c)
		}
	}
	return &packager{playlist: playlist, store: store, config: c}
}

type packager struct {
	playlist string
	store    Store
	config
	// ladder, for a Ladder's variant, hears of each finished segment.
	ladder  *Ladder
	variant *variant
}

func (p *packager) Name() string { return p.playlist }

func (p *packager) Contract() provider.Contract {
	return provider.Contract{ByteStream: true, Formats: []av.FormatID{av.FormatMP4}, Fragment: p.parts}
}

func (p *packager) Open(ctx context.Context, info provider.Info) (provider.Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.store == nil {
		return nil, errors.New("hls: no store to write the segments to")
	}
	if p.target <= 0 || p.window < 0 {
		return nil, fmt.Errorf("hls: segment duration %v and window %d must be positive", p.target, p.window)
	}
	if p.parts < 0 || p.parts > 0 && p.parts*2 > p.target {
		return nil, fmt.Errorf("hls: parts of %v need segments of at least twice that, not %v", p.parts, p.target)
	}
	if _, blocks := p.store.(announcer); p.parts > 0 && !blocks {
		return nil, errors.New("hls: LL-HLS needs a server that holds blocking playlist reloads; package into hls.NewServer() and serve it")
	}
	if a, ok := p.store.(announcer); ok {
		// The playlist is coming: requests for it wait instead of failing.
		a.announce(p.playlist, edge{msn: -1, part: -1, complete: -1, target: p.target})
	}
	// Files live beside the playlist, and the playlist names them relative to
	// itself: hd/index.m3u8 lists index-00000.m4s, stored as
	// hd/index-00000.m4s.
	dir, file := path.Split(p.playlist)
	base := strings.TrimSuffix(file, path.Ext(file))
	return &writer{packager: p, dir: dir, file: file, base: base, timescales: map[uint32]uint32{}}, nil
}

// writer splits the fragmented MP4 byte stream into the init segment and
// media segments, and keeps the playlist.
type writer struct {
	*packager
	// dir is the store directory of the playlist and its files, file the
	// playlist's name in it, base what its files' names start with.
	dir, file, base string
	buf             []byte
	init            []byte
	err             error

	// timescales maps each track to its media timescale; anchor is the
	// track whose times place fragments: the first video track, else the
	// first track.
	timescales map[uint32]uint32
	anchor     uint32
	// codecs, width, and height describe the variant for a ladder's
	// multivariant playlist.
	codecs        []string
	width, height int

	// open is the segment being built, its bytes in seg and, with parts,
	// the fragment being read in part; segments are the finished ones still
	// on the store, oldest first; sequence is the number of the first one
	// the playlist lists.
	open       *segment
	seg        []byte
	part       []byte
	partStart  float64
	partEnd    float64
	partKey    bool
	next       int
	segments   []segment
	sequence   int
	targetSecs int
	// clock is the wall time media time zero stands for, for
	// EXT-X-PROGRAM-DATE-TIME; it starts at the first fragment.
	clock time.Time
}

type segment struct {
	number     int
	name       string
	start, end float64
	parts      []part
	// partsGone marks parts removed from the store: they left the live edge.
	partsGone bool
}

type part struct {
	name        string
	duration    float64
	independent bool
}

func (s segment) duration() float64 { return s.end - s.start }

// Write takes the next bytes of the fragmented MP4 stream.
func (w *writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.buf = append(w.buf, p...)
	for {
		box, size, ok := nextBox(w.buf)
		if !ok {
			break
		}
		if err := w.handle(box); err != nil {
			w.err = err
			return 0, err
		}
		w.buf = w.buf[:copy(w.buf, w.buf[size:])]
	}
	return len(p), nil
}

// nextBox returns the first whole box in b, with its size.
func nextBox(b []byte) ([]byte, int, bool) {
	if len(b) < 8 {
		return nil, 0, false
	}
	size := uint64(binary.BigEndian.Uint32(b))
	if size == 1 {
		if len(b) < 16 {
			return nil, 0, false
		}
		size = binary.BigEndian.Uint64(b[8:])
	}
	if size < 8 || size > uint64(len(b)) {
		return nil, 0, false
	}
	return b[:size], int(size), true
}

func (w *writer) handle(box []byte) error {
	switch string(box[4:8]) {
	case "ftyp":
		w.init = append(w.init, box...)
	case "moov":
		w.init = append(w.init, box...)
		w.readTracks(box)
		return w.publishFile(w.base+"-init.mp4", w.init)
	case "moof":
		start, end, independent, ok := w.fragmentTimes(box)
		if !ok {
			start, end = w.partEnd, w.partEnd
		}
		if w.clock.IsZero() {
			w.clock = time.Now().Add(-time.Duration(start * float64(time.Second)))
		}
		// A segment ends at the first fragment at or past the target that
		// a player can start from.
		if w.open != nil && independent && start-w.open.start >= w.target.Seconds()-0.001 {
			if err := w.finishSegment(); err != nil {
				return err
			}
		}
		if w.open == nil {
			w.open = &segment{number: w.next, name: fmt.Sprintf("%s-%05d.m4s", w.base, w.next), start: start, end: start}
			w.next++
			w.seg = w.seg[:0]
		}
		w.open.end = max(w.open.end, end)
		w.part = append(w.part[:0], box...)
		w.partStart, w.partEnd, w.partKey = start, end, independent
	default: // mdat, which ends the fragment, and anything else of it
		if w.open == nil {
			return nil
		}
		w.part = append(w.part, box...)
		if string(box[4:8]) != "mdat" {
			return nil
		}
		w.seg = append(w.seg, w.part...)
		if w.parts > 0 {
			return w.publishPart()
		}
	}
	return nil
}

// publishPart publishes the fragment just read as the open segment's next
// part, then the playlist that lists it.
func (w *writer) publishPart() error {
	p := part{
		name:        fmt.Sprintf("%s-%05d.%d.m4s", w.base, w.open.number, len(w.open.parts)),
		duration:    w.partEnd - w.partStart,
		independent: w.partKey,
	}
	if err := w.publishFile(p.name, w.part); err != nil {
		return err
	}
	w.open.parts = append(w.open.parts, p)
	return w.publishPlaylist(false)
}

func (w *writer) finishSegment() error {
	done := *w.open
	w.open = nil
	if err := w.publishFile(done.name, w.seg); err != nil {
		return err
	}
	w.segments = append(w.segments, done)
	if w.ladder != nil {
		if err := w.ladder.segment(w.variant, w.codecs, w.width, w.height, len(w.seg), done.duration()); err != nil {
			return err
		}
	}
	if w.window > 0 {
		w.sequence = max(w.sequence, w.next-w.window)
		// A segment stays on the store for another window after it leaves
		// the playlist (RFC 8216 6.2.2), for players still fetching it.
		for len(w.segments) != 0 && w.segments[0].number < w.sequence-w.window {
			if err := w.removeSegment(w.segments[0]); err != nil {
				return err
			}
			w.segments = w.segments[1:]
		}
	}
	// Parts stay a target duration past the three the playlist lists them
	// for, then leave the store.
	for i := range w.segments {
		s := &w.segments[i]
		if !s.partsGone && len(s.parts) != 0 && done.end-s.end > 4*w.target.Seconds() {
			for _, p := range s.parts {
				if err := w.store.Remove(w.dir + p.name); err != nil {
					return err
				}
			}
			s.partsGone = true
		}
	}
	return w.publishPlaylist(false)
}

func (w *writer) removeSegment(s segment) error {
	if !s.partsGone {
		for _, p := range s.parts {
			if err := w.store.Remove(w.dir + p.name); err != nil {
				return err
			}
		}
	}
	return w.store.Remove(w.dir + s.name)
}

func (w *writer) publishPlaylist(end bool) error {
	listed := w.listed()
	target := int(math.Round(w.target.Seconds()))
	for _, s := range listed {
		target = max(target, int(math.Round(s.duration())))
	}
	// The target duration must not change while players read the playlist.
	w.targetSecs = max(w.targetSecs, target, 1)
	if w.ladder != nil {
		if msn, part, ok := w.position(); ok {
			w.ladder.position(w.variant, msn, part)
		}
	}
	full, _ := w.render(end, false)
	if err := w.publishFile(w.file, []byte(full)); err != nil {
		return err
	}
	if a, ok := w.store.(announcer); ok {
		e := w.edgeState(end)
		if delta, skipped := w.render(end, true); skipped {
			e.delta = []byte(delta)
		}
		a.announce(w.playlist, e)
	}
	return nil
}

// listed are the finished segments the playlist lists.
func (w *writer) listed() []segment {
	listed := w.segments
	for len(listed) != 0 && listed[0].number < w.sequence {
		listed = listed[1:]
	}
	return listed
}

// skipBoundary is how far from the live edge a delta update may skip
// segments: six target durations, the least RFC 8216bis allows.
func (w *writer) skipBoundary() float64 { return 6 * float64(w.targetSecs) }

// render writes the playlist. With skip it writes the delta update a player
// asks for with _HLS_skip=YES: the segments past the skip boundary become
// one EXT-X-SKIP, and it reports whether there were any.
func (w *writer) render(end, skip bool) (string, bool) {
	listed := w.listed()
	low := w.parts > 0 && !end
	edge := 0.0
	if w.open != nil {
		edge = w.open.end
	} else if len(listed) != 0 {
		edge = listed[len(listed)-1].end
	}
	skipped := 0
	if skip && low {
		for skipped < len(listed) && edge-listed[skipped].end >= w.skipBoundary() {
			skipped++
		}
		if skipped == 0 {
			return "", false
		}
	}
	var b strings.Builder
	version := 7
	if skipped > 0 {
		version = 9 // EXT-X-SKIP
	}
	fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-VERSION:%d\n", version)
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", w.targetSecs)
	if low {
		fmt.Fprintf(&b, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,CAN-SKIP-UNTIL=%.1f,PART-HOLD-BACK=%.3f\n", w.skipBoundary(), 3*w.parts.Seconds())
		fmt.Fprintf(&b, "#EXT-X-PART-INF:PART-TARGET=%.3f\n", w.parts.Seconds())
	}
	if w.window == 0 {
		if end {
			b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
		} else {
			b.WriteString("#EXT-X-PLAYLIST-TYPE:EVENT\n")
		}
	}
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", w.sequence)
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	fmt.Fprintf(&b, "#EXT-X-MAP:URI=%q\n", w.base+"-init.mp4")
	if skipped > 0 {
		fmt.Fprintf(&b, "#EXT-X-SKIP:SKIPPED-SEGMENTS=%d\n", skipped)
	}
	// Parts are listed for the segments within three target durations of
	// the live edge.
	writeParts := func(s segment) {
		if !low || s.partsGone || edge-s.end > 3*w.target.Seconds() {
			return
		}
		for _, p := range s.parts {
			fmt.Fprintf(&b, "#EXT-X-PART:DURATION=%.3f,URI=%q", p.duration, p.name)
			if p.independent {
				b.WriteString(",INDEPENDENT=YES")
			}
			b.WriteString("\n")
		}
	}
	for _, s := range listed[skipped:] {
		if low || w.window > 0 {
			w.writeDate(&b, s.start)
		}
		writeParts(s)
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%s\n", s.duration(), s.name)
	}
	if low && w.open != nil {
		w.writeDate(&b, w.open.start)
		writeParts(*w.open)
		fmt.Fprintf(&b, "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=%q\n", w.hint())
	}
	if low && w.ladder != nil {
		// Where the other renditions stand, so a player switches to one
		// without first reloading its playlist.
		for _, r := range w.ladder.reports(w.variant) {
			fmt.Fprintf(&b, "#EXT-X-RENDITION-REPORT:URI=%q,LAST-MSN=%d", relativeURI(w.dir, r.playlist), r.msn)
			if r.part >= 0 {
				fmt.Fprintf(&b, ",LAST-PART=%d", r.part)
			}
			b.WriteString("\n")
		}
	}
	if end {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return b.String(), skipped > 0
}

// position is the newest segment the playlist lists and its newest part
// (-1 when it lists none of its parts).
func (w *writer) position() (msn, part int, ok bool) {
	if w.open != nil && len(w.open.parts) != 0 {
		return w.open.number, len(w.open.parts) - 1, true
	}
	if n := len(w.segments); n != 0 {
		last := w.segments[n-1]
		if w.parts > 0 && !last.partsGone {
			return last.number, len(last.parts) - 1, true
		}
		return last.number, -1, true
	}
	return 0, 0, false
}

func (w *writer) writeDate(b *strings.Builder, start float64) {
	at := w.clock.Add(time.Duration(start * float64(time.Second))).UTC()
	fmt.Fprintf(b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", at.Format("2006-01-02T15:04:05.000Z07:00"))
}

// hint names the part published next: the open segment's next one, or,
// once the open segment is as long as the target, the first part of the next
// segment, which the next keyframe starts. A wrong guess costs the players
// waiting on it a 404 when the next part lands elsewhere.
func (w *writer) hint() string {
	if w.open.end-w.open.start >= w.target.Seconds()-0.001 {
		return fmt.Sprintf("%s-%05d.0.m4s", w.base, w.next)
	}
	return fmt.Sprintf("%s-%05d.%d.m4s", w.base, w.open.number, len(w.open.parts))
}

// edgeState is what a blocking reload waits on: the newest segment and
// part the playlist now lists.
func (w *writer) edgeState(end bool) edge {
	e := edge{msn: w.next - 1, part: -1, complete: w.next - 1, ended: end, target: w.target}
	if w.open != nil {
		e.complete = w.open.number - 1
		e.part = len(w.open.parts) - 1
		e.hint = w.dir + w.hint()
	}
	return e
}

// publishFile publishes name, a URI relative to the playlist, to the store.
func (w *writer) publishFile(name string, data []byte) error {
	f, err := w.store.Create(w.dir + name)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Close ends the last segment and publishes the finished playlist.
func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if w.open != nil {
		if err := w.finishSegment(); err != nil {
			return err
		}
	}
	if w.init == nil {
		return nil
	}
	return w.publishPlaylist(true)
}

// readTracks reads each track's id, timescale, and handler from the moov.
func (w *writer) readTracks(moov []byte) {
	w.anchor = 0
	video := false
	forBoxes(moov[8:], func(typ string, trak []byte) {
		if typ != "trak" {
			return
		}
		var id, timescale uint32
		var handler string
		forBoxes(trak, func(typ string, b []byte) {
			switch typ {
			case "tkhd":
				id = fullBoxField(b, 12, 20)
			case "mdia":
				forBoxes(b, func(typ string, b []byte) {
					switch typ {
					case "mdhd":
						timescale = fullBoxField(b, 12, 20)
					case "hdlr":
						if len(b) >= 12 {
							handler = string(b[8:12])
						}
					case "minf":
						w.readSampleEntry(b)
					}
				})
			}
		})
		if id == 0 || timescale == 0 {
			return
		}
		w.timescales[id] = timescale
		if w.anchor == 0 || handler == "vide" && !video {
			w.anchor, video = id, handler == "vide"
		}
	})
}

// fullBoxField reads the u32 a FullBox keeps at offset v0 in version 0 and
// v1 in version 1, counted from the payload start.
func fullBoxField(b []byte, v0, v1 int) uint32 {
	if len(b) < 4 {
		return 0
	}
	at := v0
	if b[0] == 1 {
		at = v1
	}
	if len(b) < at+4 {
		return 0
	}
	return binary.BigEndian.Uint32(b[at:])
}

// fragmentTimes reads where a fragment starts and ends, in seconds, from
// the anchor track's run, or the first run when the anchor has none.
func (w *writer) fragmentTimes(moof []byte) (start, end float64, independent, ok bool) {
	forBoxes(moof[8:], func(typ string, traf []byte) {
		if typ != "traf" {
			return
		}
		var id uint32
		var base, duration uint64
		key := true
		forBoxes(traf, func(typ string, b []byte) {
			switch typ {
			case "tfhd":
				if len(b) >= 8 {
					id = binary.BigEndian.Uint32(b[4:])
				}
			case "tfdt":
				if len(b) >= 12 && b[0] == 1 {
					base = binary.BigEndian.Uint64(b[4:])
				} else if len(b) >= 8 {
					base = uint64(binary.BigEndian.Uint32(b[4:]))
				}
			case "trun":
				d, sync := readRun(b)
				duration += d
				key = key && sync
			}
		})
		timescale := w.timescales[id]
		if timescale == 0 || ok && id != w.anchor {
			return
		}
		start = float64(base) / float64(timescale)
		end = float64(base+duration) / float64(timescale)
		independent, ok = key, true
	})
	return start, end, independent, ok
}

// readRun sums a trun's sample durations and reports whether its first
// sample is a sync sample, which a player can start decoding at.
func readRun(b []byte) (duration uint64, sync bool) {
	if len(b) < 8 {
		return 0, false
	}
	flags := binary.BigEndian.Uint32(b) & 0xffffff
	count := int(binary.BigEndian.Uint32(b[4:]))
	at := 8
	if flags&0x1 != 0 {
		at += 4
	}
	sync = true
	const nonSync = 0x00010000
	if flags&0x4 != 0 {
		if at+4 <= len(b) {
			sync = binary.BigEndian.Uint32(b[at:])&nonSync == 0
		}
		at += 4
	}
	stride, flagsAt := 0, -1
	for _, f := range []uint32{0x100, 0x200, 0x400, 0x800} {
		if flags&f != 0 {
			if f == 0x400 {
				flagsAt = stride
			}
			stride += 4
		}
	}
	if flags&0x4 == 0 && flagsAt >= 0 && at+flagsAt+4 <= len(b) {
		sync = binary.BigEndian.Uint32(b[at+flagsAt:])&nonSync == 0
	}
	if flags&0x100 == 0 {
		return 0, sync
	}
	for i := 0; i < count && at+4 <= len(b); i++ {
		duration += uint64(binary.BigEndian.Uint32(b[at:]))
		at += stride
	}
	return duration, sync
}

// forBoxes calls fn with each child box's type and payload.
func forBoxes(b []byte, fn func(typ string, payload []byte)) {
	for len(b) >= 8 {
		size := int(binary.BigEndian.Uint32(b))
		header := 8
		if size == 1 && len(b) >= 16 {
			size, header = int(binary.BigEndian.Uint64(b[8:])), 16
		}
		if size < header || size > len(b) {
			return
		}
		fn(string(b[4:8]), b[header:size])
		b = b[size:]
	}
}

// Dir is a Store in a directory of the local file system. It publishes a
// file by writing a temporary one beside it and renaming it into place,
// readable by the web server that serves the directory.
func Dir(path string) Store { return dirStore(path) }

type dirStore string

func (d dirStore) Create(name string) (io.WriteCloser, error) {
	final := filepath.Join(string(d), filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(final), "."+filepath.Base(final)+".*")
	if err != nil {
		return nil, err
	}
	return &publishing{File: f, final: final}, nil
}

func (d dirStore) Remove(name string) error {
	if err := os.Remove(filepath.Join(string(d), filepath.FromSlash(name))); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

type publishing struct {
	*os.File
	final string
}

func (p *publishing) Close() error {
	if err := p.File.Chmod(0o644); err != nil {
		p.File.Close()
		return err
	}
	if err := p.File.Close(); err != nil {
		return err
	}
	return os.Rename(p.File.Name(), p.final)
}

// readSampleEntry reads a track's codec, as an RFC 6381 codecs string, and
// a video track's size from the first sample entry under minf/stbl/stsd.
func (w *writer) readSampleEntry(minf []byte) {
	forBoxes(minf, func(typ string, stbl []byte) {
		if typ != "stbl" {
			return
		}
		forBoxes(stbl, func(typ string, stsd []byte) {
			if typ != "stsd" || len(stsd) < 8 {
				return
			}
			first := true
			forBoxes(stsd[8:], func(fourcc string, entry []byte) {
				if !first {
					return
				}
				first = false
				if codec := codecString(fourcc, entry); codec != "" {
					w.codecs = append(w.codecs, codec)
				}
				if visual[fourcc] && len(entry) >= 28 {
					w.width = int(binary.BigEndian.Uint16(entry[24:]))
					w.height = int(binary.BigEndian.Uint16(entry[26:]))
				}
			})
		})
	})
}

var visual = map[string]bool{"avc1": true, "avc3": true, "av01": true, "vp08": true, "vp09": true}

// codecString spells a sample entry as HLS CODECS wants it (RFC 6381 and
// the codecs' ISO BMFF bindings), from its configuration box.
func codecString(fourcc string, entry []byte) string {
	config := func(child string, at int) []byte {
		var found []byte
		forBoxes(entry[min(at, len(entry)):], func(typ string, b []byte) {
			if typ == child && found == nil {
				found = b
			}
		})
		return found
	}
	const visualHeader, audioHeader = 78, 28
	switch fourcc {
	case "avc1", "avc3":
		if c := config("avcC", visualHeader); len(c) >= 4 {
			return fmt.Sprintf("%s.%02x%02x%02x", fourcc, c[1], c[2], c[3])
		}
	case "av01":
		if c := config("av1C", visualHeader); len(c) >= 3 {
			tier := "M"
			if c[2]&0x80 != 0 {
				tier = "H"
			}
			depth := 8
			if c[2]&0x40 != 0 {
				depth = 10
				if c[2]&0x20 != 0 {
					depth = 12
				}
			}
			return fmt.Sprintf("av01.%d.%02d%s.%02d", c[1]>>5, c[1]&0x1f, tier, depth)
		}
	case "vp08", "vp09":
		if c := config("vpcC", visualHeader); len(c) >= 7 {
			return fmt.Sprintf("%s.%02d.%02d.%02d", fourcc, c[4], c[5], c[6]>>4)
		}
	case "mp4a":
		if c := config("esds", audioHeader); len(c) > 0 {
			if objectType, ok := aacObjectType(c); ok {
				return fmt.Sprintf("mp4a.40.%d", objectType)
			}
		}
		return "mp4a.40.2"
	case "Opus":
		return "Opus"
	case "fLaC":
		return "fLaC"
	}
	return ""
}

// aacObjectType reads the audio object type from an esds box: the first
// five bits of the AudioSpecificConfig in its ES descriptor's decoder
// configuration.
func aacObjectType(esds []byte) (byte, bool) {
	if len(esds) < 4 {
		return 0, false
	}
	p := esds[4:]
	descriptor := func(tag byte) bool {
		if len(p) < 2 || p[0] != tag {
			return false
		}
		p = p[1:]
		for len(p) > 0 && p[0]&0x80 != 0 {
			p = p[1:]
		}
		if len(p) == 0 {
			return false
		}
		p = p[1:]
		return true
	}
	skip := func(n int) bool {
		if n > len(p) {
			return false
		}
		p = p[n:]
		return true
	}
	if !descriptor(0x03) || len(p) < 3 {
		return 0, false
	}
	flags := p[2]
	p = p[3:]
	if flags&0x80 != 0 && !skip(2) {
		return 0, false
	}
	if flags&0x40 != 0 && (len(p) == 0 || !skip(1+int(p[0]))) {
		return 0, false
	}
	if flags&0x20 != 0 && !skip(2) {
		return 0, false
	}
	if !descriptor(0x04) || !skip(13) || !descriptor(0x05) || len(p) == 0 {
		return 0, false
	}
	return p[0] >> 3, true
}

// relativeURI is the URI of the store name target as a playlist in the store
// directory dir names it: sd/index.m3u8 from hd/ is ../sd/index.m3u8.
func relativeURI(dir, target string) string {
	from := strings.Split(strings.TrimSuffix(dir, "/"), "/")
	to := strings.Split(target, "/")
	if dir == "" {
		from = nil
	}
	common := 0
	for common < len(from) && common < len(to)-1 && from[common] == to[common] {
		common++
	}
	parts := make([]string, 0, len(from)-common+len(to)-common)
	for range from[common:] {
		parts = append(parts, "..")
	}
	return strings.Join(append(parts, to[common:]...), "/")
}
