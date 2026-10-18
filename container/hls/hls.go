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
	return provider.Contract{ByteStream: true, Formats: []av.FormatID{av.FormatMP4}}
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
	base := strings.TrimSuffix(p.playlist, filepath.Ext(p.playlist))
	return &writer{packager: p, base: base, timescales: map[uint32]uint32{}}, nil
}

// writer splits the fragmented MP4 byte stream into the init segment and
// media segments, and keeps the playlist.
type writer struct {
	*packager
	base string
	buf  []byte
	init []byte
	err  error

	// timescales maps each track to its media timescale; anchor is the
	// track whose times place fragments: the first video track, else the
	// first track.
	timescales map[uint32]uint32
	anchor     uint32
	// codecs, width, and height describe the variant for a ladder's
	// multivariant playlist; segBytes counts the segment being written.
	codecs        []string
	width, height int
	segBytes      int

	// seg is the segment being written; segments are the finished ones still
	// on the store, oldest first; sequence is the number of the first one
	// the playlist lists.
	seg        io.WriteCloser
	segName    string
	segStart   float64
	segEnd     float64
	next       int
	segments   []segment
	sequence   int
	targetSecs int
}

type segment struct {
	number   int
	name     string
	duration float64
}

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
		start, end, ok := w.fragmentTimes(box)
		if w.seg != nil && ok && start-w.segStart >= w.target.Seconds()-0.001 {
			if err := w.finishSegment(); err != nil {
				return err
			}
		}
		if w.seg == nil {
			if err := w.startSegment(start); err != nil {
				return err
			}
		}
		if ok {
			w.segEnd = max(w.segEnd, end)
		}
		return w.write(box)
	default: // mdat and anything else of the fragment
		if w.seg != nil {
			return w.write(box)
		}
	}
	return nil
}

func (w *writer) write(b []byte) error {
	w.segBytes += len(b)
	_, err := w.seg.Write(b)
	return err
}

func (w *writer) startSegment(start float64) error {
	w.segName = fmt.Sprintf("%s-%05d.m4s", w.base, w.next)
	w.next++
	seg, err := w.store.Create(w.segName)
	if err != nil {
		return err
	}
	w.seg, w.segStart, w.segEnd, w.segBytes = seg, start, start, 0
	return nil
}

func (w *writer) finishSegment() error {
	if err := w.seg.Close(); err != nil {
		return err
	}
	w.seg = nil
	w.segments = append(w.segments, segment{number: w.next - 1, name: w.segName, duration: w.segEnd - w.segStart})
	if w.ladder != nil {
		if err := w.ladder.segment(w.variant, w.codecs, w.width, w.height, w.segBytes, w.segEnd-w.segStart); err != nil {
			return err
		}
	}
	if w.window > 0 {
		w.sequence = max(w.sequence, w.next-w.window)
		// A segment stays on the store for another window after it leaves
		// the playlist (RFC 8216 6.2.2), for players still fetching it.
		for len(w.segments) != 0 && w.segments[0].number < w.sequence-w.window {
			if err := w.store.Remove(w.segments[0].name); err != nil {
				return err
			}
			w.segments = w.segments[1:]
		}
	}
	return w.publishPlaylist(false)
}

func (w *writer) publishPlaylist(end bool) error {
	listed := w.segments
	for len(listed) != 0 && listed[0].number < w.sequence {
		listed = listed[1:]
	}
	target := int(math.Round(w.target.Seconds()))
	for _, s := range listed {
		target = max(target, int(math.Round(s.duration)))
	}
	// The target duration must not change while players read the playlist.
	w.targetSecs = max(w.targetSecs, target, 1)
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", w.targetSecs)
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
	for _, s := range listed {
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%s\n", s.duration, s.name)
	}
	if end {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return w.publishFile(w.playlist, []byte(b.String()))
}

func (w *writer) publishFile(name string, data []byte) error {
	f, err := w.store.Create(name)
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
	if w.seg != nil {
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
func (w *writer) fragmentTimes(moof []byte) (start, end float64, ok bool) {
	forBoxes(moof[8:], func(typ string, traf []byte) {
		if typ != "traf" {
			return
		}
		var id uint32
		var base uint64
		var duration uint64
		forBoxes(traf, func(typ string, b []byte) {
			switch typ {
			case "tfhd":
				if len(b) >= 8 {
					id = binary.BigEndian.Uint32(b[4:])
				}
			case "tfdt":
				if len(b) >= 8 && b[0] == 1 && len(b) >= 12 {
					base = binary.BigEndian.Uint64(b[4:])
				} else if len(b) >= 8 {
					base = uint64(binary.BigEndian.Uint32(b[4:]))
				}
			case "trun":
				duration += runDuration(b)
			}
		})
		timescale := w.timescales[id]
		if timescale == 0 || ok && id != w.anchor {
			return
		}
		start = float64(base) / float64(timescale)
		end = float64(base+duration) / float64(timescale)
		ok = true
	})
	return start, end, ok
}

// runDuration sums a trun's sample durations.
func runDuration(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	flags := binary.BigEndian.Uint32(b) & 0xffffff
	count := int(binary.BigEndian.Uint32(b[4:]))
	at := 8
	if flags&0x1 != 0 {
		at += 4
	}
	if flags&0x4 != 0 {
		at += 4
	}
	if flags&0x100 == 0 {
		return 0
	}
	stride := 4
	for _, f := range []uint32{0x200, 0x400, 0x800} {
		if flags&f != 0 {
			stride += 4
		}
	}
	var total uint64
	for i := 0; i < count && at+4 <= len(b); i++ {
		total += uint64(binary.BigEndian.Uint32(b[at:]))
		at += stride
	}
	return total
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
	if err := os.MkdirAll(string(d), 0o755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(string(d), "."+name+".*")
	if err != nil {
		return nil, err
	}
	return &publishing{File: f, final: filepath.Join(string(d), name)}, nil
}

func (d dirStore) Remove(name string) error {
	if err := os.Remove(filepath.Join(string(d), name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
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
