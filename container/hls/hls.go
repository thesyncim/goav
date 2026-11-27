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
//	live := goav.Custom(hls.New("live.m3u8", hls.Dir("/srv/hls"), hls.Window(6)))
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
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

func (p *packager) Open(ctx context.Context, info provider.Info) (io.WriteCloser, error) {
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

type writerState uint8

const (
	writerAwaitingInit writerState = iota // before ftyp/moov
	writerGotFtyp                         // ftyp buffered, awaiting moov
	writerReady                           // init segment published, awaiting moof
	writerInFragment                      // moof seen, awaiting mdat
	writerClosed                          // cleanly finalized
	writerFailed                          // sticky error recorded in err
)

// writer splits the fragmented MP4 byte stream into the init segment and
// media segments, and keeps the playlist.
type writer struct {
	*packager
	// dir is the store directory of the playlist and its files, file the
	// playlist's name in it, base what its files' names start with.
	dir, file, base string
	buf             []byte
	init            []byte
	renderBuf       bytes.Buffer
	err             error
	state           writerState

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

func (w *writer) fail(err error) error {
	w.err = err
	w.state = writerFailed
	return err
}

// Write takes the next bytes of the fragmented MP4 stream.
func (w *writer) Write(p []byte) (int, error) {
	switch w.state {
	case writerFailed:
		return 0, w.err
	case writerClosed:
		return 0, errors.New("hls: writer is closed")
	}
	w.buf = append(w.buf, p...)
	consumed := 0
	for {
		box, size, ok, err := nextBox(w.buf[consumed:])
		if err != nil {
			w.fail(err)
			return 0, err
		}
		if !ok {
			break
		}
		if err := w.handle(box); err != nil {
			w.fail(err)
			return 0, err
		}
		consumed += size
	}
	if consumed > 0 {
		w.buf = w.buf[:copy(w.buf, w.buf[consumed:])]
	}
	return len(p), nil
}

func (w *writer) handle(box []byte) error {
	switch string(box[4:8]) {
	case "ftyp":
		w.init = append(w.init, box...)
		if w.state == writerAwaitingInit {
			w.state = writerGotFtyp
		}
	case "moov":
		w.init = append(w.init, box...)
		w.readTracks(box)
		if err := w.publishFile(w.base+"-init.mp4", w.init); err != nil {
			return err
		}
		w.state = writerReady
		return nil
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
		w.part = append(w.part[:0], box...)
		w.partStart, w.partEnd, w.partKey = start, end, independent
		w.state = writerInFragment
	default: // mdat, which ends the fragment, and anything else of it
		if w.state != writerInFragment {
			return nil
		}
		w.part = append(w.part, box...)
		if string(box[4:8]) != "mdat" {
			return nil
		}
		w.state = writerReady
		if w.open == nil {
			w.open = &segment{number: w.next, name: fmt.Sprintf("%s-%05d.m4s", w.base, w.next), start: w.partStart, end: w.partStart}
			w.next++
			w.seg = w.seg[:0]
		}
		w.open.end = max(w.open.end, w.partEnd)
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

type aborter interface {
	abort()
}

// publishFile publishes name, a URI relative to the playlist, to the store.
func (w *writer) publishFile(name string, data []byte) error {
	f, err := w.store.Create(w.dir + name)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		if a, ok := f.(aborter); ok {
			a.abort()
		} else {
			_ = f.Close()
		}
		return err
	}
	return f.Close()
}

// Close ends the last segment and publishes the finished playlist.
func (w *writer) Close() error {
	switch w.state {
	case writerClosed:
		return nil
	case writerFailed:
		w.announceEnded()
		return w.err
	}
	if w.open != nil {
		if err := w.finishSegment(); err != nil {
			w.fail(err)
			w.announceEnded()
			return err
		}
	}
	if w.state != writerReady && w.state != writerInFragment {
		w.state = writerClosed
		w.announceEnded()
		return nil
	}
	if err := w.publishPlaylist(true); err != nil {
		w.fail(err)
		w.announceEnded()
		return err
	}
	w.state = writerClosed
	return nil
}

func (w *writer) announceEnded() {
	if a, ok := w.store.(announcer); ok {
		a.announce(w.playlist, w.edgeState(true))
	}
}
