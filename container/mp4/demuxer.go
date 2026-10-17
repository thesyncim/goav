package mp4

import (
	"io"
	"sort"
	"time"

	"github.com/thesyncim/goav/av"
)

// Track is one demuxable track's identity and codec parameters.
type Track struct {
	ID        uint32
	Media     av.MediaType
	Codec     av.CodecParameters
	Timescale uint32
}

// Sample is one demuxed sample. Data is filled into the buffer the caller hands
// to ReadInto (grown if the sample does not fit), and DTS/CTS are in the
// track's Timescale.
type Sample struct {
	TrackIndex int
	Data       []byte
	DTS        int64
	CTS        int64
	Keyframe   bool
	Timescale  uint32
}

type demuxItem struct {
	track  int
	sample int
	offset int64
	// ns is the sample's decode time on a shared nanosecond clock, so items
	// from tracks with different timescales order together.
	ns int64
}

// Demuxer reads samples from a parsed MP4 in decode-time order across tracks
// (ties broken by file offset), so files whose chunks are not interleaved by
// time still deliver audio and video together.
type Demuxer struct {
	r      io.ReaderAt
	size   int64
	tracks []*track
	order  []demuxItem
	pos    int
}

// NewDemuxer parses the movie header and builds the demux order. size is the
// file size; pass a large value when it is unknown (size-to-end boxes then
// cannot be bounded).
func NewDemuxer(r io.ReaderAt, size int64) (*Demuxer, error) {
	if r == nil {
		return nil, ErrNilReader
	}
	budget := newSampleBudget(size)
	tracks, err := parseMovie(r, size, budget)
	if err != nil {
		return nil, err
	}
	byID := make(map[uint32]*track, len(tracks))
	for _, tr := range tracks {
		byID[tr.id] = tr
	}
	if err := parseFragments(r, size, byID, budget); err != nil {
		return nil, err
	}
	// Keep only tracks that produced samples — from the progressive sample tables
	// or from fragment runs.
	kept := tracks[:0]
	for _, tr := range tracks {
		if len(tr.samples) > 0 {
			kept = append(kept, tr)
		}
	}
	if len(kept) == 0 {
		return nil, ErrNoTracks
	}
	d := &Demuxer{r: r, size: size, tracks: kept}
	d.buildOrder()
	if len(d.order) == 0 {
		return nil, ErrNoTracks
	}
	return d, nil
}

func (d *Demuxer) buildOrder() {
	total := 0
	for _, tr := range d.tracks {
		total += len(tr.samples)
	}
	d.order = make([]demuxItem, 0, total)
	for ti, tr := range d.tracks {
		for si := range tr.samples {
			d.order = append(d.order, demuxItem{track: ti, sample: si, offset: tr.samples[si].offset, ns: timescaleNS(tr.samples[si].dts, tr.timescale)})
		}
	}
	sort.SliceStable(d.order, func(i, j int) bool {
		if d.order[i].ns != d.order[j].ns {
			return d.order[i].ns < d.order[j].ns
		}
		return d.order[i].offset < d.order[j].offset
	})
}

// Tracks returns the track set in declaration order; the index into this slice
// is the Sample.TrackIndex ReadInto reports.
func (d *Demuxer) Tracks() []Track {
	out := make([]Track, len(d.tracks))
	for i, tr := range d.tracks {
		out[i] = Track{ID: tr.id, Media: tr.media, Codec: cloneParams(tr.params), Timescale: tr.timescale}
	}
	return out
}

// ReadInto reads the next sample into dst (reusing its capacity, growing only
// when a sample does not fit) and fills out. It returns io.EOF after the last
// sample.
func (d *Demuxer) ReadInto(dst []byte, out *Sample) error {
	if d.pos >= len(d.order) {
		return io.EOF
	}
	item := d.order[d.pos]
	tr := d.tracks[item.track]
	s := tr.samples[item.sample]
	// A sample cannot extend past the file; reject before allocating so a
	// crafted sample size cannot exhaust memory. The comparison is written so
	// it cannot overflow for an offset near MaxInt64.
	if s.size < 0 || s.offset < 0 || s.offset > d.size || int64(s.size) > d.size-s.offset {
		return ErrInvalidData
	}
	if cap(dst) < s.size {
		dst = make([]byte, s.size)
	} else {
		dst = dst[:s.size]
	}
	if s.size > 0 {
		if _, err := d.r.ReadAt(dst, s.offset); err != nil {
			return err
		}
	}
	d.pos++
	out.TrackIndex = item.track
	out.Data = dst
	out.DTS = s.dts
	out.CTS = s.cts
	out.Keyframe = s.keyframe
	out.Timescale = tr.timescale
	return nil
}

func cloneParams(p av.CodecParameters) av.CodecParameters {
	if len(p.ExtraData.Bytes) != 0 {
		p.ExtraData.Bytes = append([]byte(nil), p.ExtraData.Bytes...)
	}
	return p
}

// nextTrack reports the track index of the sample the next ReadInto returns.
func (d *Demuxer) nextTrack() (int, bool) {
	if d.pos >= len(d.order) {
		return 0, false
	}
	return d.order[d.pos].track, true
}

// timescaleNS converts ticks of a track timescale to nanoseconds without
// overflowing for long media.
func timescaleNS(ticks int64, timescale uint32) int64 {
	if timescale == 0 {
		return 0
	}
	ts := int64(timescale)
	return ticks/ts*int64(time.Second) + ticks%ts*int64(time.Second)/ts
}

// seek repositions the read cursor so the next ReadInto resumes at the
// latest keyframe at or before position on the first video track (the
// latest sample at or before it when the file has no video), clamped to the
// first sample. Every track resumes from that point in time order.
func (d *Demuxer) seek(position time.Duration) {
	anchor := 0
	for i := range d.tracks {
		if d.tracks[i].media == av.MediaVideo {
			anchor = i
			break
		}
	}
	tr := d.tracks[anchor]
	video := tr.media == av.MediaVideo
	target := int64(position)
	best := 0
	// Samples are in decode order and DTS <= CTS, so once a decode time
	// passes the target no later sample can present at or before it.
	for si := range tr.samples {
		s := &tr.samples[si]
		if timescaleNS(s.dts, tr.timescale) > target {
			break
		}
		if (s.keyframe || !video) && timescaleNS(s.cts, tr.timescale) <= target {
			best = si
		}
	}
	for pos := range d.order {
		if d.order[pos].track == anchor && d.order[pos].sample == best {
			d.pos = pos
			return
		}
	}
	d.pos = 0
}
