package mp4

import (
	"io"
	"sort"

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
}

// Demuxer reads samples from a parsed MP4 in interleaved decode order (by file
// offset, the order the data is laid out in mdat).
type Demuxer struct {
	r      io.ReaderAt
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
	tracks, err := parseMovie(r, size)
	if err != nil {
		return nil, err
	}
	d := &Demuxer{r: r, tracks: tracks}
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
			d.order = append(d.order, demuxItem{track: ti, sample: si, offset: tr.samples[si].offset})
		}
	}
	sort.SliceStable(d.order, func(i, j int) bool { return d.order[i].offset < d.order[j].offset })
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
