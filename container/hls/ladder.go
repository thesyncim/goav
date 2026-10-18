package hls

import (
	"fmt"
	"math"
	"path"
	"strings"
	"sync"

	"github.com/thesyncim/goav/provider"
)

// Ladder is an adaptive-bitrate set of renditions. Each Variant is a
// packager with its own playlist; the ladder writes the multivariant
// playlist that lists them with their bandwidth, codecs, and resolution,
// so a player picks the rendition its connection carries and switches as
// that changes.
//
//	ladder := hls.NewLadder("master.m3u8", hls.Dir("/srv/hls"))
//	hd := goav.Custom("hd", ladder.Variant("hd.m3u8", hls.Window(6)))
//	sd := goav.Custom("sd", ladder.Variant("sd.m3u8", hls.Window(6)))
//
// It publishes the multivariant playlist once every variant has a segment,
// and again when a variant's peak bandwidth rises.
type Ladder struct {
	name  string
	store Store
	// dir is the multivariant playlist's store directory, which its
	// variants' URIs are relative to.
	dir string

	mu        sync.Mutex
	variants  []*variant
	published string
}

type variant struct {
	playlist      string
	codecs        []string
	width, height int
	// peak is the highest segment bit rate; bits and seconds add up to the
	// average.
	peak, bits, seconds float64
	segments            int
	// msn and part are where its playlist stands, for the rendition
	// reports of the others; placed is set once it stands anywhere.
	msn, part int
	placed    bool
}

// NewLadder returns a ladder writing the multivariant playlist name to
// store, beside its variants' playlists.
func NewLadder(name string, store Store) *Ladder {
	dir, _ := path.Split(name)
	return &Ladder{name: name, store: store, dir: dir}
}

// Variant returns the packager of one rendition, writing its playlist and
// segments to the ladder's store. Route each rendition's streams to its own
// variant; audio routed to every variant rides in each.
func (l *Ladder) Variant(playlist string, options ...Option) provider.Destination {
	p := New(playlist, l.store, options...).(*packager)
	v := &variant{playlist: playlist}
	l.mu.Lock()
	l.variants = append(l.variants, v)
	l.mu.Unlock()
	p.ladder, p.variant = l, v
	return p
}

// segment records a variant's finished segment and republishes the
// multivariant playlist when what it says changed.
func (l *Ladder) segment(v *variant, codecs []string, width, height, bytes int, seconds float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	v.codecs, v.width, v.height = codecs, width, height
	if seconds > 0 {
		bits := float64(bytes) * 8
		v.peak = max(v.peak, bits/seconds)
		v.bits += bits
		v.seconds += seconds
	}
	v.segments++
	for _, other := range l.variants {
		if other.segments == 0 {
			return nil
		}
	}
	text := l.render()
	if text == l.published {
		return nil
	}
	f, err := l.store.Create(l.name)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(text)); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	l.published = text
	return nil
}

// render writes the multivariant playlist. Its bandwidths are the peaks
// rounded up to 10 kb/s, so it changes only when a peak moves materially.
func (l *Ladder) render() string {
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	for _, v := range l.variants {
		peak := int(math.Ceil(v.peak/10_000) * 10_000)
		average := 0
		if v.seconds > 0 {
			average = int(math.Ceil(v.bits/v.seconds/10_000) * 10_000)
		}
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d", peak, min(average, peak))
		if len(v.codecs) != 0 {
			fmt.Fprintf(&b, ",CODECS=%q", strings.Join(v.codecs, ","))
		}
		if v.width != 0 && v.height != 0 {
			fmt.Fprintf(&b, ",RESOLUTION=%dx%d", v.width, v.height)
		}
		fmt.Fprintf(&b, "\n%s\n", relativeURI(l.dir, v.playlist))
	}
	return b.String()
}

// position records where a variant's playlist stands.
func (l *Ladder) position(v *variant, msn, part int) {
	l.mu.Lock()
	v.msn, v.part, v.placed = msn, part, true
	l.mu.Unlock()
}

// report is one EXT-X-RENDITION-REPORT: another variant's playlist and the
// newest segment and part it lists.
type report struct {
	playlist  string
	msn, part int
}

// reports are where the variants other than v stand.
func (l *Ladder) reports(v *variant) []report {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []report
	for _, other := range l.variants {
		if other != v && other.placed {
			out = append(out, report{playlist: other.playlist, msn: other.msn, part: other.part})
		}
	}
	return out
}
