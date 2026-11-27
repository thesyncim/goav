package hls

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"
)

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
	if err := w.publishFile(w.file, full); err != nil {
		return err
	}
	if a, ok := w.store.(announcer); ok {
		e := w.edgeState(end)
		if delta, skipped := w.render(end, true); skipped {
			e.delta = bytes.Clone(delta)
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
func (w *writer) render(end, skip bool) ([]byte, bool) {
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
			return nil, false
		}
	}
	b := &w.renderBuf
	b.Reset()
	version := 7
	if skipped > 0 {
		version = 9 // EXT-X-SKIP
	}
	fmt.Fprintf(b, "#EXTM3U\n#EXT-X-VERSION:%d\n", version)
	fmt.Fprintf(b, "#EXT-X-TARGETDURATION:%d\n", w.targetSecs)
	if low {
		fmt.Fprintf(b, "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,CAN-SKIP-UNTIL=%.1f,PART-HOLD-BACK=%.3f\n", w.skipBoundary(), 3*w.parts.Seconds())
		fmt.Fprintf(b, "#EXT-X-PART-INF:PART-TARGET=%.3f\n", w.parts.Seconds())
	}
	if w.window == 0 {
		if end {
			b.WriteString("#EXT-X-PLAYLIST-TYPE:VOD\n")
		} else {
			b.WriteString("#EXT-X-PLAYLIST-TYPE:EVENT\n")
		}
	}
	fmt.Fprintf(b, "#EXT-X-MEDIA-SEQUENCE:%d\n", w.sequence)
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")
	fmt.Fprintf(b, "#EXT-X-MAP:URI=%q\n", w.base+"-init.mp4")
	if skipped > 0 {
		fmt.Fprintf(b, "#EXT-X-SKIP:SKIPPED-SEGMENTS=%d\n", skipped)
	}
	// Parts are listed for the segments within three target durations of
	// the live edge.
	writeParts := func(s segment) {
		if !low || s.partsGone || edge-s.end > 3*w.target.Seconds() {
			return
		}
		for _, p := range s.parts {
			fmt.Fprintf(b, "#EXT-X-PART:DURATION=%.3f,URI=%q", p.duration, p.name)
			if p.independent {
				b.WriteString(",INDEPENDENT=YES")
			}
			b.WriteString("\n")
		}
	}
	for _, s := range listed[skipped:] {
		if low || w.window > 0 {
			w.writeDate(b, s.start)
		}
		writeParts(s)
		fmt.Fprintf(b, "#EXTINF:%.3f,\n%s\n", s.duration(), s.name)
	}
	if low && w.open != nil {
		w.writeDate(b, w.open.start)
		writeParts(*w.open)
		fmt.Fprintf(b, "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=%q\n", w.hint())
	}
	if low && w.ladder != nil {
		// Where the other renditions stand, so a player switches to one
		// without first reloading its playlist.
		for _, r := range w.ladder.reports(w.variant) {
			fmt.Fprintf(b, "#EXT-X-RENDITION-REPORT:URI=%q,LAST-MSN=%d", relativeURI(w.dir, r.playlist), r.msn)
			if r.part >= 0 {
				fmt.Fprintf(b, ",LAST-PART=%d", r.part)
			}
			b.WriteString("\n")
		}
	}
	if end {
		b.WriteString("#EXT-X-ENDLIST\n")
	}
	return b.Bytes(), skipped > 0
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

func (w *writer) writeDate(b *bytes.Buffer, start float64) {
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
