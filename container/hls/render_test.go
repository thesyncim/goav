package hls

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestDeltaUpdate pins RFC 8216bis delta updates: the segments wholly past
// the skip boundary (six target durations from the live edge) become one
// EXT-X-SKIP in a version 9 playlist, and the full playlist keeps them.
func TestDeltaUpdate(t *testing.T) {
	w := &writer{packager: &packager{playlist: "d.m3u8", config: config{target: time.Second, parts: 200 * time.Millisecond}}, base: "d", targetSecs: 1, next: 11}
	for i := range 10 {
		w.segments = append(w.segments, segment{number: i, name: fmt.Sprintf("d-%05d.m4s", i), start: float64(i), end: float64(i + 1), partsGone: i < 7})
	}
	w.open = &segment{number: 10, name: "d-00010.m4s", start: 10, end: 10.4, parts: []part{{name: "d-00010.0.m4s", duration: 0.2, independent: true}, {name: "d-00010.1.m4s", duration: 0.2}}}

	delta, skipped := w.render(false, true)
	if !skipped {
		t.Fatal("no delta for a playlist ten segments long")
	}
	for _, want := range []string{"#EXT-X-VERSION:9\n", "CAN-SKIP-UNTIL=6.0", "#EXT-X-MEDIA-SEQUENCE:0\n", "#EXT-X-SKIP:SKIPPED-SEGMENTS=4\n", "#EXTINF:1.000,\nd-00004.m4s\n", `#EXT-X-PRELOAD-HINT:TYPE=PART,URI="d-00010.2.m4s"`} {
		if !strings.Contains(delta, want) {
			t.Fatalf("delta lacks %q:\n%s", want, delta)
		}
	}
	if strings.Contains(delta, "d-00003.m4s") {
		t.Fatalf("delta lists a skipped segment:\n%s", delta)
	}
	full, _ := w.render(false, false)
	if strings.Contains(full, "EXT-X-SKIP") || !strings.Contains(full, "#EXT-X-VERSION:7\n") || !strings.Contains(full, "d-00000.m4s") {
		t.Fatalf("full playlist:\n%s", full)
	}
	// Parts are listed for the last three target durations only.
	if strings.Contains(full, "d-00007.0") || !strings.Contains(full, `URI="d-00010.1.m4s"`) {
		t.Fatalf("parts listed:\n%s", full)
	}
}

// TestHintCrossesSegments: once the open segment reaches the target, the
// next part is the next segment's first, and the hint says so.
func TestHintCrossesSegments(t *testing.T) {
	w := &writer{packager: &packager{playlist: "h.m3u8", config: config{target: time.Second, parts: 250 * time.Millisecond}}, base: "h", targetSecs: 1, next: 4}
	w.open = &segment{number: 3, start: 3, end: 3.75, parts: make([]part, 3)}
	if got := w.hint(); got != "h-00003.3.m4s" {
		t.Fatalf("mid-segment hint = %s", got)
	}
	w.open.end = 4
	w.open.parts = make([]part, 4)
	if got := w.hint(); got != "h-00004.0.m4s" {
		t.Fatalf("hint at the target = %s, want the next segment's first part", got)
	}
}

func TestRelativeURI(t *testing.T) {
	for _, tc := range []struct{ dir, target, want string }{
		{"", "hd/index.m3u8", "hd/index.m3u8"},
		{"hd/", "sd/index.m3u8", "../sd/index.m3u8"},
		{"hd/", "hd/other.m3u8", "other.m3u8"},
		{"a/b/", "a/c/x.m3u8", "../c/x.m3u8"},
		{"live/", "live.m3u8", "../live.m3u8"},
	} {
		if got := relativeURI(tc.dir, tc.target); got != tc.want {
			t.Errorf("relativeURI(%q, %q) = %q, want %q", tc.dir, tc.target, got, tc.want)
		}
	}
}
