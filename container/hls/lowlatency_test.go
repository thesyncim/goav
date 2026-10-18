package hls_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/provider"
)

var partLine = regexp.MustCompile(`#EXT-X-PART:DURATION=([0-9.]+),URI="([^"]+)"(,INDEPENDENT=YES)?`)

// TestLowLatency serves a live LL-HLS playlist while it is made: a blocking
// reload waits for the part it names, the hinted part is served once it is
// published, parts are at most the part target, and the parts a player can
// start at (keyframes) say so.
func TestLowLatency(t *testing.T) {
	srv := hls.NewServer()
	web := httptest.NewServer(srv)
	defer web.Close()
	dest := goav.Custom("ll", hls.New("ll.m3u8", srv, hls.SegmentDuration(time.Second), hls.Parts(200*time.Millisecond)))
	out := launch.Sink("out", "the packager", func(struct{}) (goav.Destination, error) { return dest, nil })
	outs, err := launch.Compile(`testsrc video size=64x48 fps=25 duration=3s live ! encode vp8 keyframe_interval=25 ! out
testsrc audio duration=3s live ! encode opus ! out`, launch.Use(out))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- bundle.Run(context.Background(), outs...) }()

	get := func(path string) (int, string) {
		t.Helper()
		resp, err := http.Get(web.URL + "/" + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		if code, _ := get("ll.m3u8"); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the playlist never appeared")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Asked for part 2 of segment 1 before it exists, the server holds the
	// reload until the playlist lists it.
	start := time.Now()
	code, playlist := get("ll.m3u8?_HLS_msn=1&_HLS_part=2")
	if code != http.StatusOK || !strings.Contains(playlist, `URI="ll-00001.2.m4s"`) {
		t.Fatalf("blocking reload = %d:\n%s", code, playlist)
	}
	if waited := time.Since(start); waited < 800*time.Millisecond {
		t.Fatalf("the reload came back after %v, before segment 1 had parts", waited)
	}
	for _, tag := range []string{"#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,CAN-SKIP-UNTIL=6.0,PART-HOLD-BACK=0.600", "#EXT-X-PART-INF:PART-TARGET=0.200", "#EXT-X-PROGRAM-DATE-TIME:"} {
		if !strings.Contains(playlist, tag) {
			t.Fatalf("playlist lacks %s:\n%s", tag, playlist)
		}
	}
	// A one-second segment is five 200ms parts.
	if n := strings.Count(playlist, `URI="ll-00000.`); n != 5 {
		t.Fatalf("segment 0 has %d parts, want 5:\n%s", n, playlist)
	}
	for _, m := range partLine.FindAllStringSubmatch(playlist, -1) {
		if d, _ := strconv.ParseFloat(m[1], 64); d > 0.2001 || d < 0.1999 {
			t.Fatalf("part %s lasts %ss, want the 0.200 part target", m[2], m[1])
		}
		if keyframe := strings.HasSuffix(m[2], ".0.m4s"); keyframe != (m[3] != "") {
			t.Fatalf("part %s independent=%v; segments start at keyframes, parts between do not", m[2], m[3] != "")
		}
	}
	hint := regexp.MustCompile(`#EXT-X-PRELOAD-HINT:TYPE=PART,URI="([^"]+)"`).FindStringSubmatch(playlist)
	if hint == nil {
		t.Fatalf("no preload hint:\n%s", playlist)
	}
	// The hinted part is held until it is published, then served.
	if code, body := get(hint[1]); code != http.StatusOK || !strings.Contains(body[:8], "moof") {
		t.Fatalf("hinted part %s = %d", hint[1], code)
	}
	if code, _ := get("ll-99999.0.m4s"); code != http.StatusNotFound {
		t.Fatalf("an unknown part = %d, want 404 at once", code)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, final := get("ll.m3u8"); !strings.Contains(final, "#EXT-X-ENDLIST") || strings.Contains(final, "#EXT-X-PART:") {
		t.Fatalf("finished playlist:\n%s", final)
	}
}

func TestLowLatencyNeedsTheServer(t *testing.T) {
	store := &memStore{files: map[string][]byte{}}
	_, err := hls.New("ll.m3u8", store, hls.Parts(200*time.Millisecond)).Open(context.Background(), provider.Info{})
	if err == nil || !strings.Contains(err.Error(), "hls.NewServer()") {
		t.Fatalf("LL-HLS into a plain store: %v", err)
	}
}

// TestLowLatencyLadder: each rendition's LL playlist reports where the
// other stands, so a player switches without first reloading it.
func TestLowLatencyLadder(t *testing.T) {
	srv := hls.NewServer()
	web := httptest.NewServer(srv)
	defer web.Close()
	ladder := hls.NewLadder("master.m3u8", srv)
	hd := goav.Custom("hd", ladder.Variant("hd.m3u8", hls.SegmentDuration(time.Second), hls.Parts(200*time.Millisecond)))
	sd := goav.Custom("sd", ladder.Variant("sd.m3u8", hls.SegmentDuration(time.Second), hls.Parts(200*time.Millisecond)))
	sink := func(name string, d goav.Destination) launch.Element {
		return launch.Sink(name, "a variant", func(struct{}) (goav.Destination, error) { return d, nil })
	}
	outs, err := launch.Compile(`testsrc video size=128x96 fps=25 duration=2s live ! encode vp8 bitrate=300k keyframe_interval=25 ! hd
testsrc video size=64x48 fps=25 duration=2s live ! encode vp8 bitrate=80k keyframe_interval=25 ! sd`, launch.Use(sink("hd", hd), sink("sd", sd)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- bundle.Run(context.Background(), outs...) }()
	var playlist string
	for deadline := time.Now().Add(5 * time.Second); ; {
		resp, err := http.Get(web.URL + "/hd.m3u8?_HLS_msn=1&_HLS_part=0")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				playlist = string(b)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("hd.m3u8 never reached segment 1")
		}
		time.Sleep(20 * time.Millisecond)
	}
	report := regexp.MustCompile(`#EXT-X-RENDITION-REPORT:URI="sd.m3u8",LAST-MSN=(\d+),LAST-PART=(\d+)`).FindStringSubmatch(playlist)
	if report == nil || strings.Contains(playlist, `URI="hd.m3u8"`) {
		t.Fatalf("hd's playlist reports:\n%s", playlist)
	}
	// Both encode the same cadence: sd stands within a segment of hd.
	if msn, _ := strconv.Atoi(report[1]); msn < 0 || msn > 2 {
		t.Fatalf("sd reported at segment %d while hd is at 1", msn)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
