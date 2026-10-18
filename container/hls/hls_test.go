package hls_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/launch"
)

// memStore is a Store in memory that records what was published.
type memStore struct {
	mu      sync.Mutex
	files   map[string][]byte
	removed []string
}

type memFile struct {
	bytes.Buffer
	store *memStore
	name  string
}

func (f *memFile) Close() error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	f.store.files[f.name] = f.Bytes()
	return nil
}

func (s *memStore) Create(name string) (io.WriteCloser, error) {
	return &memFile{store: s, name: name}, nil
}

func (s *memStore) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, name)
	s.removed = append(s.removed, name)
	return nil
}

// packageProgram runs a program whose outputs end at "out", an element that
// is the HLS packager.
func packageProgram(t *testing.T, program string, dest goav.Destination) {
	t.Helper()
	out := launch.Sink("out", "the packager", func(struct{}) (goav.Destination, error) { return dest, nil })
	outs, err := launch.Compile(program, launch.Use(out))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
}

const av3s = `testsrc video size=64x48 fps=25 frames=75 ! encode vp9 keyframe_interval=25 ! out
testsrc audio duration=3s ! encode opus ! out`

var extinf = regexp.MustCompile(`#EXTINF:([0-9.]+),\n(\S+)`)

// TestOnDemandPlaylist packages three seconds into two-second segments: an
// init segment, segments that start at keyframes, a complete playlist, and
// media that decodes whole when a player joins the segments.
func TestOnDemandPlaylist(t *testing.T) {
	store := &memStore{files: map[string][]byte{}}
	packageProgram(t, av3s, goav.Custom("vod", hls.New("vod.m3u8", store, hls.SegmentDuration(2*time.Second))))

	playlist := string(store.files["vod.m3u8"])
	for _, line := range []string{"#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-TARGETDURATION:2", "#EXT-X-PLAYLIST-TYPE:VOD", "#EXT-X-MEDIA-SEQUENCE:0", `#EXT-X-MAP:URI="vod-init.mp4"`, "#EXT-X-ENDLIST"} {
		if !strings.Contains(playlist, line+"\n") {
			t.Fatalf("playlist lacks %q:\n%s", line, playlist)
		}
	}
	segments := extinf.FindAllStringSubmatch(playlist, -1)
	if len(segments) != 2 || segments[0][1] != "2.000" || segments[1][1] != "1.000" || segments[0][2] != "vod-00000.m4s" {
		t.Fatalf("segments = %q, want 2.000 and 1.000 seconds", segments)
	}
	media := append([]byte(nil), store.files["vod-init.mp4"]...)
	for _, s := range segments {
		media = append(media, store.files[s[2]]...)
	}
	var frames, samples atomic.Int64
	in := goav.FileInput("joined.mp4", bytes.NewReader(media))
	err := bundle.Run(context.Background(),
		in.Video().Decode().To(goav.Sink(component.SinkFunc("frames", func(_ context.Context, m component.Message) error {
			if m.Frame != nil {
				frames.Add(1)
			}
			return nil
		}))),
		in.Audio().Decode().To(goav.Sink(component.SinkFunc("samples", func(_ context.Context, m component.Message) error {
			if m.Frame != nil && m.Frame.Audio != nil {
				samples.Add(int64(m.Frame.Audio.Samples))
			}
			return nil
		}))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if frames.Load() != 75 || samples.Load() < 143_000 || samples.Load() > 145_000 {
		t.Fatalf("decoded %d frames and %d samples, want 75 and about 144000", frames.Load(), samples.Load())
	}
	ffmpegPlays(t, store)
}

// TestLiveWindow keeps a sliding window: the playlist lists the last two
// segments, and a segment leaves the store a window after it left the
// playlist.
func TestLiveWindow(t *testing.T) {
	store := &memStore{files: map[string][]byte{}}
	packageProgram(t, `testsrc video size=64x48 fps=25 frames=125 ! encode vp8 keyframe_interval=25 ! out`,
		goav.Custom("live", hls.New("live.m3u8", store, hls.SegmentDuration(time.Second), hls.Window(2))))
	playlist := string(store.files["live.m3u8"])
	segments := extinf.FindAllStringSubmatch(playlist, -1)
	if !strings.Contains(playlist, "#EXT-X-MEDIA-SEQUENCE:3\n") || strings.Contains(playlist, "PLAYLIST-TYPE") || len(segments) != 2 || segments[1][2] != "live-00004.m4s" {
		t.Fatalf("live playlist after five segments:\n%s", playlist)
	}
	if len(store.removed) != 1 || store.removed[0] != "live-00000.m4s" {
		t.Fatalf("removed %v, want only the segment a window past the playlist", store.removed)
	}
	if _, ok := store.files["live-00001.m4s"]; !ok {
		t.Fatal("a segment that just left the playlist was removed")
	}
}

// TestDirPublishesWhole writes through Dir: files land whole and readable,
// and no temporary file stays behind.
func TestDirPublishesWhole(t *testing.T) {
	dir := t.TempDir()
	packageProgram(t, `testsrc video size=64x48 fps=25 frames=50 ! encode vp8 keyframe_interval=25 ! out`,
		goav.Custom("dir", hls.New("d.m3u8", hls.Dir(dir), hls.SegmentDuration(time.Second))))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
		info, _ := e.Info()
		if strings.HasPrefix(e.Name(), ".") || info.Mode().Perm() != 0o644 {
			t.Fatalf("%s left behind or not readable (%v)", e.Name(), info.Mode())
		}
	}
	if strings.Join(names, " ") != "d-00000.m4s d-00001.m4s d-init.mp4 d.m3u8" {
		t.Fatalf("files = %v", names)
	}
}

// ffmpegPlays reads the playlist with FFmpeg when it is installed.
func ffmpegPlays(t *testing.T, store *memStore) {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return
	}
	dir := t.TempDir()
	for name, data := range store.files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	out, err := exec.Command(ffmpeg, "-v", "error", "-i", filepath.Join(dir, "vod.m3u8"), "-f", "null", "-").CombinedOutput()
	if err != nil || len(bytes.TrimSpace(out)) != 0 {
		t.Fatalf("ffmpeg could not play the playlist: %v\n%s", err, out)
	}
}
