package goav_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/graphrender"
)

var readmeTextBlockPattern = regexp.MustCompile("(?s)```text\n(.*?)```")

// readmeTextBlock returns the README text block that starts with prefix.
func readmeTextBlock(t *testing.T, prefix string) []string {
	t.Helper()
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range readmeTextBlockPattern.FindAllStringSubmatch(string(readme), -1) {
		if strings.HasPrefix(m[1], prefix) {
			return strings.Split(strings.TrimRight(m[1], "\n"), "\n")
		}
	}
	t.Fatalf("README has no text block starting %q; update this pin if it moved", prefix)
	return nil
}

// TestReadmeOutputBlocksMatchReality pins the README's sample outputs to what
// goav prints: the quick start's graph is the head of its real rendering, and
// every line of the sample refusal appears, in order, in the real error.
func TestReadmeOutputBlocksMatchReality(t *testing.T) {
	in := goav.FileInput("in.webm", bytes.NewReader(nil))
	video := in.Video().Decode().Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000)))
	spec, err := bundle.Describe(video.To(goav.Write("out.webm", io.Discard)))
	if err != nil {
		t.Fatalf("describe quick start: %v", err)
	}
	rendered, err := graphrender.RenderURI(spec, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	real := strings.Split(rendered, "\n")
	for i, line := range readmeTextBlock(t, "pipeline goav") {
		if i >= len(real) || real[i] != line {
			t.Fatalf("README graph line %d = %q, real rendering:\n%s", i+1, line, rendered)
		}
	}

	_, err = bundle.Describe(goav.FileInput("in.webm", bytes.NewReader(nil)).Video().Decode().To(goav.Write("out.webm", io.Discard)))
	if err == nil {
		t.Fatal("decoded frames into a container built; the README refusal no longer happens")
	}
	actual := strings.Split(err.Error(), "\n")
	next := 0
	for _, line := range readmeTextBlock(t, "goav: cannot build") {
		for next < len(actual) && actual[next] != line {
			next++
		}
		if next == len(actual) {
			t.Fatalf("README refusal line %q is not in the real error (in order):\n%v", line, err)
		}
	}
}

// TestReadmeFanoutRuns runs the README's fanout recipe on a real VP8+Opus
// WebM: both renditions come out with a video and an audio track.
func TestReadmeFanoutRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var fixture bytes.Buffer
	clip := goav.Write("in.webm", &fixture)
	if err := offlineRuntime().Run(ctx,
		goavtest.Video(64, 48, 3).Video().Encode(codec.VP8()).To(clip),
		goavtest.Audio(48_000, 2, make([]int16, 1920), make([]int16, 1920)).Audio().Encode(codec.Opus()).To(clip),
	); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	src := bytes.NewReader(fixture.Bytes())
	var hdFile, sdFile bytes.Buffer

	// The README block, verbatim.
	in := goav.FileInput("in.webm", src)
	video := in.Video().Decode()
	audio := in.Audio().Decode().Encode(codec.Opus())
	hd, sd := goav.Write("hd.webm", &hdFile), goav.Write("sd.webm", &sdFile)

	err := bundle.Run(ctx,
		video.Resize(1280, 720).Encode(codec.VP9()).As("hd").To(hd),
		video.Resize(640, 360).Encode(codec.VP9()).As("sd").To(sd),
		audio.To(hd, sd),
	)
	if err != nil {
		t.Fatalf("README fanout: %v", err)
	}
	for name, out := range map[string]*bytes.Buffer{"hd": &hdFile, "sd": &sdFile} {
		demuxer, err := matroska.NewDemuxer(bytes.NewReader(out.Bytes()), matroska.DemuxerOptions{})
		if err != nil {
			t.Fatalf("%s.webm: %v", name, err)
		}
		if tracks := demuxer.Tracks(); len(tracks) != 2 {
			t.Fatalf("%s.webm tracks = %+v, want video and audio", name, tracks)
		}
		packet := matroska.Packet{Data: make([]byte, 0, 1<<16)}
		perTrack := map[uint32]int{}
		for demuxer.ReadPacket(&packet) == nil {
			perTrack[packet.TrackID]++
		}
		if len(perTrack) != 2 {
			t.Fatalf("%s.webm packets per track = %v, want both tracks to carry media", name, perTrack)
		}
	}
}
