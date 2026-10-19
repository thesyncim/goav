package flv_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/container/flv"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
)

// file x.flv works both ways through the bundled runtime: an MP4's H.264
// and AAC copy into FLV, whose AAC decodes, and E-FLV VP9 and Opus decode.
func TestBundleCopiesMP4ToFLVAndDecodesIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	in, err := os.ReadFile("../mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "out.flv")
	if err := bundle.Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(in)).To(goav.URI(path))); err != nil {
		t.Fatalf("copy to flv: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	d := &flv.Demuxer{}
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	var codecs []string
	for _, s := range d.Streams() {
		codecs = append(codecs, string(s.Codec.ID))
	}
	if strings.Join(codecs, ",") != "h264,aac" {
		t.Fatalf("flv streams %v", codecs)
	}
	if probe, err := exec.LookPath("ffprobe"); err == nil {
		out, err := exec.Command(probe, "-v", "error", "-show_entries", "stream=codec_name", "-of", "csv=p=0", path).CombinedOutput()
		if err != nil || strings.Join(strings.Fields(string(out)), ",") != "h264,aac" {
			t.Fatalf("ffprobe: %v %s", err, out)
		}
	}

	frames := goavtest.NewCollector()
	if err := bundle.Run(ctx, goav.URIInput("testdata/vp9_opus.flv").Video().Decode().To(frames.Sink())); err != nil {
		t.Fatalf("decode flv: %v", err)
	}
	if n := len(frames.Frames()); n != 10 || frames.Frames()[0].Video.Width != 64 {
		t.Fatalf("%d VP9 frames decoded, want 10 of 64 pixels wide", n)
	}
	audio := goavtest.NewCollector()
	if err := bundle.Run(ctx, goav.URIInput(path).Audio().Decode().To(audio.Sink())); err != nil {
		t.Fatalf("decode flv audio: %v", err)
	}
	if n := len(audio.Frames()); n == 0 || audio.Frames()[0].Type != av.MediaAudio {
		t.Fatalf("%d audio frames", n)
	}
}
