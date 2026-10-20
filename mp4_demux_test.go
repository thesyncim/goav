package goav_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
)

const mp4Fixture = "container/mp4/testdata/h264_aac.mp4"

// offlineRuntime decodes at full speed (no realtime clock pacing) so file tests
// do not wait wall-clock time.
func offlineRuntime() *goav.Runtime {
	return bundle.MustNew(runconfig.WithRealtime(false))
}

// TestMP4DemuxesAndDecodesVideoThroughGrammar proves the MP4 demuxer is wired
// into the standard format set and feeds the selected H264 stream — with its
// avcC geometry — into a decoder through the front-door grammar. The default
// build's H264 decoder is gated behind the goav_goh264 tag, so this drives a
// passthrough decoder to keep the wiring proof independent of that opt-in.
func TestMP4DemuxesAndDecodesVideoThroughGrammar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	file, err := os.Open(mp4Fixture)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer file.Close()

	rt := mustRuntime(bundle.Formats(), goavtest.Codec(av.CodecH264), runconfig.WithRealtime(false))
	out := goavtest.NewCollector()
	if err := rt.Run(ctx, goav.FileInput("h264_aac.mp4", file).
		Video().
		Decode().
		To(out.Sink())); err != nil {
		t.Fatalf("run: %v", err)
	}

	frames := out.Frames()
	if len(frames) == 0 {
		t.Fatal("no decoded video frames from the MP4")
	}
	for _, f := range frames {
		if f.Type != av.MediaVideo || f.Video == nil || f.Video.Width != 64 || f.Video.Height != 48 {
			t.Fatalf("frame = %+v, want 64x48 video", f.Video)
		}
	}
}
