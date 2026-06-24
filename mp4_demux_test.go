package goav_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/std"
)

const mp4Fixture = "container/mp4/testdata/h264_aac.mp4"
const mp4FragmentedFixture = "container/mp4/testdata/h264_aac_fragmented.mp4"

// offlineRuntime decodes at full speed (no realtime clock pacing) so file tests
// do not wait wall-clock time.
func offlineRuntime() goav.Runtime {
	return std.New(goav.WithRealtime(false))
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

	rt := std.NewFormats(goavtest.Codec(av.CodecH264), goav.WithRealtime(false))
	out := goavtest.NewCollector()
	if err := goav.From(goav.FileInput("h264_aac.mp4", file)).
		UseRuntime(rt).
		Video().
		Decode().
		To(out.Sink()).
		Run(ctx); err != nil {
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

// TestMP4DemuxesAndDecodesAudioThroughGrammar proves the AAC track demuxed from
// the same file (its AudioSpecificConfig recovered from esds) decodes to audio
// frames.
func TestMP4DemuxesAndDecodesAudioThroughGrammar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	file, err := os.Open(mp4Fixture)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer file.Close()

	out := goavtest.NewCollector()
	if err := goav.From(goav.FileInput("h264_aac.mp4", file)).
		UseRuntime(offlineRuntime()).
		Audio().
		Decode().
		To(out.Sink()).
		Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(out.Frames()) == 0 {
		t.Fatal("no decoded audio frames from the MP4")
	}
}

// TestMP4DemuxesFragmentedAudioThroughGrammar proves a fragmented (fMP4) file —
// whose samples live in moof/trun rather than the moov sample tables — demuxes
// and decodes its AAC track end to end through the grammar.
func TestMP4DemuxesFragmentedAudioThroughGrammar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	file, err := os.Open(mp4FragmentedFixture)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer file.Close()

	out := goavtest.NewCollector()
	if err := goav.From(goav.FileInput("fragmented.mp4", file)).
		UseRuntime(offlineRuntime()).
		Audio().
		Decode().
		To(out.Sink()).
		Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out.Frames()) == 0 {
		t.Fatal("no decoded audio frames from the fragmented MP4")
	}
}
