package gplcodec_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/gplcodec"
	"github.com/thesyncim/goav/runconfig"
)

// runtimeWithAAC is the bundle plus the opt-in AAC decoder, unpaced.
func runtimeWithAAC() *goav.Runtime {
	return bundle.MustNew(gplcodec.AAC(), runconfig.WithRealtime(false))
}

// TestMP4AudioDecodesThroughGrammar proves the AAC track demuxed from plain
// and fragmented MP4 (its AudioSpecificConfig recovered from esds) decodes to
// audio frames once gplcodec.AAC() is registered.
func TestMP4AudioDecodesThroughGrammar(t *testing.T) {
	for _, fixture := range []string{"h264_aac.mp4", "h264_aac_fragmented.mp4"} {
		t.Run(fixture, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			data, err := os.ReadFile("../container/mp4/testdata/" + fixture)
			if err != nil {
				t.Fatal(err)
			}
			out := goavtest.NewCollector()
			err = runtimeWithAAC().Run(ctx, goav.FileInput(fixture, bytes.NewReader(data)).Audio().Decode().To(out.Sink()))
			if errors.Is(err, codec.ErrUnavailable) {
				t.Skip("goaac backend unavailable on this target")
			}
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if len(out.Frames()) == 0 {
				t.Fatal("no decoded audio frames")
			}
		})
	}
}

// TestAACToOpusTranscodesEndToEnd pins the canonical audio leg of an MP4 to
// WebM transcode: AAC decodes in 1024-sample frames at 44.1 kHz, Resample
// streams them to 48 kHz without per-frame drift, and the Opus encoder
// re-frames to 20 ms packets. The output must cover the input's duration,
// with packet times on the 20 ms grid.
func TestAACToOpusTranscodesEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile("../container/mp4/testdata/h264_aac.mp4")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runtimeWithAAC().Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).
		Audio().
		Decode().
		Resample(48000, 1).
		Encode(codec.Opus()).
		To(goav.Write("out.webm", &out))); err != nil {
		if errors.Is(err, codec.ErrUnavailable) {
			t.Skip("goaac backend unavailable on this target")
		}
		t.Fatalf("run: %v", err)
	}
	demuxer, err := webm.NewDemuxer(bytes.NewReader(out.Bytes()), webm.DemuxerOptions{})
	if err != nil {
		t.Fatalf("demux output: %v", err)
	}
	packet := webm.Packet{Data: make([]byte, 0, 1<<16)}
	var times []int64
	for demuxer.ReadPacket(&packet) == nil {
		times = append(times, packet.TimeNS)
	}
	// The fixture carries about 0.51 s of audio: at least 25 whole 20 ms
	// packets, each 20 ms after the last.
	if len(times) < 25 {
		t.Fatalf("output has %d Opus packets, want >= 25 for ~0.5 s of audio", len(times))
	}
	for i := 1; i < len(times); i++ {
		if step := times[i] - times[i-1]; step != int64(20*time.Millisecond) {
			t.Fatalf("packet %d is %v after packet %d, want 20ms", i, time.Duration(step), i-1)
		}
	}
}
