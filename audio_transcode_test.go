package goav_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/errcode"
)

// TestAACToOpusTranscodesEndToEnd pins the canonical audio leg of an MP4 to
// WebM transcode: AAC decodes in 1024-sample frames at 44.1 kHz, Resample
// streams them to 48 kHz without per-frame drift, and the Opus encoder
// re-frames to 20 ms packets. The output must cover the input's duration,
// with packet times on the 20 ms grid.
func TestAACToOpusTranscodesEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := offlineRuntime().Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).
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

// TestOpusRefusesUnsupportedRateWithResampleFix pins the open-time descriptor
// check: a 44.1 kHz stream into Opus (8/12/16/24/48 kHz only) is refused as a
// typed BuildError naming the exact .Resample(...) to insert, not the
// adapter's bare unsupported-format sentinel.
func TestOpusRefusesUnsupportedRateWithResampleFix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	err = offlineRuntime().Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).
		Audio().
		Decode().
		Encode(codec.Opus()).
		To(goav.Write("out.webm", &bytes.Buffer{})))
	if errors.Is(err, codec.ErrUnavailable) {
		t.Skip("goaac backend unavailable on this target")
	}
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *goav.BuildError", err)
	}
	if buildErr.Code != errcode.Code("encode_adapter_incompatible") {
		t.Fatalf("code = %q, want encode_adapter_incompatible\nerr = %v", buildErr.Code, err)
	}
	if !strings.Contains(err.Error(), "insert .Resample(48000, 1) before .Encode(...)") {
		t.Fatalf("refusal does not name the resample fix:\n%v", err)
	}
}
