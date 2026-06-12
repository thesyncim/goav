package main

import (
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func TestVideoAnalyzerValidatesDecodedFrameRate(t *testing.T) {
	analyzer := newVideoAnalyzer()
	start := time.Unix(1_700_000_000, 0)

	initial := analyzer.viewAt(start)
	if initial.Valid || initial.Status != "waiting" {
		t.Fatalf("initial = %+v, want waiting invalid", initial)
	}

	analyzer.observeFrameAt(&av.Frame{
		Type:  av.MediaVideo,
		Video: &av.VideoFrame{Width: 640, Height: 360, PixelFormat: av.PixelFormatI420},
	}, start)
	warming := analyzer.viewAt(start.Add(100 * time.Millisecond))
	if warming.Valid || warming.Status != "warming" {
		t.Fatalf("warming = %+v, want warming invalid", warming)
	}

	analyzer = newVideoAnalyzer()
	for i := 0; i <= 30; i++ {
		analyzer.observeFrameAt(&av.Frame{
			Type: av.MediaVideo,
			PTS:  av.Timestamp{Value: int64(i * 3000), Base: av.TimeBase{Num: 1, Den: 90_000}},
			Video: &av.VideoFrame{
				Width:       640,
				Height:      360,
				PixelFormat: av.PixelFormatI420,
			},
		}, start.Add(time.Duration(i)*time.Second/30))
	}

	view := analyzer.viewAt(start.Add(1100 * time.Millisecond))
	if !view.Valid || view.Status != "live" {
		t.Fatalf("status = %+v, want live valid", view)
	}
	if view.Width != 640 || view.Height != 360 || view.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("format = %+v", view)
	}
	if view.Frames != 31 {
		t.Fatalf("frames = %d, want 31", view.Frames)
	}
	if view.FPS < 29.5 || view.FPS > 30.5 {
		t.Fatalf("fps = %.2f, want about 30", view.FPS)
	}
	if view.LastPTS == "" || view.LastFrameMS != 100 {
		t.Fatalf("timing = %+v", view)
	}

	stale := analyzer.viewAt(start.Add(4 * time.Second))
	if stale.Valid || stale.Status != "stale" || stale.Warning == "" {
		t.Fatalf("stale status = %+v, want stale warning", stale)
	}
}
