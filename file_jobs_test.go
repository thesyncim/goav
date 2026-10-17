package goav_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/webm"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// encodeTestWebM produces a real VP8 WebM of frames 64x48 I420 frames at
// 30 fps through the bundled encoder, as a stand-in for a user's file.
func encodeTestWebM(t *testing.T, frames int) []byte {
	t.Helper()
	const width, height = 64, 48
	base := av.TimeBase{Num: 1, Den: 30}
	src := goav.Source("camera",
		shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
			for i := range frames {
				planes := []av.Plane{
					{Buffer: av.Buffer{Bytes: bytes.Repeat([]byte{byte(16 + i*4)}, width*height)}, Stride: width},
					{Buffer: av.Buffer{Bytes: bytes.Repeat([]byte{128}, width*height/4)}, Stride: width / 2},
					{Buffer: av.Buffer{Bytes: bytes.Repeat([]byte{128}, width*height/4)}, Stride: width / 2},
				}
				frame := &av.Frame{
					Type:     av.MediaVideo,
					PTS:      av.Timestamp{Value: int64(i), Base: base},
					Duration: av.Duration{Value: 1, Base: base},
					Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
					Planes:   planes,
				}
				if _, err := push.Frame(frame); err != nil {
					return err
				}
			}
			return nil
		},
	)
	var out bytes.Buffer
	if err := bundle.Run(context.Background(), goav.From(src).Video().Encode(codec.VP8()).To(goav.Write("in.webm", &out))); err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	return out.Bytes()
}

// TestReadmeHeadlineRecipeRunsWithDefaults executes the README's headline
// shape on a real file with no runtime options: decode, resize, re-encode,
// and mux. File-to-file jobs are offline by default, so the job neither
// waits on a media clock nor needs plan-time geometry for buffered copies.
// The compile pin (TestReadmeGoExamplesCompile) cannot catch a recipe that
// builds but fails at run time; this test can.
func TestReadmeHeadlineRecipeRunsWithDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const frames = 30
	in := encodeTestWebM(t, frames)

	var out bytes.Buffer
	start := time.Now()
	err := bundle.Run(ctx, goav.From(goav.FileInput("in.webm", bytes.NewReader(in))).
		Video().
		Decode().
		Resize(32, 24).
		Encode(codec.VP9(codec.Bitrate(200_000))).
		To(goav.Write("out.webm", &out)))
	if err != nil {
		t.Fatalf("headline recipe: %v", err)
	}
	// One second of 30 fps media must not take a second of wall time: a
	// paced (realtime) file job would.
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("1s of media took %v; file-to-file jobs must run unpaced by default", elapsed)
	}

	demuxer, err := webm.NewDemuxer(bytes.NewReader(out.Bytes()), webm.DemuxerOptions{})
	if err != nil {
		t.Fatalf("demux output: %v", err)
	}
	if tracks := demuxer.Tracks(); len(tracks) != 1 || tracks[0].Codec != webm.CodecVP9 ||
		tracks[0].Video.Width != 32 || tracks[0].Video.Height != 24 {
		t.Fatalf("output tracks = %+v, want one 32x24 VP9 track", tracks)
	}
	packet := webm.Packet{Data: make([]byte, 0, 1<<16)}
	got := 0
	for demuxer.ReadPacket(&packet) == nil {
		got++
	}
	if got != frames {
		t.Fatalf("output has %d packets, want %d", got, frames)
	}
}
