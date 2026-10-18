package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type conformTestEmitter struct{ frames []*av.Frame }

func (e *conformTestEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	if msg.Kind == pipeline.MessageFrame {
		frame := *msg.Frame
		e.frames = append(e.frames, &frame)
	}
	return nil
}

func conformTestAudio(rate, channels, samples int) *av.Frame {
	return &av.Frame{
		StreamID: "b.webm",
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Base: av.TimeBase{Num: 1, Den: int64(rate)}},
		Duration: av.Duration{Value: int64(samples), Base: av.TimeBase{Num: 1, Den: int64(rate)}},
		Audio:    &av.AudioFrame{SampleRate: rate, Channels: channels, SampleFormat: av.SampleFormatS16, Samples: samples},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, samples*channels*2)}}},
	}
}

func conformTestVideo(width, height int, format string) *av.Frame {
	return &av.Frame{
		StreamID: "b.webm",
		Type:     av.MediaVideo,
		Video:    &av.VideoFrame{Width: width, Height: height, PixelFormat: format},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: make([]byte, width*height)}, Stride: width},
			{Buffer: av.Buffer{Bytes: make([]byte, width*height/4)}, Stride: width / 2},
			{Buffer: av.Buffer{Bytes: make([]byte, width*height/4)}, Stride: width / 2},
		},
	}
}

// TestJoinConformStageConvertsFramesAsTheyArrive pins the run-time half of a
// join over files: each arm's frames convert to the join format from the
// format they arrive in, frames already in it pass through, and a format
// change rebuilds the converter.
func TestJoinConformStageConvertsFramesAsTheyArrive(t *testing.T) {
	ctx := context.Background()
	service := &builder{runtime: testBundleRuntime()}

	audio := newJoinConformStage(service, "mix-conform-b.webm", "conform", shape.Frame(av.MediaAudio, shape.Audio(48_000, 2, av.SampleFormatS16)), false)
	emit := &conformTestEmitter{}
	for _, frame := range []*av.Frame{conformTestAudio(24_000, 1, 480), conformTestAudio(48_000, 2, 960), conformTestAudio(16_000, 1, 320)} {
		if err := audio.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}, emit); err != nil {
			t.Fatalf("audio conform: %v", err)
		}
	}
	if len(emit.frames) == 0 {
		t.Fatal("audio conform emitted nothing")
	}
	for _, frame := range emit.frames {
		if frame.Audio.SampleRate != 48_000 || frame.Audio.Channels != 2 || frame.StreamID != "b.webm" {
			t.Fatalf("conformed audio = %d Hz %d ch on %q, want 48000 Hz stereo on b.webm", frame.Audio.SampleRate, frame.Audio.Channels, frame.StreamID)
		}
	}
	if err := audio.Close(); err != nil {
		t.Fatal(err)
	}

	video := newJoinConformStage(service, "composite-conform-b.webm", "conform", shape.Frame(av.MediaVideo, shape.Video(0, 0, av.PixelFormatI420)), false)
	emit = &conformTestEmitter{}
	in := conformTestVideo(32, 24, av.PixelFormatI420)
	for _, frame := range []*av.Frame{in, conformTestVideo(32, 24, av.PixelFormatYUV420P)} {
		if err := video.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}, emit); err != nil {
			t.Fatalf("video conform: %v", err)
		}
	}
	if len(emit.frames) != 2 || emit.frames[0].Planes[0].Buffer.Bytes == nil || &emit.frames[0].Planes[0].Buffer.Bytes[0] != &in.Planes[0].Buffer.Bytes[0] {
		t.Fatalf("an I420 frame must pass through untouched: %+v", emit.frames)
	}
	if got := emit.frames[1].Video; got.PixelFormat != av.PixelFormatI420 || got.Width != 32 || got.Height != 24 {
		t.Fatalf("conformed video = %+v, want 32x24 I420", got)
	}
}
