package main

import (
	"context"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/std"
)

func TestDecodedTapTasksNormalizeBrowserTrackShapes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	audioTask, err := goav.From(goav.Source("mic",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, ""), shape.Realtime(true)),
		func(context.Context, goav.SourcePush) error { return nil },
	)).
		UseRuntime(std.MustNew()).
		Audio().
		Decode().
		Shape(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))).
		Resample(48_000, codec.Stereo).
		Shape(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))).
		Tap(goav.FrameTap(audioTapName)).
		To(goav.Sink(newAudioMonitorSink(newAudioAnalyzer(), nil))).
		Build(ctx)
	if err != nil {
		t.Fatalf("audio task build error = %v", err)
	}
	defer audioTask.Close()

	videoTask, err := goav.From(goav.Source("camera",
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Realtime(true)),
		func(context.Context, goav.SourcePush) error { return nil },
	)).
		UseRuntime(std.MustNew()).
		Video().
		Decode().
		Shape(shape.Frame(av.MediaVideo, shape.Video(decodedVideoMaxWidth, decodedVideoMaxHeight, av.PixelFormatI420))).
		Resize(videoTapWidth, videoTapHeight).
		Shape(shape.Frame(av.MediaVideo, shape.Video(videoTapWidth, videoTapHeight, av.PixelFormatI420))).
		Tap(goav.FrameTap(videoTapName)).
		To(goav.Sink(newVideoAnalyzer())).
		Build(ctx)
	if err != nil {
		t.Fatalf("video task build error = %v", err)
	}
	defer videoTask.Close()
}
