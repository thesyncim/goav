package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

// TestBufferedRecipesNeedNoPayloadSizeFacts pins that a buffered graph builds
// whatever the planner knows about payload sizes: queued messages hold
// references to their payload instead of copies, so there is no per-slot copy
// budget to derive. A pixel format the planner cannot size (rgb24) and a
// sample format it does not know before Build both build.
func TestBufferedRecipesNeedNoPayloadSizeFacts(t *testing.T) {
	ctx := context.Background()
	sink := Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))

	encode := mustNew(WithRealtime(true), WithEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &controlRecordingEncoderFactory{}))
	rgb := Source("rgb",
		shape.Frame(av.MediaVideo, shape.Video(16, 16, "rgb24"), shape.Stream("rgb")),
		func(context.Context, SourcePush) error { return nil },
	)
	task, err := encode.Build(ctx, rgb.Video().Encode(codec.VP8(codec.Bitrate(600_000))).To(sink))
	if err != nil {
		t.Fatalf("realtime rgb24 encode Build() = %v, want it to build", err)
	}
	_ = task.Close()

	decode := mustNew(WithRealtime(true), WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))
	audio := Source("audio",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, codec.Mono, "")),
		func(context.Context, SourcePush) error { return nil },
	)
	task, err = decode.Build(ctx, audio.Audio().Decode().To(sink))
	if err != nil {
		t.Fatalf("realtime decode with an unknown sample format Build() = %v, want it to build", err)
	}
	_ = task.Close()
}
