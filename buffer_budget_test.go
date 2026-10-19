package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

func TestRealtimeEncodeRecipeRefusesUnsupportedBufferBudgetFact(t *testing.T) {
	ctx := context.Background()
	rt := mustNew(WithRealtime(true), WithEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &controlRecordingEncoderFactory{}))
	source := Source("rgb",
		shape.Frame(av.MediaVideo, shape.Video(16, 16, "rgb24"), shape.Stream("rgb")),
		func(context.Context, SourcePush) error { return nil },
	)

	_, err := rt.Build(ctx, source.
		Video().
		Encode(codec.VP8(codec.Bitrate(600_000))).
		To(Sink(SinkFunc("packets", func(context.Context, Message) error { return nil }))))
	if err == nil {
		t.Fatal("Build() error = nil, want buffer_budget_missing")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want *BuildError", err)
	}
	if buildErr.Code != bufferBudgetMissingCode {
		t.Fatalf("code = %q, want %q", buildErr.Code, bufferBudgetMissingCode)
	}
	if !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want errUnsupportedBuild sentinel", err)
	}
	if !strings.Contains(buildErr.Reason, `unsupported pixel_format "rgb24"`) {
		t.Fatalf("reason = %q, want unsupported pixel_format fact", buildErr.Reason)
	}
	if len(buildErr.FixLines()) == 0 {
		t.Fatalf("suggestions = nil, want user fix")
	}
}

// TestRealtimeDecodeRecipeBuildsWithUnknownBufferBudgetFact pins that a fact
// the planner cannot know before Build (here the decoded sample format) no
// longer refuses a buffered graph: the copy budget falls back to a ceiling,
// which costs nothing because slot backing grows to the frames it carries.
func TestRealtimeDecodeRecipeBuildsWithUnknownBufferBudgetFact(t *testing.T) {
	ctx := context.Background()
	rt := mustNew(WithRealtime(true), WithDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))
	source := Source("audio",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, codec.Mono, "")),
		func(context.Context, SourcePush) error { return nil },
	)

	task, err := rt.Build(ctx, source.
		Audio().Decode().
		To(Sink(SinkFunc("frames", func(context.Context, Message) error { return nil }))))
	if err != nil {
		t.Fatalf("Build() error = %v, want the buffered graph to build with a ceiling budget", err)
	}
	_ = task.Close()
}
