package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/shape"
)

func TestFrameDomainSourceRejectsDecodeAndCopy(t *testing.T) {
	ctx := context.Background()
	source := mixTestAudioSource("frames", 100)
	sink := Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))
	tests := []struct {
		name string
		out  Output
		code errcode.Code
	}{
		{
			name: "direct decode",
			out: source.Audio().
				Decode().
				To(sink),
			code: sourceShapeMismatchCode,
		},
		{
			name: "copy through encode helper",
			out: source.Audio().
				Encode(codec.Copy()).
				To(sink),
			code: encodeParameterInvalidCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mustNew().Build(ctx, tt.out)
			assertBuildErrorCode(t, err, tt.code)
		})
	}
}

func TestStreamChainRejectsInvalidPostEncodeAndTapContracts(t *testing.T) {
	ctx := context.Background()
	source := mixTestAudioSource("frames", 100)
	sink := Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))
	meter := FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit Emit) error {
		return emit.Frame(frame)
	})
	tests := []struct {
		name string
		out  Output
		code errcode.Code
	}{
		{
			name: "nil stage",
			out: source.Audio().
				Do(nil).
				To(sink),
			code: stageMissingCode,
		},
		{
			name: "shape after encode",
			out: source.Audio().
				Encode(codec.Opus()).
				Shape(shape.Frame(av.MediaAudio)).
				To(sink),
			code: streamStepAfterEncodeCode,
		},
		{
			name: "stage after encode",
			out: source.Audio().
				Encode(codec.Opus()).
				Do(meter).
				To(sink),
			code: streamStepAfterEncodeCode,
		},
		{
			name: "empty tap",
			out: source.Audio().
				Tap("").
				To(sink),
			code: errcode.TapInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mustNew().Build(ctx, tt.out)
			assertBuildErrorCode(t, err, tt.code)
		})
	}
}
