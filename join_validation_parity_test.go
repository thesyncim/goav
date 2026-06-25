package goav

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
)

func TestJoinOutputFormatValidationParity(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).
		To(File("out.unknown", io.Discard)).
		UseRuntime(MustNew(WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}))).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.OutputFormatUnknown)
}

func TestJoinDestinationShapeValidationParity(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).To(File("out.ogg", io.Discard, Format(av.FormatOgg))).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.MixDestination)
}

func TestJoinTransformAdapterValidationParity(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSourceRate("b", 44_100)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(MustNew()).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.ShapeAdapterMissing)
}

func TestJoinMuxCompatibilityValidationParity(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).
		To(File("out.ivf", io.Discard, Format(av.FormatIVF))).
		UseRuntime(MustNew(
			WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
			withTestFormats(testFormatMuxer(av.FormatIVF, writerTestMuxerFactory{})),
		)).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.DestinationMuxIncompatible)
}

func TestJoinRuntimeMissingValidationParity(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).
		To(File("out.ogg", io.Discard, Format(av.FormatOgg))).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.RuntimeMissing)
}

func TestJoinTapDomainValidationParity(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).Tap(PacketTap("mixed.packets")).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.TapDomainMismatch)
}

func assertJoinParityBuildError(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != code {
		t.Fatalf("err = %v, want %s", err, code)
	}
}
