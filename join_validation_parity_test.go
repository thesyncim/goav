package goav

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
)

func TestJoinOutputFormatValidationParity(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio(),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).
		To(Write("out.unknown", io.Discard)).
		UseRuntime(mustNew(WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}))).
		Build(context.Background())
	assertJoinParityBuildError(t, err, destinationFormatUnknownCode)
}

func TestJoinDestinationShapeValidationParity(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio(),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
	).To(Write("out.ogg", io.Discard, Format(av.FormatOgg))).
		Build(context.Background())
	assertJoinParityBuildError(t, err, encodeMissingCode)
}

func TestJoinTransformAdapterValidationParity(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio(),
		recipeFrom(mixTestAudioSourceRate("b", 44_100)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		UseRuntime(mustNew()).
		Build(context.Background())
	assertJoinParityBuildError(t, err, shapeAdapterMissingCode)
}

func TestJoinMuxCompatibilityValidationParity(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio(),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).
		To(Write("out.ivf", io.Discard, Format(av.FormatIVF))).
		UseRuntime(mustNew(
			WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
			withTestFormats(testFormatMuxer(av.FormatIVF, writerTestMuxerFactory{})),
		)).
		Build(context.Background())
	assertJoinParityBuildError(t, err, destinationMuxIncompatibleCode)
}

func TestJoinRuntimeMissingValidationParity(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(mixTestAudioSource("a", 1)).Audio(),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
	).Encode(codec.Opus()).
		To(Write("out.ogg", io.Discard, Format(av.FormatOgg))).
		UseRuntime(nil).
		Build(context.Background())
	assertJoinParityBuildError(t, err, errcode.RuntimeMissing)
}

func assertJoinParityBuildError(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != code {
		t.Fatalf("err = %v, want %s", err, code)
	}
}

// TestJoinArmTapBeforeDecodeIsRefused pins that a join arm's taps sit at its
// decoded point: a tap before a packet arm's .Decode() is refused instead of
// being anchored on the decoder.
func TestJoinArmTapBeforeDecodeIsRefused(t *testing.T) {
	_, err := legacyMix(
		recipeFrom(tapArmTestPacketSource("a", "x_pcm_mono", 1)).Audio().Tap(frameTapRef("raw")).Decode(),
		recipeFrom(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Describe()
	assertJoinParityBuildError(t, err, errcode.TapDomainMismatch)
	if !strings.Contains(err.Error(), "tap the arm after its decode") {
		t.Fatalf("err = %v, want move-the-tap guidance", err)
	}
}
