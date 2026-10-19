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
	_, err := mustNew(WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}})).Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 1).Audio()).
			Encode(codec.Opus()).
			To(Write("out.unknown", io.Discard)),
	)
	assertJoinParityBuildError(t, err, destinationFormatUnknownCode)
}

func TestJoinDestinationShapeValidationParity(t *testing.T) {
	_, err := mustNew().Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 1).Audio()).
			To(Write("out.ogg", io.Discard, Format(av.FormatOgg))),
	)
	assertJoinParityBuildError(t, err, encodeMissingCode)
}

func TestJoinTransformAdapterValidationParity(t *testing.T) {
	_, err := mustNew().Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSourceRate("b", 44_100).Audio()).
			To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))),
	)
	assertJoinParityBuildError(t, err, shapeAdapterMissingCode)
}

func TestJoinMuxCompatibilityValidationParity(t *testing.T) {
	_, err := mustNew(
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		withTestFormats(testFormatMuxer(av.FormatIVF, writerTestMuxerFactory{})),
	).Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 1).Audio()).
			Encode(codec.Opus()).
			To(Write("out.ivf", io.Discard, Format(av.FormatIVF))),
	)
	assertJoinParityBuildError(t, err, destinationMuxIncompatibleCode)
}

func TestJoinRuntimeMissingValidationParity(t *testing.T) {
	_, err := (*Runtime)(nil).Build(context.Background(),
		Mix(mixTestAudioSource("a", 1).Audio(), mixTestAudioSource("b", 1).Audio()).
			Encode(codec.Opus()).
			To(Write("out.ogg", io.Discard, Format(av.FormatOgg))),
	)
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
	_, err := mustNew().Describe(
		Mix(
			tapArmTestPacketSource("a", "x_pcm_mono", 1).Audio().Tap("raw").Decode(),
			mixTestAudioSource("b", 1).Audio(),
		).
			To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))),
	)
	assertJoinParityBuildError(t, err, errcode.TapDomainMismatch)
	if !strings.Contains(err.Error(), "tap the arm after its decode") {
		t.Fatalf("err = %v, want move-the-tap guidance", err)
	}
}
