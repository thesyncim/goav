// The error-acceptance checklist: nine invalid constructions a user will
// actually write, each pinned to the full refusal contract — the error is a
// *goav.BuildError (errors.As), carries the right codes.Code, names the
// failing operation and node, and at least one Suggestion contains the exact
// user fix. This file is the living spec of error quality, consumer side:
// everything goes through the public grammar and goavtest. Deeper pins for
// some cases live next to their planners (shape_solver_test.go,
// recipe_api_test.go, recipe_compile_test.go); this file is the checklist.

package goav_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/codes"
	"github.com/thesyncim/goav/goavtest"
)

// requireBuildError enforces the four acceptance bars on one refusal: typed
// *BuildError, the expected code, the expected operation and node, and one
// Suggestion containing each decisive fix fragment.
func requireBuildError(t *testing.T, err error, code codes.Code, operation string, node string, fixes ...string) *goav.BuildError {
	t.Helper()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *goav.BuildError", err)
	}
	if buildErr.Code != code {
		t.Fatalf("code = %q, want %q\nerr = %v", buildErr.Code, code, err)
	}
	if buildErr.Operation != operation {
		t.Fatalf("operation = %q, want %q\nerr = %v", buildErr.Operation, operation, err)
	}
	if buildErr.Node != node {
		t.Fatalf("node = %q, want %q\nerr = %v", buildErr.Node, node, err)
	}
	for _, fix := range fixes {
		if !suggestionsContain(buildErr.Suggestions, fix) {
			t.Fatalf("no suggestion contains the fix %q\nerr = %v", fix, err)
		}
	}
	return buildErr
}

func suggestionsContain(suggestions []string, fragment string) bool {
	for i := range suggestions {
		if strings.Contains(suggestions[i], fragment) {
			return true
		}
	}
	return false
}

func detailsContain(details []string, fragment string) bool {
	for i := range details {
		if strings.Contains(details[i], fragment) {
			return true
		}
	}
	return false
}

func opusPacketInput() goav.InputSpec {
	return goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}})
}

// TestErrorAcceptanceCopyAfterDecode is snippet 1: .Decode().Copy() asks for
// packet copy in the frame domain. The refusal states the domain rule and the
// two real fixes — copy before decode, or re-encode the frames.
func TestErrorAcceptanceCopyAfterDecode(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Decode().Copy().
		To(goav.File("out.ogg", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	buildErr := requireBuildError(t, err, codes.OperationShapeMismatch, "build job", "audio",
		"copy only consumes packet-domain media",
		"move .Copy() before decode",
		"use .Encode(codec...) instead of .Copy()",
	)
	if !detailsContain(buildErr.Details, "actual_shape=domain=frame") {
		t.Fatalf("details should carry the frame-domain shape, err = %v", err)
	}
}

// TestErrorAcceptanceFramesIntoContainerWithoutEncode is snippet 2:
// .Decode().To(File(...)) routes decoded frames into a container. The fix is
// the exact .Encode(codec...) call to add.
func TestErrorAcceptanceFramesIntoContainerWithoutEncode(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Decode().
		To(goav.File("out.ogg", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	requireBuildError(t, err, codes.EncodeMissing, "build job", "audio",
		".Encode(codec.Opus(...))",
		"goav.Sink(...)",
	)
}

// TestErrorAcceptanceTransformAfterCopy is snippet 3: .Copy().Resize(...)
// declares a frame transform on a packet-domain chain. The refusal names the
// domain rule and says to decode first.
func TestErrorAcceptanceTransformAfterCopy(t *testing.T) {
	_, err := goav.From(goavtest.Packets(av.CodecVP8, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}})).
		Video().Copy().Resize(640, 360).
		To(goav.File("out.ivf", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	buildErr := requireBuildError(t, err, codes.OperationShapeMismatch, "build stream", "video",
		"call .Decode() before .Resize(...)",
	)
	if !strings.Contains(buildErr.Reason, ".Copy() keeps the stream packet-encoded") {
		t.Fatalf("reason should state the packet-domain rule, err = %v", err)
	}
}

// TestErrorAcceptanceDestinationFormatUnknown is snippet 4a: a File
// destination whose name resolves to no known container. The fix is the
// goav.Format(...) option.
func TestErrorAcceptanceDestinationFormatUnknown(t *testing.T) {
	_, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).
		To(goav.File("out.weird", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	requireBuildError(t, err, codes.DestinationFormatUnknown, "open destination", "out.weird",
		"pass goav.Format(...)",
	)
}

// TestErrorAcceptanceDestinationMuxerMissing is snippet 4b: the format is
// detected but the runtime has no muxer registered for it. The fix names
// goav.WithMuxer(...).
func TestErrorAcceptanceDestinationMuxerMissing(t *testing.T) {
	_, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).
		To(goav.File("out.ogg", io.Discard)).
		UseRuntime(goav.New(goav.WithStdFilters(), goavtest.Codec(av.CodecOpus))).
		Build(context.Background())
	requireBuildError(t, err, codes.DestinationMuxerMissing, "open destination", "out.ogg",
		"goav.WithMuxer(...)",
	)
}

// TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates is snippet 5:
// two inputs both match an unnarrowed .Audio() chain. The refusal lists every
// candidate with its input and suggests the exact narrowing options.
func TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates(t *testing.T) {
	_, err := goav.From(
		goavtest.Audio(48000, 1, []int16{1}).With(goav.Name("mic-a")),
		goavtest.Audio(48000, 1, []int16{1}).With(goav.Name("mic-b")),
	).
		Audio().
		To(goavtest.NewCollector().Sink()).
		Build(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != codes.StreamAmbiguous || !errors.Is(err, goav.ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "input=mic-a") || !strings.Contains(msg, "input=mic-b") {
		t.Fatalf("err = %v, want candidates listed with their inputs", err)
	}
	if !suggestionsContain(buildErr.Suggestions, `.Audio(goav.InputName("mic-a"))`) {
		t.Fatalf("err = %v, want InputName narrowing suggestion", err)
	}
	if !suggestionsContain(buildErr.Suggestions, "goav.StreamID(") {
		t.Fatalf("err = %v, want StreamID narrowing suggestion", err)
	}
}

// TestErrorAcceptanceAttachUnknownTapListsDeclaredTaps is snippet 6: a
// runtime Branch anchored on a tap the task never declared. The refusal lists
// the taps that DO exist and points at task.Taps().
func TestErrorAcceptanceAttachUnknownTapListsDeclaredTaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := goav.From(goavtest.LiveAudio("mic", 48000, 1)).
		Audio().Tap(goav.FrameTap("audio.decoded")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	_, err = task.Attach(ctx, goav.Branch("late").From(goav.FrameTap("nope")).To(goavtest.NewCollector().Sink()))
	buildErr := requireBuildError(t, err, codes.RuntimeBranchTapMissing, "attach runtime branch", "nope",
		`add .Tap(goav.FrameTap("nope"))`,
		"call task.Taps() before attaching",
	)
	if !detailsContain(buildErr.Details, "audio.decoded") {
		t.Fatalf("details should list the declared taps, err = %v", err)
	}
}

// TestErrorAcceptanceTypedTapAtWrongDomain is snippet 7: a FrameTap declared
// at a packet-domain point (after the encoder). The refusal carries the
// typed-tap fix for each domain.
func TestErrorAcceptanceTypedTapAtWrongDomain(t *testing.T) {
	_, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).Tap(goav.FrameTap("post-encode")).
		To(goav.File("out.ogg", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	buildErr := requireBuildError(t, err, codes.TapDomainMismatch, "build stream", "audio",
		"use goav.PacketTap(name) after .Copy() or an encoder",
		"use goav.FrameTap(name) after decode",
	)
	if !detailsContain(buildErr.Details, "tap=post-encode") {
		t.Fatalf("details should name the tap, err = %v", err)
	}
}

// TestErrorAcceptanceEncoderAdapterMissing is snippet 8: .Encode with a codec
// no registered encoder provides. The refusal names the codec and the
// goav.WithEncoder(...) registration fix.
func TestErrorAcceptanceEncoderAdapterMissing(t *testing.T) {
	_, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Codec("weird", av.MediaAudio)).
		To(goav.File("out.ogg", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	buildErr := requireBuildError(t, err, codes.EncodeAdapterMissing, "build job", "audio",
		"goav.WithEncoder(...)",
	)
	if !strings.Contains(buildErr.Reason, "weird") || !detailsContain(buildErr.Details, "codec=weird") {
		t.Fatalf("refusal should name the codec, err = %v", err)
	}
}

// TestErrorAcceptanceShapeConversionRefused is snippet 9: the chain needs a
// conversion (44.1kHz into a 48kHz encoder) and the active .Auto() policy
// allows nothing. The refusal carries the exact policy to add. The full
// solver contract is pinned in shape_solver_test.go; this is the checklist
// entry.
func TestErrorAcceptanceShapeConversionRefused(t *testing.T) {
	_, err := goav.From(goavtest.Audio(44100, 2, []int16{1, 1})).
		Audio().Auto().Encode(codec.Opus()).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(context.Background())
	requireBuildError(t, err, codes.ShapeConversionRefused, "build job", "audio",
		"add .Auto(shape.AllowResample())",
		"insert .Resample(48000, 2) explicitly",
	)
}
