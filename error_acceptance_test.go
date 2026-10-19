// The error-acceptance checklist: invalid constructions a user will actually
// write, each pinned to the full refusal contract — the error is a
// *goav.BuildError (errors.As), carries the right errcode.Code, names the
// failing operation and node, and at least one Suggestion contains the exact
// user fix. This file is the living spec of error quality, consumer side:
// everything goes through the public grammar and goavtest. Deeper pins for
// some cases live next to their planners (shape_solver_test.go,
// recipe_stream_test.go, recipe_compile_test.go); this file is the checklist.

package goav_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// requireBuildError enforces the four acceptance bars on one refusal: typed
// *BuildError, the expected code, the expected operation and node, and one
// Suggestion containing each decisive fix fragment.
func requireBuildError(t *testing.T, err error, code errcode.Code, operation string, node string, fixes ...string) *goav.BuildError {
	t.Helper()
	if leak := internalErrorIn(err); leak != "" {
		t.Fatalf("err chain carries the internal %s; the root converts every compiler refusal to *goav.BuildError", leak)
	}
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
		if !suggestionsContain(buildErr.FixLines(), fix) {
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

func audioFrameInput() goav.InputSpec {
	return goav.Source("audio",
		shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Mono, av.SampleFormatS16), shape.Stream("audio")),
		func(context.Context, source.Push) error {
			return nil
		})
}

// TestErrorAcceptanceNilProviderInput is snippet 0a: a nil source provider
// handed to goav.Input(...). The refusal keeps the public input constructor
// vocabulary in the suggestions.
func TestErrorAcceptanceNilProviderInput(t *testing.T) {
	_, err := mustRuntime().Describe(goav.Input(nil).
		Audio().
		Decode().
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("input_invalid"), "build input", "input",
		"check the input constructor arguments",
		"pass a non-nil provider to goav.Input(provider)",
	)
}

// TestErrorAcceptanceNilFileInputReader is snippet 0aa: FileInput needs a
// real reader, so a filename alone cannot masquerade as an open input.
func TestErrorAcceptanceNilFileInputReader(t *testing.T) {
	_, err := mustRuntime().Describe(goav.FileInput("input.ogg", nil).
		Audio().
		To(goav.Write("out.ogg", io.Discard)))
	requireBuildError(t, err, errcode.Code("input_invalid"), "build input", "input.ogg",
		"pass a non-nil io.Reader to goav.FileInput(name, reader)",
	)
}

// TestErrorAcceptanceNilCustomDestinationProvider is snippet 0ab: Custom wraps
// a provider.Destination, so a nil provider is not reported as a nil writer.
func TestErrorAcceptanceNilCustomDestinationProvider(t *testing.T) {
	_, err := mustRuntime().Describe(opusPacketInput().
		Audio().
		To(goav.Custom("out.ogg", nil)))
	buildErr := requireBuildError(t, err, errcode.Code("output_invalid"), "build job", "out.ogg",
		"pass a non-nil provider.Destination to goav.Custom(name, destination)",
	)
	if !strings.Contains(buildErr.Reason, "nil destination provider") {
		t.Fatalf("reason = %q, want nil destination provider", buildErr.Reason)
	}
}

// TestErrorAcceptanceNilWriter is snippet 0ac: writer-backed outputs need an
// actual io.Writer and are not treated like adapter-opened URIs.
func TestErrorAcceptanceNilWriter(t *testing.T) {
	_, err := mustRuntime().Describe(opusPacketInput().
		Audio().
		To(goav.Write("out.ogg", nil)))
	requireBuildError(t, err, errcode.Code("output_writer_missing"), "build job", "out.ogg",
		"pass a non-nil io.Writer to goav.Write(name, writer)",
	)
}

// TestErrorAcceptanceMissingInput is snippet 0b: a stream of an input that
// was never constructed. The fix is to start from a real input constructor.
func TestErrorAcceptanceMissingInput(t *testing.T) {
	_, err := mustRuntime().Describe(goav.InputSpec{}.Audio().To(goav.Write("out.ogg", io.Discard)))
	requireBuildError(t, err, errcode.Code("stream_invalid"), "build recipe", "",
		"start each stream from an input or a join: input.Video(), input.Audio(), goav.Mix(a, b)",
	)
}

// TestErrorAcceptanceCustomSourceMissingCallback is snippet 0c: a custom
// source with no SourceFunc. The refusal tells the author to pass the
// Source(name, shape, fn) callback or use another input seam.
func TestErrorAcceptanceCustomSourceMissingCallback(t *testing.T) {
	_, err := mustRuntime().Describe(goav.Source("mic", shape.Frame(av.MediaAudio), nil).
		Audio().
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("source_callback_missing"), "build input", "mic",
		"pass a non-nil callback to goav.Source(name, shape, fn)",
		"use goav.FileInput or goav.Input(provider)",
	)
}

// TestErrorAcceptanceCustomSourceShapeInvalid is snippet 0d: a packet-domain
// custom source that forgot to declare audio or video. The refusal names the
// shape.Packet/shape.Media fixes before any runtime opens.
func TestErrorAcceptanceCustomSourceShapeInvalid(t *testing.T) {
	_, err := mustRuntime().Describe(goav.Source("bad", shape.New(shape.Domain(shape.DomainPacket)),
		func(context.Context, source.Push) error { return nil },
	).
		Audio().
		Decode().
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("source_shape_invalid"), "build input", "bad",
		"use shape.Packet(av.MediaAudio, codec)",
		"add shape.Media(...)",
	)
}

// TestErrorAcceptanceCustomSourceShapeUnsupported is snippet 0e: a custom
// source declares a domain outside packet/frame/event. The refusal lists the
// supported source-shape constructors.
func TestErrorAcceptanceCustomSourceShapeUnsupported(t *testing.T) {
	_, err := mustRuntime().Describe(goav.Source("bytes",
		shape.New(shape.Domain(shape.MediaDomain("bytes")), shape.Media(av.MediaAudio)),
		func(context.Context, source.Push) error { return nil },
	).
		Audio().
		Decode().
		To(goavtest.NewCollector().Sink()))
	buildErr := requireBuildError(t, err, errcode.Code("source_shape_unsupported"), "build input", "bytes",
		"declare the source with shape.Packet",
		"declare raw generated media with shape.Frame",
		"declare diagnostic or lifecycle sources with shape.Event",
	)
	if !detailsContain(buildErr.DetailLines(), "actual_shape=domain=bytes") {
		t.Fatalf("details should carry the unsupported domain, err = %v", err)
	}
}

// TestErrorAcceptanceFrameSourceDecodeMismatch is snippet 0f: frame-domain
// custom sources are already decoded, so .Decode() is a shape mismatch with a
// public grammar fix.
func TestErrorAcceptanceFrameSourceDecodeMismatch(t *testing.T) {
	_, err := mustRuntime().Describe(goav.Source("frames", shape.Frame(av.MediaAudio),
		func(_ context.Context, push source.Push) error { return push.EOS() },
	).
		Audio().
		Decode().
		To(goavtest.NewCollector().Sink()))
	buildErr := requireBuildError(t, err, errcode.Code("source_shape_mismatch"), "build stream", "audio",
		"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
		"use shape.Packet(...) when the custom source pushes encoded packets",
	)
	if !detailsContain(buildErr.DetailLines(), "source_domain=frame") {
		t.Fatalf("details should carry source_domain=frame, err = %v", err)
	}
}

// TestErrorAcceptanceBranchDecodeDuplicate is snippet 0p: a branch decodes
// twice. The refusal keeps branch decode as a single initial operation.
func TestErrorAcceptanceBranchDecodeDuplicate(t *testing.T) {
	_, err := mustRuntime().Describe(opusPacketInput().
		Audio().
		Decode().
		Decode().
		As("bad").
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("branch_decode_duplicate"), "build branch", "bad",
		"call .Decode() once before frame operations",
		"remove the second .Decode() call",
	)
}

// TestErrorAcceptanceBranchDecodeOrderInvalid is snippet 0q: a fork does
// frame work before Decode. The fix is Decode first.
func TestErrorAcceptanceBranchDecodeOrderInvalid(t *testing.T) {
	_, err := mustRuntime().Describe(opusPacketInput().
		Audio().
		Resample(16_000, codec.Mono).
		Decode().
		As("bad").
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("operation_shape_mismatch"), "build stream", "bad",
		"write .Decode().Resample(...)",
	)
}

// TestErrorAcceptanceBranchDecodeDomainMismatch is snippet 0r: a fork tries
// to Decode a stream that is already decoded.
func TestErrorAcceptanceBranchDecodeDomainMismatch(t *testing.T) {
	decoded := opusPacketInput().Audio().Decode()
	_, err := mustRuntime().Describe(
		decoded.Decode().As("bad").To(goavtest.NewCollector().Sink()),
		decoded.As("good").To(goavtest.NewCollector().Sink()),
	)
	requireBuildError(t, err, errcode.Code("branch_decode_domain_mismatch"), "build branches", "bad",
		"omit .Decode() when the fork already starts after the stream's .Decode()",
		"fork before .Decode() when a packet copy and a decoding fork share the stream",
	)
}

// TestErrorAcceptancePacketBranchEncodeUnsupported is snippet 0t: a packet
// branch encodes without decoding first.
func TestErrorAcceptancePacketBranchEncodeUnsupported(t *testing.T) {
	_, err := mustRuntime().Describe(opusPacketInput().
		Audio().
		Encode(codec.Opus()).
		As("bad").
		To(goav.Write("bad.ogg", io.Discard)))
	requireBuildError(t, err, errcode.Code("operation_shape_mismatch"), "build stream", "bad",
		"call .Decode() before",
		"keep a pure packet copy",
	)
}

func TestBuildAndAttachReturnSameErrorForSameInvalidBranch(t *testing.T) {
	_, err := goavtest.Runtime().Describe(opusPacketInput().
		Audio().
		Decode().
		Resize(640, 360).
		As("bad").
		To(goavtest.NewCollector().Sink()))
	planned := requireBuildError(t, err, errcode.Code("operation_shape_mismatch"), "build job", "bad",
		"use .Video().Resize(...)",
	)

	task, err := mustRuntime().Build(context.Background(), audioFrameInput().
		Audio().
		Tap("audio.frames").
		To(goavtest.NewCollector().Sink()))
	if err != nil {
		t.Fatalf("base task build: %v", err)
	}
	defer task.Close()

	_, err = task.Attach(context.Background(),
		task.Tap("audio.frames").
			Resize(640, 360).
			As("bad").
			To(goavtest.NewCollector().Sink()),
	)
	attached := requireBuildError(t, err, errcode.Code("operation_shape_mismatch"), "attach runtime branch", "bad",
		"use .Video().Resize(...)",
	)
	for _, buildErr := range []*goav.BuildError{planned, attached} {
		if !detailsContain(buildErr.DetailLines(), "expected_shape=domain=frame media=video") ||
			!detailsContain(buildErr.DetailLines(), "actual_shape=domain=frame media=audio") {
			t.Fatalf("%s details = %v, want expected/actual audio-video frame shapes", buildErr.Operation, buildErr.DetailLines())
		}
	}
}

// TestErrorAcceptanceFramesIntoContainerWithoutEncode is snippet 2:
// .Decode().To(Write(...)) routes decoded frames into a container. The fix is
// the exact .Encode(codec...) call to add.
func TestErrorAcceptanceFramesIntoContainerWithoutEncode(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), opusPacketInput().
		Audio().
		Decode().
		To(goav.Write("out.ogg", io.Discard)))
	audioErr := requireBuildError(t, err, errcode.Code("encode_missing"), "build job", "audio",
		".Encode(codec.Opus(...))",
		"goav.Sink(...)",
	)
	if suggestionsContain(audioErr.FixLines(), "codec.VP8") {
		t.Fatalf("audio refusal suggests a video encoder\nerr = %v", err)
	}

	// The suggested encoders follow the stream's media: a video refusal
	// names every bundled video encoder and never an audio codec.
	_, err = goavtest.Runtime().Build(context.Background(), goavtest.Packets(av.CodecVP8, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}).
		Video().
		Decode().
		To(goav.Write("out.webm", io.Discard)))
	videoErr := requireBuildError(t, err, errcode.Code("encode_missing"), "build job", "video",
		".Encode(codec.VP9(...))",
		".Encode(codec.AV1(...))",
	)
	if suggestionsContain(videoErr.FixLines(), "codec.Opus") {
		t.Fatalf("video refusal suggests an audio encoder\nerr = %v", err)
	}
}

// TestErrorAcceptanceTransformAfterCopy is snippet 3: .Copy().Resize(...)
// declares a frame transform on a packet-domain chain. The refusal names the
// domain rule and says to decode first.
func TestErrorAcceptanceTransformAfterCopy(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goavtest.Packets(av.CodecVP8, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}).
		Video().
		Resize(640, 360).
		To(goav.Write("out.ivf", io.Discard)))
	buildErr := requireBuildError(t, err, errcode.Code("operation_shape_mismatch"), "build stream", "video",
		"call .Decode() before .Resize(...)",
	)
	if !strings.Contains(buildErr.Reason, "a stream without .Decode() copies its packets") {
		t.Fatalf("reason should state the packet-domain rule, err = %v", err)
	}
}

// TestErrorAcceptanceDestinationFormatUnknown is snippet 4a: a File
// destination whose name resolves to no known container. The fix is the
// goav.Format(...) option.
func TestErrorAcceptanceDestinationFormatUnknown(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goavtest.Audio(48000, 1, []int16{1}).
		Audio().
		Encode(codec.Opus()).
		To(goav.Write("out.weird", io.Discard)))
	requireBuildError(t, err, errcode.Code("destination_format_unknown"), "open destination", "out.weird",
		"pass goav.Format(...)",
	)
}

// TestErrorAcceptanceDestinationMuxerMissing is snippet 4b: the format is
// detected but the runtime has no muxer registered for it. The fix names
// runconfig.WithMuxer(...).
func TestErrorAcceptanceDestinationMuxerMissing(t *testing.T) {
	_, err := mustRuntime(bundle.Filters(), goavtest.Codec(av.CodecOpus)).Build(context.Background(), goavtest.Audio(48000, 1, []int16{1}).
		Audio().
		Encode(codec.Opus()).
		To(goav.Write("out.ogg", io.Discard)))
	requireBuildError(t, err, errcode.Code("destination_muxer_missing"), "open destination", "out.ogg",
		"runconfig.WithMuxer(...)",
	)
}

// TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates is snippet 5:
// an input carries two audio streams and an unnarrowed .Audio() matches both.
// The refusal lists every candidate and suggests the exact narrowing options.
func TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates(t *testing.T) {
	two := goav.FileInput("two.webm", bytes.NewReader(oracleTwoAudioWebM(t)))
	_, err := offlineRuntime().Build(context.Background(), two.Audio().Decode().To(goavtest.NewCollector().Sink()))

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.Code("stream_ambiguous") {
		t.Fatalf("err = %v, want stream_ambiguous with matching BuildError code", err)
	}
	if got := len(buildErr.DetailLines()); got < 2 {
		t.Fatalf("err = %v, want both candidates listed", err)
	}
	if !suggestionsContain(buildErr.FixLines(), "goav.StreamID(") {
		t.Fatalf("err = %v, want StreamID narrowing suggestion", err)
	}
}

// TestErrorAcceptanceAttachUnknownTapListsDeclaredTaps is snippet 6: a
// runtime Branch anchored on a tap the task never declared. The refusal lists
// the taps that DO exist and points at task.Taps().
func TestErrorAcceptanceAttachUnknownTapListsDeclaredTaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := goavtest.Runtime().Build(ctx, goavtest.LiveAudio("mic", 48000, 1).
		Audio().
		Tap("audio.decoded").
		To(goavtest.NewCollector().Sink()))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	_, err = task.Attach(ctx, task.Tap("nope").As("late").To(goavtest.NewCollector().Sink()))
	buildErr := requireBuildError(t, err, errcode.Code("runtime_branch_tap_missing"), "attach runtime branch", "nope",
		`add .Tap("nope") at the point you want to attach`,
		"call task.Taps() before attaching",
	)
	if !detailsContain(buildErr.DetailLines(), "audio.decoded") {
		t.Fatalf("details should list the declared taps, err = %v", err)
	}
}

// TestErrorAcceptanceEncoderAdapterMissing is snippet 8: .Encode with a codec
// no registered encoder provides. The refusal names the codec and the
// runconfig.WithEncoder(...) registration fix.
func TestErrorAcceptanceEncoderAdapterMissing(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goavtest.Audio(48000, 1, []int16{1}).
		Audio().
		Encode(codec.Codec("weird", av.MediaAudio)).
		To(goav.Write("out.ogg", io.Discard)))
	buildErr := requireBuildError(t, err, errcode.Code("encode_adapter_missing"), "build job", "audio",
		"runconfig.WithEncoder(...)",
	)
	if !strings.Contains(buildErr.Reason, "weird") || !detailsContain(buildErr.DetailLines(), "codec=weird") {
		t.Fatalf("refusal should name the codec, err = %v", err)
	}
}

// TestErrorAcceptanceShapeConversionRefused is snippet 9: the chain needs a
// conversion (44.1kHz into a 48kHz encoder) and the active .Auto() policy
// allows nothing. The refusal carries the exact policy to add. The full
// solver contract is pinned in shape_solver_test.go; this is the checklist
// entry.
func TestErrorAcceptanceShapeConversionRefused(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(), goavtest.Audio(44100, 2, []int16{1, 1}).
		Audio().
		Auto().
		Encode(codec.Opus()).
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("shape_conversion_refused"), "build job", "audio",
		"add .Auto(shape.AllowResample())",
		"insert .Resample(48000, 2) explicitly",
	)
}

// TestErrorAcceptanceStreamMissing: a stream the input does not carry is
// refused by name, with the streams it does carry.
func TestErrorAcceptanceStreamMissing(t *testing.T) {
	_, err := goavtest.Runtime().Build(context.Background(),
		goavtest.Audio(48_000, 1, []int16{1}).Video().To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("stream_missing"), "select stream", "type=video",
		"select one it has: input.Audio()",
	)
}

// TestErrorAcceptanceInputFormatUnknown: an input no prober recognizes is
// refused before anything opens.
func TestErrorAcceptanceInputFormatUnknown(t *testing.T) {
	_, err := mustRuntime().Build(context.Background(),
		goav.FileInput("in.unknown", strings.NewReader("")).Video().Decode().To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("input_format_unknown"), "open input", "in.unknown",
		"a name, URI, or MIME type a registered prober can recognize",
	)
}

// TestErrorAcceptanceFramesIntoFileNeedAnEncoder: a frame source written to a
// file needs an encoder, the same refusal decoded frames get.
func TestErrorAcceptanceFramesIntoFileNeedAnEncoder(t *testing.T) {
	_, err := goavtest.Runtime().Describe(
		goavtest.Audio(48_000, 1, []int16{1}).Audio().To(goav.Write("out.ogg", io.Discard)))
	requireBuildError(t, err, errcode.Code("encode_missing"), "build job", "audio",
		"call .Encode(codec.Opus(...)) before .To(goav.Write(...))",
		"send decoded frames to goav.Sink(...)",
	)
}

// internalErrorIn names the first error in err's chain whose type is
// declared under goav's internal packages (a compiler refusal that escaped
// the root's conversion), or "".
func internalErrorIn(err error) string {
	if err == nil {
		return ""
	}
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if strings.HasPrefix(t.PkgPath(), "github.com/thesyncim/goav/internal/") {
		return t.String()
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			if leak := internalErrorIn(inner); leak != "" {
				return leak
			}
		}
	case interface{ Unwrap() error }:
		return internalErrorIn(e.Unwrap())
	}
	return ""
}
