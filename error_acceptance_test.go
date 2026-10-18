// The error-acceptance checklist: invalid constructions a user will actually
// write, each pinned to the full refusal contract — the error is a
// *goav.BuildError (errors.As), carries the right errcode.Code, names the
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
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
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

// TestErrorAcceptanceMissingInput is snippet 0b: a recipe with destinations
// but no input. The fix is to start from a real input constructor.
func TestErrorAcceptanceMissingInput(t *testing.T) {
	_, err := goav.From().
		Copy().
		To(goav.Write("out.ogg", io.Discard)).
		Describe()
	requireBuildError(t, err, errcode.Code("input_missing"), "build job", "",
		`goav.FileInput("in.webm", reader).Video()`,
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

// TestErrorAcceptanceNilFlow is snippet 0g: applying a nil flow. The refusal
// keeps Flow(name).Audio/Video as the only creation path.
func TestErrorAcceptanceNilFlow(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(nil).
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.Code("flow_invalid"), "build flow", "",
		"write reusable work as a Go function: func(s goav.Stream) goav.Stream",
	)
}

// TestErrorAcceptanceFlowMediaMismatch is snippet 0h: a video flow applied to
// an audio chain. The fix is to make the flow media match the selected stream.
func TestErrorAcceptanceFlowMediaMismatch(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(goav.Flow("thumbs").Video().Resize(320, 180)).
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.Code("flow_media_mismatch"), "build stream", "thumbs",
		"apply audio steps such as .Resample(...) to input.Audio() streams",
		"apply video steps such as .Resize(...) to input.Video() streams",
	)
}

// TestErrorAcceptanceFlowDecodeDuplicate is snippet 0i: a flow that decodes
// twice. The refusal points at keeping decode as a single initial operation.
func TestErrorAcceptanceFlowDecodeDuplicate(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(goav.Flow("voice").Audio().Decode().Decode()).
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.Code("flow_decode_duplicate"), "build flow", "voice",
		"call .Decode() once at the start of the flow",
		"remove the second .Decode() call",
	)
}

// TestErrorAcceptanceFlowDecodeOrderInvalid is snippet 0j: a flow declares a
// frame operation before Decode. The fix is to put Decode first.
func TestErrorAcceptanceFlowDecodeOrderInvalid(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(goav.Flow("voice").Audio().Resample(16_000, codec.Mono).Decode()).
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.Code("flow_decode_order_invalid"), "build flow", "voice",
		"decode first in the reusable function",
		"omit .Decode() when the function is only applied after the stream's .Decode()",
	)
}

// TestErrorAcceptanceFlowDecodeDomainMismatch is snippet 0k: a flow tries to
// decode after the parent chain already produced frames.
func TestErrorAcceptanceFlowDecodeDomainMismatch(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Decode().
		Apply(goav.Flow("voice").Audio().Decode()).
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.Code("flow_decode_domain_mismatch"), "build stream", "voice",
		"omit .Decode() when applying the reusable function after the stream's .Decode()",
		"apply it to a packet stream (before .Decode(), or a packet tap)",
	)
}

// TestErrorAcceptanceFlowCopyDomainMismatch is snippet 0l: a flow asks for
// packet Copy after frame-domain work. The fix is to either copy from packets
// or make the flow an encode path.
func TestErrorAcceptanceFlowCopyDomainMismatch(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(goav.Flow("packets").Audio().Resample(16_000, codec.Mono).Copy()).
		To(goav.Write("copy.ogg", io.Discard)).
		Describe()
	requireBuildError(t, err, errcode.Code("flow_copy_domain_mismatch"), "build flow", "packets",
		"keep packet-preserving reusable work free of .Decode()",
		"use .Decode().Resample(...).Encode(codec.Opus(...))",
	)
}

// TestErrorAcceptanceBranchMissing is snippet 0m: Branches() with no branch
// specs. The fix is to pass at least one named branch with a destination.
func TestErrorAcceptanceBranchMissing(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Branches().
		Describe()
	requireBuildError(t, err, errcode.Code("branch_missing"), "build branches", "audio",
		"route each fork with v.Encode(",
		"same destination value",
	)
}

// TestErrorAcceptanceBranchSpecUnconstructed is snippet 0n: a zero BranchSpec
// reaches Branches(...). The refusal says real branches come from Branch(name).
func TestErrorAcceptanceBranchSpecUnconstructed(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Branches(goav.BranchSpec{}).
		Describe()
	requireBuildError(t, err, errcode.Code("branch_invalid"), "build branches", "branch-0",
		"build each fork from a Stream routed with .To(destination)",
	)
}

// TestErrorAcceptanceBranchTapMissing is snippet 0o: a planned branch anchors
// from a tap that the parent stream never declared.
func TestErrorAcceptanceBranchTapMissing(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Decode().
		Branches(
			goav.Branch("levels").
				From(goav.FrameTap("audio.missing")).
				To(goavtest.NewCollector().Sink()),
		).
		Describe()
	requireBuildError(t, err, errcode.Code("branch_tap_missing"), "build branches", "levels",
		`add .Tap("audio.missing") on the selected stream`,
		"reuse the Stream value to fork from its current point",
	)
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

// TestErrorAcceptanceBranchDecodeOrderInvalid is snippet 0q: a branch does
// frame work before Decode. The fix is Decode first or start from a frame tap.
func TestErrorAcceptanceBranchDecodeOrderInvalid(t *testing.T) {
	_, err := mustRuntime().Describe(opusPacketInput().
		Audio().
		Resample(16_000, codec.Mono).
		Decode().
		As("bad").
		To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("branch_decode_order_invalid"), "build branch", "bad",
		"write in.Audio().Decode().Resample(...).To(target)",
		"attach from a tap after .Decode()",
	)
}

// TestErrorAcceptanceBranchDecodeDomainMismatch is snippet 0r: a branch tries
// to Decode when the parent stream already starts in frame domain.
func TestErrorAcceptanceBranchDecodeDomainMismatch(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Decode().
		Branches(
			goav.Branch("bad").
				Decode().
				To(goavtest.NewCollector().Sink()),
		).
		Describe()
	requireBuildError(t, err, errcode.Code("branch_decode_domain_mismatch"), "build branches", "bad",
		"omit .Decode() when the fork already starts after the stream's .Decode()",
		"fork before .Decode() when a packet copy and a decoding fork share the stream",
	)
}

// TestErrorAcceptanceBranchDecodeCopyInvalid is snippet 0s: a branch decodes
// packets and then asks to Copy the original packets.
func TestErrorAcceptanceBranchDecodeCopyInvalid(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Copy().
		Branches(
			goav.Branch("bad").
				Decode().
				Copy().
				To(goavtest.NewCollector().Sink()),
		).
		Describe()
	requireBuildError(t, err, errcode.Code("branch_decode_copy_invalid"), "build branch", "bad",
		"leave out .Decode() to copy the packets",
		"use .Decode().Encode(codec).To(destination)",
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
	requireBuildError(t, err, errcode.Code("packet_branch_encode_unsupported"), "build branches", "bad",
		"decode before encoding: s.Decode().Encode(",
		"route s.To(destination) without .Decode()",
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

// TestErrorAcceptanceCopyAfterDecode is snippet 1: .Decode().Copy() asks for
// packet copy in the frame domain. The refusal states the domain rule and the
// two real fixes — copy before decode, or re-encode the frames.
func TestErrorAcceptanceCopyAfterDecode(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Decode().Copy().
		To(goav.Write("out.ogg", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		BuildLive(context.Background())
	buildErr := requireBuildError(t, err, errcode.Code("operation_shape_mismatch"), "build job", "audio",
		"copy only consumes packet-domain media",
		"leave out .Decode() to copy",
		"use .Encode(codec...) to turn decoded frames back into packets",
	)
	if !detailsContain(buildErr.DetailLines(), "actual_shape=domain=frame") {
		t.Fatalf("details should carry the frame-domain shape, err = %v", err)
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
// two inputs both match an unnarrowed .Audio() chain. The refusal lists every
// candidate with its input and suggests the exact narrowing options.
func TestErrorAcceptanceAmbiguousStreamSelectionListsCandidates(t *testing.T) {
	_, err := goav.From(
		goavtest.Audio(48000, 1, []int16{1}).With(goav.Name("mic-a")),
		goavtest.Audio(48000, 1, []int16{1}).With(goav.Name("mic-b")),
	).
		Audio().
		Decode().
		To(goavtest.NewCollector().Sink()).
		BuildLive(context.Background())

	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.Code("stream_ambiguous") {
		t.Fatalf("err = %v, want stream_ambiguous with matching BuildError code", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "input=mic-a") || !strings.Contains(msg, "input=mic-b") {
		t.Fatalf("err = %v, want candidates listed with their inputs", err)
	}
	if !suggestionsContain(buildErr.FixLines(), `select from the input named "mic-a" directly: input.Audio()`) {
		t.Fatalf("err = %v, want per-input selection suggestion", err)
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

// TestErrorAcceptanceTypedTapAtWrongDomain is snippet 7: a FrameTap declared
// at a packet-domain point (after the encoder). The refusal carries the
// typed-tap fix for each domain.
func TestErrorAcceptanceTypedTapAtWrongDomain(t *testing.T) {
	_, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Encode(codec.Opus()).Tap(goav.FrameTap("post-encode")).
		To(goav.Write("out.ogg", io.Discard)).
		UseRuntime(goavtest.Runtime()).
		BuildLive(context.Background())
	buildErr := requireBuildError(t, err, errcode.TapDomainMismatch, "build stream", "audio",
		"move .Tap(name) before .Decode() or after an encoder to tap packets",
		"move .Tap(name) after decode",
	)
	if !detailsContain(buildErr.DetailLines(), "tap=post-encode") {
		t.Fatalf("details should name the tap, err = %v", err)
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

// TestErrorAcceptanceChainReusedAfterTo: reusing one chain value as a fork —
// a second operation on a chain that already ended at .To(...) — used to
// surface as "recipe intent did not match a supported graph plan". It now
// names the slip and the two real forks.
func TestErrorAcceptanceChainReusedAfterTo(t *testing.T) {
	base := goav.From(opusPacketInput()).Audio().Decode()
	base.Resample(16_000, codec.Mono).To(goav.Sink(component.SinkFunc("narrow", func(context.Context, component.Message) error { return nil })))
	_, err := base.Resample(8_000, codec.Mono).
		To(goav.Sink(component.SinkFunc("phone", func(context.Context, component.Message) error { return nil }))).
		UseRuntime(goavtest.Runtime()).
		BuildLive(context.Background())
	requireBuildError(t, err, errcode.Code("stream_step_after_destination"), "build stream", "audio",
		"by reusing its Stream value",
		"input.Audio() or input.Video()",
	)
}
