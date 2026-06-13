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
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
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

// TestErrorAcceptanceNilProviderInput is snippet 0a: a nil source provider
// handed to goav.Input(...). The refusal keeps the public input constructor
// vocabulary in the suggestions and preserves ErrNilSource as the cause.
func TestErrorAcceptanceNilProviderInput(t *testing.T) {
	_, err := goav.From(goav.Input(nil)).
		Audio().
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.InputInvalid, "build input", "input",
		"check the input constructor arguments",
		"pass a non-nil provider to goav.Input(provider)",
	)
	if !errors.Is(err, goav.ErrNilSource) {
		t.Fatalf("err = %v, want ErrNilSource cause", err)
	}
}

// TestErrorAcceptanceMissingInput is snippet 0b: a recipe with destinations
// but no input. The fix is to start from a real input constructor.
func TestErrorAcceptanceMissingInput(t *testing.T) {
	_, err := goav.From().
		Copy().
		To(goav.File("out.ogg", io.Discard)).
		Describe()
	requireBuildError(t, err, errcode.InputMissing, "build job", "",
		"goav.From(goav.FileInput",
	)
}

// TestErrorAcceptanceCustomSourceMissingCallback is snippet 0c: a custom
// source with no SourceFunc. The refusal tells the author to pass the
// Source(name, shape, fn) callback or use another input seam.
func TestErrorAcceptanceCustomSourceMissingCallback(t *testing.T) {
	_, err := goav.From(goav.Source("mic", shape.Frame(av.MediaAudio), nil)).
		Audio().
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.SourceCallbackMissing, "build input", "mic",
		"pass a non-nil callback to goav.Source(name, shape, fn)",
		"use goav.FileInput or goav.Input(provider)",
	)
	if !errors.Is(err, goav.ErrNilSource) {
		t.Fatalf("err = %v, want ErrNilSource cause", err)
	}
}

// TestErrorAcceptanceCustomSourceShapeInvalid is snippet 0d: a packet-domain
// custom source that forgot to declare audio or video. The refusal names the
// shape.Packet/shape.Media fixes before any runtime opens.
func TestErrorAcceptanceCustomSourceShapeInvalid(t *testing.T) {
	_, err := goav.From(goav.Source("bad", shape.New(shape.Domain(shape.DomainPacket)),
		func(context.Context, goav.SourcePush) error { return nil },
	)).
		Audio().
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.SourceShapeInvalid, "build input", "bad",
		"use shape.Packet(av.MediaAudio, codec)",
		"add shape.Media(...)",
	)
}

// TestErrorAcceptanceCustomSourceShapeUnsupported is snippet 0e: a custom
// source declares a domain outside packet/frame/event. The refusal lists the
// supported source-shape constructors.
func TestErrorAcceptanceCustomSourceShapeUnsupported(t *testing.T) {
	_, err := goav.From(goav.Source("bytes",
		shape.New(shape.Domain(shape.MediaDomain("bytes")), shape.Media(av.MediaAudio)),
		func(context.Context, goav.SourcePush) error { return nil },
	)).
		Audio().
		To(goavtest.NewCollector().Sink()).
		Describe()
	buildErr := requireBuildError(t, err, errcode.SourceShapeUnsupported, "build input", "bytes",
		"declare the source with shape.Packet",
		"declare raw generated media with shape.Frame",
		"declare diagnostic or lifecycle sources with shape.Event",
	)
	if !detailsContain(buildErr.Details, "actual_shape=domain=bytes") {
		t.Fatalf("details should carry the unsupported domain, err = %v", err)
	}
}

// TestErrorAcceptanceFrameSourceDecodeMismatch is snippet 0f: frame-domain
// custom sources are already decoded, so .Decode() is a shape mismatch with a
// public grammar fix.
func TestErrorAcceptanceFrameSourceDecodeMismatch(t *testing.T) {
	_, err := goav.From(goav.Source("frames", shape.Frame(av.MediaAudio),
		func(_ context.Context, push goav.SourcePush) error { return push.EOS() },
	)).
		Audio().Decode().
		To(goavtest.NewCollector().Sink()).
		Describe()
	buildErr := requireBuildError(t, err, errcode.SourceShapeMismatch, "build stream", "audio",
		"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
		"use shape.Packet(...) when the custom source pushes encoded packets",
	)
	if !detailsContain(buildErr.Details, "source_domain=frame") {
		t.Fatalf("details should carry source_domain=frame, err = %v", err)
	}
}

// TestErrorAcceptanceNilFlow is snippet 0g: applying a nil flow. The refusal
// keeps Flow(name).Audio/Video as the only creation path.
func TestErrorAcceptanceNilFlow(t *testing.T) {
	var flow goav.Chain
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(flow).
		To(goavtest.NewCollector().Sink()).
		Describe()
	requireBuildError(t, err, errcode.FlowInvalid, "build flow", "",
		"build flows with goav.Flow(name).Audio() or goav.Flow(name).Video()",
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
	requireBuildError(t, err, errcode.FlowMediaMismatch, "build stream", "thumbs",
		"use goav.Flow(name).Audio() with .Audio()",
		"use goav.Flow(name).Video() with .Video()",
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
	requireBuildError(t, err, errcode.FlowDecodeDuplicate, "build flow", "voice",
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
	requireBuildError(t, err, errcode.FlowDecodeOrderInvalid, "build flow", "voice",
		"write goav.Flow(name).Audio().Decode().Resample(...)",
		"omit .Decode() when the flow is only applied after stream decode",
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
	requireBuildError(t, err, errcode.FlowDecodeDomainMismatch, "build stream", "voice",
		"omit .Decode() when applying the flow after stream decode",
		"use the flow from a packet branch or packet tap",
	)
}

// TestErrorAcceptanceFlowCopyDomainMismatch is snippet 0l: a flow asks for
// packet Copy after frame-domain work. The fix is to either copy from packets
// or make the flow an encode path.
func TestErrorAcceptanceFlowCopyDomainMismatch(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Apply(goav.Flow("packets").Audio().Resample(16_000, codec.Mono).Copy()).
		To(goav.File("copy.ogg", io.Discard)).
		Describe()
	requireBuildError(t, err, errcode.FlowCopyDomainMismatch, "build flow", "packets",
		"start packet-preserving reusable work with goav.Flow(name).Audio().Copy()",
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
	requireBuildError(t, err, errcode.BranchMissing, "build branches", "audio",
		"pass branches with goav.Branch(name).Encode",
		"reuse the same destination value",
	)
}

// TestErrorAcceptanceBranchNameMissing is snippet 0n: a zero BranchSpec
// reaches Branches(...). The refusal says branches need stable names.
func TestErrorAcceptanceBranchNameMissing(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().
		Branches(goav.BranchSpec{}).
		Describe()
	requireBuildError(t, err, errcode.StreamNameMissing, "build branch composition", "branch-0",
		"use branch names as handles for graph inspection and destination planning",
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
	requireBuildError(t, err, errcode.BranchTapMissing, "build branches", "levels",
		`add .Tap(goav.FrameTap("audio.missing"))`,
		"omit .From(...) to branch from the current stream point",
	)
}

// TestErrorAcceptanceBranchDecodeDuplicate is snippet 0p: a branch decodes
// twice. The refusal keeps branch decode as a single initial operation.
func TestErrorAcceptanceBranchDecodeDuplicate(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Copy().
		Branches(
			goav.Branch("bad").
				Decode().
				Decode().
				To(goavtest.NewCollector().Sink()),
		).
		Describe()
	requireBuildError(t, err, errcode.BranchDecodeDuplicate, "build branch", "bad",
		"call .Decode() once before frame operations",
		"remove the second .Decode() call",
	)
}

// TestErrorAcceptanceBranchDecodeOrderInvalid is snippet 0q: a branch does
// frame work before Decode. The fix is Decode first or start from a frame tap.
func TestErrorAcceptanceBranchDecodeOrderInvalid(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Copy().
		Branches(
			goav.Branch("bad").
				Resample(16_000, codec.Mono).
				Decode().
				To(goavtest.NewCollector().Sink()),
		).
		Describe()
	requireBuildError(t, err, errcode.BranchDecodeOrderInvalid, "build branch", "bad",
		"write goav.Branch(name).Decode().Resample(...).To(target)",
		"start from a frame tap",
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
	requireBuildError(t, err, errcode.BranchDecodeDomainMismatch, "build branches", "bad",
		"omit .Decode() when the branch already starts after stream decode",
		"use .Copy().Branches",
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
	requireBuildError(t, err, errcode.BranchDecodeCopyInvalid, "build branch", "bad",
		"use .Copy() for packet-preserving branches",
		"use .Decode().Encode(codec).To(destination)",
	)
}

// TestErrorAcceptancePacketBranchEncodeUnsupported is snippet 0t: a packet
// branch encodes without decoding first.
func TestErrorAcceptancePacketBranchEncodeUnsupported(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Copy().
		Branches(
			goav.Branch("bad").
				Encode(codec.Opus()).
				To(goav.File("bad.ogg", io.Discard)),
		).
		Describe()
	requireBuildError(t, err, errcode.PacketBranchEncodeUnsupported, "build branches", "bad",
		"use .Decode().Branches",
		"use .Copy().Branches",
	)
}

// TestErrorAcceptancePacketBranchTransformUnsupported is snippet 0u: a packet
// branch resamples without decoding first.
func TestErrorAcceptancePacketBranchTransformUnsupported(t *testing.T) {
	_, err := goav.From(opusPacketInput()).
		Audio().Copy().
		Branches(
			goav.Branch("bad").
				Resample(16_000, codec.Mono).
				To(goavtest.NewCollector().Sink()),
		).
		Build(context.Background())
	requireBuildError(t, err, errcode.PacketBranchTransformUnsupported, "build branches", "bad",
		"use .Decode().Branches(...) when branch variants need frame transforms",
		"use .Copy().Branches(...) only for packet-preserving branches",
	)
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
	buildErr := requireBuildError(t, err, errcode.OperationShapeMismatch, "build job", "audio",
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
	requireBuildError(t, err, errcode.EncodeMissing, "build job", "audio",
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
	buildErr := requireBuildError(t, err, errcode.OperationShapeMismatch, "build stream", "video",
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
	requireBuildError(t, err, errcode.DestinationFormatUnknown, "open destination", "out.weird",
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
	requireBuildError(t, err, errcode.DestinationMuxerMissing, "open destination", "out.ogg",
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
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.StreamAmbiguous || !errors.Is(err, goav.ErrUnsupportedBuild) {
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
	buildErr := requireBuildError(t, err, errcode.RuntimeBranchTapMissing, "attach runtime branch", "nope",
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
	buildErr := requireBuildError(t, err, errcode.TapDomainMismatch, "build stream", "audio",
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
	buildErr := requireBuildError(t, err, errcode.EncodeAdapterMissing, "build job", "audio",
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
	requireBuildError(t, err, errcode.ShapeConversionRefused, "build job", "audio",
		"add .Auto(shape.AllowResample())",
		"insert .Resample(48000, 2) explicitly",
	)
}
