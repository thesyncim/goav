package goav

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestFlowChainAccessorsAndNilSafety(t *testing.T) {
	var root *flowRoot
	if got := root.Audio().Name(); got != "" {
		t.Fatalf("nil root Audio().Name() = %q, want empty", got)
	}
	if got := root.Video().Name(); got != "" {
		t.Fatalf("nil root Video().Name() = %q, want empty", got)
	}

	audio := Flow("voice").Audio()
	video := Flow("preview").Video()
	if got := audio.Name(); got != "voice" {
		t.Fatalf("audio Name() = %q, want voice", got)
	}
	if got := video.Name(); got != "preview" {
		t.Fatalf("video Name() = %q, want preview", got)
	}
	var chain Chain = audio
	chain.isChain()
	chain = video
	chain.isChain()

	var nilAudio *audioChain
	if got := nilAudio.Name(); got != "" {
		t.Fatalf("nil audio Name() = %q, want empty", got)
	}
	if got := nilAudio.InputShapes(); got != nil {
		t.Fatalf("nil audio InputShapes() = %#v, want nil", got)
	}
	if got := nilAudio.OutputShapes(shape.Frame(av.MediaAudio)); got != nil {
		t.Fatalf("nil audio OutputShapes() = %#v, want nil", got)
	}
	if got := nilAudio.Taps(); got != nil {
		t.Fatalf("nil audio Taps() = %#v, want nil", got)
	}
	if nilAudio.Decode() != nil ||
		nilAudio.Resample(48000, 1) != nil ||
		nilAudio.Do(nil) != nil ||
		nilAudio.Shape(shape.Frame(av.MediaAudio)) != nil ||
		nilAudio.Auto(shape.AllowResample()) != nil ||
		nilAudio.Require(shape.Frame(av.MediaAudio)) != nil ||
		nilAudio.Prefer(shape.Frame(av.MediaAudio)) != nil ||
		nilAudio.Apply(audio) != nil ||
		nilAudio.Tap(FrameTap("voice.frames")) != nil ||
		nilAudio.Encode(codec.Opus()) != nil ||
		nilAudio.Copy() != nil {
		t.Fatal("nil audio chain method returned non-nil")
	}
	assertBuildErrorCode(t, nilAudio.chainSpec().err, flowInvalidCode)

	var nilVideo *videoChain
	if got := nilVideo.Name(); got != "" {
		t.Fatalf("nil video Name() = %q, want empty", got)
	}
	if got := nilVideo.InputShapes(); got != nil {
		t.Fatalf("nil video InputShapes() = %#v, want nil", got)
	}
	if got := nilVideo.OutputShapes(shape.Frame(av.MediaVideo)); got != nil {
		t.Fatalf("nil video OutputShapes() = %#v, want nil", got)
	}
	if got := nilVideo.Taps(); got != nil {
		t.Fatalf("nil video Taps() = %#v, want nil", got)
	}
	if nilVideo.Decode() != nil ||
		nilVideo.Resize(320, 180) != nil ||
		nilVideo.Do(nil) != nil ||
		nilVideo.Shape(shape.Frame(av.MediaVideo)) != nil ||
		nilVideo.Auto(shape.AllowResize()) != nil ||
		nilVideo.Require(shape.Frame(av.MediaVideo)) != nil ||
		nilVideo.Prefer(shape.Frame(av.MediaVideo)) != nil ||
		nilVideo.Apply(video) != nil ||
		nilVideo.Tap(FrameTap("preview.frames")) != nil ||
		nilVideo.Encode(codec.VP8()) != nil ||
		nilVideo.Copy() != nil {
		t.Fatal("nil video chain method returned non-nil")
	}
	assertBuildErrorCode(t, nilVideo.chainSpec().err, flowInvalidCode)

	var builder *chainBuilder
	if got := builder.name(); got != "" {
		t.Fatalf("nil builder name() = %q, want empty", got)
	}
	if got := builder.inputShapes(); got != nil {
		t.Fatalf("nil builder inputShapes() = %#v, want nil", got)
	}
	if got := builder.outputShapes(shape.Frame(av.MediaAudio)); got != nil {
		t.Fatalf("nil builder outputShapes() = %#v, want nil", got)
	}
	if got := builder.taps(); got != nil {
		t.Fatalf("nil builder taps() = %#v, want nil", got)
	}
	builder.decode()
	builder.transform(Resize(16, 16))
	builder.stage(nil)
	builder.shape(shape.Frame(av.MediaVideo))
	builder.auto([]shape.Policy{shape.AllowResize()})
	builder.require(shape.Frame(av.MediaVideo))
	builder.prefer(shape.Frame(av.MediaVideo))
	builder.apply(audio)
	builder.tap(FrameTap("preview.frames"))
	builder.encode(codec.VP8())
	assertBuildErrorCode(t, builder.snapshot().err, flowInvalidCode)
}

func TestVideoFlowOperationsExposeContractsAndCloneSnapshots(t *testing.T) {
	meter := FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit Emit) error {
		return emit.Frame(frame)
	})
	flow := Flow("preview").Video().
		Decode().
		Resize(320, 180).
		Do(meter).
		Shape(shape.Frame(av.MediaVideo, shape.Video(320, 180, av.PixelFormatI420))).
		Auto(shape.AllowResize(), shape.AllowConvert()).
		Require(shape.Frame(av.MediaVideo, shape.Video(320, 180, ""))).
		Prefer(shape.Frame(av.MediaVideo, shape.Video(0, 0, av.PixelFormatI420))).
		Tap(FrameTap("preview.frames")).
		Encode(codec.VP8(codec.Bitrate(250_000)))

	if got := flow.InputShapes(); len(got) != 1 || got[0].Domain != shape.DomainPacket || got[0].MediaKind != av.MediaVideo {
		t.Fatalf("InputShapes() = %#v, want video packets", got)
	}
	outputs := flow.OutputShapes(shape.Packet(av.MediaVideo, av.CodecVP9))
	if len(outputs) != 1 || outputs[0].Domain != shape.DomainPacket || outputs[0].Codec != av.CodecVP8 {
		t.Fatalf("OutputShapes() = %#v, want VP8 packets", outputs)
	}
	if got := flow.Taps(); !reflect.DeepEqual(got, []tapRef{FrameTap("preview.frames")}) {
		t.Fatalf("Taps() = %#v, want preview frame tap", got)
	}

	spec, err := chainSpecFrom(flow)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := operationKindsForFlowTest(spec.operations), []plan.OperationKind{
		plan.OpDecode,
		plan.OpTransform,
		plan.OpStage,
		plan.OpShape,
		plan.OpShape,
		plan.OpShape,
		plan.OpShape,
		plan.OpTap,
		plan.OpEncode,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operation kinds = %v, want %v", got, want)
	}
	if spec.operations[1].Transform.resize == nil || spec.operations[1].Transform.resize.Width != 320 {
		t.Fatalf("resize operation = %#v, want 320px resize", spec.operations[1])
	}
	if spec.operations[4].Auto == nil || !spec.operations[4].Auto.AllowsResize() || !spec.operations[4].Auto.AllowsConvert() {
		t.Fatalf("auto operation = %#v, want resize+convert policy", spec.operations[4])
	}
	if spec.operations[5].Require == nil || spec.operations[5].Require.Width != 320 {
		t.Fatalf("require operation = %#v, want width requirement", spec.operations[5])
	}
	if spec.operations[6].Prefer == nil || spec.operations[6].Prefer.PixelFormat != av.PixelFormatI420 {
		t.Fatalf("prefer operation = %#v, want I420 preference", spec.operations[6])
	}

	spec.operations[1].Transform.resize.Width = 999
	specAgain, err := chainSpecFrom(flow)
	if err != nil {
		t.Fatal(err)
	}
	if got := specAgain.operations[1].Transform.resize.Width; got != 320 {
		t.Fatalf("chainSpecFrom reused resize config, got width %d want 320", got)
	}

	copyFlow := Flow("copy").Video().Copy()
	if got := copyFlow.InputShapes(); len(got) != 1 || got[0].Domain != shape.DomainPacket || got[0].MediaKind != av.MediaVideo {
		t.Fatalf("copy InputShapes() = %#v, want video packets", got)
	}
	copyOutputs := copyFlow.OutputShapes(shape.Spec{MediaKind: av.MediaVideo})
	if len(copyOutputs) != 1 || copyOutputs[0].Domain != shape.DomainPacket || copyOutputs[0].MediaKind != av.MediaVideo {
		t.Fatalf("copy OutputShapes() = %#v, want video packets", copyOutputs)
	}

	applied := Flow("outer").Video().Apply(Flow("inner").Video().Resize(64, 36))
	appliedSpec, err := chainSpecFrom(applied)
	if err != nil {
		t.Fatal(err)
	}
	if len(appliedSpec.operations) != 1 ||
		appliedSpec.operations[0].Kind != plan.OpTransform ||
		appliedSpec.operations[0].Transform.resize == nil ||
		appliedSpec.operations[0].Transform.resize.Width != 64 {
		t.Fatalf("applied video flow operations = %#v, want cloned resize", appliedSpec.operations)
	}
}

func TestAudioFlowShapePreferenceCopyAndTapContracts(t *testing.T) {
	flow := Flow("voice").Audio().
		Shape(shape.Frame(av.MediaAudio, shape.Audio(48_000, 1, av.SampleFormatS16))).
		Prefer(shape.Frame(av.MediaAudio, shape.Audio(0, 0, av.SampleFormatF32))).
		Tap(FrameTap("voice.auto")).
		Encode(codec.Opus(codec.Bitrate(96_000)))
	spec, err := chainSpecFrom(flow)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := operationKindsForFlowTest(spec.operations), []plan.OperationKind{
		plan.OpShape,
		plan.OpShape,
		plan.OpTap,
		plan.OpEncode,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("audio operation kinds = %v, want %v", got, want)
	}
	if spec.operations[2].Tap.Domain != shape.DomainFrame {
		t.Fatalf("domain-less tap before encode inferred %q, want frame", spec.operations[2].Tap.Domain)
	}

	encodedTap := Flow("encoded").Audio().
		Encode(codec.Opus()).
		Tap(PacketTap("voice.packets"))
	encodedSpec, err := chainSpecFrom(encodedTap)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := operationKindsForFlowTest(encodedSpec.operations), []plan.OperationKind{plan.OpEncode, plan.OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("encoded operation kinds = %v, want %v", got, want)
	}
	if encodedSpec.operations[1].Tap.Domain != shape.DomainPacket || encodedSpec.operations[1].Tap.After != plan.OpEncode {
		t.Fatalf("packet tap after encode = %#v, want packet after encode", encodedSpec.operations[1].Tap)
	}

	badStage := Flow("bad").Audio().Do(nil)
	_, err = chainSpecFrom(badStage)
	assertBuildErrorCode(t, err, stageMissingCode)

	if got := chainTransformStepName(transformSpec{}); got != "transform" {
		t.Fatalf("chainTransformStepName(empty) = %q, want transform", got)
	}
	if got := chainTransformStepName(Resize(320, 180)); got != "resize" {
		t.Fatalf("chainTransformStepName(resize) = %q, want resize", got)
	}
	if got := chainTransformStepName(Resample(48_000, codec.Stereo)); got != "resample" {
		t.Fatalf("chainTransformStepName(resample) = %q, want resample", got)
	}
}

func TestFlowBuilderDefaultShapeContracts(t *testing.T) {
	audio := Flow("voice").Audio()
	if got := audio.InputShapes(); len(got) != 1 || got[0].Domain != shape.DomainFrame || got[0].MediaKind != av.MediaAudio {
		t.Fatalf("audio InputShapes() = %#v, want audio frames", got)
	}
	if got := audio.OutputShapes(shape.Spec{}); len(got) != 1 || got[0].Domain != shape.DomainFrame || got[0].MediaKind != av.MediaAudio {
		t.Fatalf("audio OutputShapes(empty) = %#v, want audio frames", got)
	}

	video := Flow("preview").Video()
	if got := video.InputShapes(); len(got) != 1 || got[0].Domain != shape.DomainFrame || got[0].MediaKind != av.MediaVideo {
		t.Fatalf("video InputShapes() = %#v, want video frames", got)
	}
	if got := video.OutputShapes(shape.Spec{}); len(got) != 1 || got[0].Domain != shape.DomainFrame || got[0].MediaKind != av.MediaVideo {
		t.Fatalf("video OutputShapes(empty) = %#v, want video frames", got)
	}
}

func TestFlowBuilderRejectsInvalidCompositionContracts(t *testing.T) {
	meter := FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit Emit) error {
		return emit.Frame(frame)
	})

	tests := []struct {
		name string
		flow Chain
		code errcode.Code
	}{
		{
			name: "shape after encode",
			flow: Flow("encoded").Audio().
				Encode(codec.Opus()).
				Shape(shape.Frame(av.MediaAudio)),
			code: streamStepAfterEncodeCode,
		},
		{
			name: "decode after encode",
			flow: Flow("encoded").Video().
				Encode(codec.VP8()).
				Decode(),
			code: streamStepAfterEncodeCode,
		},
		{
			name: "shape after copy",
			flow: Flow("copied").Audio().
				Copy().
				Shape(shape.Frame(av.MediaAudio)),
			code: operationShapeMismatchCode,
		},
		{
			name: "flow after encode",
			flow: Flow("encoded").Audio().
				Encode(codec.Opus()).
				Apply(Flow("meter").Audio().Do(meter)),
			code: streamStepAfterEncodeCode,
		},
		{
			name: "decode flow after frame step",
			flow: Flow("bad").Audio().
				Do(meter).
				Apply(Flow("decode").Audio().Decode()),
			code: flowDecodeOrderInvalidCode,
		},
		{
			name: "duplicate decode through apply",
			flow: Flow("bad").Audio().
				Decode().
				Apply(Flow("decode").Audio().Decode()),
			code: flowDecodeDuplicateCode,
		},
		{
			name: "copy flow after decode",
			flow: Flow("bad").Audio().
				Decode().
				Apply(Flow("copy").Audio().Copy()),
			code: flowCopyDomainMismatchCode,
		},
		{
			name: "typed packet tap before encode",
			flow: Flow("bad").Audio().
				Tap(PacketTap("voice.packets")),
			code: errcode.TapDomainMismatch,
		},
		{
			name: "typed frame tap after encode",
			flow: Flow("bad").Audio().
				Encode(codec.Opus()).
				Tap(FrameTap("voice.frames")),
			code: errcode.TapDomainMismatch,
		},
		{
			name: "empty tap",
			flow: Flow("bad").Audio().
				Tap(FrameTap("")),
			code: errcode.TapInvalid,
		},
		{
			name: "non snapshot chain",
			flow: Flow("bad").Audio().
				Apply(nonSnapshotFlow{}),
			code: flowInvalidCode,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := chainSpecFrom(tt.flow)
			assertBuildErrorCode(t, err, tt.code)
		})
	}
}

func TestFlowDuplicateEncodeAndDecodeErrors(t *testing.T) {
	dupEncode := Flow("voice").Audio().Encode(codec.Opus()).Encode(codec.Opus())
	_, err := chainSpecFrom(dupEncode)
	assertBuildErrorCode(t, err, encodeDuplicateCode)

	dupDecode := Flow("preview").Video().Decode().Decode()
	_, err = chainSpecFrom(dupDecode)
	assertBuildErrorCode(t, err, flowDecodeDuplicateCode)
}

type nonSnapshotFlow struct{}

func (nonSnapshotFlow) Name() string { return "foreign" }
func (nonSnapshotFlow) InputShapes() shape.Set {
	return nil
}
func (nonSnapshotFlow) OutputShapes(shape.Spec) shape.Set {
	return nil
}
func (nonSnapshotFlow) Taps() []tapRef { return nil }
func (nonSnapshotFlow) isChain()       {}

func operationKindsForFlowTest(operations []operationSpec) []plan.OperationKind {
	out := make([]plan.OperationKind, len(operations))
	for i := range operations {
		out[i] = operations[i].Kind
	}
	return out
}

func assertBuildErrorCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("error = %v, want *BuildError %s", err, code)
	}
	if buildErr.Code != code {
		t.Fatalf("BuildError code = %s, want %s (err=%v)", buildErr.Code, code, err)
	}
}
