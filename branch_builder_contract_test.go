package goav

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func branchBuilderTestSink(name string) Destination {
	return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
}

func branchBuilderTestMeter(name string) pipeline.Stage {
	return FrameFunc(name, func(_ context.Context, frame *av.Frame, emit Emit) error {
		return emit.Frame(frame)
	})
}

func TestBranchBuilderSnapshotContracts(t *testing.T) {
	meter := branchBuilderTestMeter("meter")
	require := shape.Frame(av.MediaVideo, shape.Video(320, 180, av.PixelFormatI420))
	prefer := shape.Frame(av.MediaVideo, shape.Video(0, 0, av.PixelFormatI420))
	builder := Branch("preview").
		From(PacketTap("encoded")).
		Buffer(flow.Blocking(4, flow.BufferCopyBounds(128, 4096))).
		Decode(codec.Profile("main")).
		Apply(Flow("thumbnail").Video().
			Resize(160, 90).
			Tap(FrameTap("thumbnail.frames"))).
		Do(meter).
		Auto(shape.AllowResize(), shape.AllowConvert()).
		Require(require).
		Prefer(prefer).
		Shape(require).
		Resize(320, 180).
		Tap(FrameTap("preview.frames")).
		Encode(codec.VP8(codec.Bitrate(500_000))).
		Tap(PacketTap("preview.packets"))

	snapshot := builder.snapshot()
	if snapshot.err != nil {
		t.Fatal(snapshot.err)
	}
	if snapshot.name != "preview" || snapshot.media != av.MediaVideo {
		t.Fatalf("snapshot identity = name %q media %q, want preview video", snapshot.name, snapshot.media)
	}
	if snapshot.source.tap != "encoded" || snapshot.source.tapDomain != shape.DomainPacket {
		t.Fatalf("source = %+v, want encoded packet tap", snapshot.source)
	}
	if snapshot.branchBuffer.Mode != flow.BufferBlocking ||
		snapshot.branchBuffer.Capacity != 4 ||
		snapshot.branchBuffer.CopyPacketBytes != 128 ||
		snapshot.branchBuffer.CopyFrameBytes != 4096 {
		t.Fatalf("branch buffer = %+v, want blocking copy-bounded buffer", snapshot.branchBuffer)
	}
	if got, want := operationKindsForFlowTest(snapshot.operations), []plan.OperationKind{
		plan.OpDecode,
		plan.OpTransform,
		plan.OpTap,
		plan.OpStage,
		plan.OpShape,
		plan.OpShape,
		plan.OpShape,
		plan.OpShape,
		plan.OpTransform,
		plan.OpTap,
		plan.OpEncode,
		plan.OpTap,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operation kinds = %v, want %v", got, want)
	}
	if snapshot.operations[1].Transform.resize == nil || snapshot.operations[1].Transform.resize.Width != 160 {
		t.Fatalf("applied resize operation = %+v, want 160px thumbnail", snapshot.operations[1])
	}
	if snapshot.operations[4].Auto == nil ||
		!snapshot.operations[4].Auto.AllowsResize() ||
		!snapshot.operations[4].Auto.AllowsConvert() {
		t.Fatalf("auto operation = %+v, want resize+convert policy", snapshot.operations[4])
	}
	if snapshot.operations[5].Require == nil || *snapshot.operations[5].Require != require {
		t.Fatalf("require operation = %+v, want %+v", snapshot.operations[5], require)
	}
	if snapshot.operations[6].Prefer == nil || *snapshot.operations[6].Prefer != prefer {
		t.Fatalf("prefer operation = %+v, want %+v", snapshot.operations[6], prefer)
	}
	if snapshot.operations[10].Encode.ID != av.CodecVP8 || snapshot.operations[10].Encode.Settings.Bitrate != 500_000 {
		t.Fatalf("encode operation = %+v, want VP8 500k", snapshot.operations[10].Encode)
	}
	if snapshot.operations[11].Tap.Name != "preview.packets" ||
		snapshot.operations[11].Tap.Domain != shape.DomainPacket ||
		snapshot.operations[11].Tap.After != plan.OpEncode {
		t.Fatalf("packet tap = %+v, want packet tap after encode", snapshot.operations[11].Tap)
	}

	snapshot.operations[1].Transform.resize.Width = 999
	again := builder.snapshot()
	if again.operations[1].Transform.resize.Width != 160 {
		t.Fatalf("snapshot operations were not cloned: %+v", again.operations[1].Transform.resize)
	}

	spec := builder.To(branchBuilderTestSink("preview-out"))
	if spec.err != nil {
		t.Fatal(spec.err)
	}
	if got, want := branchDestinationNames(spec.destinations), []string{"preview-out"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("destination names = %v, want %v", got, want)
	}
}

func TestBranchSpecOriginContracts(t *testing.T) {
	typ := reflect.TypeOf(BranchSpec{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("BranchSpec field %s is exported; use constructors instead", typ.Field(i).Name)
		}
	}

	var zero BranchSpec
	if zero.origin != branchSpecOriginZero {
		t.Fatalf("zero BranchSpec origin = %v, want zero", zero.origin)
	}
	branch := Branch("preview").To(branchBuilderTestSink("preview"))
	if branch.origin != branchSpecOriginBranch {
		t.Fatalf("branch origin = %v, want branch", branch.origin)
	}
}

func TestBranchBuilderRouteAndDestinationHelpers(t *testing.T) {
	streamRoute := Branch("stream").Stream("camera").snapshot()
	if streamRoute.source.policy != pipeline.RouteByStream || streamRoute.source.label != "camera" {
		t.Fatalf("stream route = %+v, want RouteByStream camera", streamRoute.source)
	}
	eventRoute := Branch("events").Event(av.EventKeyframeRequired).snapshot()
	if eventRoute.source.policy != pipeline.RouteByEvent || eventRoute.source.label != string(av.EventKeyframeRequired) {
		t.Fatalf("event route = %+v, want RouteByEvent keyframe_required", eventRoute.source)
	}
	graphRoute := Branch("graph").From(fakeBranchRouteAnchor{route: pipeline.Route{From: "decoder"}}).snapshot()
	if graphRoute.source.from != "decoder" || graphRoute.source.policy != pipeline.RouteAll {
		t.Fatalf("graph route = %+v, want RouteAll from decoder", graphRoute.source)
	}

	if got := branchDestinationNames(nil); got != nil {
		t.Fatalf("branchDestinationNames(nil) = %v, want nil", got)
	}
	allocated := newDirectDestinationRef("fallback", destinationSpec{})
	if allocated.id == 0 || allocated.dest.name != "fallback" {
		t.Fatalf("allocated destination ref = %+v, want fallback name and allocated id", allocated)
	}
	preserved := newDirectDestinationRef("route", destinationSpec{id: 42, name: "declared"})
	if preserved.id != 42 || preserved.dest.name != "declared" {
		t.Fatalf("preserved destination ref = %+v, want id 42 and declared name", preserved)
	}
	if got, want := branchDestinationNames([]destinationRef{allocated, preserved}), []string{"fallback", "route"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("branchDestinationNames = %v, want %v", got, want)
	}

	sinkSpec, err := destinationSpecFromDestination(branchBuilderTestSink("sink"))
	if err != nil {
		t.Fatal(err)
	}
	if !branchDestinationsAllSinkDestinations([]destinationRef{newDirectDestinationRef("sink", sinkSpec)}) {
		t.Fatal("sink destinations should be recognized as sink-only")
	}
	if branchDestinationsAllSinkDestinations(nil) {
		t.Fatal("empty destination list should not be sink-only")
	}
	if branchDestinationsAllSinkDestinations([]destinationRef{newDirectDestinationRef("file", fileDestination("file.ivf", io.Discard))}) {
		t.Fatal("writer destinations should not be sink-only")
	}
}

func TestBranchOperationStepDetectionContracts(t *testing.T) {
	if branchOperationSpecsContainStep([]operationSpec{operationSpecForAutoPolicy([]shape.Policy{shape.AllowResize()})}) {
		t.Fatal("auto policy carriers should not count as branch chain steps")
	}
	if branchOperationSpecsContainStep([]operationSpec{operationSpecForRequire(shape.Frame(av.MediaVideo))}) {
		t.Fatal("shape requirements should stay annotation-only for branch ordering")
	}
	if !branchOperationSpecsContainStep([]operationSpec{operationSpecForShape(shape.Frame(av.MediaVideo))}) {
		t.Fatal("explicit shape operations should count as branch chain steps")
	}
	if branchOperationSpecsContainStep([]operationSpec{operationSpecForTap(PacketTap("encoded"), av.MediaVideo, plan.OpEncode)}) {
		t.Fatal("terminal packet taps should not count as branch chain steps")
	}
	if !branchOperationSpecsContainStep([]operationSpec{operationSpecForTap(FrameTap("frames"), av.MediaVideo, plan.OpDecode)}) {
		t.Fatal("frame taps should count as branch chain steps")
	}
}

func TestBranchBuilderNilAndErrorContracts(t *testing.T) {
	var nilBuilder *branchBuilder
	if nilBuilder.From(FrameTap("tap")) != nil ||
		nilBuilder.Stream("stream") != nil ||
		nilBuilder.Event(av.EventEndOfStream) != nil ||
		nilBuilder.Buffer(flow.Blocking(1)) != nil ||
		nilBuilder.Decode() != nil ||
		nilBuilder.Apply(Flow("voice").Audio()) != nil ||
		nilBuilder.Do(branchBuilderTestMeter("meter")) != nil ||
		nilBuilder.Auto(shape.AllowConvert()) != nil ||
		nilBuilder.Require(shape.Frame(av.MediaAudio)) != nil ||
		nilBuilder.Prefer(shape.Frame(av.MediaAudio)) != nil ||
		nilBuilder.Shape(shape.Frame(av.MediaAudio)) != nil ||
		nilBuilder.Resize(16, 16) != nil ||
		nilBuilder.Resample(48_000, codec.Mono) != nil ||
		nilBuilder.Tap(FrameTap("tap")) != nil ||
		nilBuilder.Encode(codec.Opus()) != nil ||
		nilBuilder.Copy() != nil {
		t.Fatal("nil branch builder method returned non-nil")
	}
	assertBuildErrorCode(t, nilBuilder.To(branchBuilderTestSink("nil")).err, branchInvalidCode)

	meter := branchBuilderTestMeter("meter")
	tests := []struct {
		name string
		spec BranchSpec
		code errcode.Code
	}{
		{name: "missing destination", spec: Branch("bad").To(), code: destinationMissingCode},
		{name: "invalid destination", spec: Branch("bad").To(Destination{}), code: destinationInvalidCode},
		{name: "invalid source", spec: Branch("bad").From(invalidBranchSource{}).To(branchBuilderTestSink("out")), code: branchSourceInvalidCode},
		{name: "invalid buffer", spec: Branch("bad").Buffer(flow.DropOldest(0)).To(branchBuilderTestSink("out")), code: branchBufferInvalidCode},
		{name: "nil stage", spec: Branch("bad").Do(nil).To(branchBuilderTestSink("out")), code: stageMissingCode},
		{name: "duplicate decode", spec: Branch("bad").Decode().Decode().To(branchBuilderTestSink("out")), code: branchDecodeDuplicateCode},
		{name: "decode after frame step", spec: Branch("bad").Do(meter).Decode().To(branchBuilderTestSink("out")), code: branchDecodeOrderInvalidCode},
		{name: "decode after encode", spec: Branch("bad").Encode(codec.Opus()).Decode().To(branchBuilderTestSink("out")), code: streamStepAfterEncodeCode},
		{name: "shape after encode", spec: Branch("bad").Encode(codec.Opus()).Shape(shape.Frame(av.MediaAudio)).To(branchBuilderTestSink("out")), code: streamStepAfterEncodeCode},
		{name: "resize after encode", spec: Branch("bad").Encode(codec.VP8()).Resize(320, 180).To(branchBuilderTestSink("out")), code: streamStepAfterEncodeCode},
		{name: "resample after encode", spec: Branch("bad").Encode(codec.Opus()).Resample(48_000, codec.Stereo).To(branchBuilderTestSink("out")), code: streamStepAfterEncodeCode},
		{name: "stage after encode", spec: Branch("bad").Encode(codec.Opus()).Do(meter).To(branchBuilderTestSink("out")), code: streamStepAfterEncodeCode},
		{name: "flow after encode", spec: Branch("bad").Encode(codec.Opus()).Apply(Flow("meter").Audio().Do(meter)).To(branchBuilderTestSink("out")), code: streamStepAfterEncodeCode},
		{name: "duplicate decode through apply", spec: Branch("bad").Decode().Apply(Flow("decode").Audio().Decode()).To(branchBuilderTestSink("out")), code: branchDecodeDuplicateCode},
		{name: "decode flow after frame step", spec: Branch("bad").Do(meter).Apply(Flow("decode").Audio().Decode()).To(branchBuilderTestSink("out")), code: branchDecodeOrderInvalidCode},
		{name: "copy flow after decode", spec: Branch("bad").Decode().Apply(Flow("copy").Audio().Copy()).To(branchBuilderTestSink("out")), code: flowCopyDomainMismatchCode},
		{name: "media mismatch through apply", spec: Branch("bad").Apply(Flow("voice").Audio()).Apply(Flow("preview").Video()).To(branchBuilderTestSink("out")), code: flowMediaMismatchCode},
		{name: "decode then copy", spec: Branch("bad").Decode().Copy().To(branchBuilderTestSink("out")), code: branchDecodeCopyInvalidCode},
		{name: "frame step after copy", spec: Branch("bad").Copy().Resample(48_000, codec.Mono).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "copy after frame step", spec: Branch("bad").Do(meter).Copy().To(branchBuilderTestSink("out")), code: flowCopyDomainMismatchCode},
		{name: "packet tap flow without decode", spec: Branch("bad").From(PacketTap("packets")).Apply(Flow("meter").Audio().Do(meter)).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "packet tap stage without decode", spec: Branch("bad").From(PacketTap("packets")).Do(meter).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "packet tap resize without decode", spec: Branch("bad").From(PacketTap("packets")).Resize(320, 180).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "packet tap resample without decode", spec: Branch("bad").From(PacketTap("packets")).Resample(48_000, codec.Mono).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "packet tap frame tap without decode", spec: Branch("bad").From(PacketTap("packets")).Tap(FrameTap("frames")).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "packet tap encode without decode", spec: Branch("bad").From(PacketTap("packets")).Encode(codec.Opus()).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "packet tap sink without domain", spec: Branch("bad").From(PacketTap("packets")).To(branchBuilderTestSink("out")), code: operationShapeMismatchCode},
		{name: "frame tap decode", spec: Branch("bad").From(FrameTap("frames")).Decode().To(branchBuilderTestSink("out")), code: sourceShapeMismatchCode},
		{name: "frame tap decode flow", spec: Branch("bad").From(FrameTap("frames")).Apply(Flow("decode").Audio().Decode()).To(branchBuilderTestSink("out")), code: sourceShapeMismatchCode},
		{name: "frame tap copy", spec: Branch("bad").From(FrameTap("frames")).Copy().To(branchBuilderTestSink("out")), code: sourceShapeMismatchCode},
		{name: "frame tap copy flow", spec: Branch("bad").From(FrameTap("frames")).Apply(Flow("copy").Audio().Copy()).To(branchBuilderTestSink("out")), code: sourceShapeMismatchCode},
		{name: "frame tap packet tap before encode", spec: Branch("bad").From(FrameTap("frames")).Tap(PacketTap("packets")).To(branchBuilderTestSink("out")), code: errcode.TapDomainMismatch},
		{name: "decode then packet tap before encode", spec: Branch("bad").Decode().Tap(PacketTap("packets")).To(branchBuilderTestSink("out")), code: errcode.TapDomainMismatch},
		{name: "duplicate encode", spec: Branch("bad").Encode(codec.Opus()).Encode(codec.Opus()).To(branchBuilderTestSink("out")), code: encodeDuplicateCode},
		{name: "empty tap", spec: Branch("bad").Tap(FrameTap("")).To(branchBuilderTestSink("out")), code: errcode.TapInvalid},
		{name: "typed frame tap after encode", spec: Branch("bad").Encode(codec.Opus()).Tap(FrameTap("frames")).To(branchBuilderTestSink("out")), code: errcode.TapDomainMismatch},
		{name: "nil flow", spec: Branch("bad").Apply(nil).To(branchBuilderTestSink("out")), code: flowInvalidCode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertBuildErrorCode(t, tt.spec.err, tt.code)
		})
	}
}

func TestBranchBuilderExplicitPacketSourceAllowsExplicitDomains(t *testing.T) {
	tests := []struct {
		name string
		spec BranchSpec
	}{
		{
			name: "copy to sink",
			spec: Branch("copy").
				From(PacketTap("packets")).
				Copy().
				To(branchBuilderTestSink("out")),
		},
		{
			name: "decode then encode to sink",
			spec: Branch("voice").
				From(PacketTap("packets")).
				Decode().
				Resample(48_000, codec.Mono).
				Encode(codec.Opus()).
				To(branchBuilderTestSink("out")),
		},
		{
			name: "packet tap after copy",
			spec: Branch("observe").
				From(PacketTap("packets")).
				Copy().
				Tap(PacketTap("packets.branch")).
				To(branchBuilderTestSink("out")),
		},
		{
			name: "frame tap source",
			spec: Branch("frames").
				From(FrameTap("frames")).
				Do(branchBuilderTestMeter("meter")).
				To(branchBuilderTestSink("out")),
		},
		{
			name: "frame tap encode then packet tap",
			spec: Branch("archive").
				From(FrameTap("frames")).
				Encode(codec.Opus()).
				Tap(PacketTap("packets")).
				To(branchBuilderTestSink("out")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.spec.err != nil {
				t.Fatal(tt.spec.err)
			}
		})
	}
}

func TestBranchConstructionErrorsAreJoined(t *testing.T) {
	spec := Branch("bad").
		Encode(codec.Opus()).
		Encode(codec.Opus()).
		To(Destination{})
	assertBuildErrorCode(t, spec.err, encodeDuplicateCode)
	err := spec.recipeErr()
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("branch err = %T, want joined construction errors", err)
	}
	codes := make(map[errcode.Code]bool)
	for _, cause := range joined.Unwrap() {
		var buildErr *BuildError
		if errors.As(cause, &buildErr) {
			codes[buildErr.Code] = true
		}
	}
	for _, code := range []errcode.Code{encodeDuplicateCode, destinationInvalidCode} {
		if !codes[code] {
			t.Fatalf("joined branch codes = %v, want %s", codes, code)
		}
	}
}

type invalidBranchSource struct{}

func (invalidBranchSource) Name() string { return "invalid" }

type fakeBranchRouteAnchor struct {
	route pipeline.Route
}

func (f fakeBranchRouteAnchor) Name() string          { return f.route.From }
func (f fakeBranchRouteAnchor) Route() pipeline.Route { return f.route }
