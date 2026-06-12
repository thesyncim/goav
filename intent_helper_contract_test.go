package goav

import (
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestOperationSpecsContainChainStepContracts(t *testing.T) {
	tests := []struct {
		name       string
		operation  operationSpec
		wantIsStep bool
	}{
		{name: "select is not a chain step", operation: operationSpec{Kind: plan.OpSelect}},
		{name: "auto annotation is not a chain step", operation: operationSpecForAutoPolicy([]shape.Policy{shape.AllowResize()})},
		{name: "require annotation is not a chain step", operation: operationSpecForRequire(shape.Frame(av.MediaVideo))},
		{name: "prefer annotation is not a chain step", operation: operationSpecForPreference(shape.Frame(av.MediaVideo))},
		{name: "shape facts are a chain step", operation: operationSpecForShape(shape.Frame(av.MediaVideo)), wantIsStep: true},
		{name: "custom stage is a chain step", operation: operationSpec{Kind: plan.OpStage}, wantIsStep: true},
		{name: "transform is a chain step", operation: operationSpecForTransform(Resize(320, 180)), wantIsStep: true},
		{name: "packet tap is not a frame chain step", operation: operationSpecForTap(PacketTap("packets"), av.MediaAudio, plan.OpEncode)},
		{name: "frame tap is a chain step", operation: operationSpecForTap(FrameTap("frames"), av.MediaAudio, plan.OpDecode), wantIsStep: true},
		{name: "inferred frame tap is a chain step", operation: operationSpecForTap(Tap("decoded"), av.MediaAudio, plan.OpDecode), wantIsStep: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := operationSpecsContainChainStep([]operationSpec{tt.operation}); got != tt.wantIsStep {
				t.Fatalf("contains chain step = %v, want %v", got, tt.wantIsStep)
			}
		})
	}
	if operationSpecsContainChainStep(nil) {
		t.Fatal("nil operations reported a chain step")
	}
}

func TestJobStreamOperationHelperContracts(t *testing.T) {
	if jobStreamHasDecodeOperation(nil) {
		t.Fatal("nil stream reported decode")
	}
	if got := jobOperationSpecs(nil); got != nil {
		t.Fatalf("nil jobOperationSpecs = %#v, want nil", got)
	}

	stream := &jobStreamBuild{
		name:     "voice",
		selector: av.StreamSelector{Type: av.MediaAudio, ID: "a0", Codec: av.CodecOpus},
		operations: []operationSpec{
			operationSpecForDecode(codec.Opus(), "opus"),
			operationSpecForTransform(Resample(16_000, codec.Mono)),
			operationSpecForAutoPolicy([]shape.Policy{shape.AllowResample()}),
			operationSpecForRequire(shape.Frame(av.MediaAudio, shape.Audio(16_000, codec.Mono, ""))),
			operationSpecForPreference(shape.Frame(av.MediaAudio, shape.Audio(0, 0, av.SampleFormatF32))),
		},
	}
	if !jobStreamHasDecodeOperation(stream) {
		t.Fatal("stream with decode operation did not report decode")
	}
	ops := jobOperationSpecs(stream)
	if got, want := operationKindsForFlowTest(ops), []plan.OperationKind{
		plan.OpDecode,
		plan.OpTransform,
		plan.OpShape,
		plan.OpShape,
		plan.OpShape,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operation kinds = %v, want %v", got, want)
	}
	if ops[1].Transform.Resample == stream.operations[1].Transform.Resample {
		t.Fatal("jobOperationSpecs reused resample config pointer")
	}
	if ops[2].Auto == stream.operations[2].Auto {
		t.Fatal("jobOperationSpecs reused auto policy pointer")
	}
	if ops[3].Require == stream.operations[3].Require {
		t.Fatal("jobOperationSpecs reused require pointer")
	}
	if ops[4].Prefer == stream.operations[4].Prefer {
		t.Fatal("jobOperationSpecs reused prefer pointer")
	}
	ops[1].Transform.Resample.SampleRate = 8_000
	if got := jobOperationSpecs(stream)[1].Transform.Resample.SampleRate; got != 16_000 {
		t.Fatalf("operation clone mutation leaked into stream, sample rate = %d", got)
	}
}

func TestJobStreamNameAndChainStepContracts(t *testing.T) {
	if got := jobStreamName(nil); got != "stream" {
		t.Fatalf("nil jobStreamName = %q, want stream", got)
	}
	tests := []struct {
		name   string
		stream *jobStreamBuild
		want   string
	}{
		{name: "explicit name", stream: &jobStreamBuild{name: "voice"}, want: "voice"},
		{name: "selector id", stream: &jobStreamBuild{selector: av.StreamSelector{ID: "a0", Type: av.MediaAudio}}, want: "a0"},
		{name: "selector type", stream: &jobStreamBuild{selector: av.StreamSelector{Type: av.MediaVideo}}, want: "video"},
		{name: "fallback", stream: &jobStreamBuild{}, want: "stream"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := jobStreamName(tt.stream); got != tt.want {
				t.Fatalf("jobStreamName = %q, want %q", got, tt.want)
			}
		})
	}

	if got := jobStreamChainSteps(nil); got != nil {
		t.Fatalf("nil jobStreamChainSteps = %#v, want nil", got)
	}
	stream := &jobStreamBuild{
		operations: []operationSpec{
			operationSpecForTransform(Resize(160, 90)),
			operationSpecForTap(FrameTap("preview.frames"), av.MediaVideo, plan.OpTransform),
			operationSpecForTap(PacketTap("preview.packets"), av.MediaVideo, plan.OpEncode),
		},
	}
	steps := jobStreamChainSteps(stream)
	if len(steps) != 2 {
		t.Fatalf("chain steps = %#v, want transform and frame tap", steps)
	}
	if steps[0].transform.Resize == nil || steps[0].transform.Resize.Width != 160 {
		t.Fatalf("first chain step = %#v, want cloned resize", steps[0])
	}
	if steps[1].tap != "preview.frames" || steps[1].tapDomain != shape.DomainFrame {
		t.Fatalf("second chain step = %#v, want frame tap", steps[1])
	}
	steps[0].transform.Resize.Width = 999
	if got := jobStreamChainSteps(stream)[0].transform.Resize.Width; got != 160 {
		t.Fatalf("chain step clone mutation leaked into stream, width = %d", got)
	}
}

func TestPlannedBranchSharedOperationSpecsContracts(t *testing.T) {
	parent := &jobStreamBuild{
		selector: av.StreamSelector{Type: av.MediaAudio, Codec: av.CodecOpus},
		operations: []operationSpec{
			operationSpecForDecode(codec.Opus(), "opus"),
			operationSpecForTransform(Resample(16_000, codec.Mono)),
			operationSpecForTap(FrameTap("voice.frames"), av.MediaAudio, plan.OpTransform),
			operationSpecForEncode(codec.Opus(codec.Bitrate(64_000))),
		},
	}

	if got := plannedBranchSharedOperationSpecs(nil, BranchSpec{}, false); got != nil {
		t.Fatalf("nil stream shared ops = %#v, want nil", got)
	}
	if got := plannedBranchSharedOperationSpecs(parent, BranchSpec{}, true); got != nil {
		t.Fatalf("parent packet shared ops = %#v, want nil", got)
	}
	if got := plannedBranchSharedOperationSpecs(&jobStreamBuild{}, BranchSpec{}, false); got != nil {
		t.Fatalf("empty stream shared ops = %#v, want nil", got)
	}

	all := plannedBranchSharedOperationSpecs(parent, BranchSpec{}, false)
	if got, want := operationKindsForFlowTest(all), []plan.OperationKind{
		plan.OpDecode,
		plan.OpTransform,
		plan.OpTap,
		plan.OpEncode,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("all shared operation kinds = %v, want %v", got, want)
	}
	assertSharedOperations(t, all)
	if parent.operations[1].Shared {
		t.Fatal("shared operation clone mutated parent operation")
	}

	throughTap := plannedBranchSharedOperationSpecs(parent, BranchSpec{
		source: branchSourceBinding{tap: "voice.frames"},
	}, false)
	if got, want := operationKindsForFlowTest(throughTap), []plan.OperationKind{
		plan.OpDecode,
		plan.OpTransform,
		plan.OpTap,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tap shared operation kinds = %v, want %v", got, want)
	}
	assertSharedOperations(t, throughTap)

	decoded := plannedBranchSharedOperationSpecs(parent, BranchSpec{
		source: branchSourceBinding{tap: defaultDecodedTapName(av.MediaAudio)},
	}, false)
	if got, want := operationKindsForFlowTest(decoded), []plan.OperationKind{plan.OpDecode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded shared operation kinds = %v, want %v", got, want)
	}
	assertSharedOperations(t, decoded)

	if got := plannedBranchSharedOperationSpecs(parent, BranchSpec{
		source: branchSourceBinding{tap: "missing"},
	}, false); got != nil {
		t.Fatalf("missing tap shared ops = %#v, want nil", got)
	}
}

func assertSharedOperations(t *testing.T, operations []operationSpec) {
	t.Helper()
	for i := range operations {
		if !operations[i].Shared {
			t.Fatalf("operation %d is not marked shared: %#v", i, operations[i])
		}
	}
}
