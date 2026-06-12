package goav

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func TestJobAllOutputsContracts(t *testing.T) {
	root := File("root.webm", io.Discard).spec
	stream := File("stream.webm", io.Discard).spec
	branch := File("branch.webm", io.Discard).spec

	job := &Job{
		outputs: []destinationSpec{root},
		streams: []*jobStreamBuild{{
			outputs:     []destinationSpec{stream},
			outputNames: []string{"stream.webm"},
		}},
	}
	outputs := job.allOutputs()
	if len(outputs) != 2 || outputs[0].name != "root.webm" || outputs[1].name != "stream.webm" {
		t.Fatalf("stream outputs = %v, want root then stream", destinationNamesForTest(outputs))
	}

	job.branchDestinations = []namedDestinationSpec{{name: "branch", output: branch}}
	outputs = job.allOutputs()
	if len(outputs) != 1 || outputs[0].name != "branch.webm" {
		t.Fatalf("branch outputs = %v, want branch only", destinationNamesForTest(outputs))
	}

	cloned := jobAllOutputs([]destinationSpec{root}, nil)
	cloned[0].name = "mutated"
	if root.name != "root.webm" {
		t.Fatalf("jobAllOutputs did not clone root outputs, root name = %q", root.name)
	}
}

func TestBufferBudgetHelperContracts(t *testing.T) {
	base := pipeline.BufferPolicy{CopyPacketBytes: 16, CopyFrameBytes: 32}
	next := pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropBlock, CopyPacketBytes: 64, CopyFrameBytes: 8, CopyAlways: true}
	merged := mergeBufferCopyBounds(base, next)
	if merged.Capacity != 4 || merged.Drop != pipeline.DropBlock || merged.CopyPacketBytes != 64 || merged.CopyFrameBytes != 8 || !merged.CopyAlways {
		t.Fatalf("merged buffer policy = %+v", merged)
	}
	merged = mergeBufferCopyBounds(
		pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropOldest, CopyPacketBytes: 128, CopyFrameBytes: 32},
		pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropBlock, CopyPacketBytes: 64, CopyFrameBytes: 256},
	)
	if merged.Capacity != 2 || merged.Drop != pipeline.DropOldest || merged.CopyPacketBytes != 128 || merged.CopyFrameBytes != 256 {
		t.Fatalf("non-direct merged buffer policy = %+v", merged)
	}
	merged = mergeBufferCopyBounds(
		pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropOldest, CopyPacketBytes: 16},
		pipeline.BufferPolicy{Capacity: 4, Drop: pipeline.DropBlock, CopyPacketBytes: 64},
	)
	if merged.Capacity != 2 || merged.Drop != pipeline.DropOldest || merged.CopyPacketBytes != 64 {
		t.Fatalf("non-direct packet bounds merge = %+v", merged)
	}

	if got, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 1, av.PixelFormatI420))); err != nil || got != 12288 {
		t.Fatalf("i420 budget = %d, %v; want 12288", got, err)
	}
	if got, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 1, av.PixelFormatGray8))); err != nil || got != 4096 {
		t.Fatalf("gray8 budget = %d, %v; want 4096", got, err)
	}
	if _, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(0, 1, av.PixelFormatI420))); err == nil || err.Error() != "missing width" {
		t.Fatalf("missing width err = %v", err)
	}
	if _, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 0, av.PixelFormatI420))); err == nil || err.Error() != "missing height" {
		t.Fatalf("missing height err = %v", err)
	}
	if _, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 1, ""))); err == nil || err.Error() != "missing pixel_format" {
		t.Fatalf("missing pixel format err = %v", err)
	}
	if _, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 1, "rgb24"))); err == nil || !strings.Contains(err.Error(), "unsupported pixel_format") {
		t.Fatalf("unsupported pixel format err = %v", err)
	}

	operation := workOperation{
		Branch:  "preview",
		Kind:    plan.OpStage,
		ShapeIn: shape.Frame(av.MediaVideo, shape.Video(1920, 1080, "rgb24")),
	}
	if got := bufferBudgetSuggestions(operation, "missing width"); !strings.Contains(got[0], "declare video geometry") {
		t.Fatalf("missing width suggestions = %#v", got)
	}
	if got := bufferBudgetSuggestions(operation, "missing pixel_format"); !strings.Contains(got[0], "pixel format") {
		t.Fatalf("missing pixel format suggestions = %#v", got)
	}
	if got := bufferBudgetSuggestions(operation, "unsupported pixel_format \"rgb24\""); !strings.Contains(got[0], "I420") || !strings.Contains(got[2], "rgb24") {
		t.Fatalf("unsupported pixel suggestions = %#v", got)
	}
	audioOp := workOperation{
		Branch:  "voice",
		Kind:    plan.OpStage,
		ShapeIn: shape.Frame(av.MediaAudio, shape.Audio(0, codec.Stereo, "")),
	}
	if got := bufferBudgetSuggestions(audioOp, "missing sample_rate"); !strings.Contains(got[0], "complete audio shape") {
		t.Fatalf("audio suggestions = %#v", got)
	}
	if got := bufferBudgetSuggestions(operation, "mystery"); !strings.Contains(got[1], "CopyPacketBytes") {
		t.Fatalf("fallback suggestions = %#v", got)
	}
}

func TestBufferBudgetDecisionHelperContracts(t *testing.T) {
	frameOp := workOperation{
		Name:    "scale",
		Kind:    plan.OpStage,
		Branch:  "preview",
		Node:    pipeline.NodeRef("scale-node"),
		ShapeIn: shape.Frame(av.MediaVideo, shape.Video(320, 180, av.PixelFormatI422)),
	}
	packetOp := workOperation{
		Name:    "encode",
		Kind:    plan.OpEncode,
		Branch:  "archive",
		Node:    pipeline.NodeRef("encode-node"),
		ShapeIn: shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, codec.Stereo, av.SampleFormatS16)),
	}
	operations := []workOperation{
		{Name: "shape", Kind: plan.OpShape, Node: pipeline.NodeRef("shape-node"), ShapeIn: shape.Frame(av.MediaVideo, shape.Video(1, 1, av.PixelFormatGray8))},
		frameOp,
		{Name: "tap", Kind: plan.OpTap, Node: pipeline.NodeRef("tap-node"), ShapeIn: shape.Frame(av.MediaVideo, shape.Video(1920, 1080, av.PixelFormatI420))},
		packetOp,
	}

	found, ok := workOperationForNodeKind(operations, pipeline.NodeRef("scale-node"), plan.OpStage)
	if !ok || found.Name != "scale" {
		t.Fatalf("found operation = %+v, %v; want scale stage", found, ok)
	}
	if _, ok := workOperationForNodeKind(operations, pipeline.NodeRef("scale-node"), plan.OpEncode); ok {
		t.Fatal("workOperationForNodeKind matched the wrong operation kind")
	}

	budget, err := copyBudgetForOperations(operations, true, true)
	if err != nil {
		t.Fatalf("copyBudgetForOperations error = %v", err)
	}
	if budget.packetBytes != 4096 || budget.frameBytes != 135168 {
		t.Fatalf("copy budget = %+v, want packet=4096 frame=135168", budget)
	}
	if budget, err := copyBudgetForNodeInput(packetOp, false, true); err != nil || budget != (copyBudget{}) {
		t.Fatalf("packet opt-out budget = %+v, %v; want zero nil", budget, err)
	}
	if budget, err := copyBudgetForNodeInput(frameOp, true, false); err != nil || budget != (copyBudget{}) {
		t.Fatalf("frame opt-out budget = %+v, %v; want zero nil", budget, err)
	}
	eventOp := workOperation{Kind: plan.OpStage, Node: pipeline.NodeRef("event-node"), ShapeIn: shape.Event()}
	if budget, err := copyBudgetForNodeInput(eventOp, true, true); err != nil || budget != (copyBudget{}) {
		t.Fatalf("event budget = %+v, %v; want zero nil", budget, err)
	}

	policy, err := bufferPolicyWithShapeBudgetsForOperations(pipeline.BufferPolicy{Capacity: 3, Drop: pipeline.DropOldest}, operations)
	if err != nil {
		t.Fatalf("bufferPolicyWithShapeBudgetsForOperations error = %v", err)
	}
	if policy.CopyPacketBytes != 4096 || policy.CopyFrameBytes != 135168 {
		t.Fatalf("policy copy bounds = %+v, want packet=4096 frame=135168", policy)
	}
	invalidShapeOp := workOperation{
		Name:    "custom-filter",
		Kind:    plan.OpStage,
		Branch:  "preview",
		Node:    pipeline.NodeRef("custom-node"),
		ShapeIn: shape.Frame(av.MediaVideo, shape.Video(16, 16, "rgb24")),
	}
	explicit, err := bufferPolicyWithShapeBudgetsForOperations(
		pipeline.BufferPolicy{Capacity: 3, Drop: pipeline.DropOldest, CopyPacketBytes: 512, CopyFrameBytes: 4096},
		[]workOperation{invalidShapeOp},
	)
	if err != nil {
		t.Fatalf("explicit copy bounds should skip shape derivation, got %v", err)
	}
	if explicit.CopyPacketBytes != 512 || explicit.CopyFrameBytes != 4096 {
		t.Fatalf("explicit policy = %+v, want caller-provided bounds", explicit)
	}

	_, err = copyBudgetForNodeInput(invalidShapeOp, true, true)
	if err == nil {
		t.Fatal("copyBudgetForNodeInput error = nil, want buffer budget diagnostic")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("error = %T, want *BuildError", err)
	}
	if buildErr.Code != errcode.BufferBudgetMissing || buildErr.Node != "custom-node" || !strings.Contains(buildErr.Reason, `unsupported pixel_format "rgb24"`) {
		t.Fatalf("BuildError = %+v, want buffer_budget_missing on custom-node with rgb24 reason", buildErr)
	}
	if len(buildErr.Details) != 3 || len(buildErr.Suggestions) == 0 {
		t.Fatalf("BuildError details/suggestions = %#v / %#v", buildErr.Details, buildErr.Suggestions)
	}
}

func TestBufferBudgetMathHelperContracts(t *testing.T) {
	if got := alignedVideoCopyDimension(0); got != 0 {
		t.Fatalf("alignedVideoCopyDimension(0) = %d, want 0", got)
	}
	if got := alignedVideoCopyDimension(65); got != 128 {
		t.Fatalf("alignedVideoCopyDimension(65) = %d, want 128", got)
	}
	if got, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 1, av.PixelFormatI422))); err != nil || got != 12288 {
		t.Fatalf("i422 budget = %d, %v; want 12288", got, err)
	}
	if got, err := videoFrameCopyBudget(shape.Frame(av.MediaVideo, shape.Video(1, 1, av.PixelFormatI444))); err != nil || got != 12288 {
		t.Fatalf("i444 budget = %d, %v; want 12288", got, err)
	}
	if got, err := frameCopyBudget(shape.Frame(av.MediaData)); err != nil || got != 0 {
		t.Fatalf("data frame budget = %d, %v; want zero nil", got, err)
	}

	if got, ok := audioSampleBytes(av.SampleFormatS16); got != 2 || !ok {
		t.Fatalf("s16 bytes = %d, %v; want 2 true", got, ok)
	}
	if got, ok := audioSampleBytes(av.SampleFormatF32); got != 4 || !ok {
		t.Fatalf("f32 bytes = %d, %v; want 4 true", got, ok)
	}
	if got, ok := audioSampleBytes("flt_planar"); got != 0 || ok {
		t.Fatalf("unknown audio bytes = %d, %v; want 0 false", got, ok)
	}

	if got, err := audioFrameCopyBudget(shape.Frame(av.MediaAudio, shape.Audio(4000, codec.Mono, av.SampleFormatF32))); err != nil || got != 3840 {
		t.Fatalf("f32 low-rate audio budget = %d, %v; want 3840", got, err)
	}
	if got, err := audioFrameCopyBudget(shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Stereo, av.SampleFormatS16))); err != nil || got != 23040 {
		t.Fatalf("s16 stereo audio budget = %d, %v; want 23040", got, err)
	}
	for _, tt := range []struct {
		name string
		spec shape.Spec
		want string
	}{
		{name: "missing rate", spec: shape.Frame(av.MediaAudio, shape.Audio(0, codec.Stereo, av.SampleFormatS16)), want: "missing sample_rate"},
		{name: "missing channels", spec: shape.Frame(av.MediaAudio, shape.Audio(48000, 0, av.SampleFormatS16)), want: "missing channels"},
		{name: "missing format", spec: shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Stereo, "")), want: "missing sample_format"},
		{name: "unsupported format", spec: shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Stereo, "flt_planar")), want: `unsupported sample_format "flt_planar"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := audioFrameCopyBudget(tt.spec); err == nil || err.Error() != tt.want {
				t.Fatalf("audioFrameCopyBudget error = %v, want %q", err, tt.want)
			}
		})
	}
	if got := bufferBudgetSuggestions(workOperation{ShapeIn: shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Stereo, "flt_planar"))}, `unsupported sample_format "flt_planar"`); len(got) == 0 || !strings.Contains(got[0], "SampleFormatS16") {
		t.Fatalf("unsupported audio suggestions = %#v", got)
	}
}

func TestRecipeGraphConfigBufferContracts(t *testing.T) {
	if _, err := recipeGraphConfig(nil, "", workPlan{}, false); err == nil {
		t.Fatal("recipeGraphConfig nil runtime error = nil")
	}
	rt := New(WithEventCapacity(7)).(*runtime)
	config, err := recipeGraphConfig(rt, "", workPlan{Name: "camera"}, true)
	if err != nil {
		t.Fatalf("recipeGraphConfig error = %v", err)
	}
	if config.Name != "camera" || !config.Realtime || config.EventCapacity != 7 {
		t.Fatalf("config identity = %+v, want name camera realtime eventCapacity 7", config)
	}
	if config.Buffer.Capacity != realtimeRecipeBufferCapacity || config.Buffer.Drop != pipeline.DropBlock {
		t.Fatalf("realtime buffer = %+v, want capacity %d drop block", config.Buffer, realtimeRecipeBufferCapacity)
	}

	rt = New(WithRealtime(false), WithBufferPolicy(pipeline.BufferPolicy{Capacity: 2, Drop: pipeline.DropNewest, CopyPacketBytes: 99, CopyFrameBytes: 100})).(*runtime)
	config, err = recipeGraphConfig(rt, "override", workPlan{Name: "ignored"}, true)
	if err != nil {
		t.Fatalf("offline recipeGraphConfig error = %v", err)
	}
	if config.Name != "override" || config.Realtime || config.Buffer.CopyPacketBytes != 99 || config.Buffer.CopyFrameBytes != 100 {
		t.Fatalf("offline config = %+v, want explicit name, non-realtime, preserved copy bounds", config)
	}
}

func destinationNamesForTest(outputs []destinationSpec) []string {
	names := make([]string, len(outputs))
	for i := range outputs {
		names[i] = outputs[i].name
	}
	return names
}

func TestJobAllOutputsAppendKeepsOrder(t *testing.T) {
	first := File("first.webm", io.Discard).spec
	second := File("second.webm", io.Discard).spec
	got := jobAllOutputs([]destinationSpec{first}, []destinationSpec{second})
	if !reflect.DeepEqual(destinationNamesForTest(got), []string{"first.webm", "second.webm"}) {
		t.Fatalf("all output names = %v", destinationNamesForTest(got))
	}
}
