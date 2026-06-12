package goav

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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
