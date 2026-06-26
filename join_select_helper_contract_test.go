package goav

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestSelectJoinArmContracts(t *testing.T) {
	var nilSelect *selectorStream
	if arm := nilSelect.joinArm(); arm.chain != nil || arm.join != nil || arm.tap != nil || arm.region != nil {
		t.Fatalf("nil selector join arm = %+v, want zero", arm)
	}

	selector := Select(
		From(selectTestOneShotSource("a", 1)).Audio(),
		From(selectTestOneShotSource("b", 2)).Audio(),
	).Tap(FrameTap("selected.frames")).Region(3, 4)
	arm := selector.joinArm()
	if arm.join == nil || arm.join.kind != joinSelect || len(arm.join.arms) != 2 || len(arm.join.taps) != 1 {
		t.Fatalf("selector join arm = %+v, want select join with arms and tap", arm)
	}
	if arm.region == nil || arm.region.x != 3 || arm.region.y != 4 {
		t.Fatalf("selector region = %+v, want (3,4)", arm.region)
	}
}

func TestJoinStagePreallocDepthContracts(t *testing.T) {
	if got := joinStagePreallocDepth(nil); got != 1 {
		t.Fatalf("nil runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew()); got != 1 {
		t.Fatalf("direct runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Drop: pipeline.DropOldest}))); got != 1 {
		t.Fatalf("zero-capacity buffered prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 128, Drop: pipeline.DropOldest}))); got != joinStagePreallocDepthLimit {
		t.Fatalf("large buffered prealloc depth = %d, want cap %d", got, joinStagePreallocDepthLimit)
	}
}

func TestJoinArmTransformStreamContracts(t *testing.T) {
	video := videoVP8TranscodeTestStream()
	if got := joinArmTransformStream(video, av.MediaVideo); got.ID != video.ID || got.Codec.ID != video.Codec.ID {
		t.Fatalf("video transform stream = %+v, want original stream", got)
	}

	audio := joinArmTransformStream(av.Stream{
		ID:   "voice",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			SampleRate: 16000,
		},
	}, av.MediaAudio)
	if audio.ID != "voice" || audio.Type != av.MediaAudio || audio.Codec.Channels != 1 ||
		audio.Codec.SampleFormat != av.SampleFormatS16 || audio.Codec.ClockRate != 16000 {
		t.Fatalf("audio transform stream = %+v, want normalized audio stream", audio)
	}

	preserved := joinArmTransformStream(av.Stream{
		ID:   "stereo",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatF32,
		},
	}, av.MediaAudio)
	if preserved.Codec.Channels != 2 || preserved.Codec.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("audio transform stream = %+v, want preserved channel/sample format", preserved)
	}
}

func TestInsertJoinArmStageConnectsUpstream(t *testing.T) {
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "join-arm"})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	sourceRef, err := graph.AddSource(&runtimeTestSource{name: "source"}, pipeline.BufferPolicy{})
	if err != nil {
		t.Fatal(err)
	}

	ref, err := insertJoinArmStage(graph, mustNew(), &runtimeTestStage{name: "arm-stage"}, string(sourceRef))
	if err != nil {
		t.Fatal(err)
	}
	if ref != "arm-stage" {
		t.Fatalf("insertJoinArmStage ref = %q, want arm-stage", ref)
	}
	spec := graph.Spec()
	if len(spec.Edges) != 1 || spec.Edges[0].From != sourceRef || spec.Edges[0].To != pipeline.NodeRef(ref) {
		t.Fatalf("graph edges = %+v, want source -> arm-stage", spec.Edges)
	}
}

func TestInsertJoinArmStageFailureContracts(t *testing.T) {
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "join-arm-fail"})
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	_, err = insertJoinArmStage(
		graph,
		mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 1})),
		&runtimeTestStage{name: "buffered-stage"},
		"source",
	)
	if !errors.Is(err, pipeline.ErrBufferedEdgesUnsupported) {
		t.Fatalf("buffered insert error = %v, want ErrBufferedEdgesUnsupported", err)
	}

	_, err = insertJoinArmStage(graph, mustNew(), &runtimeTestStage{name: "orphan-stage"}, "missing")
	if err == nil {
		t.Fatal("missing upstream insert error = nil, want connect error")
	}
}
