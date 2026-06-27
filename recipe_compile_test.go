package goav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// decodeIntentOperations / encodeIntentOperations / decodeEncodeIntentOperations
// build streamIntent fixtures the way the builders do: decode, copy, and encode
// requests are operations on the chain's operation list, not parallel fields.
func decodeIntentOperations() []operationSpec {
	return []operationSpec{operationSpecForDecode(codec.CodecSpec{}, "")}
}

func encodeIntentOperations(spec codec.CodecSpec) []operationSpec {
	return []operationSpec{operationSpecForEncode(spec)}
}

func decodeEncodeIntentOperations(spec codec.CodecSpec) []operationSpec {
	return append(decodeIntentOperations(), operationSpecForEncode(spec))
}

func moveTestStreamsToRecipeIR(state *recipeCompileState, kind recipeir.Kind) {
	if state == nil {
		return
	}
	if state.recipe.Kind == "" {
		state.recipe.Kind = kind
	}
	if len(state.recipe.Streams) == 0 && len(state.intent.Streams) != 0 {
		state.recipe.Streams = make([]recipeir.Stream, 0, len(state.intent.Streams))
		for i := range state.intent.Streams {
			state.recipe.Streams = append(state.recipe.Streams, recipeIRStreamFromIntent(state.intent.Streams[i]))
		}
	}
	state.intent.Streams = nil
}

func moveTestIntentToRecipeIR(state *recipeCompileState, kind recipeir.Kind) {
	if state == nil {
		return
	}
	state.recipe = recipeIRFromIntent(state.intent, kind)
	state.intent = intent{}
	state.outputAttachments = nil
	state.outputDestinationNames = nil
	state.branchDestinationAttachments = nil
}

func moveTestOutputsToRecipeIR(state *recipeCompileState, kind recipeir.Kind, destinations ...recipeir.Destination) {
	if state == nil {
		return
	}
	if len(state.recipe.Inputs) == 0 && len(state.intent.Inputs) != 0 {
		state.recipe.Inputs = make([]recipeir.Input, 0, len(state.intent.Inputs))
		for i := range state.intent.Inputs {
			state.recipe.Inputs = append(state.recipe.Inputs, recipeIRInputFromIntent(state.intent.Inputs[i]))
		}
	}
	moveTestStreamsToRecipeIR(state, kind)
	if len(destinations) != 0 {
		state.recipe.Destinations = append([]recipeir.Destination(nil), destinations...)
	} else if len(state.recipe.Destinations) == 0 && len(state.intent.Destinations) != 0 {
		state.recipe.Destinations = make([]recipeir.Destination, 0, len(state.intent.Destinations))
		for i := range state.intent.Destinations {
			state.recipe.Destinations = append(state.recipe.Destinations, recipeIRDestinationFromIntent(state.intent.Destinations[i]))
		}
	}
	state.intent.Inputs = nil
	state.intent.Destinations = nil
	state.outputAttachments = nil
	state.outputDestinationNames = nil
	state.branchDestinationAttachments = nil
}

func testRecipeIRDestination(name string, kind recipeir.DestinationKind) recipeir.Destination {
	return recipeir.Destination{Name: name, Kind: kind}
}

type graphPlanTestLowerer struct {
	runtime *runtime
	called  bool
}

func (l *graphPlanTestLowerer) spec() (pipeline.Spec, error) {
	return pipeline.Spec{Name: "goav"}, nil
}

func (l *graphPlanTestLowerer) runtimeRef() *runtime {
	return l.runtime
}

func (l *graphPlanTestLowerer) lower(context.Context, graphPlan, pipeline.Graph, *builder) error {
	l.called = true
	return nil
}

func requireGraphPlanLowerer[T any](t *testing.T, resolved recipeResolved) {
	t.Helper()
	if !resolved.graphPlan.ready() {
		t.Fatal("resolved graph plan is not ready")
	}
	if _, ok := resolved.graphPlan.lowerer.(T); !ok {
		var want T
		t.Fatalf("graph plan lowerer = %T, want %T", resolved.graphPlan.lowerer, want)
	}
}

func TestGraphPlanBuildValidatesOperationsBeforeLowerer(t *testing.T) {
	runtime := mustNew()
	lowerer := &graphPlanTestLowerer{runtime: runtime}
	gp := graphPlan{
		runtime: runtime,
		name:    "goav",
		nodes: []pipeline.NodeSpec{{
			Name: "source",
			Kind: pipeline.NodeSource,
		}},
		lowerer: lowerer,
	}

	task, err := buildGraphPlanTask(context.Background(), gp)
	if err == nil {
		task.Close()
		t.Fatal("buildGraphPlanTask() error = nil, want invalid graph-plan error")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want graph_plan_invalid with matching BuildError code", err)
	}
	if lowerer.called {
		t.Fatal("graph-plan lowerer was called after invalid ordered operations")
	}
}

func TestGraphPlanCarriesCloneSafeWorkPlan(t *testing.T) {
	runtime := mustNew()
	branches := []planBranch{{
		Name:  "preview",
		Input: "input.ivf",
		Stream: plan.StreamSelect{
			Type:  av.MediaVideo,
			Codec: av.CodecVP8,
		},
		Shape: shape.Packet(av.MediaVideo, av.CodecVP8),
		Operations: []planOperation{{
			Kind:      plan.OpDecode,
			Component: string(av.CodecVP8),
			Shape:     shape.Frame(av.MediaVideo, shape.Video(640, 360, "i420")),
		}, {
			Kind:      plan.OpTap,
			Component: "video.decoded",
			After:     plan.OpDecode,
			Shape:     shape.Frame(av.MediaVideo, shape.Video(640, 360, "i420")),
		}},
		Outputs: []string{"frames"},
	}}
	outputs := []planOutput{{
		Name:       "frames",
		Operation:  plan.OpSink,
		Component:  "sink",
		BranchRefs: []string{"preview"},
	}}
	spec := pipeline.Spec{
		Name: "work-plan-test",
		Nodes: []pipeline.NodeSpec{{
			Name: "decode",
			Kind: pipeline.NodeStage,
		}, {
			Name: "frames",
			Kind: pipeline.NodeSink,
		}},
		Edges: []pipeline.EdgeSpec{{
			From:   "decode",
			To:     "frames",
			Policy: pipeline.RouteAll,
		}},
	}
	composed := composeWorkPlan(
		spec,
		"work-plan-test",
		[]workInput{{
			Name:      "input.ivf",
			Protocol:  av.ProtocolFile,
			Codec:     av.CodecVP8,
			CodecSpec: codec.VP8(codec.ClockRate(90_000)),
		}},
		[]workStream{{
			Name: "video",
			Select: plan.StreamSelect{
				Type:  av.MediaVideo,
				Codec: av.CodecVP8,
			},
			Operations:   []operationSpec{operationSpecForEncode(codec.VP9())},
			Destinations: []string{"frames"},
		}},
		branches,
		outputs,
		nil,
	)
	graph := newGraphPlan(runtime, spec, composed, &graphPlanTestLowerer{runtime: runtime})

	work := graph.workPlan()
	if work.Name != "work-plan-test" {
		t.Fatalf("work plan name = %q, want work-plan-test", work.Name)
	}
	if len(work.Inputs) != 1 || work.Inputs[0].Name != "input.ivf" {
		t.Fatalf("work inputs = %+v, want input.ivf", work.Inputs)
	}
	if work.Inputs[0].CodecSpec.ID != av.CodecVP8 || work.Inputs[0].CodecSpec.Parameters.ClockRate != 90_000 {
		t.Fatalf("work input codec spec = %+v, want VP8 90kHz", work.Inputs[0].CodecSpec)
	}
	if len(work.Streams) != 1 || work.Streams[0].Name != "video" || work.Streams[0].Operations[0].Encode.ID != av.CodecVP9 {
		t.Fatalf("work streams = %+v, want video stream with VP9 operation facts", work.Streams)
	}
	if len(work.Branches) != 1 || work.Branches[0].Name != "preview" {
		t.Fatalf("work branches = %+v, want preview branch", work.Branches)
	}
	if got, want := workPlanOperationKindsForBranch(work.Operations, "preview"), []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpSink}; !reflect.DeepEqual(got, want) {
		t.Fatalf("work operations = %+v, want %+v", got, want)
	}
	if len(work.Destinations) != 1 || work.Destinations[0].Name != "frames" || len(work.Destinations[0].Branches) != 1 {
		t.Fatalf("work destinations = %+v, want frames destination bound to preview", work.Destinations)
	}
	if len(work.Edges) != 1 || work.Edges[0].Branch != "preview" {
		t.Fatalf("work edges = %+v, want preview edge", work.Edges)
	}

	work.Inputs[0].CodecSpec.ID = av.CodecAV1
	work.Streams[0].Operations[0].Encode.ID = av.CodecAV1
	work.Streams[0].Destinations[0] = "mutated"
	work.Branches[0].Operations[0] = "mutated"
	work.Operations[0].Destinations = append(work.Operations[0].Destinations, "mutated")
	work.Destinations[0].Branches[0] = "mutated"

	next := graph.workPlan()
	if next.Inputs[0].CodecSpec.ID != av.CodecVP8 {
		t.Fatal("graphPlan.workPlan() exposed input codec spec")
	}
	if next.Streams[0].Operations[0].Encode.ID != av.CodecVP9 {
		t.Fatal("graphPlan.workPlan() exposed stream operation slice")
	}
	if next.Streams[0].Destinations[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() exposed stream destination slice")
	}
	if next.Branches[0].Operations[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() exposed branch operation slice")
	}
	if len(next.Operations[0].Destinations) != 0 {
		t.Fatal("graphPlan.workPlan() exposed operation destination slice")
	}
	if next.Destinations[0].Branches[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() exposed destination branch slice")
	}
}

func TestBranchPacketTransformWithoutDecodeFails(t *testing.T) {
	_, err := From(FileInput("input.ogg", strings.NewReader(""))).
		UseRuntime(northStarTranscodeRuntime()).
		Audio().
		Copy().
		Branches(Branch("resampled").
			Resample(48_000, 2).
			To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))).
		Build(context.Background())
	if err == nil {
		t.Fatal("expected a packet branch with a transform (no decode) to be rejected")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "packet_branch_transform_unsupported" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("error = %v, want packet_branch_transform_unsupported BuildError", err)
	}
}

func TestStoredOperationListsMirrorFlowBranchAndDirectStreamWork(t *testing.T) {
	voice := Flow("voice").Audio().
		Resample(16_000, codec.Mono).
		Tap(FrameTap("audio.voice.frames")).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono))).
		Tap(PacketTap("audio.voice.packets"))

	flowSpec, err := chainSpecFrom(voice)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := operationSpecKindsForTest(flowSpec.operations), []plan.OperationKind{plan.OpTransform, plan.OpTap, plan.OpEncode, plan.OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("flow operations = %+v, want %+v", got, want)
	}
	flowOutputs := voice.OutputShapes(shape.Frame(
		av.MediaAudio,
		shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
	))
	if len(flowOutputs) != 1 || flowOutputs[0].Domain != shape.DomainPacket || flowOutputs[0].MediaKind != av.MediaAudio || flowOutputs[0].Codec != av.CodecOpus {
		t.Fatalf("flow output shapes = %+v, want Opus packets", flowOutputs)
	}
	flowTaps := voice.Taps()
	if len(flowTaps) != 2 ||
		flowTaps[0].Name() != "audio.voice.frames" ||
		flowTaps[0].Domain() != shape.DomainFrame ||
		flowTaps[1].Name() != "audio.voice.packets" ||
		flowTaps[1].Domain() != shape.DomainPacket {
		t.Fatalf("flow taps = %+v, want frame then packet taps", flowTaps)
	}

	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Apply(voice).
		To(Write("voice.ogg", io.Discard))
	if job.err != nil {
		t.Fatal(job.err)
	}
	if job.currentStream() == nil {
		t.Fatal("job stream is nil")
	}
	if got, want := operationSpecKindsForTest(job.currentStream().operations), []plan.OperationKind{plan.OpDecode, plan.OpTransform, plan.OpTap, plan.OpEncode, plan.OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("recipe chain stored operations = %+v, want %+v", got, want)
	}

	branch := Branch("archive").
		Apply(voice).
		To(Write("archive.ogg", io.Discard))
	if branch.err != nil {
		t.Fatal(branch.err)
	}
	if got, want := operationSpecKindsForTest(branch.operations), []plan.OperationKind{plan.OpTransform, plan.OpTap, plan.OpEncode, plan.OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("branch stored operations = %+v, want %+v", got, want)
	}
}

func TestPlannedBranchSplitOperationsInsertImplicitDecode(t *testing.T) {
	voice := Flow("voice").Audio().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))

	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Branches(Branch("voice").Apply(voice).To(Write("voice.ogg", io.Discard)))
	if job.err != nil {
		t.Fatal(job.err)
	}
	if len(job.branchStreams) != 1 {
		t.Fatalf("branch streams = %d, want 1", len(job.branchStreams))
	}
	stream := job.branchStreams[0]
	if len(stream.sharedOps) != 0 {
		t.Fatalf("shared operations = %+v, want none before normalized decode", stream.sharedOps)
	}
	if got, want := operationSpecKindsForTest(stream.privateOps), []plan.OperationKind{plan.OpTransform, plan.OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("private operations = %+v, want %+v", got, want)
	}
	if got, want := operationSpecKindsForTest(streamBuildOperationSpecs(stream)), []plan.OperationKind{plan.OpDecode, plan.OpTransform, plan.OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized operations = %+v, want %+v", got, want)
	}
}

func TestPlannedBranchSplitOperationsTreatParentCopyAsPacketAnchor(t *testing.T) {
	decodeFlow := Flow("voice").Audio().
		Decode().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(64_000)))

	decodeJob := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Branches(Branch("voice").Apply(decodeFlow).To(Write("voice.ogg", io.Discard)))
	if decodeJob.err != nil {
		t.Fatal(decodeJob.err)
	}
	if len(decodeJob.branchStreams) != 1 {
		t.Fatalf("decode branch streams = %d, want 1", len(decodeJob.branchStreams))
	}
	if got, want := operationSpecKindsForTest(streamBuildOperationSpecs(decodeJob.branchStreams[0])), []plan.OperationKind{plan.OpDecode, plan.OpTransform, plan.OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("decode branch operations = %+v, want %+v", got, want)
	}

	copyJob := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Tap(PacketTap("audio.packets")).
		Branches(Branch("packets").
			Tap(PacketTap("packets.branch")).
			To(Sink(SinkFunc("packets", func(context.Context, Message) error {
				return nil
			}))))
	if copyJob.err != nil {
		t.Fatal(copyJob.err)
	}
	if len(copyJob.branchStreams) != 1 {
		t.Fatalf("copy branch streams = %d, want 1", len(copyJob.branchStreams))
	}
	if got, want := operationSpecKindsForTest(streamBuildOperationSpecs(copyJob.branchStreams[0])), []plan.OperationKind{plan.OpCopy, plan.OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("copy branch operations = %+v, want %+v", got, want)
	}
	if tap := streamBuildOperationSpecs(copyJob.branchStreams[0])[1].Tap; tap.Name != "packets.branch" || tap.Domain != shape.DomainPacket {
		t.Fatalf("copy branch tap = %+v, want packet branch tap", tap)
	}
	copyPlan, err := planBranchCompositionRecipe(recipeIRFromIntent(copyJob.plan(), recipeir.KindBranchComposition), copyJob.inputs[0], copyJob.branchDestinations)
	if err != nil {
		t.Fatal(err)
	}
	if len(copyPlan.Branches) != 1 {
		t.Fatalf("copy plan branches = %d, want 1", len(copyPlan.Branches))
	}
	if got, want := operationSpecKindsForTest(copyPlan.Branches[0].Operations), []plan.OperationKind{plan.OpCopy, plan.OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("copy plan operations = %+v, want %+v", got, want)
	}
	if len(copyPlan.Branches[0].PrivateOperations) != 2 ||
		copyPlan.Branches[0].PrivateOperations[1].Tap.Name != "packets.branch" {
		t.Fatalf("copy private operations = %+v, want copy and packet tap", copyPlan.Branches[0].PrivateOperations)
	}
}

func TestPlannedBranchSplitOperationsRespectEarlierTapAnchors(t *testing.T) {
	thumbnail := Sink(SinkFunc("thumbnail", func(context.Context, Message) error {
		return nil
	}))
	web := Write("web.ivf", io.Discard)

	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Tap(FrameTap("video.decoded")).
		Resize(1280, 720).
		Tap(FrameTap("video.720p.frames")).
		Branches(
			Branch("raw-preview").
				From(FrameTap("video.decoded")).
				Resize(320, 180).
				To(thumbnail),
			Branch("web").
				From(FrameTap("video.720p.frames")).
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(web),
		)
	if job.err != nil {
		t.Fatal(job.err)
	}
	if len(job.branchStreams) != 2 {
		t.Fatalf("branch streams = %d, want 2", len(job.branchStreams))
	}

	rawOps := streamBuildOperationSpecs(job.branchStreams[0])
	if got, want := operationSpecKindsForTest(rawOps), []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpTransform}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw operations = %+v, want %+v", got, want)
	}
	if !rawOps[0].Shared || !rawOps[1].Shared || rawOps[2].Shared {
		t.Fatalf("raw operation sharing = %+v, want shared decode/tap and private resize", rawOps)
	}
	if rawOps[1].Tap.Name != "video.decoded" {
		t.Fatalf("raw operations = %+v, want anchor tap video.decoded", rawOps)
	}

	webOps := streamBuildOperationSpecs(job.branchStreams[1])
	if got, want := operationSpecKindsForTest(webOps), []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpTap, plan.OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("web operations = %+v, want %+v", got, want)
	}
	for i := 0; i < 4; i++ {
		if !webOps[i].Shared {
			t.Fatalf("web operations = %+v, want parent operation %d shared", webOps, i)
		}
	}
	if webOps[4].Shared {
		t.Fatalf("web operations = %+v, want private encode", webOps)
	}
	if webOps[3].Tap.Name != "video.720p.frames" {
		t.Fatalf("web operations = %+v, want anchor tap video.720p.frames", webOps)
	}

	gp, err := planBranchCompositionRecipe(recipeIRFromIntent(job.plan(), recipeir.KindBranchComposition), job.inputs[0], job.branchDestinations)
	if err != nil {
		t.Fatal(err)
	}
	if len(gp.Branches) != 2 {
		t.Fatalf("plan branches = %d, want 2", len(gp.Branches))
	}
	if len(gp.Branches[0].SharedOperations) != 2 ||
		len(gp.Branches[0].PrivateOperations) != 1 ||
		gp.Branches[0].PrivateOperations[0].Transform.resize == nil ||
		gp.Branches[0].PrivateOperations[0].Transform.resize.Width != 320 {
		t.Fatalf("raw plan branch = %+v, want private thumbnail resize from operation split", gp.Branches[0])
	}
	if len(gp.Branches[1].SharedOperations) != 4 ||
		gp.Branches[1].SharedOperations[2].Transform.resize == nil ||
		gp.Branches[1].SharedOperations[2].Transform.resize.Width != 1280 ||
		len(gp.Branches[1].PrivateOperations) != 1 {
		t.Fatalf("web plan branch = %+v, want shared 720p resize from operation split", gp.Branches[1])
	}

	if got, want := operationSpecKindsForTest(gp.Branches[0].Operations), []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpTransform}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw plan operations = %+v, want %+v", got, want)
	}
	if got, want := operationSpecKindsForTest(gp.Branches[1].Operations), []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpTap, plan.OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("web plan operations = %+v, want %+v", got, want)
	}

	routes, _, err := prepareBranchComposePlan(gp)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 {
		t.Fatalf("routes = %d, want 2", len(routes))
	}
	rawShared := branchComposeRouteStageOperations(routes[0].sharedOperations)
	rawPrivate := branchComposeRouteStageOperations(routes[0].privateOperations)
	if len(rawShared) != 0 ||
		len(rawPrivate) != 1 ||
		rawPrivate[0].Kind != plan.OpTransform ||
		rawPrivate[0].Transform.resize == nil ||
		rawPrivate[0].Transform.resize.Width != 320 {
		t.Fatalf("raw route = %+v, want private thumbnail resize from operation fields", routes[0])
	}
	webShared := branchComposeRouteStageOperations(routes[1].sharedOperations)
	webPrivate := branchComposeRouteStageOperations(routes[1].privateOperations)
	if len(webShared) != 1 ||
		webShared[0].Kind != plan.OpTransform ||
		webShared[0].Transform.resize == nil ||
		webShared[0].Transform.resize.Width != 1280 ||
		len(webPrivate) != 0 {
		t.Fatalf("web route = %+v, want shared 720p resize from operation fields", routes[1])
	}

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	state.intent = intent{}
	state.plan = gp
	work := buildWorkPlan(&state, pipeline.Spec{}, nil)
	if len(work.Branches) != 2 {
		t.Fatalf("work branches = %d, want 2", len(work.Branches))
	}
	if got, want := workPlanOperationKindsForBranch(work.Operations, "raw-preview"), []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpSink}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw work operations = %+v, want %+v", got, want)
	}
	if got, want := workPlanOperationKindsForBranch(work.Operations, "web"), []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpTap, plan.OpEncode, plan.OpMux}; !reflect.DeepEqual(got, want) {
		t.Fatalf("web work operations = %+v, want %+v", got, want)
	}
	webWorkOps := workPlanOperationsForBranch(work.Operations, "web")
	if !webWorkOps[4].Shared || !webWorkOps[5].Shared || webWorkOps[6].Shared {
		t.Fatalf("web work operation sharing = %+v, want shared parent transform/tap and private encode", webWorkOps)
	}
	if len(work.Destinations) != 2 {
		t.Fatalf("work destinations = %d, want 2", len(work.Destinations))
	}
	for i := range work.Branches {
		if len(work.Branches[i].Operations) == 0 {
			t.Fatalf("work branch %q has no operation IDs", work.Branches[i].Name)
		}
	}
}

func operationSpecKindsForTest(operations []operationSpec) []plan.OperationKind {
	out := make([]plan.OperationKind, 0, len(operations))
	for i := range operations {
		out = append(out, operations[i].Kind)
	}
	return out
}

func workPlanOperationsForBranch(operations []workOperation, branch string) []workOperation {
	out := make([]workOperation, 0)
	for i := range operations {
		if operations[i].Branch == branch {
			out = append(out, operations[i])
		}
	}
	return out
}

func workPlanOperationKindsForBranch(operations []workOperation, branch string) []plan.OperationKind {
	operations = workPlanOperationsForBranch(operations, branch)
	out := make([]plan.OperationKind, 0, len(operations))
	for i := range operations {
		out = append(out, operations[i].Kind)
	}
	return out
}

func TestRecipeCompileStateDoesNotCarryRecipeBuilders(t *testing.T) {
	stateType := reflect.TypeOf(recipeCompileState{})
	forbidden := map[reflect.Type]string{
		reflect.TypeOf((*Job)(nil)):            "*Job",
		reflect.TypeOf((*jobStreamBuild)(nil)): "*jobStreamBuild",
		reflect.TypeOf([]streamBuild(nil)):     "[]streamBuild",
		reflect.TypeOf((*builder)(nil)):        "*builder",
	}
	for i := 0; i < stateType.NumField(); i++ {
		field := stateType.Field(i)
		if name, ok := forbidden[field.Type]; ok {
			t.Fatalf("recipeCompileState field %s carries %s; compiler passes should use captured intent attachments", field.Name, name)
		}
		switch field.Name {
		case "inputs", "outputs", "jobOutputs", "streamOutputs":
			t.Fatalf("recipeCompileState field %s uses builder-shaped attachment naming", field.Name)
		}
	}
}

func TestJobConstructionErrorsAreJoined(t *testing.T) {
	_, err := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus()).
		Encode(codec.Opus()).
		To(Destination{}).
		Describe()
	if err == nil {
		t.Fatal("Describe() err = nil, want joined construction errors")
	}
	var first *BuildError
	if !errors.As(err, &first) || first.Code != encodeDuplicateCode {
		t.Fatalf("err = %v, want first BuildError code %s", err, encodeDuplicateCode)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("err = %T, want joined construction errors", err)
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
			t.Fatalf("joined codes = %v, want %s", codes, code)
		}
	}
}

func TestRecipeCompilePhaseSequencesArePinned(t *testing.T) {
	tests := []struct {
		name   string
		phases recipeCompilePhaseSet
		want   []string
	}{
		{
			name:   "job",
			phases: jobRecipeCompilePhases(),
			want: []string{
				"validate job recipe",
				"validate stream rules",
				"validate job intent shape",
				"validate recipe attachments",
				"validate job attachments",
				"validate job output bindings",
				"validate job stream output kinds",
				"validate packet job outputs",
				"validate job live stream selection",
				"validate job output format adapters",
				"validate job decode adapters",
				"validate job encode adapters",
				"validate job transform adapters",
				"validate job input format adapters",
				"validate job known input stream selection",
				"validate recipe operation shapes",
				"validate recipe destination shapes",
				"validate job known input decode adapters",
				"emit graph plan spec",
				"validate mux compatibility",
				"require graph plan spec",
				"validate recipe runtime",
			},
		},
		{
			name:   "join",
			phases: joinRecipeCompilePhases(),
			want: []string{
				"validate join recipe",
				"validate stream rules",
				"validate recipe attachments",
				"validate job input format adapters",
				"emit graph plan spec",
				"validate mux compatibility",
				"require graph plan spec",
				"validate recipe runtime",
			},
		},
		{
			name:   "branch composition",
			phases: branchCompositionRecipeCompilePhases(),
			want: []string{
				"validate transcode recipe",
				"validate stream rules",
				"validate transcode intent shape",
				"validate recipe attachments",
				"validate transcode attachments",
				"validate branch destination bindings",
				"validate branch destination kinds",
				"validate branch destination format adapters",
				"validate transcode encode adapters",
				"validate transcode transform adapters",
				"validate transcode input format adapters",
				"validate transcode known input stream selection",
				"validate recipe operation shapes",
				"validate recipe destination shapes",
				"validate transcode known input decode adapters",
				"plan branch composition intent",
				"emit graph plan spec",
				"validate mux compatibility",
				"require graph plan spec",
				"validate recipe runtime",
			},
		},
	}
	for _, tt := range tests {
		if got := recipeCompilePassNames(recipeCompilePhaseSequence(tt.phases)); !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("%s phase sequence = %#v, want %#v", tt.name, got, tt.want)
		}
	}
}

func recipeCompilePassNames(passes []recipeCompilePass) []string {
	names := make([]string, 0, len(passes))
	for _, pass := range passes {
		names = append(names, pass.Name())
	}
	return names
}

func TestRecipeAttachmentConsistencyRejectsMismatches(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		want  string
	}{
		{
			name: "job inputs",
			state: recipeCompileState{
				operation:         "build job",
				jobPresent:        true,
				intent:            intent{},
				recipe:            recipeir.Recipe{Kind: recipeir.KindJob, Inputs: []recipeir.Input{{Name: "input.ivf"}}},
				outputAttachments: []destinationSpec{fileDestination("recording.ivf", io.Discard)},
			},
			want: "inputs",
		},
		{
			name: "job outputs",
			state: recipeCompileState{
				operation:         "build job",
				jobPresent:        true,
				intent:            intent{},
				recipe:            recipeir.Recipe{Kind: recipeir.KindJob, Inputs: []recipeir.Input{{Name: "input.ivf"}}, Destinations: []recipeir.Destination{{Name: "recording.ivf"}}},
				inputAttachments:  []InputSpec{FileInput("input.ivf", strings.NewReader(""))},
				outputAttachments: nil,
			},
			want: "destinations",
		},
		{
			name: "branch destinations",
			state: recipeCompileState{
				operation:                    branchCompositionOperation,
				branchCompositionPresent:     true,
				intent:                       intent{},
				recipe:                       recipeir.Recipe{Kind: recipeir.KindBranchComposition, Inputs: []recipeir.Input{{Name: "input.ivf"}}, Destinations: []recipeir.Destination{{Name: "web.ivf"}}},
				branchInputAttachment:        FileInput("input.ivf", strings.NewReader("")),
				branchDestinationAttachments: nil,
			},
			want: "destinations",
		},
		{
			name: "job output mirror ignored",
			state: recipeCompileState{
				operation:  "build job",
				jobPresent: true,
				intent: intent{
					Inputs:       []inputIntent{{Name: "legacy-input"}},
					Destinations: []destinationIntent{},
				},
				recipe:            recipeir.Recipe{Kind: recipeir.KindJob, Inputs: []recipeir.Input{{Name: "input.ivf"}}, Destinations: []recipeir.Destination{{Name: "recording.ivf"}}},
				inputAttachments:  []InputSpec{FileInput("input.ivf", strings.NewReader(""))},
				outputAttachments: nil,
			},
			want: "destinations",
		},
	}
	pass := validateRecipeAttachmentConsistencyPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !strings.Contains(err.Error(), tt.want) ||
				!strings.Contains(err.Error(), "intent") ||
				!strings.Contains(err.Error(), "attached") ||
				!strings.Contains(err.Error(), "custom compiler passes") ||
				!strings.Contains(err.Error(), "goav.From") {
				t.Fatalf("err = %v, want attachment mismatch guidance", err)
			}
			if !errors.As(err, &buildErr) || buildErr.Code != recipeAttachmentMismatchCode || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want recipe_attachment_mismatch with matching BuildError code", err)
			}
		})
	}
}

func TestJobIntentShapePassRejectsInvalidPublicShape(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  errcode.Code
		want  string
	}{
		{
			name: "input missing",
			state: recipeCompileState{
				operation: "build job",
				intent: intent{
					Destinations: []destinationIntent{{Name: "recording.ivf"}},
				},
			},
			code: "input_missing",
			want: "no input is configured",
		},
		{
			name: "multi stream chain without destination",
			state: recipeCompileState{
				operation: "build job",
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{
						{Name: "audio", Operations: decodeIntentOperations(), Destinations: []string{"audio"}},
						{Name: "video", Operations: decodeIntentOperations()},
					},
					Destinations: []destinationIntent{{Name: "audio"}, {Name: "video"}},
				},
			},
			code: "output_missing",
			want: "stream chain has no destination",
		},
		{
			name: "mixed output scope",
			state: recipeCompileState{
				operation:      "build job",
				jobOutputCount: 1,
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{{
						Name:         "audio",
						Operations:   decodeIntentOperations(),
						Destinations: []string{"frames"},
					}},
					Destinations: []destinationIntent{{Name: "archive.ivf"}, {Name: "frames"}},
				},
			},
			code: "output_scope_mixed",
			want: "stream recipes use stream-local outputs",
		},
		{
			name: "operation spec missing",
			state: recipeCompileState{
				operation: "build job",
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{{
						Name:         "audio",
						Destinations: []string{"frames"},
					}},
					Destinations: []destinationIntent{{Name: "frames"}},
				},
			},
			code: "stream_operation_missing",
			want: "no decode, processing stage, or encoder was requested",
		},
	}
	pass := validateJobIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestJobOutputBindingsPassRejectsUndefinedStreamRoutes(t *testing.T) {
	stream := streamIntent{
		Name:         "audio",
		Operations:   decodeIntentOperations(),
		Destinations: []string{"missing"},
	}
	state := recipeCompileState{
		operation: "build job",
		intent:    intent{Streams: []streamIntent{stream}},
	}
	moveTestOutputsToRecipeIR(&state, recipeir.KindJob, testRecipeIRDestination("archive.ogg", recipeir.DestinationKindByteStream))

	err := validateJobOutputBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "stream route output missing is not attached") ||
		!strings.Contains(err.Error(), "selected stream chain") {
		t.Fatalf("err = %v, want stream output binding guidance", err)
	}
}

func TestOutputFormatAdapterPassesRejectMissingMuxers(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  string
	}{
		{
			name: "job probed format",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   testBundleRuntime(),
				outputAttachments: []destinationSpec{
					fileDestination("recording.mp4", io.Discard),
				},
			},
			want: `format "mp4"`,
		},
		{
			name: "job explicit format",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   testBundleRuntime(),
				outputAttachments: []destinationSpec{
					fileDestination("", io.Discard).withFormat(av.FormatOgg),
				},
			},
			want: `format "ogg"`,
		},
		{
			name: "transcode probed format",
			pass: validateBranchDestinationFormatAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   testBundleRuntime(),
				branchDestinationAttachments: []namedDestinationSpec{{
					name:   "web",
					output: fileDestination("web.mp4", io.Discard),
				}},
			},
			want: `format "mp4"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "destination_muxer_missing" || !errors.Is(err, format.ErrNotFound) {
				t.Fatalf("err = %v, want destination_muxer_missing wrapping format.ErrNotFound", err)
			}
			if buildErr.Operation != "open destination" {
				t.Fatalf("operation = %q, want open destination", buildErr.Operation)
			}
			if !strings.Contains(err.Error(), tt.want) ||
				!strings.Contains(err.Error(), "no muxer is registered") ||
				!strings.Contains(err.Error(), "WithFormatAdapter") {
				t.Fatalf("err = %v, want muxer guidance with %q", err, tt.want)
			}
		})
	}
}

func TestOutputFormatAdapterPassesStoreResolvedFormats(t *testing.T) {
	tests := []struct {
		name     string
		pass     recipeCompilePass
		state    recipeCompileState
		validate func(*testing.T, recipeCompileState)
	}{
		{
			name: "job probed output format",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: mustNew(withTestFormats(
					testFormatProber(remuxTestProber{}),
					testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
				)),
				outputAttachments: []destinationSpec{
					fileDestination("recording.ogg", io.Discard),
				},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.outputAttachments) != 1 ||
					state.outputAttachments[0].format != "" ||
					state.outputAttachments[0].resolvedFormat != av.FormatOgg {
					t.Fatalf("output attachments = %+v, want resolved Ogg format", state.outputAttachments)
				}
			},
		},
		{
			name: "transcode probed output format",
			pass: validateBranchDestinationFormatAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: mustNew(withTestFormats(
					testFormatProber(remuxTestProber{}),
					testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
				)),
				branchDestinationAttachments: []namedDestinationSpec{{
					name:   "web",
					output: fileDestination("web.ogg", io.Discard),
				}},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.branchDestinationAttachments) != 1 ||
					state.branchDestinationAttachments[0].output.format != "" ||
					state.branchDestinationAttachments[0].output.resolvedFormat != av.FormatOgg ||
					state.branchDestinationAttachments[0].output.output.Name != "web.ogg" {
					t.Fatalf("branch destination attachments = %+v, want resolved Ogg format", state.branchDestinationAttachments)
				}
			},
		},
		{
			name: "explicit output format stays explicit",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: mustNew(withTestFormats(
					testFormatMuxer(av.FormatIVF, &remuxTestMuxerFactory{}),
				)),
				outputAttachments: []destinationSpec{
					fileDestination("recording.media", io.Discard).withFormat(av.FormatIVF),
				},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.outputAttachments) != 1 || state.outputAttachments[0].format != av.FormatIVF {
					t.Fatalf("output attachments = %+v, want explicit IVF format preserved", state.outputAttachments)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.pass.Apply(&tt.state); err != nil {
				t.Fatalf("err = %v, want resolved output formats", err)
			}
			tt.validate(t, tt.state)
		})
	}
}

func TestResolvedJobOutputFormatsEnterMediaPlanBuild(t *testing.T) {
	runtime := mustNew(append(
		testBundleOptions(),
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
	)...)
	job := From(
		Input(&liveTestProvider{
			name:    "audio",
			media:   av.MediaAudio,
			codecID: av.CodecOpus,
			streams: []Stream{{
				ID:   "audio",
				Type: av.MediaAudio,
				Codec: av.CodecParameters{
					ID:   av.CodecOpus,
					Type: av.MediaAudio,
				},
			}},
		}),
	).Copy().To(destinationHandle(fileDestination("recording.ogg", io.Discard))).UseRuntime(runtime)

	resolved, err := compileJobRecipeForBuild(job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuild() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	if len(resolved.outputAttachments) != 1 {
		t.Fatalf("resolved output attachments = %d, want 1", len(resolved.outputAttachments))
	}
	if got := destinationOpenFormat(resolved.outputAttachments[0]); got != av.FormatOgg {
		t.Fatalf("open output format = %q, want resolved Ogg format", got)
	}
	if got := destinationGraphFormat(resolved.outputAttachments[0]); got != "" {
		t.Fatalf("graph detail output format = %q, want inferred format hidden from graph detail", got)
	}
	task, err := resolved.Build(context.Background())
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	spec := task.Describe()
	if len(spec.Nodes) != 2 || spec.Nodes[1].Name != "recording.ogg" || spec.Nodes[1].Detail != "mux, protocol=file" {
		t.Fatalf("built spec = %+v, want inferred format hidden from mux detail", spec)
	}
}

func TestResolvedTranscodeOutputFormatsEnterPlan(t *testing.T) {
	runtime := mustNew(withTestFormats(
		testFormatProber(remuxTestProber{}),
		testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
	))
	job := From(FileInput("input.ivf", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(Branch("archive").Encode(codec.Opus(codec.Bitrate(96_000))).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))
	state := recipeCompileStateFromSnapshot(
		newJobRecipeSnapshot(job),
		recipeCompileOptions{preflightOutputAdapters: true},
	)
	state.intent.Streams = nil
	state.intent.Destinations = nil
	state.intent.Inputs = nil

	if err := validateBranchDestinationFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchDestinationFormatAdaptersPass() error = %v", err)
	}
	if err := planBranchCompositionIntentPass().Apply(&state); err != nil {
		t.Fatalf("planBranchCompositionIntentPass() error = %v", err)
	}
	if len(state.plan.Destinations) != 1 ||
		state.plan.Destinations[0].Format != "" ||
		state.plan.Destinations[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("plan destinations = %+v, want resolved Ogg open format without graph detail format", state.plan.Destinations)
	}
}

func TestResolvedBranchRecipeOutputFormatsRefreshPreplannedDestinations(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(Branch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(context.Background(), job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if len(resolved.plan.Destinations) != 1 ||
		resolved.plan.Destinations[0].Format != "" ||
		resolved.plan.Destinations[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("resolved plan destinations = %+v, want resolved Ogg open format", resolved.plan.Destinations)
	}
}

func TestInputFormatAdapterPassesRejectMissingDemuxers(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  errcode.Code
		want  []string
	}{
		{
			name: "job probed format",
			pass: validateJobInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightInputAdapters: true},
				runtime:   testBundleRuntime(),
				inputAttachments: []InputSpec{
					FileInput("input.ogg", strings.NewReader("")),
				},
			},
			code: "input_demuxer_missing",
			want: []string{`format "ogg"`, "no demuxer is registered", "WithFormatAdapter"},
		},
		{
			name: "transcode probed format",
			pass: validateBranchInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation:             branchCompositionOperation,
				options:               recipeCompileOptions{preflightInputAdapters: true},
				runtime:               testBundleRuntime(),
				branchInputAttachment: FileInput("input.flv", strings.NewReader("")),
			},
			code: "input_demuxer_missing",
			want: []string{`format "flv"`, "no demuxer is registered", "WithFormatAdapter"},
		},
		{
			name: "unknown input format",
			pass: validateJobInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightInputAdapters: true},
				runtime:   testBundleRuntime(),
				inputAttachments: []InputSpec{
					FileInput("input.unknown", strings.NewReader("")),
				},
			},
			code: "input_format_unknown",
			want: []string{"could not be detected", "name=input.unknown", "goav.Input"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, format.ErrNotFound) {
				t.Fatalf("err = %v, want %s wrapping format.ErrNotFound", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestInputFormatAdapterPassSkipsLiveReceiveInputs(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightInputAdapters: true},
		runtime:   testBundleRuntime(),
		inputAttachments: []InputSpec{
			Input(&liveTestProvider{media: av.MediaAudio, codecID: av.CodecOpus}),
		},
	}
	if err := validateJobInputFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want RTP input format preflight skipped", err)
	}
}

func TestInputFormatAdapterPassStoresProbeStreams(t *testing.T) {
	streams := []av.Stream{{
		Index: 0,
		ID:    "eng",
		Name:  "English",
		Type:  av.MediaAudio,
		Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
	}}
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightInputAdapters: true},
		runtime: mustNew(withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{}),
		)),
		inputAttachments: []InputSpec{
			FileInput("input.ogg", strings.NewReader("")),
		},
	}

	if err := validateJobInputFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want input probe stored", err)
	}
	if len(state.inputProbes) != 1 || len(state.inputProbes[0].Streams) != 1 {
		t.Fatalf("input probes = %+v, want one probed stream", state.inputProbes)
	}
	if got := state.inputProbes[0].Streams[0]; got.ID != "eng" || got.Codec.ID != av.CodecOpus {
		t.Fatalf("probed stream = %+v, want English Opus stream", got)
	}
}

func TestKnownInputStreamSelectionPassRejectsProbedAmbiguousAndMissingStreams(t *testing.T) {
	streams := []av.Stream{
		{
			Index: 0,
			ID:    "eng",
			Name:  "English",
			Type:  av.MediaAudio,
			Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
		},
		{
			Index: 1,
			ID:    "spa",
			Name:  "Spanish",
			Type:  av.MediaAudio,
			Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
		},
	}
	tests := []struct {
		name   string
		stream streamIntent
		code   errcode.Code
		want   []string
	}{
		{
			name: "ambiguous probed audio",
			stream: streamIntent{
				Name:       "audio",
				Select:     plan.StreamSelect{Type: av.MediaAudio},
				Operations: decodeIntentOperations(),
			},
			code: "stream_ambiguous",
			want: []string{"multiple streams match type=audio", "id=eng", "id=spa", `.Audio(goav.StreamID("eng"))`, ".Audio(goav.StreamIndex(0))"},
		},
		{
			name: "missing probed video",
			stream: streamIntent{
				Name:       "video",
				Select:     plan.StreamSelect{Type: av.MediaVideo},
				Operations: decodeIntentOperations(),
			},
			code: "stream_missing",
			want: []string{"no stream matches type=video", "audio[0]", "id=eng", "codec=opus"},
		},
	}
	pass := validateJobKnownInputStreamSelectionPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: intent{Streams: []streamIntent{
					tt.stream,
				}},
				inputProbes: []format.ProbeResult{{
					Format:  av.FormatOgg,
					Streams: streams,
				}},
			}
			moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestLiveStreamSelectionPassRejectsAmbiguousAndMissingStreams(t *testing.T) {
	tests := []struct {
		name   string
		intent intent
		code   errcode.Code
		want   []string
	}{
		{
			name: "ambiguous live video",
			intent: intent{
				Inputs: []inputIntent{
					{Name: "front", Protocol: av.ProtocolRTP, Codec: codec.VP8(), Realtime: true},
					{Name: "screen", Protocol: av.ProtocolRTP, Codec: codec.VP8(), Realtime: true},
				},
				Streams: []streamIntent{{
					Name:       "video",
					Select:     plan.StreamSelect{Type: av.MediaVideo},
					Operations: decodeIntentOperations(),
				}},
			},
			code: "stream_ambiguous",
			want: []string{"multiple streams match type=video", "id=front", "id=screen", `.Video(goav.StreamID("front"))`, ".Video(goav.StreamIndex(0))"},
		},
		{
			name: "missing live audio",
			intent: intent{
				Inputs: []inputIntent{
					{Name: "camera", Protocol: av.ProtocolRTP, Codec: codec.VP8(), Realtime: true},
				},
				Streams: []streamIntent{{
					Name:       "audio",
					Select:     plan.StreamSelect{Type: av.MediaAudio},
					Operations: decodeIntentOperations(),
				}},
			},
			code: "stream_missing",
			want: []string{"no stream matches type=audio", "video[0]", "id=camera", "codec=vp8"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightLiveStreams: true},
				intent:    tt.intent,
			}
			moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			err := validateJobLiveStreamSelectionPass().Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestLiveStreamSelectionPassSkipsPacketOnlyJobs(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightLiveStreams: true},
		intent: intent{Inputs: []inputIntent{{
			Name:     "video",
			Protocol: av.ProtocolRTP,
			Codec:    codec.VP8(),
			Realtime: true,
		}}},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindJob)
	if err := validateJobLiveStreamSelectionPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want packet-only record recipe skipped", err)
	}
}

func TestDecodeAdapterPassRejectsKnownLiveMissingDecoders(t *testing.T) {
	descriptorOnly := codec.NewRegistry()
	descriptorOnly.RegisterDescriptor(codec.Descriptor{
		ID:    av.CodecH264,
		Name:  "h264",
		Modes: []codec.Mode{codec.ModeDecode},
		Capabilities: codec.Capabilities{
			BuildTags: []string{"goav_goh264"},
		},
		Backend: codec.Backend{
			Name:   "goh264",
			Status: "planned-build-tagged",
		},
	})
	descriptorRuntime := mustNew(func(config *Config) error {
		config.Codecs = descriptorOnly
		return nil
	})

	tests := []struct {
		name  string
		state recipeCompileState
		code  errcode.Code
		cause error
		want  []string
	}{
		{
			name: "missing decoder",
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   mustNew(),
				intent: intent{
					Inputs: []inputIntent{{
						Name:     "audio",
						Protocol: av.ProtocolRTP,
						Codec:    codec.Opus(),
						Realtime: true,
					}},
					Streams: []streamIntent{{
						Name:       "audio",
						Select:     plan.StreamSelect{Type: av.MediaAudio},
						Operations: decodeIntentOperations(),
					}},
				},
			},
			code:  "decode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no decoder adapter", "codec=opus", "goav.From"},
		},
		{
			name: "descriptor-only decoder",
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   descriptorRuntime,
				intent: intent{
					Inputs: []inputIntent{{
						Name:     "video",
						Protocol: av.ProtocolRTP,
						Codec:    codec.H264(),
						Realtime: true,
					}},
					Streams: []streamIntent{{
						Name:       "video",
						Select:     plan.StreamSelect{Type: av.MediaVideo},
						Operations: decodeIntentOperations(),
					}},
				},
			},
			code:  "decode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=h264", "backend=goh264", "build_tags=goav_goh264"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			err := validateJobDecodeAdaptersPass().Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestDecodeAdapterPassDefersAmbiguousLiveSelection(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightDecodeAdapters: true},
		runtime:   mustNew(),
		intent: intent{
			Inputs: []inputIntent{
				{Name: "front", Protocol: av.ProtocolRTP, Codec: codec.H264(), Realtime: true},
				{Name: "screen", Protocol: av.ProtocolRTP, Codec: codec.H264(), Realtime: true},
			},
			Streams: []streamIntent{{
				Name:       "video",
				Select:     plan.StreamSelect{Type: av.MediaVideo},
				Operations: decodeIntentOperations(),
			}},
		},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindJob)
	if err := validateJobDecodeAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want ambiguity to stay with stream resolution", err)
	}
}

func TestKnownInputDecodeAdapterPassesRejectMissingDecoders(t *testing.T) {
	descriptorOnly := codec.NewRegistry()
	descriptorOnly.RegisterDescriptor(codec.Descriptor{
		ID:    av.CodecH264,
		Name:  "h264",
		Modes: []codec.Mode{codec.ModeDecode},
		Capabilities: codec.Capabilities{
			BuildTags: []string{"goav_goh264"},
		},
		Backend: codec.Backend{
			Name:   "goh264",
			Status: "planned-build-tagged",
		},
	})
	descriptorRuntime := mustNew(func(config *Config) error {
		config.Codecs = descriptorOnly
		return nil
	})

	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  errcode.Code
		cause error
		want  []string
	}{
		{
			name: "job probed decoder",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   mustNew(),
				intent: intent{Streams: []streamIntent{{
					Name:       "audio",
					Select:     plan.StreamSelect{Type: av.MediaAudio},
					Operations: decodeIntentOperations(),
				}}},
				inputProbes: []format.ProbeResult{{
					Format: av.FormatOgg,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "audio",
						Type:  av.MediaAudio,
						Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio},
					}},
				}},
			},
			code:  "decode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no decoder adapter", "codec=opus", "goav.From"},
		},
		{
			name: "job probed descriptor-only decoder",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   descriptorRuntime,
				intent: intent{Streams: []streamIntent{{
					Name:       "video",
					Select:     plan.StreamSelect{Type: av.MediaVideo},
					Operations: decodeIntentOperations(),
				}}},
				inputProbes: []format.ProbeResult{{
					Format: av.FormatMatroska,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "video",
						Type:  av.MediaVideo,
						Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo},
					}},
				}},
			},
			code:  "decode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=h264", "backend=goh264", "build_tags=goav_goh264"},
		},
		{
			name: "transcode probed decoder",
			pass: validateKnownBranchInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   mustNew(),
				intent: intent{Streams: []streamIntent{{
					Name:         "360p",
					Select:       plan.StreamSelect{Type: av.MediaVideo},
					Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
					Destinations: []string{"web"},
				}}},
				branchInputProbeReady: true,
				branchInputProbe: format.ProbeResult{
					Format: av.FormatMatroska,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "video",
						Type:  av.MediaVideo,
						Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo},
					}},
				},
			},
			code:  "decode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no decoder adapter", "codec=vp9", "goav.From"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			kind := recipeir.KindJob
			if state.operation == branchCompositionOperation {
				kind = recipeir.KindBranchComposition
			}
			moveTestIntentToRecipeIR(&state, kind)
			err := tt.pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestKnownInputDecodeAdapterPassDefersAmbiguousSelection(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightDecodeAdapters: true},
		runtime:   mustNew(),
		intent: intent{Streams: []streamIntent{{
			Name:       "audio",
			Select:     plan.StreamSelect{Type: av.MediaAudio},
			Operations: decodeIntentOperations(),
		}}},
		inputProbes: []format.ProbeResult{{
			Format: av.FormatOgg,
			Streams: []av.Stream{
				{Index: 0, ID: "eng", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
				{Index: 1, ID: "spa", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			},
		}},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindJob)
	if err := validateJobKnownInputDecodeAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("err = %v, want ambiguity to stay with stream resolution", err)
	}
}

func TestDecodeAdapterPassesRejectIncompatibleDescriptors(t *testing.T) {
	audioCodec := av.CodecID("x_audio")
	videoCodec := av.CodecID("x_video")
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job decoder advertises video for audio live stream",
			pass: validateJobDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime: mustNew(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaVideo,
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: intent{
					Inputs: []inputIntent{{
						Name:     "audio",
						Protocol: av.ProtocolRTP,
						Codec:    codec.Codec(audioCodec, av.MediaAudio),
						Realtime: true,
					}},
					Streams: []streamIntent{{
						Name:       "audio",
						Select:     plan.StreamSelect{Type: av.MediaAudio},
						Operations: decodeIntentOperations(),
					}},
				},
			},
			want: []string{"decoder adapter does not support the requested media", "codec=x_audio", "field=media", "requested=audio", "supported=video"},
		},
		{
			name: "job decoder rejects probed sample format",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime: mustNew(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaAudio,
					Capabilities: codec.Capabilities{
						SampleFormats: []string{av.SampleFormatS16},
					},
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: intent{Streams: []streamIntent{{
					Name:       "audio",
					Select:     plan.StreamSelect{Type: av.MediaAudio},
					Operations: decodeIntentOperations(),
				}}},
				inputProbes: []format.ProbeResult{{
					Format: av.FormatOgg,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "audio",
						Type:  av.MediaAudio,
						Codec: av.CodecParameters{
							ID:           audioCodec,
							Type:         av.MediaAudio,
							SampleFormat: av.SampleFormatF32,
						},
					}},
				}},
			},
			want: []string{"decoder adapter does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
		{
			name: "branch decoder rejects probed pixel format",
			pass: validateKnownBranchInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime: mustNew(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   videoCodec,
					Type: av.MediaVideo,
					Capabilities: codec.Capabilities{
						PixelFormats: []string{av.PixelFormatI420},
					},
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: intent{Streams: []streamIntent{{
					Name:         "preview",
					Select:       plan.StreamSelect{Type: av.MediaVideo},
					Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
					Destinations: []string{"web"},
				}}},
				branchInputProbeReady: true,
				branchInputProbe: format.ProbeResult{
					Format: av.FormatMatroska,
					Streams: []av.Stream{{
						Index: 0,
						ID:    "video",
						Type:  av.MediaVideo,
						Codec: av.CodecParameters{
							ID:          videoCodec,
							Type:        av.MediaVideo,
							PixelFormat: av.PixelFormatYUV420P,
						},
					}},
				},
			},
			want: []string{"decoder adapter does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			kind := recipeir.KindJob
			if state.operation == branchCompositionOperation {
				kind = recipeir.KindBranchComposition
			}
			moveTestIntentToRecipeIR(&state, kind)
			err := tt.pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_incompatible" || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want decode_adapter_incompatible with matching BuildError code", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestEncodeAdapterPassesRejectMissingEncoders(t *testing.T) {
	descriptorOnly := codec.NewRegistry()
	descriptorOnly.RegisterDescriptor(codec.Descriptor{
		ID:    av.CodecVP9,
		Name:  "vp9",
		Modes: []codec.Mode{codec.ModeEncode},
		Backend: codec.Backend{
			Name:   "govpx",
			Status: "descriptor-only",
		},
	})
	descriptorRuntime := mustNew(func(config *Config) error {
		config.Codecs = descriptorOnly
		return nil
	})

	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  errcode.Code
		cause error
		want  []string
	}{
		{
			name: "job missing encoder",
			pass: validateJobEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime:   mustNew(),
				intent: intent{Streams: []streamIntent{{
					Name:       "audio",
					Operations: encodeIntentOperations(codec.Opus(codec.Bitrate(96_000))),
				}}},
			},
			code:  "encode_adapter_missing",
			cause: codec.ErrNotFound,
			want:  []string{"no encoder adapter", "codec=opus", "Sink"},
		},
		{
			name: "transcode descriptor-only encoder",
			pass: validateBranchEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime:   descriptorRuntime,
				intent: intent{Streams: []streamIntent{{
					Name:       "360p",
					Operations: encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
				}}},
			},
			code:  "encode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=vp9", "backend=govpx"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.pass.Name() {
			case "validate job encode adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindJob)
			case "validate transcode encode adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindBranchComposition)
			}
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("err = %v, want %s wrapping %v", err, tt.code, tt.cause)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestEncodeAdapterPassesRejectIncompatibleDescriptors(t *testing.T) {
	audioCodec := av.CodecID("x_audio")
	videoCodec := av.CodecID("x_video")
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job encoder advertises video for audio stream",
			pass: validateJobEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime: mustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaVideo,
				}, &encodeTestEncoderFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:       "audio",
					Select:     plan.StreamSelect{Type: av.MediaAudio},
					Operations: encodeIntentOperations(codec.Codec(audioCodec, av.MediaAudio)),
				}}},
			},
			want: []string{"encoder adapter does not support the requested media", "codec=x_audio", "field=media", "requested=audio", "supported=video"},
		},
		{
			name: "branch encoder rejects sample format",
			pass: validateBranchEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime: mustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaAudio,
					Capabilities: codec.Capabilities{
						SampleFormats: []string{av.SampleFormatS16},
					},
				}, &encodeTestEncoderFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:   "voice",
					Select: plan.StreamSelect{Type: av.MediaAudio},
					Operations: encodeIntentOperations(codec.CodecSpec{ID: audioCodec, Type: av.MediaAudio, Parameters: av.CodecParameters{
						ID:           audioCodec,
						Type:         av.MediaAudio,
						SampleFormat: av.SampleFormatF32,
					}}),
				}}},
			},
			want: []string{"encoder adapter does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
		{
			name: "branch encoder rejects transformed pixel format",
			pass: validateBranchEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime: mustNew(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   videoCodec,
					Type: av.MediaVideo,
					Capabilities: codec.Capabilities{
						PixelFormats: []string{av.PixelFormatI420},
					},
				}, &encodeTestEncoderFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:   "preview",
					Select: plan.StreamSelect{Type: av.MediaVideo},
					Operations: append([]operationSpec{operationSpecForTransform(transformSpec{
						resize: &filter.ResizeConfig{
							Width:       640,
							Height:      360,
							PixelFormat: av.PixelFormatYUV420P,
						},
					})}, operationSpecForEncode(codec.Codec(videoCodec, av.MediaVideo))),
				}}},
			},
			want: []string{"encoder adapter does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.pass.Name() {
			case "validate job encode adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindJob)
			case "validate transcode encode adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindBranchComposition)
			}
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_incompatible" || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want encode_adapter_incompatible with matching BuildError code", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestTransformAdapterPassesRejectMissingFilters(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job missing resample filter",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime:   mustNew(),
				intent: intent{Streams: []streamIntent{{
					Name:       "audio",
					Select:     plan.StreamSelect{Type: av.MediaAudio},
					Operations: []operationSpec{operationSpecForTransform(Resample(16_000, codec.Mono))},
				}}},
			},
			want: []string{"no resample filter adapter", "transform=resample", "bundle.MustNewFilters", ".Resample"},
		},
		{
			name: "transcode missing resize filter",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime:   mustNew(),
				intent: intent{Streams: []streamIntent{{
					Name:       "720p",
					Select:     plan.StreamSelect{Type: av.MediaVideo},
					Operations: []operationSpec{operationSpecForTransform(Resize(1280, 720))},
				}}},
			},
			want: []string{"no resize filter adapter", "transform=resize", "bundle.MustNewFilters", ".Resize"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.pass.Name() {
			case "validate job transform adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindJob)
			case "validate transcode transform adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindBranchComposition)
			}
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_missing" || !errors.Is(err, filter.ErrNotFound) {
				t.Fatalf("err = %v, want transform_adapter_missing wrapping filter.ErrNotFound", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestTransformAdapterPassesRejectIncompatibleDescriptors(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		want  []string
	}{
		{
			name: "job resample filter advertises video",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:   filter.FactoryResample,
					Input:  av.MediaVideo,
					Output: av.MediaVideo,
				}, &transcodeTestFilterFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:       "audio",
					Select:     plan.StreamSelect{Type: av.MediaAudio},
					Operations: []operationSpec{operationSpecForTransform(Resample(16_000, codec.Mono))},
				}}},
			},
			want: []string{"resample filter adapter declares incompatible media", "expected_input=audio", "actual_input=video", "Audio().Resample"},
		},
		{
			name: "branch resize filter advertises audio",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:   filter.FactoryResize,
					Input:  av.MediaAudio,
					Output: av.MediaAudio,
				}, &transcodeTestFilterFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:       "720p",
					Select:     plan.StreamSelect{Type: av.MediaVideo},
					Operations: []operationSpec{operationSpecForTransform(Resize(1280, 720))},
				}}},
			},
			want: []string{"resize filter adapter declares incompatible media", "expected_input=video", "actual_input=audio", "Video().Resize"},
		},
		{
			name: "job resize mode unsupported by descriptor",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:        filter.FactoryResize,
					Input:       av.MediaVideo,
					Output:      av.MediaVideo,
					ResizeModes: []filter.ResizeMode{filter.ResizeFill},
				}, &transcodeTestFilterFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:       "video",
					Select:     plan.StreamSelect{Type: av.MediaVideo},
					Operations: []operationSpec{operationSpecForTransform(Resize(1280, 720))},
				}}},
			},
			want: []string{"does not support the requested resize mode", "field=resize_mode", "requested=exact", "supported=fill"},
		},
		{
			name: "branch resize pixel format unsupported by descriptor",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:         filter.FactoryResize,
					Input:        av.MediaVideo,
					Output:       av.MediaVideo,
					PixelFormats: []string{av.PixelFormatI420},
					ResizeModes:  []filter.ResizeMode{filter.ResizeFit},
				}, &transcodeTestFilterFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:   "preview",
					Select: plan.StreamSelect{Type: av.MediaVideo},
					Operations: []operationSpec{operationSpecForTransform(transformSpec{
						resize: &filter.ResizeConfig{
							Width:       640,
							Height:      360,
							Mode:        filter.ResizeFit,
							PixelFormat: av.PixelFormatYUV420P,
						},
					})},
				}}},
			},
			want: []string{"does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
		{
			name: "job resample sample format unsupported by descriptor",
			pass: validateJobTransformAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime: mustNew(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:          filter.FactoryResample,
					Input:         av.MediaAudio,
					Output:        av.MediaAudio,
					SampleFormats: []string{av.SampleFormatS16},
				}, &transcodeTestFilterFactory{}))),
				intent: intent{Streams: []streamIntent{{
					Name:   "audio",
					Select: plan.StreamSelect{Type: av.MediaAudio},
					Operations: []operationSpec{operationSpecForTransform(transformSpec{
						resample: &filter.ResampleConfig{
							SampleRate:   16_000,
							Channels:     codec.Mono,
							SampleFormat: av.SampleFormatF32,
						},
					})},
				}}},
			},
			want: []string{"does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.pass.Name() {
			case "validate job transform adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindJob)
			case "validate transcode transform adapters":
				moveTestStreamsToRecipeIR(&tt.state, recipeir.KindBranchComposition)
			}
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want transform_adapter_incompatible with matching BuildError code", err)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestJobStreamOutputKindsPassRejectsInvalidOutputShapes(t *testing.T) {
	frameSink := sinkDestination(SinkFunc("frames", func(context.Context, Message) error { return nil }))
	fileOutput := fileDestination("archive.ogg", io.Discard)
	tests := []struct {
		name    string
		stream  streamIntent
		outputs []destinationSpec
		code    errcode.Code
		want    []string
	}{
		{
			name: "mixed frame and mux outputs",
			stream: streamIntent{
				Name:         "audio",
				Operations:   decodeIntentOperations(),
				Destinations: []string{"frames", "archive.ogg"},
			},
			outputs: []destinationSpec{frameSink, fileOutput},
			code:    "output_kind_mixed",
			want:    []string{"cannot mix sinks and muxed outputs", ".Branches(...)"},
		},
		{
			name: "mux output without encoder",
			stream: streamIntent{
				Name:         "audio",
				Operations:   decodeIntentOperations(),
				Destinations: []string{"archive.ogg"},
			},
			outputs: []destinationSpec{fileOutput},
			code:    "encode_missing",
			want:    []string{"decoded frames cannot be written", "expected_shape=domain=packet", "actual_shape=domain=frame", ".Opus"},
		},
	}
	pass := validateJobStreamOutputKindsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent:    intent{Streams: []streamIntent{tt.stream}},
			}
			destinations := make([]recipeir.Destination, 0, len(tt.outputs))
			for i := range tt.outputs {
				name := tt.outputs[i].label(fmt.Sprintf("output-%d", i))
				destinations = append(destinations, testRecipeIRDestination(name, recipeIRDestinationKindFromSpec(tt.outputs[i])))
			}
			moveTestOutputsToRecipeIR(&state, recipeir.KindJob, destinations...)
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestJobStreamOutputKindsPassAllowsEncodedPacketFanout(t *testing.T) {
	stream := streamIntent{
		Name:         "audio",
		Operations:   decodeEncodeIntentOperations(codec.Opus(codec.Bitrate(96_000))),
		Destinations: []string{"packets", "archive.ogg"},
	}
	state := recipeCompileState{
		operation: "build job",
		intent:    intent{Streams: []streamIntent{stream}},
	}
	moveTestOutputsToRecipeIR(
		&state,
		recipeir.KindJob,
		testRecipeIRDestination("packets", recipeir.DestinationKindSink),
		testRecipeIRDestination("archive.ogg", recipeir.DestinationKindByteStream),
	)
	if err := validateJobStreamOutputKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateJobStreamOutputKindsPass() error = %v", err)
	}
}

func TestShapeErrorsReportExpectedAndActualShape(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  errcode.Code
		want  []string
	}{
		{
			name: "job resize on audio",
			pass: validateJobIntentShapePass(),
			state: recipeCompileState{
				operation: "build job",
				intent: intent{
					Inputs: []inputIntent{{Name: "input"}},
					Streams: []streamIntent{{
						Name:         "audio",
						Select:       plan.StreamSelect{Type: av.MediaAudio},
						Operations:   append(decodeIntentOperations(), operationSpecForTransform(Resize(320, 180))),
						Destinations: []string{"frames"},
					}},
					Destinations: []destinationIntent{{Name: "frames"}},
				},
			},
			code: "transform_media_mismatch",
			want: []string{"resize applies to video streams", "expected_shape=domain=frame media=video", "actual_shape=domain=frame media=audio"},
		},
		{
			name: "branch resample on video",
			pass: validateRecipeOperationShapesPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Inputs: []inputIntent{{Name: "input"}},
					Streams: []streamIntent{{
						Name:   "video",
						Select: plan.StreamSelect{Type: av.MediaVideo},
						Operations: append(
							append(decodeIntentOperations(), operationSpecForTransform(Resample(48_000, codec.Stereo))),
							operationSpecForEncode(codec.VP9(codec.Bitrate(2_000_000))),
						),
						Destinations: []string{"web"},
					}},
					Destinations: []destinationIntent{{Name: "web"}},
				},
			},
			code: "operation_shape_mismatch",
			want: []string{"resample cannot consume the current media shape", "expected_shape=domain=frame media=audio", "actual_shape=domain=frame media=video"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			if tt.name == "job resize on audio" {
				moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			} else {
				moveTestIntentToRecipeIR(&state, recipeir.KindBranchComposition)
			}
			err := tt.pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestRecipeOperationShapePassRejectsInvalidOrderedOperations(t *testing.T) {
	tests := []struct {
		name   string
		stream streamIntent
		code   errcode.Code
		want   []string
	}{
		{
			name: "encode after packet annotation",
			stream: streamIntent{
				Name:   "video",
				Select: plan.StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []operationSpec{
					{Kind: plan.OpDecode, Decode: codec.VP8()},
					{Kind: plan.OpShape, Component: "shape", Shape: shape.New(shape.Domain(shape.DomainPacket), shape.Media(av.MediaVideo))},
					{Kind: plan.OpEncode, Component: string(av.CodecVP9), Encode: codec.VP9(codec.Bitrate(2_000_000))},
				},
				Destinations: []string{"web"},
			},
			code: "operation_shape_mismatch",
			want: []string{
				"shape annotation cannot change packet/frame domain",
				"operation_index=1",
				"expected_shape=domain=frame media=video",
				"actual_shape=domain=packet media=video",
				"keep .Shape(...) annotations in the current packet/frame domain",
			},
		},
		{
			name: "copy after frame annotation",
			stream: streamIntent{
				Name:   "audio",
				Select: plan.StreamSelect{Type: av.MediaAudio, Codec: av.CodecOpus},
				Operations: []operationSpec{
					{Kind: plan.OpShape, Component: "shape", Shape: shape.Frame(av.MediaAudio)},
					{Kind: plan.OpCopy, Component: "packet-copy", Encode: codec.Copy()},
				},
				Destinations: []string{"packets"},
			},
			code: "operation_shape_mismatch",
			want: []string{
				"shape annotation cannot change packet/frame domain",
				"operation_index=0",
				"expected_shape=domain=packet media=audio",
				"actual_shape=domain=frame media=audio",
				"keep .Shape(...) annotations in the current packet/frame domain",
			},
		},
		{
			name: "resize after audio annotation",
			stream: streamIntent{
				Name:   "video",
				Select: plan.StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []operationSpec{
					{Kind: plan.OpDecode, Decode: codec.VP8()},
					{Kind: plan.OpShape, Component: "shape", Shape: shape.New(shape.Media(av.MediaAudio))},
					{Kind: plan.OpTransform, Component: filter.FactoryResize, Transform: Resize(640, 360)},
				},
				Destinations: []string{"frames"},
			},
			code: "operation_shape_mismatch",
			want: []string{
				"resize cannot consume the current media shape",
				"operation_index=2",
				"expected_shape=domain=frame media=video",
				"actual_shape=domain=frame media=audio",
				"use .Video().Resize(...) for video frames",
			},
		},
		{
			name: "copy after decode",
			stream: streamIntent{
				Name:   "audio",
				Select: plan.StreamSelect{Type: av.MediaAudio, Codec: av.CodecOpus},
				Operations: []operationSpec{
					{Kind: plan.OpDecode, Decode: codec.Opus()},
					{Kind: plan.OpCopy, Component: "packet-copy", Encode: codec.Copy()},
				},
				Destinations: []string{"packets"},
			},
			code: "operation_shape_mismatch",
			want: []string{
				"packet-copy cannot consume the current media shape",
				"operation_index=1",
				"expected_shape=domain=packet",
				"actual_shape=domain=frame media=audio",
				"move .Copy() before decode",
			},
		},
	}
	pass := validateRecipeOperationShapesPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: intent{
					Inputs:       []inputIntent{{Name: "input"}},
					Streams:      []streamIntent{tt.stream},
					Destinations: []destinationIntent{{Name: "web"}, {Name: "frames"}, {Name: "packets"}},
				},
			}
			moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want %q", err, want)
				}
			}
		})
	}
}

func TestRecipeOperationShapePassAllowsCustomStageShapeDeclaration(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: intent{
			Inputs: []inputIntent{{Name: "input"}},
			Streams: []streamIntent{{
				Name:   "visualized",
				Select: plan.StreamSelect{Type: av.MediaAudio, Codec: av.CodecOpus},
				Operations: []operationSpec{
					{Kind: plan.OpDecode, Decode: codec.Opus()},
					{Kind: plan.OpStage, Component: "visualizer", Stage: &runtimeTestStage{name: "visualizer"}},
					{Kind: plan.OpShape, Component: "shape", Shape: shape.Frame(av.MediaVideo, shape.Video(640, 360, av.PixelFormatYUV420P))},
					{Kind: plan.OpEncode, Component: string(av.CodecVP9), Encode: codec.VP9(codec.Bitrate(600_000))},
				},
				Destinations: []string{"web"},
			}},
			Destinations: []destinationIntent{{Name: "web"}},
		},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindJob)
	if err := validateRecipeOperationShapesPass().Apply(&state); err != nil {
		t.Fatalf("validateRecipeOperationShapesPass() error = %v", err)
	}
}

func TestRecipeDestinationShapePassRejectsFrameShapeForMuxDestination(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: intent{
			Inputs: []inputIntent{{Name: "input"}},
			Streams: []streamIntent{{
				Name:   "preview",
				Select: plan.StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []operationSpec{
					{Kind: plan.OpDecode, Decode: codec.VP8()},
					{Kind: plan.OpStage, Component: "inspect", Stage: &runtimeTestStage{name: "inspect"}},
				},
				Destinations: []string{"archive.ivf"},
			}},
			Destinations: []destinationIntent{{Name: "archive.ivf"}},
		},
		outputAttachments: []destinationSpec{fileDestination("archive.ivf", io.Discard)},
	}
	state.recipe = recipeIRFromIntent(state.intent, recipeir.KindJob)
	annotateRecipeIRDestinationsFromSpecs(&state.recipe, state.outputAttachments)
	state.intent = intent{}

	err := validateRecipeDestinationShapesPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_shape_mismatch" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want destination_shape_mismatch with matching BuildError code", err)
	}
	for _, want := range []string{
		"byte or mux destination requires packet-domain media",
		"destination=archive.ivf",
		"expected_shape=domain=packet media=video",
		"actual_shape=domain=frame media=video",
		"goav.Sink",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

func TestRecipeDestinationShapePassAllowsFrameShapeForSinkDestination(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: intent{
			Inputs: []inputIntent{{Name: "input"}},
			Streams: []streamIntent{{
				Name:   "preview",
				Select: plan.StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []operationSpec{
					{Kind: plan.OpDecode, Decode: codec.VP8()},
				},
				Destinations: []string{"frames"},
			}},
			Destinations: []destinationIntent{{Name: "frames"}},
		},
		outputAttachments: []destinationSpec{sinkDestination(SinkFunc("frames", func(context.Context, Message) error { return nil }))},
	}
	state.recipe = recipeIRFromIntent(state.intent, recipeir.KindJob)
	annotateRecipeIRDestinationsFromSpecs(&state.recipe, state.outputAttachments)
	state.intent = intent{}
	if err := validateRecipeDestinationShapesPass().Apply(&state); err != nil {
		t.Fatalf("validateRecipeDestinationShapesPass() error = %v", err)
	}
}

func TestRecipeRuntimePassRejectsNilRuntime(t *testing.T) {
	state := recipeCompileState{
		operation:       "build job",
		runtimeExplicit: true,
		options:         recipeCompileOptions{requireExplicitRuntime: true},
	}
	err := validateRecipeRuntimePass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_missing" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime_missing with matching BuildError code", err)
	}
	for _, want := range []string{"no runtime is configured", "bundle.MustNew", "goav.New"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

func TestRecipeRuntimePassRequiresRuntimeFromRecipeIRStreams(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{requireExplicitRuntime: true},
		intent: intent{Streams: []streamIntent{{
			Name:       "audio",
			Operations: decodeIntentOperations(),
		}}},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindJob)

	err := validateRecipeRuntimePass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_missing" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime_missing with matching BuildError code", err)
	}
}

func TestRequireGraphPlanSpecPassWrapsUnsupportedRecipeShape(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: intent{
			Name:   "record",
			Inputs: []inputIntent{{Name: "input.ivf"}},
		},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindJob)

	err := requireGraphPlanSpecPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "recipe_graph_unsupported" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want recipe_graph_unsupported with matching BuildError code", err)
	}
	for _, want := range []string{"recipe intent", "inputs: 1", "destinations: 0", "goav.From", ".Copy().To", ".Branches"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
	if state.specReady || state.graphPlan.ready() {
		t.Fatalf("state specReady=%v state graphPlan=%T, want unset after unsupported selection", state.specReady, state.graphPlan)
	}
}

func TestJobIntentShapePassRejectsOperationTransforms(t *testing.T) {
	tests := []struct {
		name   string
		stream streamIntent
		code   errcode.Code
		want   string
	}{
		{
			name: "invalid resize",
			stream: streamIntent{
				Name:         "video",
				Select:       plan.StreamSelect{Type: av.MediaVideo},
				Operations:   append(decodeIntentOperations(), operationSpecForTransform(Resize(0, 720))),
				Destinations: []string{"frames"},
			},
			code: "transform_invalid",
			want: "positive width and height",
		},
		{
			name: "wrong media",
			stream: streamIntent{
				Name:         "audio",
				Select:       plan.StreamSelect{Type: av.MediaAudio},
				Operations:   append(decodeIntentOperations(), operationSpecForTransform(Resize(320, 180))),
				Destinations: []string{"frames"},
			},
			code: "transform_media_mismatch",
			want: "resize applies to video streams",
		},
		{
			name: "empty transform",
			stream: streamIntent{
				Name:         "video",
				Select:       plan.StreamSelect{Type: av.MediaVideo},
				Operations:   append(decodeIntentOperations(), operationSpecForTransform(transformSpec{})),
				Destinations: []string{"frames"},
			},
			code: "transform_invalid",
			want: "empty stream transform",
		},
	}
	pass := validateJobIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := recipeCompileState{
				operation: "build job",
				intent: intent{
					Inputs:       []inputIntent{{Name: "input"}},
					Streams:      []streamIntent{tt.stream},
					Destinations: []destinationIntent{{Name: "frames"}},
				},
			}
			moveTestIntentToRecipeIR(&state, recipeir.KindJob)
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code {
				t.Fatalf("err = %v, want %s", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestMediaPlanTransformFiltersUseOperationSpecs(t *testing.T) {
	stream := streamIntent{
		Name:   "preview",
		Select: plan.StreamSelect{Type: av.MediaVideo},
		Operations: []operationSpec{
			operationSpecForTransform(Resize(320, 180)),
		},
	}
	filters, err := mediaPlanStreamTransformFilters(stream, av.StreamSelector{Type: av.MediaVideo})
	if err != nil {
		t.Fatal(err)
	}
	if len(filters) != 1 || filters[0].transform == nil || filters[0].transform.video == nil {
		t.Fatalf("filters = %+v, want one resize filter from operationSpec", filters)
	}
	resize := filters[0].transform.video
	if resize.Width != 320 || resize.Height != 180 {
		t.Fatalf("resize = %+v, want operation-backed 320x180", resize)
	}
}

func TestTranscodeIntentShapePassRejectsInvalidPublicShape(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  errcode.Code
		want  string
	}{
		{
			name: "input missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Streams: []streamIntent{{
						Name:         "360p",
						Select:       plan.StreamSelect{Type: av.MediaVideo},
						Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
						Destinations: []string{"web"},
					}},
				},
			},
			code: "input_missing",
			want: "no input is configured",
		},
		{
			name: "stream missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
				},
			},
			code: "stream_missing",
			want: "no audio or video branches are configured",
		},
		{
			name: "branch name missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{{
						Select:       plan.StreamSelect{Type: av.MediaVideo},
						Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
						Destinations: []string{"web"},
					}},
				},
			},
			code: "stream_name_missing",
			want: "branches need stable names",
		},
		{
			name: "copy after decode unsupported",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{{
						Name:         "360p",
						Select:       plan.StreamSelect{Type: av.MediaVideo},
						Operations:   decodeEncodeIntentOperations(codec.Copy()),
						Destinations: []string{"web"},
					}},
				},
			},
			code: "copy_unsupported",
			want: "packet-domain stream point",
		},
		{
			name: "auto unresolved",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{{
						Name:         "360p",
						Select:       plan.StreamSelect{Type: av.MediaVideo},
						Operations:   encodeIntentOperations(codec.Auto()),
						Destinations: []string{"web"},
					}},
				},
			},
			code: "encode_auto_unresolved",
			want: "automatic codec selection",
		},
		{
			name: "duplicate branch destination",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: intent{
					Inputs: []inputIntent{{Name: "input.ivf"}},
					Streams: []streamIntent{{
						Name:         "360p",
						Select:       plan.StreamSelect{Type: av.MediaVideo},
						Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
						Destinations: []string{"web", "web"},
					}},
				},
			},
			code: "destination_duplicate",
			want: "more than once",
		},
	}
	pass := validateBranchCompositionIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := tt.state
			moveTestIntentToRecipeIR(&state, recipeir.KindBranchComposition)
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTranscodeAttachmentsPassRejectsInvalidConcreteAttachments(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  errcode.Code
		want  string
	}{
		{
			name: "live provider input",
			state: recipeCompileState{
				branchInputAttachment: Input(liveVideoVP8Provider("video")),
				branchDestinationAttachments: []namedDestinationSpec{{
					name:   "web",
					output: fileDestination("web.ivf", io.Discard),
				}},
			},
			code: "unsupported_input",
			want: "live provider transcode recipes",
		},
		{
			name: "duplicate destinations",
			state: recipeCompileState{
				branchInputAttachment: FileInput("input.ivf", strings.NewReader("")),
				branchDestinationAttachments: []namedDestinationSpec{
					{name: "web", output: fileDestination("web.ivf", io.Discard)},
					{name: "web", output: fileDestination("preview.ivf", io.Discard)},
				},
			},
			code: "destination_duplicate",
			want: "defined more than once",
		},
	}
	pass := validateBranchCompositionAttachmentsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, errUnsupportedBuild) {
				t.Fatalf("err = %v, want %s with matching BuildError code", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTranscodeBranchTargetKindsPassAllowsCopyMuxBranches(t *testing.T) {
	stream := streamIntent{
		Name:         "archive",
		Select:       plan.StreamSelect{Type: av.MediaVideo},
		Operations:   encodeIntentOperations(codec.Copy()),
		Destinations: []string{"web"},
	}
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent:    intent{Inputs: []inputIntent{{Name: "input.ivf"}}, Streams: []streamIntent{stream}},
	}
	moveTestOutputsToRecipeIR(&state, recipeir.KindBranchComposition, testRecipeIRDestination("web", recipeir.DestinationKindByteStream))

	if err := validateBranchCompositionIntentShapePass().Apply(&state); err != nil {
		t.Fatalf("validateBranchCompositionIntentShapePass() error = %v", err)
	}
	if err := validateBranchDestinationKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchDestinationKindsPass() error = %v", err)
	}
}

func TestTranscodeBranchTargetKindsPassAllowsRawSinkBranches(t *testing.T) {
	stream := streamIntent{
		Name:         "preview",
		Select:       plan.StreamSelect{Type: av.MediaVideo},
		Destinations: []string{"frames"},
	}
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent:    intent{Streams: []streamIntent{stream}},
	}
	moveTestOutputsToRecipeIR(&state, recipeir.KindBranchComposition, testRecipeIRDestination("frames", recipeir.DestinationKindSink))

	if err := validateBranchDestinationKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchDestinationKindsPass() error = %v", err)
	}
}

func TestTranscodeBranchTargetKindsPassRejectsRawMuxBranches(t *testing.T) {
	stream := streamIntent{
		Name:         "preview",
		Select:       plan.StreamSelect{Type: av.MediaVideo},
		Destinations: []string{"web"},
	}
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent:    intent{Streams: []streamIntent{stream}},
	}
	moveTestOutputsToRecipeIR(&state, recipeir.KindBranchComposition, testRecipeIRDestination("web", recipeir.DestinationKindByteStream))

	err := validateBranchDestinationKindsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "muxed destination") || !strings.Contains(err.Error(), "Sink") {
		t.Fatalf("err = %v, want mux and sink guidance", err)
	}
}

func TestTranscodeOutputBindingsPassRejectsUndefinedRoutes(t *testing.T) {
	stream := streamIntent{
		Name:         "360p",
		Select:       plan.StreamSelect{Type: av.MediaVideo},
		Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(600_000))),
		Destinations: []string{"missing"},
	}
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent:    intent{Streams: []streamIntent{stream}},
	}
	moveTestOutputsToRecipeIR(&state, recipeir.KindBranchComposition, testRecipeIRDestination("web", recipeir.DestinationKindByteStream))

	err := validateBranchDestinationBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_missing" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want destination_missing with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "destination missing is referenced but not defined") ||
		!strings.Contains(err.Error(), "goav.Mux(name, destination)") {
		t.Fatalf("err = %v, want destination binding guidance", err)
	}
}

func TestTranscodeKnownInputStreamSelectionPassRejectsProbedBranchAmbiguity(t *testing.T) {
	streams := []av.Stream{
		{
			Index: 0,
			ID:    "camera",
			Type:  av.MediaVideo,
			Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo},
		},
		{
			Index: 1,
			ID:    "screen",
			Type:  av.MediaVideo,
			Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo},
		},
	}
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: intent{Streams: []streamIntent{{
			Name:         "720p",
			Select:       plan.StreamSelect{Type: av.MediaVideo},
			Operations:   encodeIntentOperations(codec.VP9(codec.Bitrate(2_000_000))),
			Destinations: []string{"web"},
		}}},
		branchInputProbeReady: true,
		branchInputProbe: format.ProbeResult{
			Format:  av.FormatMatroska,
			Streams: streams,
		},
	}
	moveTestIntentToRecipeIR(&state, recipeir.KindBranchComposition)

	err := validateKnownBranchInputStreamSelectionPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" || !errors.Is(err, errUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous with matching BuildError code", err)
	}
	if !strings.Contains(err.Error(), "multiple streams match type=video") ||
		!strings.Contains(err.Error(), "id=camera") ||
		!strings.Contains(err.Error(), "id=screen") ||
		!strings.Contains(err.Error(), `.Video(goav.StreamID("camera"))`) {
		t.Fatalf("err = %v, want probed transcode stream-selection guidance", err)
	}
}

func TestCompileJobRecipeCarriesIntentAndGraphPlanBuild(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if !resolved.specReady {
		t.Fatal("compileJobRecipe() did not emit a planned graph spec")
	}
	if resolved.specOrigin != graphSpecOriginGraphPlan {
		t.Fatalf("resolved spec origin = %q, want %q", resolved.specOrigin, graphSpecOriginGraphPlan)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	if resolved.intent.Name != "from" {
		t.Fatalf("intent name = %q, want from", resolved.intent.Name)
	}
	if len(resolved.intent.Inputs) != 1 || resolved.intent.Inputs[0].Name != "input.ivf" {
		t.Fatalf("intent inputs = %+v", resolved.intent.Inputs)
	}
	if len(resolved.intent.Destinations) != 1 || resolved.intent.Destinations[0].Name != "recording.ivf" {
		t.Fatalf("intent destinations = %+v", resolved.intent.Destinations)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if len(spec.Nodes) == 0 || len(spec.Edges) == 0 {
		t.Fatalf("resolved spec = %+v, want planned graph nodes and edges", spec)
	}
	if got, want := spec, resolved.spec; got.Name != want.Name || len(got.Nodes) != len(want.Nodes) || len(got.Edges) != len(want.Edges) {
		t.Fatalf("resolved.Describe() = %+v, want stored spec %+v", got, want)
	}
}

func TestGraphPlanCarriesReportMetadata(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard).withFormat(av.FormatIVF))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Tap(FrameTap("video.decoded")).
		Branches(
			Branch("360p").
				Resize(640, 360).
				Encode(codec.VP9(codec.Bitrate(600_000))).
				To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	work := resolved.graphPlan.workPlan()
	if len(resolved.graphPlan.nodes) == 0 || len(resolved.graphPlan.edges) == 0 {
		t.Fatalf("graphPlan nodes=%+v edges=%+v, want planned operation graph", resolved.graphPlan.nodes, resolved.graphPlan.edges)
	}
	if len(work.Branches) != 1 || work.Branches[0].Name != "360p" {
		t.Fatalf("graphPlan work plan branches = %+v, want 360p branch", work.Branches)
	}
	if len(work.Taps) != 1 || work.Taps[0].Name != "video.decoded" {
		t.Fatalf("graphPlan work plan taps = %+v, want video.decoded tap", work.Taps)
	}
	if len(work.Destinations) != 1 || work.Destinations[0].Name != "web.ivf" || !reflect.DeepEqual(work.Destinations[0].Branches, []string{"360p"}) {
		t.Fatalf("graphPlan work plan destinations = %+v, want web.ivf owned by 360p", work.Destinations)
	}
	operations := work.Operations
	for _, want := range []plan.OperationKind{plan.OpDemux, plan.OpSelect, plan.OpDecode, plan.OpTap, plan.OpTransform, plan.OpEncode, plan.OpMux} {
		if !graphPlanOperationKindPresent(operations, want) {
			t.Fatalf("graphPlan operations = %+v, want %s", operations, want)
		}
	}
	if !graphPlanOperationNodePresent(operations, pipeline.NodeRef("resize-360p")) ||
		!graphPlanOperationNodePresent(operations, pipeline.NodeRef("encode-360p")) ||
		!graphPlanOperationTargetPresent(operations, "web.ivf") {
		t.Fatalf("graphPlan operations = %+v, want resize, encode, and web.ivf destination operations", operations)
	}
	report, err := newPlanReport("build job", resolved)
	if err != nil {
		t.Fatalf("newPlanReport() error = %v", err)
	}
	if len(report.Branches) != 1 || report.Branches[0].Name != "360p" {
		t.Fatalf("report branches = %+v, want graph-plan branch metadata", report.Branches)
	}
	if len(report.Taps) != 1 || report.Taps[0].Name != "video.decoded" {
		t.Fatalf("report taps = %+v, want graph-plan tap metadata", report.Taps)
	}
}

func TestGraphPlanViewsAreImmutable(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard).withFormat(av.FormatIVF)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	spec := resolved.graphPlan.spec()
	spec.Nodes[0].Name = "mutated"
	work := resolved.graphPlan.workPlan()
	work.Branches[0].Operations[0] = "mutated"
	work.Destinations[0].Branches[0] = "mutated"
	work.Operations[len(work.Operations)-1].Destinations[0] = "mutated"

	nextSpec := resolved.graphPlan.spec()
	if nextSpec.Nodes[0].Name == "mutated" {
		t.Fatal("graphPlan.spec() returned aliased nodes")
	}
	nextWork := resolved.graphPlan.workPlan()
	if nextWork.Branches[0].Operations[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() returned aliased branch operations")
	}
	if nextWork.Destinations[0].Branches[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() returned aliased destination branch refs")
	}
	nextOperations := resolved.graphPlan.workPlan().Operations
	if nextOperations[len(nextOperations)-1].Destinations[0] == "mutated" {
		t.Fatal("graphPlan.workPlan() returned aliased operation destination refs")
	}
}

func graphPlanOperationKindPresent(operations []workOperation, kind plan.OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func graphPlanOperationNodePresent(operations []workOperation, node pipeline.NodeRef) bool {
	for i := range operations {
		if operations[i].Node == node {
			return true
		}
	}
	return false
}

func graphPlanOperationTargetPresent(operations []workOperation, target string) bool {
	id := workDestinationID(target)
	for i := range operations {
		for _, next := range operations[i].Destinations {
			if next == id {
				return true
			}
		}
	}
	return false
}

func graphPlanOperationsWithoutDestinations(operations []workOperation) []workOperation {
	out := make([]workOperation, 0, len(operations))
	for i := range operations {
		if graphPlanOperationDestinationsRequired(operations[i].Kind) {
			continue
		}
		out = append(out, operations[i])
	}
	return out
}

func graphPlanOperationsWithoutKind(operations []workOperation, kind plan.OperationKind) []workOperation {
	out := make([]workOperation, 0, len(operations))
	for i := range operations {
		if operations[i].Kind == kind {
			continue
		}
		out = append(out, operations[i])
	}
	return out
}

func graphPlanOperationsWithoutBranch(operations []workOperation, branch string) []workOperation {
	out := make([]workOperation, 0, len(operations))
	for i := range operations {
		if operations[i].Branch == branch {
			continue
		}
		out = append(out, operations[i])
	}
	return out
}

func graphPlanOperationsWithoutBranchTarget(operations []workOperation, branch string, target string) []workOperation {
	id := workDestinationID(target)
	out := make([]workOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		if operation.Branch != branch || !graphPlanOperationDestinationsRequired(operation.Kind) {
			out = append(out, operation)
			continue
		}
		destinations := make([]string, 0, len(operation.Destinations))
		for _, next := range operation.Destinations {
			if next != id {
				destinations = append(destinations, next)
			}
		}
		if len(destinations) == 0 {
			continue
		}
		operation.Destinations = destinations
		out = append(out, operation)
	}
	return out
}

func graphPlanOperationsWithBranchTargetNode(operations []workOperation, branch string, target string, node pipeline.NodeRef) []workOperation {
	out := cloneWorkOperations(operations)
	for i := range out {
		operation := out[i]
		if operation.Branch != branch || !graphPlanOperationDestinationsRequired(operation.Kind) {
			continue
		}
		if !stringInSlice(workDestinationID(target), operation.Destinations) {
			continue
		}
		out[i].Node = node
	}
	return out
}

func TestGraphPlanSpecPassPlansFileCopy(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard).withFormat(av.FormatIVF)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.specOrigin != graphSpecOriginGraphPlan {
		t.Fatalf("resolved spec origin = %q, want %q", resolved.specOrigin, graphSpecOriginGraphPlan)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	want := pipeline.Spec{
		Name:     "goav",
		Realtime: true,
		Nodes: []pipeline.NodeSpec{
			{Name: "input.ivf", Kind: pipeline.NodeSource, Detail: "demux, protocol=file"},
			{Name: "recording.ivf", Kind: pipeline.NodeStage, Detail: "mux, format=ivf, protocol=file"},
		},
		Edges: []pipeline.EdgeSpec{{
			From:   pipeline.NodeRef("input.ivf"),
			To:     pipeline.NodeRef("recording.ivf"),
			Policy: pipeline.RouteAll,
		}},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("spec = %+v, want %+v", spec, want)
	}
}

func TestGraphPlanSpecPassPlansRTPCopy(t *testing.T) {
	job := From(
		Input(liveVideoVP8Provider("video")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard).withFormat(av.FormatIVF)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.specOrigin != graphSpecOriginGraphPlan {
		t.Fatalf("resolved spec origin = %q, want %q", resolved.specOrigin, graphSpecOriginGraphPlan)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	want := pipeline.Spec{
		Name:     "goav",
		Realtime: true,
		Nodes: []pipeline.NodeSpec{
			{Name: "video", Kind: pipeline.NodeSource, Detail: "live receive, codec=vp8"},
			{Name: "recording.ivf", Kind: pipeline.NodeStage, Detail: "mux, format=ivf, protocol=file"},
		},
		Edges: []pipeline.EdgeSpec{{
			From:   pipeline.NodeRef("video"),
			To:     pipeline.NodeRef("recording.ivf"),
			Policy: pipeline.RouteAll,
		}},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("spec = %+v, want %+v", spec, want)
	}
}

func TestCompileBranchCompositionRecipeCarriesIntentAndPlan(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Tap(FrameTap("video.decoded")).
		Branches(
			Branch("360p").
				Resize(640, 360).
				Encode(codec.VP9(codec.Bitrate(600_000))).
				To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if len(resolved.plan.Branches) != 1 || resolved.plan.Branches[0].Name != "360p" {
		t.Fatalf("resolved plan branches = %+v, want 360p branch", resolved.plan.Branches)
	}
	requireGraphPlanLowerer[mediaPlanBranchComposeGraph](t, resolved)
	if !resolved.specReady {
		t.Fatal("compileJobRecipe() did not emit a planned graph spec")
	}
	if resolved.intent.Name != "from" {
		t.Fatalf("intent name = %q, want from", resolved.intent.Name)
	}
	if len(resolved.intent.Streams) != 1 || resolved.intent.Streams[0].Name != "360p" {
		t.Fatalf("intent streams = %+v", resolved.intent.Streams)
	}
	if got := resolved.intent.Streams[0].Destinations; len(got) != 1 || got[0] != "web.ivf" {
		t.Fatalf("intent route destinations = %+v, want [web.ivf]", got)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if len(spec.Nodes) == 0 || len(spec.Edges) == 0 {
		t.Fatalf("resolved spec = %+v, want planned graph nodes and edges", spec)
	}
}

func TestCompileLiveFlowBranchesRecipeUsesMediaPlanBranchComposer(t *testing.T) {
	voice := destinationHandle(fileDestination("voice.ogg", io.Discard).withFormat(av.FormatOgg))
	archive := destinationHandle(fileDestination("archive.ogg", io.Discard).withFormat(av.FormatOgg))
	job := From(Input(liveAudioOpusProvider("audio"))).
		Audio().
		Branches(
			Branch("voice").Apply(Flow("voice").Audio().Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))).To(voice),
			Branch("archive").Apply(Flow("archive").Audio().Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))).To(archive),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.branchInputAttachment.provider == nil {
		t.Fatal("resolved branch input = nil provider, want live branch composer input carried on resolved plan")
	}
	if len(resolved.plan.Branches) != 2 {
		t.Fatalf("resolved plan branches = %+v, want two live flow branches", resolved.plan.Branches)
	}
	requireGraphPlanLowerer[mediaPlanBranchComposeGraph](t, resolved)
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if !specHasNode(spec, "encode-voice") || !specHasNode(spec, "encode-archive") {
		t.Fatalf("spec = %+v, want flow branch encoders", spec)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanBranchComposer(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		Branches(Branch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanBranchComposeGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestBranchComposeGraphPlanOperationsUseSharedNodeRefs(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	thumbnail := Sink(SinkFunc("thumbnail", func(context.Context, Message) error {
		return nil
	}))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Tap(FrameTap("video.720p.frames")).
		Branches(
			Branch("web").Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web),
			Branch("thumb").Resize(320, 180).To(thumbnail),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	operations := resolved.graphPlan.workPlan().Operations
	for _, want := range []pipeline.NodeRef{"select-video", "decode-video", "resize-video", "encode-web", "resize-thumb"} {
		if !graphPlanOperationNodePresent(operations, want) {
			t.Fatalf("graphPlan operations = %+v, want node %s", operations, want)
		}
	}
	for _, duplicate := range []pipeline.NodeRef{"decode-web", "decode-thumb", "resize-web"} {
		if graphPlanOperationNodePresent(operations, duplicate) {
			t.Fatalf("graphPlan operations = %+v, want shared node instead of %s", operations, duplicate)
		}
	}
}

func TestBranchComposeLowererUsesPlanInputOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(Branch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "select-audio", "select-plan-audio")
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "decode-audio", "decode-plan-audio")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "select-plan-audio") || !specHasNode(planned, "decode-plan-audio") {
		t.Fatalf("planned = %+v, want renamed plan input nodes", planned)
	}
}

func TestBranchComposeLowererUsesPlanSharedStepOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, &transcodeTestFilterFactory{})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Resample(48_000, codec.Stereo).
		Branches(Branch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "resample-audio", "resample-plan-audio")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "resample-plan-audio") {
		t.Fatalf("planned = %+v, want renamed plan shared step node", planned)
	}
}

func TestBranchComposeLowererUsesPlanPrivateStepAndEncodeOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, &transcodeTestFilterFactory{})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(Branch("main").
			Resample(16_000, codec.Mono).
			Encode(codec.Opus(codec.Bitrate(96_000))).
			To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	// The implicit "main" branch names its private transform and encode nodes from
	// the selector scope, matching a direct chain (#2): resample-audio / encode-audio,
	// not resample-main / encode-main.
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "resample-audio", "resample-plan-main")
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "encode-audio", "encode-plan-main")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "resample-plan-main") || !specHasNode(planned, "encode-plan-main") {
		t.Fatalf("planned = %+v, want renamed private step and encode nodes", planned)
	}
}

func TestBranchComposeLowererUsesPlanDestinationOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	archive := destinationHandle(fileDestination("archive.ogg", io.Discard))
	frames := Sink(SinkFunc("frames", func(context.Context, Message) error {
		return nil
	}))
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(
			Branch("archive").Encode(codec.Opus(codec.Bitrate(96_000))).To(archive),
			Branch("frames").To(frames),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	archiveNode, ok := graphPlanDestinationOperationNode(resolved.graphPlan.work.Operations, "archive.ogg")
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want archive.ogg destination operation", resolved.graphPlan.work.Operations)
	}
	framesNode, ok := graphPlanDestinationOperationNode(resolved.graphPlan.work.Operations, "frames")
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want frames destination operation", resolved.graphPlan.work.Operations)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, archiveNode.String(), "target-plan-archive")
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, framesNode.String(), "target-plan-frames")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "target-plan-archive") || !specHasNode(planned, "target-plan-frames") {
		t.Fatalf("planned = %+v, want renamed destination nodes", planned)
	}
}

func graphPlanDestinationOperationNode(operations []workOperation, target string) (pipeline.NodeRef, bool) {
	id := workDestinationID(target)
	for i := range operations {
		if !graphPlanOperationDestinationsRequired(operations[i].Kind) {
			continue
		}
		for _, next := range operations[i].Destinations {
			if next == id {
				return operations[i].Node, true
			}
		}
	}
	return "", false
}

func graphPlanOperationNode(operations []workOperation, kind plan.OperationKind) (pipeline.NodeRef, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i].Node, true
		}
	}
	return "", false
}

func renameResolvedGraphPlanOperationNode(t *testing.T, resolved recipeResolved, kind plan.OperationKind, name string) recipeResolved {
	t.Helper()
	node, ok := graphPlanOperationNode(resolved.graphPlan.work.Operations, kind)
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want %s operation", resolved.graphPlan.work.Operations, kind)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, node.String(), name)
	return resolved
}

func renameResolvedGraphPlanTargetNode(t *testing.T, resolved recipeResolved, target string, name string) recipeResolved {
	t.Helper()
	node, ok := graphPlanDestinationOperationNode(resolved.graphPlan.work.Operations, target)
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want destination operation %q", resolved.graphPlan.work.Operations, target)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, node.String(), name)
	return resolved
}

func renameGraphPlanNodeRef(gp graphPlan, oldName string, newName string) graphPlan {
	oldRef := pipeline.NodeRef(oldName)
	newRef := pipeline.NodeRef(newName)
	for i := range gp.nodes {
		if gp.nodes[i].Name == oldName {
			gp.nodes[i].Name = newName
		}
	}
	for i := range gp.edges {
		if gp.edges[i].From == oldRef {
			gp.edges[i].From = newRef
		}
		if gp.edges[i].To == oldRef {
			gp.edges[i].To = newRef
		}
	}
	for i := range gp.work.Operations {
		if gp.work.Operations[i].Node == oldRef {
			gp.work.Operations[i].Node = newRef
		}
	}
	for i := range gp.work.Taps {
		if gp.work.Taps[i].Node == oldRef {
			gp.work.Taps[i].Node = newRef
		}
	}
	return gp
}

func TestBranchComposeLowererRequiresBranchOperationsBeforeSources(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	mobile := destinationHandle(fileDestination("mobile.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			Branch("720p").Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web),
			Branch("360p").Resize(640, 360).Encode(codec.VP8(codec.Bitrate(600_000))).To(mobile),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutBranch(resolved.graphPlan.work.Operations, "360p")
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "branch composition graph plan has no operations for branch") {
		t.Fatalf("err = %v, want missing branch-operation graph-plan error", err)
	}
}

func TestBranchComposeLowererRequiresDecodeOperationBeforeSources(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			Branch("720p").Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutKind(resolved.graphPlan.work.Operations, plan.OpDecode)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "branch composition graph plan has no decode operation for branch") {
		t.Fatalf("err = %v, want missing decode-operation graph-plan error", err)
	}
}

func TestBranchComposeLowererRequiresDestinationOperationsBeforeSources(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			Branch("720p").Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000))).To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutDestinations(resolved.graphPlan.work.Operations)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "branch composition graph plan has no destination operations") {
		t.Fatalf("err = %v, want missing destination-operation graph-plan error", err)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanPacketCopy(t *testing.T) {
	job := From(
		Input(liveVideoVP8Provider("video")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard))).UseRuntime(testBundleRuntime())

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(context.Background())
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if len(planned.Nodes) != len(built.Nodes) || len(planned.Edges) != len(built.Edges) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestStreamGraphLowererUsesPlanPacketCopyDestinationOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(
			destinationHandle(fileDestination("archive.ogg", io.Discard)),
			Sink(&runtimeTestSink{name: "packets"}),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanTargetNode(t, resolved, "archive.ogg", "target-plan-archive")
	resolved = renameResolvedGraphPlanTargetNode(t, resolved, "packets", "target-plan-packets")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "target-plan-archive") || !specHasNode(planned, "target-plan-packets") {
		t.Fatalf("planned = %+v, want renamed packet-copy destination nodes", planned)
	}
}

func TestSelectedPacketCopyLowererUsesPlanSelectOperationNode(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, plan.OpSelect, "select-plan-audio")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "select-plan-audio") {
		t.Fatalf("planned = %+v, want renamed selected packet-copy select node", planned)
	}
}

func TestPacketCopyLowererRequiresDestinationOperationsBeforeSources(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutDestinations(resolved.graphPlan.work.Operations)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "packet-copy graph plan has no destination operations") {
		t.Fatalf("err = %v, want missing destination-operation graph-plan error", err)
	}
}

func TestPacketCopyLowererRequiresCopyOperationBeforeSources(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutKind(resolved.graphPlan.work.Operations, plan.OpCopy)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "packet-copy graph plan has no copy operation for branch") {
		t.Fatalf("err = %v, want missing copy-operation graph-plan error", err)
	}
}

func TestPacketCopyLowererRequiresTargetBranchBindingsBeforeSources(t *testing.T) {
	job := From(
		Input(liveAudioOpusProvider("left")),
	).And(
		Input(liveAudioOpusProvider("right")),
	).Copy().To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutBranchTarget(resolved.graphPlan.work.Operations, "right", "packets")
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "packet-copy destination operation branches do not match output branches") {
		t.Fatalf("err = %v, want packet-copy destination branch binding graph-plan error", err)
	}
}

func TestPacketCopyLowererRequiresConsistentDestinationOperationsBeforeSources(t *testing.T) {
	job := From(
		Input(liveAudioOpusProvider("left")),
	).And(
		Input(liveAudioOpusProvider("right")),
	).Copy().To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithBranchTargetNode(resolved.graphPlan.work.Operations, "right", "packets", "packets-right")
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "packet-copy destination operation is not consistent across branches") {
		t.Fatalf("err = %v, want duplicate target consistency graph-plan error", err)
	}
}

func TestPacketCopyTargetStreamsUseMatchedSourceGroups(t *testing.T) {
	left := []av.Stream{{
		ID:   "left-audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:   av.CodecOpus,
			Type: av.MediaAudio,
		},
	}}
	right := []av.Stream{{
		ID:   "right-audio",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:   av.CodecOpus,
			Type: av.MediaAudio,
		},
	}, {
		ID:   "right-video",
		Type: av.MediaVideo,
		Codec: av.CodecParameters{
			ID:   av.CodecVP8,
			Type: av.MediaVideo,
		},
	}}

	streams, err := packetCopyDestinationStreams(graphPlanDestinationOperation{
		Name:    "recording",
		Matches: []int{1},
	}, [][]av.Stream{left, right})
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 2 || streams[0].ID != "right-audio" || streams[1].ID != "right-video" {
		t.Fatalf("streams = %+v, want only matched source group", streams)
	}
}

func TestPacketCopyLowererPreservesAllStreamsForSingleSourceRemux(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream(), videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, muxers),
		),
	)
	task, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Copy().
		To(destinationHandle(fileDestination("recording.ogg", io.Discard))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if len(muxers.muxers) != 1 || muxers.muxers[0].streamCount != 2 ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"audio", "video"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestStreamGraphLowererUsesPlanDecodedSinkDestinationOperationNode(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanTargetNode(t, resolved, "frames", "target-plan-frames")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "target-plan-frames") {
		t.Fatalf("planned = %+v, want renamed decoded sink destination node", planned)
	}
}

func TestStreamGraphLowererUsesPlanSelectDecodeFilterOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
		withTestFilters(testFilterFactory(filter.Descriptor{
			Name:   filter.FactoryResample,
			Input:  av.MediaAudio,
			Output: av.MediaAudio,
		}, &transcodeTestFilterFactory{})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, plan.OpSelect, "select-plan-audio")
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, plan.OpDecode, "decode-plan-audio")
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, plan.OpTransform, "resample-plan-audio")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "select-plan-audio") ||
		!specHasNode(planned, "decode-plan-audio") ||
		!specHasNode(planned, "resample-plan-audio") {
		t.Fatalf("planned = %+v, want renamed direct select, decode, and filter nodes", planned)
	}
}

func TestFrameStreamLowererRequiresDecodeOperationBeforeSources(t *testing.T) {
	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutKind(resolved.graphPlan.work.Operations, plan.OpDecode)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "frame stream graph plan has no decode operation") {
		t.Fatalf("err = %v, want missing decode-operation graph-plan error", err)
	}
}

func TestFrameStreamLowererRequiresDestinationOperationsBeforeSources(t *testing.T) {
	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutDestinations(resolved.graphPlan.work.Operations)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "frame stream graph plan has no destination operations") {
		t.Fatalf("err = %v, want missing destination-operation graph-plan error", err)
	}
}

func TestFrameStreamLowererRequiresSingleBranchOperationSet(t *testing.T) {
	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = append(resolved.graphPlan.work.Operations, workOperation{
		Branch: "other",
		Node:   "select-other",
		Kind:   plan.OpSelect,
	})
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "frame stream graph plan must have exactly one branch operation set") {
		t.Fatalf("err = %v, want single-branch graph-plan error", err)
	}
}

func TestRecipeResolvedMediaPlanSinkDestinationPreservesCustomStage(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Do(&runtimeTestStage{name: "meter"}).
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	spec := task.Describe()
	if !specHasNode(spec, "meter") {
		t.Fatalf("built spec = %+v, want custom stage node", spec)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanRTPSinkDestination(t *testing.T) {
	ctx := context.Background()
	runtime := mustNew(withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})))
	job := From(Input(liveAudioOpusProvider("audio"))).UseRuntime(runtime).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanSelectedPacketSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestSelectedPacketCopyLowererRequiresSelectOperationBeforeSources(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutKind(resolved.graphPlan.work.Operations, plan.OpSelect)
	task, err := resolved.Build(ctx)
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "selected packet-copy graph plan has no select operation") {
		t.Fatalf("err = %v, want missing select-operation graph-plan error", err)
	}
}

func TestSelectedPacketCopyLowererRequiresCopyOperationBeforeSources(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutKind(resolved.graphPlan.work.Operations, plan.OpCopy)
	task, err := resolved.Build(ctx)
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "selected packet-copy graph plan has no copy operation") {
		t.Fatalf("err = %v, want selected copy-operation graph-plan error", err)
	}
}

func TestSelectedPacketCopyLowererRequiresSingleBranchOperationSet(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Copy().
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved.graphPlan.work.Operations = append(resolved.graphPlan.work.Operations, workOperation{
		Branch: "other",
		Node:   "select-other",
		Kind:   plan.OpSelect,
	})
	task, err := resolved.Build(ctx)
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "selected packet-copy graph plan must have exactly one branch operation set") {
		t.Fatalf("err = %v, want single-branch graph-plan error", err)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeOutput(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Do(&runtimeTestStage{name: "meter"}).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "meter") || !specHasNode(built, "encode-audio") || !specHasNode(built, "archive.ogg") {
		t.Fatalf("built spec = %+v, want meter, encode, and mux nodes", built)
	}
}

func TestStreamGraphLowererUsesPlanEncodedDestinationOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(
			destinationHandle(fileDestination("archive.ogg", io.Discard)),
			Sink(&runtimeTestSink{name: "packets"}),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanTargetNode(t, resolved, "archive.ogg", "target-plan-archive")
	resolved = renameResolvedGraphPlanTargetNode(t, resolved, "packets", "target-plan-packets")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "target-plan-archive") || !specHasNode(planned, "target-plan-packets") {
		t.Fatalf("planned = %+v, want renamed encoded destination nodes", planned)
	}
}

func TestStreamGraphLowererUsesPlanEncodeOperationNode(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, plan.OpEncode, "encode-plan-audio")
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(planned, "encode-plan-audio") {
		t.Fatalf("planned = %+v, want renamed direct encode node", planned)
	}
}

func TestEncodedFrameStreamLowererRequiresEncodeOperationBeforeSources(t *testing.T) {
	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.work.Operations = graphPlanOperationsWithoutKind(resolved.graphPlan.work.Operations, plan.OpEncode)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != graphPlanInvalidCode ||
		!strings.Contains(err.Error(), "encoded frame stream graph plan has no encode operation") {
		t.Fatalf("err = %v, want missing encode-operation graph-plan error", err)
	}
}

func TestMediaPlanDirectStreamUsesResolvedAttachments(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		Do(&runtimeTestStage{name: "meter"}).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	stream, ok := resolved.singleStreamIntent()
	if !ok {
		t.Fatalf("resolved intent streams = %+v, want one stream", resolved.intent.Streams)
	}
	gp, ok, err := newMediaPlanDecodeStreamGraph(resolved.runtime, resolved.inputAttachments, resolved.outputAttachments, stream)
	if err != nil || !ok {
		t.Fatalf("newMediaPlanDecodeStreamGraph ok=%v err=%v", ok, err)
	}
	spec, err := gp.encodeOutputSpec()
	if err != nil {
		t.Fatalf("encodeOutputSpec() error = %v", err)
	}
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if !reflect.DeepEqual(spec, planned) {
		t.Fatalf("attachment-built spec = %+v, resolved spec = %+v", spec, planned)
	}
	if !specHasNode(spec, "meter") || !specHasNode(spec, "encode-audio") || !specHasNode(spec, "archive.ogg") {
		t.Fatalf("spec = %+v, want stage, encoder, and target from resolved attachments", spec)
	}
	kinds := operationSpecKindsForTest(stream.Operations)
	want := []plan.OperationKind{plan.OpDecode, plan.OpTap, plan.OpStage, plan.OpEncode}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("resolved stream operations = %+v, want %+v", kinds, want)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "encode-audio") || !specHasNode(built, "packets") {
		t.Fatalf("built spec = %+v, want encode and sink nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanEncodeMuxAndSinkDestinations(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{streams: streams}),
			testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(
			destinationHandle(fileDestination("archive.ogg", io.Discard)),
			Sink(&runtimeTestSink{name: "packets"}),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if !specHasNode(built, "encode-audio") || !specHasNode(built, "archive.ogg") || !specHasNode(built, "packets") {
		t.Fatalf("built spec = %+v, want encode, mux, and packet sink nodes", built)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanRTPEncodeOutput(t *testing.T) {
	ctx := context.Background()
	runtime := mustNew(
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(Input(liveAudioOpusProvider("audio"))).UseRuntime(runtime).
		Audio().
		Decode().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	requireGraphPlanLowerer[mediaPlanStreamGraph](t, resolved)
	planned, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	task, err := resolved.Build(ctx)
	if err != nil {
		t.Fatalf("resolved.Build() error = %v", err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}
