package goav

import (
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type stubRuntime struct{}

func (stubRuntime) Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error) {
	return format.ProbeResult{}, nil
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

func TestGraphPlanUsesSharedBuildLifecycle(t *testing.T) {
	lowerer := reflect.TypeOf((*graphPlanLowerer)(nil)).Elem()
	if _, ok := lowerer.MethodByName("build"); ok {
		t.Fatal("graphPlanLowerer should not expose per-plan build; use graphPlan.Build")
	}
	for _, name := range []string{"spec", "runtimeRef", "lower"} {
		if _, ok := lowerer.MethodByName(name); !ok {
			t.Fatalf("graphPlanLowerer is missing %s", name)
		}
	}
	if _, ok := reflect.TypeOf(recipeResolved{}).FieldByName("builder"); ok {
		t.Fatal("recipeResolved should not carry the old runtime builder after graph-plan recognition")
	}
	if _, ok := reflect.TypeOf(recipeResolved{}).FieldByName("mediaGraph"); ok {
		t.Fatal("recipeResolved should not carry mediaGraph; use graphPlan as the executable boundary")
	}
	if _, ok := reflect.TypeOf(recipeResolved{}).FieldByName("mediaPlan"); ok {
		t.Fatal("recipeResolved should not carry mediaPlan; graphPlan owns report metadata")
	}
	if _, ok := reflect.TypeOf(recipeResolved{}).FieldByName("graphPlan"); !ok {
		t.Fatal("recipeResolved should carry graphPlan as the executable boundary")
	}
	graphPlanType := reflect.TypeOf(graphPlan{})
	for _, name := range []string{"nodes", "edges", "operations", "inputs", "streams", "taps", "branches", "outputs", "decisions", "diagnostics", "lowerer"} {
		if _, ok := graphPlanType.FieldByName(name); !ok {
			t.Fatalf("graphPlan should carry %s as cold-path plan metadata", name)
		}
	}
	if _, ok := graphPlanType.FieldByName("spec"); ok {
		t.Fatal("graphPlan should own nodes and edges directly instead of wrapping a stored pipeline.Spec")
	}
}

func TestGraphPlanBuildValidatesOperationsBeforeLowerer(t *testing.T) {
	runtime := New().(*runtime)
	lowerer := &graphPlanTestLowerer{runtime: runtime}
	plan := graphPlan{
		runtime: runtime,
		name:    "goav",
		nodes: []pipeline.NodeSpec{{
			Name: "source",
			Kind: pipeline.NodeSource,
		}},
		lowerer: lowerer,
	}

	task, err := buildGraphPlanTask(context.Background(), plan)
	if err == nil {
		task.Close()
		t.Fatal("buildGraphPlanTask() error = nil, want invalid graph-plan error")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want graph_plan_invalid wrapping ErrUnsupportedBuild", err)
	}
	if lowerer.called {
		t.Fatal("graph-plan lowerer was called after invalid ordered operations")
	}
}

func TestMediaPlanStreamGraphOwnsPacketCopyAndDirectStreams(t *testing.T) {
	if reflect.TypeOf((*mediaPlanStreamGraph)(nil)).Elem().Name() != "mediaPlanStreamGraph" {
		t.Fatal("mediaPlanStreamGraph should remain the common stream lowerer")
	}
	var body strings.Builder
	for _, file := range []string{"media_plan_build.go", "media_plan_spec.go"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	for _, forbidden := range []string{
		"mediaPlanPacketCopyGraph",
		"mediaPlanSingleStreamGraph",
		"mediaPlanPacketCopySources",
		"func (p mediaPlanBranchComposeGraph) specSources",
		"func (p mediaPlanBranchComposeGraph) compileSources",
	} {
		if strings.Contains(body.String(), forbidden) || strings.Contains(body.String(), "type "+forbidden) {
			t.Fatalf("%s should not be a separate stream graph-plan lowerer", forbidden)
		}
	}
	if !strings.Contains(body.String(), "compileMediaPlanSources") {
		t.Fatal("graph plans should share source compilation")
	}
}

func TestDirectFrameStreamsUseBranchRoutePlanner(t *testing.T) {
	body, err := os.ReadFile("media_plan_build.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"frameStreamBranchComposeSpec",
		"compileFrameStreamBranchCompose",
		"planBranchComposeRoutes",
		"compileBranchComposeInputs",
		"compileBranchComposeRoutes",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("direct frame streams should use branch route planner; missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"compileSinkDestination",
		"compileEncodeOutput",
		"lowerEncodeTargets",
		"planDecodeFilterPath(",
		"planEncodeDestinationPath(",
		"planEncodeSinkPath(",
		"planSinkPath(",
		"compileDecodeFilterPathNamed(",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("direct frame streams should not use legacy per-workflow helper %q", forbidden)
		}
	}
}

func TestSelectedPacketCopyUsesBranchRoutePlanner(t *testing.T) {
	body, err := os.ReadFile("media_plan_build.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"selectedPacketCopyBranchComposeRoutes",
		"compileSelectedPacketCopyBranchCompose",
		"planBranchComposeRoutes",
		"compileBranchComposeInputs",
		"compileBranchComposeRoutes",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("selected packet-copy should use branch route planner; missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"selectStage := newStreamSelectStage",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("selected packet-copy should not use legacy selected-copy helper %q", forbidden)
		}
	}
}

func TestOperationChainInternalsUseChainVocabulary(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{"recipe.go", "branch.go", "flow.go", "runtime_attach.go", "runtime_transcode.go", "branch_compose_plan.go", "recipe_compile.go", "media_plan.go", "media_plan_spec.go", "media_plan_build.go"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	text := body.String()
	for _, forbidden := range []string{
		"jobStreamStep",
		"streamStepOperations",
		"streamStepTapIntents",
		"jobStepTapDomain",
		"streamStepsFromTransforms",
		"cloneJobStreamSteps",
		"appendBranchSteps",
		"transformSpecsFromJobSteps",
		"branchComposeStep",
		"branchComposeStepsFromJobSteps",
		"streamFlowSpec",
		"flowBuilder",
		"flowSnapshotter",
		"flowSpecFrom",
		"validateFlowMedia",
		"flowTransformStepName",
		"isFlow",
		"newAudioFlow",
		"newVideoFlow",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("operation chains should not use old chain implementation vocabulary %q", forbidden)
		}
	}
	for _, required := range []string{
		"type chainStep struct",
		"func chainStepOperations",
		"func branchChainStepsFromOperations",
		"func branchChainStepsFromOperationList",
		"func branchChainStepsFromChain",
		"func branchChainStepsFromTranscode",
		"func runtimeBranchStepsFromChain",
		"type chainSpec struct",
		"type chainBuilder struct",
		"func chainSpecFrom",
		"func validateChainMedia",
		"func cloneChainSteps",
		"operations     []StreamOperation",
		"func streamOperationForTransform",
		"func streamOperationForTap",
		"func ensureJobStreamDecodeOperation",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("operation chains should keep shared chain vocabulary %q", required)
		}
	}
}

func TestStoredOperationListsMirrorFlowBranchAndDirectStreamWork(t *testing.T) {
	voice := Flow("voice").Audio().
		Resample(16_000, Mono).
		Tap(FrameTap("audio.voice.frames")).
		OpusVoice().
		Tap(PacketTap("audio.voice.packets"))

	flowSpec, err := chainSpecFrom(voice)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := streamOperationKindsForTest(flowSpec.operations), []OperationKind{OpTransform, OpTap, OpEncode, OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("flow operations = %+v, want %+v", got, want)
	}
	flowOutputs := voice.OutputShapes(FrameShape(
		av.MediaAudio,
		ShapeAudio(48_000, Stereo, av.SampleFormatS16),
	))
	if len(flowOutputs) != 1 || flowOutputs[0].Domain != DomainPacket || flowOutputs[0].MediaKind != av.MediaAudio || flowOutputs[0].Codec != av.CodecOpus {
		t.Fatalf("flow output shapes = %+v, want Opus packets", flowOutputs)
	}
	flowTaps := voice.Taps()
	if len(flowTaps) != 2 ||
		flowTaps[0].Name() != "audio.voice.frames" ||
		flowTaps[0].Domain() != DomainFrame ||
		flowTaps[1].Name() != "audio.voice.packets" ||
		flowTaps[1].Domain() != DomainPacket {
		t.Fatalf("flow taps = %+v, want frame then packet taps", flowTaps)
	}

	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Apply(voice).
		To(File("voice.ogg", io.Discard))
	if job.err != nil {
		t.Fatal(job.err)
	}
	if job.stream == nil {
		t.Fatal("job stream is nil")
	}
	if got, want := streamOperationKindsForTest(job.stream.operations), []OperationKind{OpDecode, OpTransform, OpTap, OpEncode, OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("direct stream stored operations = %+v, want %+v", got, want)
	}

	branch := Branch("archive").
		Apply(voice).
		To(File("archive.ogg", io.Discard))
	if branch.err != nil {
		t.Fatal(branch.err)
	}
	if got, want := streamOperationKindsForTest(branch.operations), []OperationKind{OpTransform, OpTap, OpEncode, OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("branch stored operations = %+v, want %+v", got, want)
	}
}

func TestPlannedBranchSplitOperationsInsertImplicitDecode(t *testing.T) {
	voice := Flow("voice").Audio().
		Resample(16_000, Mono).
		OpusVoice()

	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Branches(Branch("voice").Apply(voice).To(File("voice.ogg", io.Discard)))
	if job.err != nil {
		t.Fatal(job.err)
	}
	if len(job.branchStreams) != 1 {
		t.Fatalf("branch streams = %d, want 1", len(job.branchStreams))
	}
	stream := job.branchStreams[0]
	if !stream.operationSplit {
		t.Fatal("planned branch should carry split operation metadata")
	}
	if len(stream.sharedOps) != 0 {
		t.Fatalf("shared operations = %+v, want none before implicit decode", stream.sharedOps)
	}
	if got, want := streamOperationKindsForTest(stream.privateOps), []OperationKind{OpTransform, OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("private operations = %+v, want %+v", got, want)
	}
	if got, want := streamOperationKindsForTest(streamBuildOperations(stream)), []OperationKind{OpDecode, OpTransform, OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized operations = %+v, want %+v", got, want)
	}
}

func TestPlannedBranchSplitOperationsTreatParentCopyAsPacketAnchor(t *testing.T) {
	decodeFlow := Flow("voice").Audio().
		Decode().
		Resample(16_000, Mono).
		Opus(64_000)

	decodeJob := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Copy().
		Branches(Branch("voice").Apply(decodeFlow).To(File("voice.ogg", io.Discard)))
	if decodeJob.err != nil {
		t.Fatal(decodeJob.err)
	}
	if len(decodeJob.branchStreams) != 1 {
		t.Fatalf("decode branch streams = %d, want 1", len(decodeJob.branchStreams))
	}
	if got, want := streamOperationKindsForTest(streamBuildOperations(decodeJob.branchStreams[0])), []OperationKind{OpDecode, OpTransform, OpEncode}; !reflect.DeepEqual(got, want) {
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
	if got, want := streamOperationKindsForTest(streamBuildOperations(copyJob.branchStreams[0])), []OperationKind{OpCopy, OpTap}; !reflect.DeepEqual(got, want) {
		t.Fatalf("copy branch operations = %+v, want %+v", got, want)
	}
	if tap := streamBuildOperations(copyJob.branchStreams[0])[1].Tap; tap.Name != "packets.branch" || tap.Domain != DomainPacket {
		t.Fatalf("copy branch tap = %+v, want packet branch tap", tap)
	}
	copyPlan, err := planBranchCompositionRecipe(copyJob.Intent(), copyJob.inputs[0], copyJob.branchTargets, copyJob.branchStreams)
	if err != nil {
		t.Fatal(err)
	}
	if len(copyPlan.Branches) != 1 {
		t.Fatalf("copy plan branches = %d, want 1", len(copyPlan.Branches))
	}
	if got, want := streamOperationKindsForTest(copyPlan.Branches[0].Operations), []OperationKind{OpCopy, OpTap}; !reflect.DeepEqual(got, want) {
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
	web := File("web.ivf", io.Discard)

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
				VP9(2_000_000).
				To(web),
		)
	if job.err != nil {
		t.Fatal(job.err)
	}
	if len(job.branchStreams) != 2 {
		t.Fatalf("branch streams = %d, want 2", len(job.branchStreams))
	}

	rawOps := streamBuildOperations(job.branchStreams[0])
	if got, want := streamOperationKindsForTest(rawOps), []OperationKind{OpDecode, OpTap, OpTransform}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw operations = %+v, want %+v", got, want)
	}
	if !rawOps[0].Shared || !rawOps[1].Shared || rawOps[2].Shared {
		t.Fatalf("raw operation sharing = %+v, want shared decode/tap and private resize", rawOps)
	}
	if rawOps[1].Tap.Name != "video.decoded" {
		t.Fatalf("raw operations = %+v, want anchor tap video.decoded", rawOps)
	}

	webOps := streamBuildOperations(job.branchStreams[1])
	if got, want := streamOperationKindsForTest(webOps), []OperationKind{OpDecode, OpTap, OpTransform, OpTap, OpEncode}; !reflect.DeepEqual(got, want) {
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

	operationOnlyStreams := append([]streamBuild(nil), job.branchStreams...)
	for i := range operationOnlyStreams {
		operationOnlyStreams[i].sharedSteps = nil
		operationOnlyStreams[i].steps = nil
	}
	plan, err := planBranchCompositionRecipe(job.Intent(), job.inputs[0], job.branchTargets, operationOnlyStreams)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Branches) != 2 {
		t.Fatalf("plan branches = %d, want 2", len(plan.Branches))
	}
	if len(plan.Branches[0].SharedSteps) != 0 ||
		len(plan.Branches[0].Steps) != 1 ||
		plan.Branches[0].Steps[0].transform.Resize == nil ||
		plan.Branches[0].Steps[0].transform.Resize.Width != 320 {
		t.Fatalf("raw plan branch = %+v, want private thumbnail resize from operation split", plan.Branches[0])
	}
	if len(plan.Branches[1].SharedSteps) != 1 ||
		plan.Branches[1].SharedSteps[0].transform.Resize == nil ||
		plan.Branches[1].SharedSteps[0].transform.Resize.Width != 1280 ||
		len(plan.Branches[1].Steps) != 0 {
		t.Fatalf("web plan branch = %+v, want shared 720p resize from operation split", plan.Branches[1])
	}

	if got, want := streamOperationKindsForTest(plan.Branches[0].Operations), []OperationKind{OpDecode, OpTap, OpTransform}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw plan operations = %+v, want %+v", got, want)
	}
	if got, want := streamOperationKindsForTest(plan.Branches[1].Operations), []OperationKind{OpDecode, OpTap, OpTransform, OpTap, OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("web plan operations = %+v, want %+v", got, want)
	}

	operationOnlyPlan := plan
	for i := range operationOnlyPlan.Branches {
		operationOnlyPlan.Branches[i].SharedSteps = nil
		operationOnlyPlan.Branches[i].Steps = nil
	}
	routes, _, err := prepareBranchComposePlan(operationOnlyPlan)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 {
		t.Fatalf("routes = %d, want 2", len(routes))
	}
	if len(routes[0].sharedSteps) != 0 ||
		len(routes[0].steps) != 1 ||
		routes[0].steps[0].video == nil ||
		routes[0].steps[0].video.Width != 320 {
		t.Fatalf("raw route = %+v, want private thumbnail resize from operation fields", routes[0])
	}
	if len(routes[1].sharedSteps) != 1 ||
		routes[1].sharedSteps[0].video == nil ||
		routes[1].sharedSteps[0].video.Width != 1280 ||
		len(routes[1].steps) != 0 {
		t.Fatalf("web route = %+v, want shared 720p resize from operation fields", routes[1])
	}

	intentWithoutOperations := job.Intent()
	for i := range intentWithoutOperations.Streams {
		intentWithoutOperations.Streams[i].Operations = nil
		intentWithoutOperations.Streams[i].Transforms = nil
	}
	media := buildMediaPlan(&recipeCompileState{
		operation:                branchCompositionOperation,
		branchCompositionPresent: true,
		intent:                   intentWithoutOperations,
		branchInputAttachment:    job.inputs[0],
		branchTargetAttachments:  job.branchTargets,
		plan:                     plan,
	})
	if len(media.Branches) != 2 {
		t.Fatalf("media branches = %d, want 2", len(media.Branches))
	}
	if got, want := planOperationKindsForTest(media.Branches[0].Operations), []OperationKind{OpDemux, OpSelect, OpDecode, OpTap, OpTransform}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw media operations = %+v, want %+v", got, want)
	}
	if got, want := planOperationKindsForTest(media.Branches[1].Operations), []OperationKind{OpDemux, OpSelect, OpDecode, OpTap, OpTransform, OpTap, OpEncode}; !reflect.DeepEqual(got, want) {
		t.Fatalf("web media operations = %+v, want %+v", got, want)
	}
	if !media.Branches[1].Operations[4].Shared || !media.Branches[1].Operations[5].Shared || media.Branches[1].Operations[6].Shared {
		t.Fatalf("web media operation sharing = %+v, want shared parent transform/tap and private encode", media.Branches[1].Operations)
	}
	graphOperations := graphPlanOperationsFromMediaPlan(pipeline.Spec{}, media)
	if got, want := graphPlanOperationKindsForBranch(graphOperations, "raw-preview"), []OperationKind{OpDemux, OpSelect, OpDecode, OpTap, OpTransform, OpSink}; !reflect.DeepEqual(got, want) {
		t.Fatalf("raw graph operations = %+v, want %+v", got, want)
	}
	if got, want := graphPlanOperationKindsForBranch(graphOperations, "web"), []OperationKind{OpDemux, OpSelect, OpDecode, OpTap, OpTransform, OpTap, OpEncode, OpMux}; !reflect.DeepEqual(got, want) {
		t.Fatalf("web graph operations = %+v, want %+v", got, want)
	}
}

func streamOperationKindsForTest(operations []StreamOperation) []OperationKind {
	out := make([]OperationKind, 0, len(operations))
	for i := range operations {
		out = append(out, operations[i].Kind)
	}
	return out
}

func planOperationKindsForTest(operations []planOperation) []OperationKind {
	out := make([]OperationKind, 0, len(operations))
	for i := range operations {
		out = append(out, operations[i].Kind)
	}
	return out
}

func graphPlanOperationKindsForBranch(operations []graphPlanOperation, branch string) []OperationKind {
	out := make([]OperationKind, 0)
	for i := range operations {
		if operations[i].Branch == branch {
			out = append(out, operations[i].Kind)
		}
	}
	return out
}

func TestProductionDiagnosticsUseCurrentVocabulary(t *testing.T) {
	var body strings.Builder
	for _, file := range []string{"recipe.go", "branch.go", "flow.go", "runtime_attach.go", "runtime_compile.go", "runtime_plan.go", "runtime_transcode.go", "recipe_compile.go"} {
		fileBody, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(fileBody)
	}
	text := body.String()
	for _, forbidden := range []string{
		"Record, From, Decode, or Transcode",
		"target endpoint",
		"sink endpoints",
		"muxed endpoints",
		"goav.FileOutput",
		"goav.URIOutput",
		"SinkEndpoint",
		".FromTap(",
		"goav.FromTap(",
		".TapName(",
		"goav.TapName(",
		"goav.AudioFlow(",
		"goav.VideoFlow(",
		"branchComposeTargetHasMuxEndpoint",
		"branchComposeTargetEndpointInvalidError",
		"runtime_builder_missing",
		"standard runtime builder",
		"recipe compiler produced no runtime builder",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("production diagnostics keep old public vocabulary %q", forbidden)
		}
	}
}

func TestRuntimeBuilderUsesMuxVerbNotOutput(t *testing.T) {
	builder := reflect.TypeOf((*builderAPI)(nil)).Elem()
	if _, ok := builder.MethodByName("Output"); ok {
		t.Fatal("private runtime builder should not expose Output; use Mux for mux destinations")
	}
	if _, ok := builder.MethodByName("Mux"); !ok {
		t.Fatal("private runtime builder should expose Mux for mux destinations")
	}
}

func TestRecipeCompileStateDoesNotCarryRecipeBuilders(t *testing.T) {
	stateType := reflect.TypeOf(recipeCompileState{})
	forbidden := map[reflect.Type]string{
		reflect.TypeOf((*Job)(nil)):                  "*Job",
		reflect.TypeOf((*branchCompositionJob)(nil)): "*branchCompositionJob",
		reflect.TypeOf((*jobStreamBuild)(nil)):       "*jobStreamBuild",
		reflect.TypeOf([]streamBuild(nil)):           "[]streamBuild",
		reflect.TypeOf((*builderAPI)(nil)).Elem():    "builderAPI",
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
				intent:            Intent{Inputs: []InputIntent{{Name: "input.ivf"}}},
				outputAttachments: []destinationSpec{fileDestination("recording.ivf", io.Discard)},
			},
			want: "inputs",
		},
		{
			name: "job outputs",
			state: recipeCompileState{
				operation:         "build job",
				jobPresent:        true,
				intent:            Intent{Inputs: []InputIntent{{Name: "input.ivf"}}, Targets: []TargetIntent{{Name: "recording.ivf"}}},
				inputAttachments:  []InputSpec{FileInput("input.ivf", strings.NewReader(""))},
				outputAttachments: nil,
			},
			want: "targets",
		},
		{
			name: "branch targets",
			state: recipeCompileState{
				operation:                branchCompositionOperation,
				branchCompositionPresent: true,
				intent:                   Intent{Inputs: []InputIntent{{Name: "input.ivf"}}, Targets: []TargetIntent{{Name: "web.ivf"}}},
				branchInputAttachment:    FileInput("input.ivf", strings.NewReader("")),
				branchTargetAttachments:  nil,
			},
			want: "targets",
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
			if !errors.As(err, &buildErr) || buildErr.Code != "recipe_attachment_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want recipe_attachment_mismatch wrapping ErrUnsupportedBuild", err)
			}
		})
	}
}

func TestJobIntentShapePassRejectsInvalidPublicShape(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		want  string
	}{
		{
			name: "input missing",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Targets: []TargetIntent{{Name: "recording.ivf"}},
				},
			},
			code: "input_missing",
			want: "no input is configured",
		},
		{
			name: "duplicate stream intent",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{
						{Name: "audio", Decode: true, Targets: []string{"audio"}},
						{Name: "video", Decode: true, Targets: []string{"video"}},
					},
					Targets: []TargetIntent{{Name: "audio"}, {Name: "video"}},
				},
			},
			code: "stream_duplicate",
			want: "ordinary stream recipes select one audio or video stream",
		},
		{
			name: "mixed output scope",
			state: recipeCompileState{
				operation:      "build job",
				jobOutputCount: 1,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "audio",
						Decode:  true,
						Targets: []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "archive.ivf"}, {Name: "frames"}},
				},
			},
			code: "output_scope_mixed",
			want: "stream recipes use stream-local outputs",
		},
		{
			name: "stream operation missing",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "audio",
						Targets: []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
			},
			code: "stream_operation_missing",
			want: "no decode, processing stage, or encoder was requested",
		},
	}
	pass := validateJobIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestJobOutputBindingsPassRejectsUndefinedStreamRoutes(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ogg"}},
			Streams: []StreamIntent{{
				Name:    "audio",
				Decode:  true,
				Targets: []string{"missing"},
			}},
			Targets: []TargetIntent{{Name: "archive.ogg"}},
		},
		outputAttachments: []destinationSpec{
			fileDestination("archive.ogg", io.Discard),
		},
	}

	err := validateJobOutputBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "output_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want output_missing wrapping ErrUnsupportedBuild", err)
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
				runtime:   Default(),
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
				runtime:   Default(),
				outputAttachments: []destinationSpec{
					fileDestination("", io.Discard).withFormat(av.FormatOgg),
				},
			},
			want: `format "ogg"`,
		},
		{
			name: "transcode probed format",
			pass: validateBranchTargetFormatAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime:   Default(),
				branchTargetAttachments: []namedTargetSpec{{
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
			if !errors.As(err, &buildErr) || buildErr.Code != "target_muxer_missing" || !errors.Is(err, format.ErrNotFound) {
				t.Fatalf("err = %v, want target_muxer_missing wrapping format.ErrNotFound", err)
			}
			if buildErr.Operation != "open target" {
				t.Fatalf("operation = %q, want open target", buildErr.Operation)
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
				runtime: New(withTestFormats(
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
			pass: validateBranchTargetFormatAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: New(withTestFormats(
					testFormatProber(remuxTestProber{}),
					testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
				)),
				branchTargetAttachments: []namedTargetSpec{{
					name:   "web",
					output: fileDestination("web.ogg", io.Discard),
				}},
			},
			validate: func(t *testing.T, state recipeCompileState) {
				t.Helper()
				if len(state.branchTargetAttachments) != 1 ||
					state.branchTargetAttachments[0].output.format != "" ||
					state.branchTargetAttachments[0].output.resolvedFormat != av.FormatOgg ||
					state.branchTargetAttachments[0].output.output.Name != "web.ogg" {
					t.Fatalf("branch target attachments = %+v, want resolved Ogg format", state.branchTargetAttachments)
				}
			},
		},
		{
			name: "explicit output format stays explicit",
			pass: validateJobOutputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightOutputAdapters: true},
				runtime: New(withTestFormats(
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
	runtime := New(
		WithDefaults(),
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
	)
	job := From(
		RTP(&runtimeRTPReceiver{
			streams: []Stream{{
				ID:   "audio",
				Type: av.MediaAudio,
				Codec: av.CodecParameters{
					ID:   av.CodecOpus,
					Type: av.MediaAudio,
				},
			}},
		}).Name("audio").Codec(Opus()),
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
	state := recipeCompileState{
		operation: branchCompositionOperation,
		options:   recipeCompileOptions{preflightOutputAdapters: true},
		runtime: New(withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		)),
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "audio",
				Select:  StreamSelect{Type: av.MediaAudio},
				Encode:  Opus(Bitrate(96_000)),
				Targets: []string{"archive"},
			}},
			Targets: []TargetIntent{{Name: "archive"}},
		},
		branchInputAttachment: FileInput("input.ivf", strings.NewReader("")),
		branchTargetAttachments: []namedTargetSpec{{
			name:   "archive",
			output: fileDestination("archive.ogg", io.Discard),
		}},
	}

	if err := validateBranchTargetFormatAdaptersPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchTargetFormatAdaptersPass() error = %v", err)
	}
	if err := planBranchCompositionIntentPass().Apply(&state); err != nil {
		t.Fatalf("planBranchCompositionIntentPass() error = %v", err)
	}
	if len(state.plan.Targets) != 1 ||
		state.plan.Targets[0].Format != "" ||
		state.plan.Targets[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("plan targets = %+v, want resolved Ogg open format without graph detail format", state.plan.Targets)
	}
}

func TestResolvedBranchRecipeOutputFormatsRefreshPreplannedTargets(t *testing.T) {
	streams := []av.Stream{audioOpusTestStream()}
	runtime := New(
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
		Branches(Branch("main").Opus(96_000).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(context.Background(), job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	if len(resolved.plan.Targets) != 1 ||
		resolved.plan.Targets[0].Format != "" ||
		resolved.plan.Targets[0].OpenFormat() != av.FormatOgg {
		t.Fatalf("resolved plan targets = %+v, want resolved Ogg open format", resolved.plan.Targets)
	}
}

func TestInputFormatAdapterPassesRejectMissingDemuxers(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		want  []string
	}{
		{
			name: "job probed format",
			pass: validateJobInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightInputAdapters: true},
				runtime:   Default(),
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
				runtime:               Default(),
				branchInputAttachment: FileInput("input.mp4", strings.NewReader("")),
			},
			code: "input_demuxer_missing",
			want: []string{`format "mp4"`, "no demuxer is registered", "WithFormatAdapter"},
		},
		{
			name: "unknown input format",
			pass: validateJobInputFormatAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightInputAdapters: true},
				runtime:   Default(),
				inputAttachments: []InputSpec{
					FileInput("input.unknown", strings.NewReader("")),
				},
			},
			code: "input_format_unknown",
			want: []string{"could not be detected", "name=input.unknown", "goav.RTP"},
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
		runtime:   Default(),
		inputAttachments: []InputSpec{
			RTP(nil).Codec(Opus()),
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
		runtime: New(withTestFormats(
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
		stream StreamIntent
		code   string
		want   []string
	}{
		{
			name: "ambiguous probed audio",
			stream: StreamIntent{
				Name:   "audio",
				Select: StreamSelect{Type: av.MediaAudio},
				Decode: true,
			},
			code: "stream_ambiguous",
			want: []string{"multiple streams match type=audio", "id=eng", "id=spa", `.Audio(goav.StreamID("eng"))`, ".Audio(goav.StreamIndex(0))"},
		},
		{
			name: "missing probed video",
			stream: StreamIntent{
				Name:   "video",
				Select: StreamSelect{Type: av.MediaVideo},
				Decode: true,
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
				intent: Intent{Streams: []StreamIntent{
					tt.stream,
				}},
				inputProbes: []format.ProbeResult{{
					Format:  av.FormatOgg,
					Streams: streams,
				}},
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
		intent Intent
		code   string
		want   []string
	}{
		{
			name: "ambiguous live video",
			intent: Intent{
				Inputs: []InputIntent{
					{Name: "front", Protocol: av.ProtocolRTP, Codec: VP8(), Realtime: true},
					{Name: "screen", Protocol: av.ProtocolRTP, Codec: VP8(), Realtime: true},
				},
				Streams: []StreamIntent{{
					Name:   "video",
					Select: StreamSelect{Type: av.MediaVideo},
					Decode: true,
				}},
			},
			code: "stream_ambiguous",
			want: []string{"multiple streams match type=video", "id=front", "id=screen", `.Video(goav.StreamID("front"))`, ".Video(goav.StreamIndex(0))"},
		},
		{
			name: "missing live audio",
			intent: Intent{
				Inputs: []InputIntent{
					{Name: "camera", Protocol: av.ProtocolRTP, Codec: VP8(), Realtime: true},
				},
				Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Decode: true,
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
			err := validateJobLiveStreamSelectionPass().Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
		intent: Intent{Inputs: []InputIntent{{
			Name:     "video",
			Protocol: av.ProtocolRTP,
			Codec:    VP8(),
			Realtime: true,
		}}},
	}
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
	descriptorRuntime := New(func(runtime *runtime) {
		runtime.codecs = descriptorOnly
	})

	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "missing decoder",
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   New(),
				intent: Intent{
					Inputs: []InputIntent{{
						Name:     "audio",
						Protocol: av.ProtocolRTP,
						Codec:    Opus(),
						Realtime: true,
					}},
					Streams: []StreamIntent{{
						Name:   "audio",
						Select: StreamSelect{Type: av.MediaAudio},
						Decode: true,
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
				intent: Intent{
					Inputs: []InputIntent{{
						Name:     "video",
						Protocol: av.ProtocolRTP,
						Codec:    H264(),
						Realtime: true,
					}},
					Streams: []StreamIntent{{
						Name:   "video",
						Select: StreamSelect{Type: av.MediaVideo},
						Decode: true,
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
			err := validateJobDecodeAdaptersPass().Apply(&tt.state)
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
		runtime:   New(),
		intent: Intent{
			Inputs: []InputIntent{
				{Name: "front", Protocol: av.ProtocolRTP, Codec: H264(), Realtime: true},
				{Name: "screen", Protocol: av.ProtocolRTP, Codec: H264(), Realtime: true},
			},
			Streams: []StreamIntent{{
				Name:   "video",
				Select: StreamSelect{Type: av.MediaVideo},
				Decode: true,
			}},
		},
	}
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
	descriptorRuntime := New(func(runtime *runtime) {
		runtime.codecs = descriptorOnly
	})

	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "job probed decoder",
			pass: validateJobKnownInputDecodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightDecodeAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Decode: true,
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
				intent: Intent{Streams: []StreamIntent{{
					Name:   "video",
					Select: StreamSelect{Type: av.MediaVideo},
					Decode: true,
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
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:    "360p",
					Select:  StreamSelect{Type: av.MediaVideo},
					Encode:  VP9(Bitrate(600_000)),
					Targets: []string{"web"},
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

func TestKnownInputDecodeAdapterPassDefersAmbiguousSelection(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		options:   recipeCompileOptions{preflightDecodeAdapters: true},
		runtime:   New(),
		intent: Intent{Streams: []StreamIntent{{
			Name:   "audio",
			Select: StreamSelect{Type: av.MediaAudio},
			Decode: true,
		}}},
		inputProbes: []format.ProbeResult{{
			Format: av.FormatOgg,
			Streams: []av.Stream{
				{Index: 0, ID: "eng", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
				{Index: 1, ID: "spa", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
			},
		}},
	}
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
				runtime: New(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaVideo,
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: Intent{
					Inputs: []InputIntent{{
						Name:     "audio",
						Protocol: av.ProtocolRTP,
						Codec:    Codec(audioCodec, av.MediaAudio),
						Realtime: true,
					}},
					Streams: []StreamIntent{{
						Name:   "audio",
						Select: StreamSelect{Type: av.MediaAudio},
						Decode: true,
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
				runtime: New(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaAudio,
					Capabilities: codec.Capabilities{
						SampleFormats: []string{av.SampleFormatS16},
					},
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Decode: true,
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
				runtime: New(withTestCodecs(testCodecDecoder(codec.Descriptor{
					ID:   videoCodec,
					Type: av.MediaVideo,
					Capabilities: codec.Capabilities{
						PixelFormats: []string{av.PixelFormatI420},
					},
				}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:    "preview",
					Select:  StreamSelect{Type: av.MediaVideo},
					Encode:  VP9(Bitrate(600_000)),
					Targets: []string{"web"},
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
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "decode_adapter_incompatible" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want decode_adapter_incompatible wrapping ErrUnsupportedBuild", err)
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
	descriptorRuntime := New(func(runtime *runtime) {
		runtime.codecs = descriptorOnly
	})

	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "job missing encoder",
			pass: validateJobEncodeAdaptersPass(),
			state: recipeCompileState{
				operation: "build job",
				options:   recipeCompileOptions{preflightEncodeAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Encode: Opus(Bitrate(96_000)),
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
				intent: Intent{Streams: []StreamIntent{{
					Name:   "360p",
					Encode: VP9(Bitrate(600_000)),
				}}},
			},
			code:  "encode_adapter_unavailable",
			cause: codec.ErrUnavailable,
			want:  []string{"descriptor-only", "codec=vp9", "backend=govpx"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				runtime: New(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaVideo,
				}, &encodeTestEncoderFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Encode: Codec(audioCodec, av.MediaAudio),
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
				runtime: New(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   audioCodec,
					Type: av.MediaAudio,
					Capabilities: codec.Capabilities{
						SampleFormats: []string{av.SampleFormatS16},
					},
				}, &encodeTestEncoderFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "voice",
					Select: StreamSelect{Type: av.MediaAudio},
					Encode: Codec(audioCodec, av.MediaAudio, Parameters(av.CodecParameters{
						ID:           audioCodec,
						Type:         av.MediaAudio,
						SampleFormat: av.SampleFormatF32,
					})),
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
				runtime: New(withTestCodecs(testCodecEncoder(codec.Descriptor{
					ID:   videoCodec,
					Type: av.MediaVideo,
					Capabilities: codec.Capabilities{
						PixelFormats: []string{av.PixelFormatI420},
					},
				}, &encodeTestEncoderFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "preview",
					Select: StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{{
						Resize: &filter.ResizeConfig{
							Width:       640,
							Height:      360,
							PixelFormat: av.PixelFormatYUV420P,
						},
					}},
					Encode: Codec(videoCodec, av.MediaVideo),
				}}},
			},
			want: []string{"encoder adapter does not support the requested pixel format", "field=pixel_format", "requested=yuv420p", "supported=i420"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "encode_adapter_incompatible" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want encode_adapter_incompatible wrapping ErrUnsupportedBuild", err)
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
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "audio",
					Select:     StreamSelect{Type: av.MediaAudio},
					Transforms: []TransformSpec{Resample(16_000, Mono)},
				}}},
			},
			want: []string{"no resample filter adapter", "transform=resample", "goav.Default", ".Resample"},
		},
		{
			name: "transcode missing resize filter",
			pass: validateBranchTransformAdaptersPass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				options:   recipeCompileOptions{preflightTransformAdapters: true},
				runtime:   New(),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "720p",
					Select:     StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{Resize(1280, 720)},
				}}},
			},
			want: []string{"no resize filter adapter", "transform=resize", "goav.Default", ".Resize"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:   filter.FactoryResample,
					Input:  av.MediaVideo,
					Output: av.MediaVideo,
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "audio",
					Select:     StreamSelect{Type: av.MediaAudio},
					Transforms: []TransformSpec{Resample(16_000, Mono)},
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
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:   filter.FactoryResize,
					Input:  av.MediaAudio,
					Output: av.MediaAudio,
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "720p",
					Select:     StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{Resize(1280, 720)},
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
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:        filter.FactoryResize,
					Input:       av.MediaVideo,
					Output:      av.MediaVideo,
					ResizeModes: []filter.ResizeMode{filter.ResizeFill},
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:       "video",
					Select:     StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{Resize(1280, 720)},
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
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:         filter.FactoryResize,
					Input:        av.MediaVideo,
					Output:       av.MediaVideo,
					PixelFormats: []string{av.PixelFormatI420},
					ResizeModes:  []filter.ResizeMode{filter.ResizeFit},
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "preview",
					Select: StreamSelect{Type: av.MediaVideo},
					Transforms: []TransformSpec{{
						Resize: &filter.ResizeConfig{
							Width:       640,
							Height:      360,
							Mode:        filter.ResizeFit,
							PixelFormat: av.PixelFormatYUV420P,
						},
					}},
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
				runtime: New(withTestFilters(testFilterFactory(filter.Descriptor{
					Name:          filter.FactoryResample,
					Input:         av.MediaAudio,
					Output:        av.MediaAudio,
					SampleFormats: []string{av.SampleFormatS16},
				}, &transcodeTestFilterFactory{}))),
				intent: Intent{Streams: []StreamIntent{{
					Name:   "audio",
					Select: StreamSelect{Type: av.MediaAudio},
					Transforms: []TransformSpec{{
						Resample: &filter.ResampleConfig{
							SampleRate:   16_000,
							Channels:     Mono,
							SampleFormat: av.SampleFormatF32,
						},
					}},
				}}},
			},
			want: []string{"does not support the requested sample format", "field=sample_format", "requested=f32", "supported=s16"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != "transform_adapter_incompatible" || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want transform_adapter_incompatible wrapping ErrUnsupportedBuild", err)
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
		stream  StreamIntent
		outputs []destinationSpec
		code    string
		want    []string
	}{
		{
			name: "mixed frame and mux outputs",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				Targets: []string{"frames", "archive.ogg"},
			},
			outputs: []destinationSpec{frameSink, fileOutput},
			code:    "output_kind_mixed",
			want:    []string{"cannot mix sinks and muxed outputs", ".Branches(...)"},
		},
		{
			name: "mux output without encoder",
			stream: StreamIntent{
				Name:    "audio",
				Decode:  true,
				Targets: []string{"archive.ogg"},
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
				intent: Intent{
					Inputs:  []InputIntent{{Name: "input.ogg"}},
					Streams: []StreamIntent{tt.stream},
					Targets: []TargetIntent{{Name: "unused"}},
				},
				outputAttachments: tt.outputs,
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
	packetSink := sinkDestination(SinkFunc("packets", func(context.Context, Message) error { return nil }))
	fileOutput := fileDestination("archive.ogg", io.Discard)
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ogg"}},
			Streams: []StreamIntent{{
				Name:    "audio",
				Decode:  true,
				Encode:  Opus(Bitrate(96_000)),
				Targets: []string{"packets", "archive.ogg"},
			}},
			Targets: []TargetIntent{{Name: "packets"}, {Name: "archive.ogg"}},
		},
		outputAttachments: []destinationSpec{packetSink, fileOutput},
	}
	if err := validateJobStreamOutputKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateJobStreamOutputKindsPass() error = %v", err)
	}
}

func TestShapeErrorsReportExpectedAndActualShape(t *testing.T) {
	tests := []struct {
		name  string
		pass  recipeCompilePass
		state recipeCompileState
		code  string
		want  []string
	}{
		{
			name: "job resize on audio",
			pass: validateJobIntentShapePass(),
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input"}},
					Streams: []StreamIntent{{
						Name:       "audio",
						Select:     StreamSelect{Type: av.MediaAudio},
						Decode:     true,
						Transforms: []TransformSpec{Resize(320, 180)},
						Targets:    []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
			},
			code: "transform_media_mismatch",
			want: []string{"resize applies to video streams", "expected_shape=domain=frame media=video", "actual_shape=domain=frame media=audio"},
		},
		{
			name: "branch resample on video",
			pass: validateBranchCompositionIntentShapePass(),
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input"}},
					Streams: []StreamIntent{{
						Name:       "video",
						Select:     StreamSelect{Type: av.MediaVideo},
						Transforms: []TransformSpec{Resample(48_000, Stereo)},
						Encode:     VP9(Bitrate(2_000_000)),
						Targets:    []string{"web"},
					}},
					Targets: []TargetIntent{{Name: "web"}},
				},
			},
			code: "transform_media_mismatch",
			want: []string{"resample applies to audio branches", "expected_shape=domain=frame media=audio", "actual_shape=domain=frame media=video"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
		stream StreamIntent
		code   string
		want   []string
	}{
		{
			name: "encode after packet annotation",
			stream: StreamIntent{
				Name:   "video",
				Select: StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []StreamOperation{
					{Kind: OpDecode, Decode: VP8()},
					{Kind: OpShape, Component: "shape", Shape: Shape(ShapeDomain(DomainPacket), ShapeMedia(av.MediaVideo))},
					{Kind: OpEncode, Component: string(av.CodecVP9), Encode: VP9(Bitrate(2_000_000))},
				},
				Targets: []string{"web"},
			},
			code: "operation_shape_mismatch",
			want: []string{
				"vp9 cannot consume the current media shape",
				"operation_index=2",
				"expected_shape=domain=frame media=video",
				"actual_shape=domain=packet media=video",
				"keep .Shape(...) annotations in the frame domain before encoders",
			},
		},
		{
			name: "resize after audio annotation",
			stream: StreamIntent{
				Name:   "video",
				Select: StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []StreamOperation{
					{Kind: OpDecode, Decode: VP8()},
					{Kind: OpShape, Component: "shape", Shape: Shape(ShapeMedia(av.MediaAudio))},
					{Kind: OpTransform, Component: filter.FactoryResize, Transform: Resize(640, 360)},
				},
				Targets: []string{"frames"},
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
			stream: StreamIntent{
				Name:   "audio",
				Select: StreamSelect{Type: av.MediaAudio, Codec: av.CodecOpus},
				Operations: []StreamOperation{
					{Kind: OpDecode, Decode: Opus()},
					{Kind: OpCopy, Component: "packet-copy", Encode: Copy()},
				},
				Targets: []string{"packets"},
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
				intent: Intent{
					Inputs:  []InputIntent{{Name: "input"}},
					Streams: []StreamIntent{tt.stream},
					Targets: []TargetIntent{{Name: "web"}, {Name: "frames"}, {Name: "packets"}},
				},
			}
			err := pass.Apply(&state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
		intent: Intent{
			Inputs: []InputIntent{{Name: "input"}},
			Streams: []StreamIntent{{
				Name:   "visualized",
				Select: StreamSelect{Type: av.MediaAudio, Codec: av.CodecOpus},
				Operations: []StreamOperation{
					{Kind: OpDecode, Decode: Opus()},
					{Kind: OpStage, Component: "visualizer", Stage: &runtimeTestStage{name: "visualizer"}},
					{Kind: OpShape, Component: "shape", Shape: FrameShape(av.MediaVideo, ShapeVideo(640, 360, av.PixelFormatYUV420P))},
					{Kind: OpEncode, Component: string(av.CodecVP9), Encode: VP9(Bitrate(600_000))},
				},
				Targets: []string{"web"},
			}},
			Targets: []TargetIntent{{Name: "web"}},
		},
	}
	if err := validateRecipeOperationShapesPass().Apply(&state); err != nil {
		t.Fatalf("validateRecipeOperationShapesPass() error = %v", err)
	}
}

func TestRecipeTargetShapePassRejectsFrameShapeForMuxTarget(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Inputs: []InputIntent{{Name: "input"}},
			Streams: []StreamIntent{{
				Name:   "preview",
				Select: StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []StreamOperation{
					{Kind: OpDecode, Decode: VP8()},
					{Kind: OpStage, Component: "inspect", Stage: &runtimeTestStage{name: "inspect"}},
				},
				Targets: []string{"archive.ivf"},
			}},
			Targets: []TargetIntent{{Name: "archive.ivf"}},
		},
		outputAttachments: []destinationSpec{fileDestination("archive.ivf", io.Discard)},
	}

	err := validateRecipeTargetShapesPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_shape_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_shape_mismatch wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{
		"byte or mux target requires packet-domain media",
		"target=archive.ivf",
		"expected_shape=domain=packet media=video",
		"actual_shape=domain=frame media=video",
		"goav.Sink",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

func TestRecipeTargetShapePassAllowsFrameShapeForSinkTarget(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Inputs: []InputIntent{{Name: "input"}},
			Streams: []StreamIntent{{
				Name:   "preview",
				Select: StreamSelect{Type: av.MediaVideo, Codec: av.CodecVP8},
				Operations: []StreamOperation{
					{Kind: OpDecode, Decode: VP8()},
				},
				Targets: []string{"frames"},
			}},
			Targets: []TargetIntent{{Name: "frames"}},
		},
		outputAttachments: []destinationSpec{sinkDestination(SinkFunc("frames", func(context.Context, Message) error { return nil }))},
	}
	if err := validateRecipeTargetShapesPass().Apply(&state); err != nil {
		t.Fatalf("validateRecipeTargetShapesPass() error = %v", err)
	}
}

func TestRecipeRuntimePassRejectsCustomRuntime(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		runtime:   stubRuntime{},
	}
	err := validateRecipeRuntimePass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime_unsupported wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{"recipe compilation requires a goav runtime", "goav.Default", "goav.New", "goav.Expert"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

func TestRequireGraphPlanSpecPassWrapsUnsupportedRecipeShape(t *testing.T) {
	state := recipeCompileState{
		operation: "build job",
		intent: Intent{
			Name:   "record",
			Inputs: []InputIntent{{Name: "input.ivf"}},
		},
	}

	err := requireGraphPlanSpecPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "recipe_graph_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want recipe_graph_unsupported wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{"recipe intent", "inputs: 1", "targets: 0", "goav.From", ".Copy().To", ".Branches"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
	if state.specReady || state.graphPlan.ready() {
		t.Fatalf("state specReady=%v state graphPlan=%T, want unset after unsupported selection", state.specReady, state.graphPlan)
	}
}

func TestJobStreamAttachmentsPassRejectsInvalidConcreteSteps(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		cause error
		want  []string
	}{
		{
			name: "nil custom stage",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ogg"}},
					Streams: []StreamIntent{{
						Name:    "audio",
						Decode:  true,
						Targets: []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
				chainSteps: []chainStepAttachment{{stepIndex: 0}},
			},
			code:  "stage_missing",
			cause: ErrNilStage,
			want:  []string{".Do(stage)", "goav.FrameFunc"},
		},
		{
			name: "transform attachment mismatch",
			state: recipeCompileState{
				operation: "build job",
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ogg"}},
					Streams: []StreamIntent{{
						Name:       "audio",
						Select:     StreamSelect{Type: av.MediaAudio},
						Decode:     true,
						Transforms: []TransformSpec{Resample(48_000, Stereo)},
						Targets:    []string{"frames"},
					}},
					Targets: []TargetIntent{{Name: "frames"}},
				},
				chainSteps: []chainStepAttachment{{
					hasTransform:   true,
					transformIndex: 1,
					stepIndex:      0,
				}},
			},
			code:  "recipe_attachment_mismatch",
			cause: ErrUnsupportedBuild,
			want:  []string{"transform attachment", "transform index: 1", "intent transforms: 1", "Intent.Transforms"},
		},
	}
	pass := validateJobStreamAttachmentsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
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

func TestJobIntentShapePassRejectsStreamTransforms(t *testing.T) {
	tests := []struct {
		name   string
		stream StreamIntent
		code   string
		want   string
	}{
		{
			name: "invalid resize",
			stream: StreamIntent{
				Name:       "video",
				Select:     StreamSelect{Type: av.MediaVideo},
				Decode:     true,
				Transforms: []TransformSpec{Resize(0, 720)},
				Targets:    []string{"frames"},
			},
			code: "transform_invalid",
			want: "positive width and height",
		},
		{
			name: "wrong media",
			stream: StreamIntent{
				Name:       "audio",
				Select:     StreamSelect{Type: av.MediaAudio},
				Decode:     true,
				Transforms: []TransformSpec{Resize(320, 180)},
				Targets:    []string{"frames"},
			},
			code: "transform_media_mismatch",
			want: "resize applies to video streams",
		},
		{
			name: "empty transform",
			stream: StreamIntent{
				Name:       "video",
				Select:     StreamSelect{Type: av.MediaVideo},
				Decode:     true,
				Transforms: []TransformSpec{{}},
				Targets:    []string{"frames"},
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
				intent: Intent{
					Inputs:  []InputIntent{{Name: "input"}},
					Streams: []StreamIntent{tt.stream},
					Targets: []TargetIntent{{Name: "frames"}},
				},
			}
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

func TestTranscodeIntentShapePassRejectsInvalidPublicShape(t *testing.T) {
	tests := []struct {
		name  string
		state recipeCompileState
		code  string
		want  string
	}{
		{
			name: "input missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						Targets: []string{"web"},
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
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
				},
			},
			code: "stream_missing",
			want: "no audio or video branches are configured",
		},
		{
			name: "branch name missing",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						Targets: []string{"web"},
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
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Decode:  true,
						Encode:  Copy(),
						Targets: []string{"web"},
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
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  Auto(),
						Targets: []string{"web"},
					}},
				},
			},
			code: "encode_auto_unresolved",
			want: "automatic codec selection",
		},
		{
			name: "duplicate branch target",
			state: recipeCompileState{
				operation: branchCompositionOperation,
				intent: Intent{
					Inputs: []InputIntent{{Name: "input.ivf"}},
					Streams: []StreamIntent{{
						Name:    "360p",
						Select:  StreamSelect{Type: av.MediaVideo},
						Encode:  VP9(Bitrate(600_000)),
						Targets: []string{"web", "web"},
					}},
				},
			},
			code: "target_duplicate",
			want: "more than once",
		},
	}
	pass := validateBranchCompositionIntentShapePass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
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
		code  string
		want  string
	}{
		{
			name: "rtp input",
			state: recipeCompileState{
				branchInputAttachment: RTP(&runtimeRTPReceiver{}).Name("video").Codec(VP8()),
				branchTargetAttachments: []namedTargetSpec{{
					name:   "web",
					output: fileDestination("web.ivf", io.Discard),
				}},
			},
			code: "unsupported_input",
			want: "RTP transcode recipes",
		},
		{
			name: "duplicate targets",
			state: recipeCompileState{
				branchInputAttachment: FileInput("input.ivf", strings.NewReader("")),
				branchTargetAttachments: []namedTargetSpec{
					{name: "web", output: fileDestination("web.ivf", io.Discard)},
					{name: "web", output: fileDestination("preview.ivf", io.Discard)},
				},
			},
			code: "target_duplicate",
			want: "defined more than once",
		},
	}
	pass := validateBranchCompositionAttachmentsPass()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pass.Apply(&tt.state)
			var buildErr *BuildError
			if !errors.As(err, &buildErr) || buildErr.Code != tt.code || !errors.Is(err, ErrUnsupportedBuild) {
				t.Fatalf("err = %v, want %s wrapping ErrUnsupportedBuild", err, tt.code)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestTranscodeBranchTargetKindsPassAllowsCopyMuxBranches(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "archive",
				Select:  StreamSelect{Type: av.MediaVideo},
				Encode:  Copy(),
				Targets: []string{"web"},
			}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "web",
			output: fileDestination("web.ivf", io.Discard),
		}},
	}

	if err := validateBranchCompositionIntentShapePass().Apply(&state); err != nil {
		t.Fatalf("validateBranchCompositionIntentShapePass() error = %v", err)
	}
	if err := validateBranchTargetKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchTargetKindsPass() error = %v", err)
	}
}

func TestTranscodeBranchTargetKindsPassAllowsRawSinkBranches(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "preview",
				Select:  StreamSelect{Type: av.MediaVideo},
				Targets: []string{"frames"},
			}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "frames",
			output: sinkDestination(SinkFunc("frames", func(context.Context, Message) error { return nil })),
		}},
	}

	if err := validateBranchTargetKindsPass().Apply(&state); err != nil {
		t.Fatalf("validateBranchTargetKindsPass() error = %v", err)
	}
}

func TestTranscodeBranchTargetKindsPassRejectsRawMuxBranches(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "preview",
				Select:  StreamSelect{Type: av.MediaVideo},
				Targets: []string{"web"},
			}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "web",
			output: fileDestination("web.ivf", io.Discard),
		}},
	}

	err := validateBranchTargetKindsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "encode_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want encode_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "muxed target") || !strings.Contains(err.Error(), "Sink") {
		t.Fatalf("err = %v, want mux and sink guidance", err)
	}
}

func TestTranscodeOutputBindingsPassRejectsUndefinedRoutes(t *testing.T) {
	state := recipeCompileState{
		operation: branchCompositionOperation,
		intent: Intent{
			Inputs: []InputIntent{{Name: "input.ivf"}},
			Streams: []StreamIntent{{
				Name:    "360p",
				Select:  StreamSelect{Type: av.MediaVideo},
				Encode:  VP9(Bitrate(600_000)),
				Targets: []string{"missing"},
			}},
			Targets: []TargetIntent{{Name: "web.ivf"}},
		},
		branchTargetAttachments: []namedTargetSpec{{
			name:   "web",
			output: fileDestination("web.ivf", io.Discard),
		}},
	}

	err := validateBranchTargetBindingsPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "target_missing" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want target_missing wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "destination missing is referenced but not defined") ||
		!strings.Contains(err.Error(), "destination values") {
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
		intent: Intent{Streams: []StreamIntent{{
			Name:    "720p",
			Select:  StreamSelect{Type: av.MediaVideo},
			Encode:  VP9(Bitrate(2_000_000)),
			Targets: []string{"web"},
		}}},
		branchInputProbeReady: true,
		branchInputProbe: format.ProbeResult{
			Format:  av.FormatMatroska,
			Streams: streams,
		},
	}

	err := validateKnownBranchInputStreamSelectionPass().Apply(&state)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
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
	if len(resolved.intent.Targets) != 1 || resolved.intent.Targets[0].Name != "recording.ivf" {
		t.Fatalf("intent targets = %+v", resolved.intent.Targets)
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
				VP9(600_000).
				To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	plan := resolved.graphPlan.mediaPlan()
	if len(resolved.graphPlan.nodes) == 0 || len(resolved.graphPlan.edges) == 0 {
		t.Fatalf("graphPlan nodes=%+v edges=%+v, want planned operation graph", resolved.graphPlan.nodes, resolved.graphPlan.edges)
	}
	if len(plan.Branches) != 1 || plan.Branches[0].Name != "360p" {
		t.Fatalf("graphPlan media plan branches = %+v, want 360p branch", plan.Branches)
	}
	if len(plan.Taps) != 1 || plan.Taps[0].Name != "video.decoded" {
		t.Fatalf("graphPlan media plan taps = %+v, want video.decoded tap", plan.Taps)
	}
	if len(plan.Outputs) != 1 || plan.Outputs[0].Name != "web.ivf" || !reflect.DeepEqual(plan.Outputs[0].BranchRefs, []string{"360p"}) {
		t.Fatalf("graphPlan media plan outputs = %+v, want web.ivf owned by 360p", plan.Outputs)
	}
	operations := resolved.graphPlan.operationPlan()
	for _, want := range []OperationKind{OpDemux, OpSelect, OpDecode, OpTap, OpTransform, OpEncode, OpMux} {
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
	plan := resolved.graphPlan.mediaPlan()
	plan.Branches[0].Operations[0].Component = "mutated"
	plan.Outputs[0].BranchRefs[0] = "mutated"
	operations := resolved.graphPlan.operationPlan()
	operations[len(operations)-1].Targets[0] = "mutated"

	nextSpec := resolved.graphPlan.spec()
	if nextSpec.Nodes[0].Name == "mutated" {
		t.Fatal("graphPlan.spec() returned aliased nodes")
	}
	nextPlan := resolved.graphPlan.mediaPlan()
	if nextPlan.Branches[0].Operations[0].Component == "mutated" {
		t.Fatal("graphPlan.mediaPlan() returned aliased branch operations")
	}
	if nextPlan.Outputs[0].BranchRefs[0] == "mutated" {
		t.Fatal("graphPlan.mediaPlan() returned aliased output branch refs")
	}
	nextOperations := resolved.graphPlan.operationPlan()
	if nextOperations[len(nextOperations)-1].Targets[0] == "mutated" {
		t.Fatal("graphPlan.operationPlan() returned aliased target refs")
	}
}

func graphPlanOperationKindPresent(operations []graphPlanOperation, kind OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func graphPlanOperationNodePresent(operations []graphPlanOperation, node pipeline.NodeRef) bool {
	for i := range operations {
		if operations[i].Node == node {
			return true
		}
	}
	return false
}

func graphPlanOperationTargetPresent(operations []graphPlanOperation, target string) bool {
	for i := range operations {
		for _, next := range operations[i].Targets {
			if next == target {
				return true
			}
		}
	}
	return false
}

func graphPlanOperationsWithoutTargets(operations []graphPlanOperation) []graphPlanOperation {
	out := make([]graphPlanOperation, 0, len(operations))
	for i := range operations {
		if graphPlanOperationTargetsRequired(operations[i].Kind) {
			continue
		}
		out = append(out, operations[i])
	}
	return out
}

func graphPlanOperationsWithoutKind(operations []graphPlanOperation, kind OperationKind) []graphPlanOperation {
	out := make([]graphPlanOperation, 0, len(operations))
	for i := range operations {
		if operations[i].Kind == kind {
			continue
		}
		out = append(out, operations[i])
	}
	return out
}

func graphPlanOperationsWithoutBranch(operations []graphPlanOperation, branch string) []graphPlanOperation {
	out := make([]graphPlanOperation, 0, len(operations))
	for i := range operations {
		if operations[i].Branch == branch {
			continue
		}
		out = append(out, operations[i])
	}
	return out
}

func graphPlanOperationsWithoutBranchTarget(operations []graphPlanOperation, branch string, target string) []graphPlanOperation {
	out := make([]graphPlanOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		if operation.Branch != branch || !graphPlanOperationTargetsRequired(operation.Kind) {
			out = append(out, operation)
			continue
		}
		targets := make([]string, 0, len(operation.Targets))
		for _, next := range operation.Targets {
			if next != target {
				targets = append(targets, next)
			}
		}
		if len(targets) == 0 {
			continue
		}
		operation.Targets = targets
		out = append(out, operation)
	}
	return out
}

func graphPlanOperationsWithBranchTargetNode(operations []graphPlanOperation, branch string, target string, node pipeline.NodeRef) []graphPlanOperation {
	out := cloneGraphPlanOperations(operations)
	for i := range out {
		operation := out[i]
		if operation.Branch != branch || !graphPlanOperationTargetsRequired(operation.Kind) {
			continue
		}
		if !stringInSlice(target, operation.Targets) {
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
		RTP(&runtimeRTPReceiver{streams: []Stream{{
			ID:   "video",
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				ID:   av.CodecVP8,
				Type: av.MediaVideo,
			},
		}}}).Name("video").Codec(VP8()),
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
			{Name: "video", Kind: pipeline.NodeSource, Detail: "rtp receive, codec=vp8"},
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
				VP9(600_000).
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
	if got := resolved.intent.Streams[0].Targets; len(got) != 1 || got[0] != "web.ivf" {
		t.Fatalf("intent route targets = %+v, want [web.ivf]", got)
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
	job := From(RTP(&runtimeRTPReceiver{
		streams: []Stream{audioOpusTestStream()},
	}).Name("audio").Codec(Opus())).
		Audio().
		Branches(
			Branch("voice").Apply(Flow("voice").Audio().OpusVoice()).To(voice),
			Branch("archive").Apply(Flow("archive").Audio().OpusMusic()).To(archive),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.branchInputAttachment.rtp == nil {
		t.Fatal("resolved branch input = nil RTP, want live branch composer input carried on resolved plan")
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
	runtime := New(
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
		Branches(Branch("main").Opus(96_000).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

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
			Branch("web").VP9(2_000_000).To(web),
			Branch("thumb").Resize(320, 180).To(thumbnail),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	operations := resolved.graphPlan.operationPlan()
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
	runtime := New(
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
		Branches(Branch("main").Opus(96_000).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

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
	runtime := New(
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
		Resample(48_000, Stereo).
		Branches(Branch("main").Opus(96_000).To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

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
	runtime := New(
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
			Resample(16_000, Mono).
			Opus(96_000).
			To(destinationHandle(fileDestination("archive.ogg", io.Discard))))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "resample-main", "resample-plan-main")
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, "encode-main", "encode-plan-main")
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

func TestBranchComposeLowererUsesPlanTargetOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
			Branch("archive").Opus(96_000).To(archive),
			Branch("frames").To(frames),
		)

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	archiveNode, ok := graphPlanTargetOperationNode(resolved.graphPlan.operations, "archive.ogg")
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want archive.ogg destination operation", resolved.graphPlan.operations)
	}
	framesNode, ok := graphPlanTargetOperationNode(resolved.graphPlan.operations, "frames")
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want frames target operation", resolved.graphPlan.operations)
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
		t.Fatalf("planned = %+v, want renamed target nodes", planned)
	}
}

func graphPlanTargetOperationNode(operations []graphPlanOperation, target string) (pipeline.NodeRef, bool) {
	for i := range operations {
		if !graphPlanOperationTargetsRequired(operations[i].Kind) {
			continue
		}
		for _, next := range operations[i].Targets {
			if next == target {
				return operations[i].Node, true
			}
		}
	}
	return "", false
}

func graphPlanOperationNode(operations []graphPlanOperation, kind OperationKind) (pipeline.NodeRef, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return operations[i].Node, true
		}
	}
	return "", false
}

func renameResolvedGraphPlanOperationNode(t *testing.T, resolved recipeResolved, kind OperationKind, name string) recipeResolved {
	t.Helper()
	node, ok := graphPlanOperationNode(resolved.graphPlan.operations, kind)
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want %s operation", resolved.graphPlan.operations, kind)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, node.String(), name)
	return resolved
}

func renameResolvedGraphPlanTargetNode(t *testing.T, resolved recipeResolved, target string, name string) recipeResolved {
	t.Helper()
	node, ok := graphPlanTargetOperationNode(resolved.graphPlan.operations, target)
	if !ok {
		t.Fatalf("graphPlan operations = %+v, want target operation %q", resolved.graphPlan.operations, target)
	}
	resolved.graphPlan = renameGraphPlanNodeRef(resolved.graphPlan, node.String(), name)
	return resolved
}

func renameGraphPlanNodeRef(plan graphPlan, oldName string, newName string) graphPlan {
	oldRef := pipeline.NodeRef(oldName)
	newRef := pipeline.NodeRef(newName)
	for i := range plan.nodes {
		if plan.nodes[i].Name == oldName {
			plan.nodes[i].Name = newName
		}
	}
	for i := range plan.edges {
		if plan.edges[i].From == oldRef {
			plan.edges[i].From = newRef
		}
		if plan.edges[i].To == oldRef {
			plan.edges[i].To = newRef
		}
	}
	for i := range plan.operations {
		if plan.operations[i].Node == oldRef {
			plan.operations[i].Node = newRef
		}
	}
	for i := range plan.taps {
		if plan.taps[i].Node == oldRef {
			plan.taps[i].Node = newRef
		}
	}
	return plan
}

func TestBranchComposeLowererRequiresBranchOperationsBeforeSources(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	mobile := destinationHandle(fileDestination("mobile.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			Branch("720p").Resize(1280, 720).VP9(2_000_000).To(web),
			Branch("360p").Resize(640, 360).VP8(600_000).To(mobile),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutBranch(resolved.graphPlan.operations, "360p")
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
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
			Branch("720p").Resize(1280, 720).VP9(2_000_000).To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutKind(resolved.graphPlan.operations, OpDecode)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "branch composition graph plan has no decode operation for branch") {
		t.Fatalf("err = %v, want missing decode-operation graph-plan error", err)
	}
}

func TestBranchComposeLowererRequiresTargetOperationsBeforeSources(t *testing.T) {
	web := destinationHandle(fileDestination("web.ivf", io.Discard))
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Branches(
			Branch("720p").Resize(1280, 720).VP9(2_000_000).To(web),
		)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutTargets(resolved.graphPlan.operations)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "branch composition graph plan has no target operations") {
		t.Fatalf("err = %v, want missing target-operation graph-plan error", err)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanPacketCopy(t *testing.T) {
	job := From(
		RTP(&runtimeRTPReceiver{
			streams: []Stream{{
				ID:   "video",
				Type: av.MediaVideo,
				Codec: av.CodecParameters{
					ID:   av.CodecVP8,
					Type: av.MediaVideo,
				},
			}},
		}).Name("video").Codec(VP8()),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard)))

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

func TestStreamGraphLowererUsesPlanPacketCopyTargetOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		t.Fatalf("planned = %+v, want renamed packet-copy target nodes", planned)
	}
}

func TestSelectedPacketCopyLowererUsesPlanSelectOperationNode(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, OpSelect, "select-plan-audio")
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

func TestPacketCopyLowererRequiresTargetOperationsBeforeSources(t *testing.T) {
	job := From(
		FileInput("input.ivf", strings.NewReader("")),
	).Copy().To(destinationHandle(fileDestination("recording.ivf", io.Discard)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutTargets(resolved.graphPlan.operations)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "packet-copy graph plan has no target operations") {
		t.Fatalf("err = %v, want missing target-operation graph-plan error", err)
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
	resolved.graphPlan.operations = graphPlanOperationsWithoutKind(resolved.graphPlan.operations, OpCopy)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "packet-copy graph plan has no copy operation for branch") {
		t.Fatalf("err = %v, want missing copy-operation graph-plan error", err)
	}
}

func TestPacketCopyLowererRequiresTargetBranchBindingsBeforeSources(t *testing.T) {
	job := From(
		RTP(&runtimeRTPReceiver{streams: []Stream{audioOpusTestStream()}}).Name("left").Codec(Opus()),
	).And(
		RTP(&runtimeRTPReceiver{streams: []Stream{audioOpusTestStream()}}).Name("right").Codec(Opus()),
	).Copy().To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutBranchTarget(resolved.graphPlan.operations, "right", "packets")
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "packet-copy target operation branches do not match output branches") {
		t.Fatalf("err = %v, want packet-copy target branch binding graph-plan error", err)
	}
}

func TestPacketCopyLowererRequiresConsistentTargetOperationsBeforeSources(t *testing.T) {
	job := From(
		RTP(&runtimeRTPReceiver{streams: []Stream{audioOpusTestStream()}}).Name("left").Codec(Opus()),
	).And(
		RTP(&runtimeRTPReceiver{streams: []Stream{audioOpusTestStream()}}).Name("right").Codec(Opus()),
	).Copy().To(Sink(&runtimeTestSink{name: "packets"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithBranchTargetNode(resolved.graphPlan.operations, "right", "packets", "packets-right")
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "packet-copy target operation is not consistent across branches") {
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

	streams, err := packetCopyTargetStreams(graphPlanTargetOperation{
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
	runtime := New(
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
	runtime := New(
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

func TestStreamGraphLowererUsesPlanDecodedSinkTargetOperationNode(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		t.Fatalf("planned = %+v, want renamed decoded sink target node", planned)
	}
}

func TestStreamGraphLowererUsesPlanSelectDecodeFilterOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		Resample(16_000, Mono).
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, OpSelect, "select-plan-audio")
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, OpDecode, "decode-plan-audio")
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, OpTransform, "resample-plan-audio")
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
	resolved.graphPlan.operations = graphPlanOperationsWithoutKind(resolved.graphPlan.operations, OpDecode)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "frame stream graph plan has no decode operation") {
		t.Fatalf("err = %v, want missing decode-operation graph-plan error", err)
	}
}

func TestFrameStreamLowererRequiresTargetOperationsBeforeSources(t *testing.T) {
	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		To(Sink(&runtimeTestSink{name: "frames"}))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutTargets(resolved.graphPlan.operations)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "frame stream graph plan has no target operations") {
		t.Fatalf("err = %v, want missing target-operation graph-plan error", err)
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
	resolved.graphPlan.operations = append(resolved.graphPlan.operations, graphPlanOperation{
		Branch: "other",
		Node:   "select-other",
		Kind:   OpSelect,
	})
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "frame stream graph plan must have exactly one branch operation set") {
		t.Fatalf("err = %v, want single-branch graph-plan error", err)
	}
}

func TestRecipeResolvedMediaPlanSinkDestinationPreservesCustomStage(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{streams: []Stream{stream}}
	runtime := New(withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}})))
	job := From(RTP(receiver).Name("audio").Codec(Opus())).UseRuntime(runtime).
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
	runtime := New(
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
	runtime := New(
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
	resolved.graphPlan.operations = graphPlanOperationsWithoutKind(resolved.graphPlan.operations, OpSelect)
	task, err := resolved.Build(ctx)
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "selected packet-copy graph plan has no select operation") {
		t.Fatalf("err = %v, want missing select-operation graph-plan error", err)
	}
}

func TestSelectedPacketCopyLowererRequiresCopyOperationBeforeSources(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
	resolved.graphPlan.operations = graphPlanOperationsWithoutKind(resolved.graphPlan.operations, OpCopy)
	task, err := resolved.Build(ctx)
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "selected packet-copy graph plan has no copy operation") {
		t.Fatalf("err = %v, want selected copy-operation graph-plan error", err)
	}
}

func TestSelectedPacketCopyLowererRequiresSingleBranchOperationSet(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
	resolved.graphPlan.operations = append(resolved.graphPlan.operations, graphPlanOperation{
		Branch: "other",
		Node:   "select-other",
		Kind:   OpSelect,
	})
	task, err := resolved.Build(ctx)
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "selected packet-copy graph plan must have exactly one branch operation set") {
		t.Fatalf("err = %v, want single-branch graph-plan error", err)
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeOutput(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		Opus(96_000).
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

func TestStreamGraphLowererUsesPlanEncodedTargetOperationNodes(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		Opus(96_000).
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
		t.Fatalf("planned = %+v, want renamed encoded target nodes", planned)
	}
}

func TestStreamGraphLowererUsesPlanEncodeOperationNode(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		Opus(96_000).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	resolved = renameResolvedGraphPlanOperationNode(t, resolved, OpEncode, "encode-plan-audio")
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
		Opus(96_000).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	resolved.graphPlan.operations = graphPlanOperationsWithoutKind(resolved.graphPlan.operations, OpEncode)
	task, err := resolved.Build(context.Background())
	if err == nil {
		task.Close()
		t.Fatal("resolved.Build() error = nil, want graph_plan_invalid")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "encoded frame stream graph plan has no encode operation") {
		t.Fatalf("err = %v, want missing encode-operation graph-plan error", err)
	}
}

func TestMediaPlanDirectStreamUsesResolvedAttachments(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	runtime := New(
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
		Opus(96_000).
		To(destinationHandle(fileDestination("archive.ogg", io.Discard)))

	resolved, err := compileJobRecipeForBuildContext(ctx, job)
	if err != nil {
		t.Fatalf("compileJobRecipeForBuildContext() error = %v", err)
	}
	stream, ok := resolved.singleStreamIntent()
	if !ok {
		t.Fatalf("resolved intent streams = %+v, want one stream", resolved.intent.Streams)
	}
	plan, ok, err := newMediaPlanDecodeStreamGraph(resolved.runtime, resolved.inputAttachments, resolved.outputAttachments, stream)
	if err != nil || !ok {
		t.Fatalf("newMediaPlanDecodeStreamGraph ok=%v err=%v", ok, err)
	}
	spec, err := plan.encodeOutputSpec()
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
	if len(resolved.chainAttachments) == 0 {
		t.Fatalf("resolved stream attachments are empty; taps and custom stages should be carried on the resolved recipe")
	}
}

func TestRecipeResolvedBuildUsesMediaPlanFileEncodeSinkDestination(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	runtime := New(
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
		Opus(96_000).
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
	runtime := New(
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
		Opus(96_000).
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
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{streams: []Stream{stream}}
	runtime := New(
		withTestFormats(
			testFormatProber(remuxTestProber{}),
			testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{}),
		),
		withTestCodecs(
			testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
			testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		),
	)
	job := From(RTP(receiver).Name("audio").Codec(Opus())).UseRuntime(runtime).
		Audio().
		Decode().
		Opus(96_000).
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
