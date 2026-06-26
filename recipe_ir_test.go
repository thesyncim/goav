package goav

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

func TestRecipeIRSnapshotRoundTripsJobIntent(t *testing.T) {
	var out bytes.Buffer
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Tap(FrameTap("frames")).
		Resize(320, 180).
		Encode(codec.VP8()).
		To(Write("output.ivf", &out))

	snapshot := newJobRecipeSnapshot(job)
	if snapshot.recipe.Kind != recipeir.KindJob {
		t.Fatalf("recipe kind = %q, want %q", snapshot.recipe.Kind, recipeir.KindJob)
	}
	if !recipeIRHasOperationKind(snapshot.recipe, plan.OpDecode) ||
		!recipeIRHasOperationKind(snapshot.recipe, plan.OpTransform) ||
		!recipeIRHasOperationKind(snapshot.recipe, plan.OpEncode) ||
		!recipeIRHasFrameTap(snapshot.recipe) {
		t.Fatalf("recipe IR did not capture decode/tap/transform/encode operations: %+v", snapshot.recipe.Streams)
	}
	if len(snapshot.recipe.Inputs) != 1 ||
		snapshot.recipe.Inputs[0].Kind != recipeir.InputKindByteStream {
		t.Fatalf("recipe IR inputs = %+v, want file-like byte-stream kind", snapshot.recipe.Inputs)
	}
	transform := recipeIRTransformForTest(t, snapshot.recipe, plan.OpTransform)
	if transform.Kind != recipeir.TransformResize ||
		transform.Resize.Width != 320 ||
		transform.Resize.Height != 180 {
		t.Fatalf("recipe IR transform = %+v, want typed resize config", transform)
	}
	if len(snapshot.recipe.Destinations) != 1 ||
		snapshot.recipe.Destinations[0].Kind != recipeir.DestinationKindByteStream {
		t.Fatalf("recipe IR destinations = %+v, want writer-backed byte-stream kind", snapshot.recipe.Destinations)
	}
	if got, want := intentFromRecipeIR(snapshot.recipe), job.plan(); !reflect.DeepEqual(got, want) {
		t.Fatalf("IR round trip drifted\n got: %#v\nwant: %#v", got, want)
	}
}

func TestRecipeIRSnapshotCapturesCustomSourceShape(t *testing.T) {
	input := Source("camera", shape.Frame(av.MediaVideo), SourceFunc(func(context.Context, source.Push) error { return nil }))
	snapshot := newJobRecipeSnapshot(From(input))
	if len(snapshot.recipe.Inputs) != 1 ||
		snapshot.recipe.Inputs[0].Kind != recipeir.InputKindCustomSource ||
		snapshot.recipe.Inputs[0].SourceShape.Domain != shape.DomainFrame ||
		snapshot.recipe.Inputs[0].SourceShape.MediaKind != av.MediaVideo {
		t.Fatalf("recipe IR inputs = %+v, want custom source frame shape", snapshot.recipe.Inputs)
	}

	state := recipeCompileStateFromSnapshot(snapshot, recipeCompileOptions{})
	state.inputAttachments = nil
	spec, ok := compileStateCustomSourceShape(&state)
	if !ok || spec.Domain != shape.DomainFrame || spec.MediaKind != av.MediaVideo {
		t.Fatalf("compile state source shape = %+v, %v; want IR-backed video frame shape", spec, ok)
	}
}

func TestRecipeIRSnapshotCapturesStreamRules(t *testing.T) {
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		OnStream(
			MatchMedia(av.MediaAudio),
			Branch("record").Apply(Flow("audio").Audio()).Copy().Buffer(flow.DropOldest(4)).To(Sink(SinkFunc("late", func(context.Context, Message) error { return nil }))),
		)

	snapshot := newJobRecipeSnapshot(job)
	if len(snapshot.recipe.StreamRules) != 1 {
		t.Fatalf("stream rule count = %d, want 1", len(snapshot.recipe.StreamRules))
	}
	rule := snapshot.recipe.StreamRules[0]
	if rule.MatchDescription != "media=audio" ||
		len(rule.Branches) != 1 ||
		rule.Branches[0].Name != "record" ||
		rule.Branches[0].Media != av.MediaAudio ||
		len(rule.Branches[0].Operations) != 1 ||
		rule.Branches[0].Operations[0].Kind != plan.OpCopy ||
		rule.Branches[0].Buffer.Mode != flow.BufferDropOldest ||
		rule.Branches[0].Buffer.Capacity != 4 ||
		!reflect.DeepEqual(rule.Branches[0].Destinations, []string{"late"}) {
		t.Fatalf("recipe IR stream rule = %+v", rule)
	}

	state := recipeCompileStateFromSnapshot(snapshot, recipeCompileOptions{})
	snapshot.recipe.StreamRules[0].Branches[0].Operations[0].Kind = plan.OpDecode
	if state.streamRuleFacts[0].Branches[0].Operations[0].Kind != plan.OpCopy {
		t.Fatalf("compile state stream rule operation mutated through snapshot: %+v", state.streamRuleFacts[0].Branches[0].Operations)
	}
	state.streamRules = nil
	state.inputAttachments = nil
	if err := validateStreamRulesPass().Apply(&state); err != nil {
		t.Fatalf("validate stream rules from IR facts: %v", err)
	}
	decisions := explainStreamRuleFacts(snapshot.recipe.StreamRules)
	if len(decisions) != 1 ||
		decisions[0].Branch != "record-<stream>" ||
		!strings.Contains(decisions[0].Message, "media=audio") ||
		!strings.Contains(decisions[0].Message, "late") {
		t.Fatalf("stream rule decisions = %+v", decisions)
	}
}

func TestRecipeIRSnapshotCapturesJoinFacts(t *testing.T) {
	job := Mix(
		From(FileInput("a.ogg", strings.NewReader(""))).Audio(),
		From(FileInput("b.ogg", strings.NewReader(""))).Audio(),
	).Encode(codec.Opus()).To(Write("mixed.ogg", &bytes.Buffer{}))

	snapshot := newJobRecipeSnapshot(job)
	if snapshot.recipe.Kind != recipeir.KindJoin {
		t.Fatalf("recipe kind = %q, want %q", snapshot.recipe.Kind, recipeir.KindJoin)
	}
	join := snapshot.recipe.Join
	if join.Kind != "mix" ||
		join.ArmCount != 2 ||
		join.InputCount != 2 ||
		join.DestinationCount != 1 ||
		!join.HasEncode {
		t.Fatalf("recipe IR join = %+v", join)
	}
	state := recipeCompileStateFromSnapshot(snapshot, recipeCompileOptions{})
	state.joinTree = nil
	if err := validateJoinRecipePass().Apply(&state); err != nil {
		t.Fatalf("validate join from IR facts: %v", err)
	}
}

func TestJoinPlannerConsumesRecipeIRInput(t *testing.T) {
	job := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("mixed", func(context.Context, Message) error { return nil })))

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	state.inputAttachments = nil
	lowerer, ok, err := mediaPlanJoinLowererForState(&state)
	if err != nil {
		t.Fatalf("plan join from IR input facts: %v", err)
	}
	if !ok {
		t.Fatal("join lowerer was not selected")
	}
	planned, ok := lowerer.(*joinPlan)
	if !ok {
		t.Fatalf("join lowerer = %T, %v; want *joinPlan", lowerer, ok)
	}
	if len(planned.arms) != 2 ||
		planned.arms[0].inputName != "a" ||
		planned.arms[1].inputName != "b" ||
		planned.arms[0].domain != shape.DomainFrame ||
		planned.arms[1].domain != shape.DomainFrame {
		t.Fatalf("join arms = %+v, want IR-derived frame inputs a and b", planned.arms)
	}

	spec, err := planned.spec()
	if err != nil {
		t.Fatalf("join spec: %v", err)
	}
	workFromLowerer := buildWorkPlan(&state, spec, planned)
	if len(workFromLowerer.Inputs) != 2 || len(workFromLowerer.Destinations) != 1 {
		t.Fatalf("join work from selected lowerer inputs=%+v destinations=%+v, want planned join work", workFromLowerer.Inputs, workFromLowerer.Destinations)
	}
	workInput := joinWorkPlanInputFromCompileState(&state)
	state.intent.Inputs = nil
	state.intent.Destinations = nil
	work := planned.buildJoinWorkPlan(workInput, spec)
	if len(work.Inputs) != 2 || len(work.Destinations) != 1 {
		t.Fatalf("join work plan inputs=%+v destinations=%+v, want captured handoff data", work.Inputs, work.Destinations)
	}
}

func TestNormalWorkPlanConsumesHandoff(t *testing.T) {
	var out bytes.Buffer
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		Resize(320, 180).
		Encode(codec.VP8()).
		To(Write("output.ivf", &out))

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	state.shapeDiagnostics = []plan.Diagnostic{{
		Code:    "captured",
		Details: []string{"before"},
	}}
	state.intent.Inputs[0].Name = "mutated-input"
	state.intent.Streams = nil
	state.intent.Destinations = nil
	input := workPlanInputFromCompileState(&state)

	state.operation = "mutated"
	state.intent.Inputs = nil
	state.intent.Streams = nil
	state.intent.Destinations = nil
	state.shapeDiagnostics[0].Details[0] = "after"
	state.shapeDiagnostics = []plan.Diagnostic{{Code: "mutated"}}

	work := buildNormalWorkPlan(input, pipeline.Spec{Name: "goav-test"})
	if len(work.Inputs) != 1 ||
		work.Inputs[0].Name != "input.ivf" ||
		len(work.Streams) != 1 ||
		len(work.Branches) != 1 ||
		work.Branches[0].Input != "input.ivf" ||
		len(work.Destinations) != 1 ||
		work.Destinations[0].Name != "output.ivf" {
		t.Fatalf("normal work plan = inputs:%+v streams:%+v branches:%+v destinations:%+v, want captured handoff data",
			work.Inputs, work.Streams, work.Branches, work.Destinations)
	}
	if len(work.Diagnostics) != 1 ||
		work.Diagnostics[0].Code != "captured" ||
		!reflect.DeepEqual(work.Diagnostics[0].Details, []string{"before"}) {
		t.Fatalf("work diagnostics = %+v, want cloned captured diagnostics", work.Diagnostics)
	}
}

func TestMultiStreamGraphLowererConsumesHandoff(t *testing.T) {
	job := From(
		mixTestAudioSource("mic-a", 11, 22),
		mixTestAudioSource("mic-b", 33, 44),
	).
		Audio(InputName("mic-a")).To(Sink(SinkFunc("sink-a", func(context.Context, Message) error { return nil }))).
		Audio(InputName("mic-b")).To(Sink(SinkFunc("sink-b", func(context.Context, Message) error { return nil })))

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	input, ok := mediaPlanMultiStreamJobInputFromCompileState(&state)
	if !ok {
		t.Fatal("multi-stream graph lowerer input was not selected")
	}

	state.runtime = nil
	state.intent.Streams = nil
	state.inputAttachments = nil
	state.outputAttachments = nil
	state.outputDestinationNames = nil

	lowerer, ok, err := newMediaPlanMultiStreamJobLowerer(input)
	if err != nil {
		t.Fatalf("lower multi-stream job from captured input: %v", err)
	}
	if !ok {
		t.Fatal("multi-stream graph lowerer was not built from captured input")
	}
	graph, ok := lowerer.(mediaPlanBranchComposeGraph)
	if !ok {
		t.Fatalf("lowerer = %T, want mediaPlanBranchComposeGraph", lowerer)
	}
	if len(graph.inputs) != 2 ||
		len(graph.plan.Branches) != 2 ||
		len(graph.plan.Destinations) != 2 ||
		graph.plan.Branches[0].Input != "mic-a" ||
		graph.plan.Branches[1].Input != "mic-b" {
		t.Fatalf("multi-stream graph = inputs:%+v branches:%+v destinations:%+v, want captured handoff data",
			graph.inputs, graph.plan.Branches, graph.plan.Destinations)
	}
}

func TestSingleStreamGraphLowerersConsumeHandoffs(t *testing.T) {
	t.Run("packet copy", func(t *testing.T) {
		var out bytes.Buffer
		job := From(FileInput("input.ivf", strings.NewReader(""))).
			Video().
			Copy().
			To(Write("output.ivf", &out))

		state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
		state.intent.Streams = nil
		input, ok := mediaPlanPacketCopyStreamInputFromCompileState(&state)
		if !ok {
			t.Fatal("packet-copy graph lowerer input was not selected")
		}
		state.inputAttachments = nil
		state.outputAttachments = nil

		lowerer, ok, err := newMediaPlanPacketCopyStreamLowerer(input)
		if err != nil {
			t.Fatalf("lower packet-copy stream from captured input: %v", err)
		}
		if !ok {
			t.Fatal("packet-copy graph lowerer was not built from captured input")
		}
		graph, ok := lowerer.(mediaPlanStreamGraph)
		if !ok {
			t.Fatalf("lowerer = %T, want mediaPlanStreamGraph", lowerer)
		}
		if !graph.copyPackets ||
			len(graph.inputs) != 1 ||
			len(graph.outputs) != 1 ||
			graph.stream.Name != "video" {
			t.Fatalf("packet-copy graph = %+v, want captured packet-copy stream data", graph)
		}
	})

	t.Run("decode", func(t *testing.T) {
		job := From(mixTestAudioSource("mic", 11, 22)).
			Audio().
			To(Sink(SinkFunc("frames", func(context.Context, Message) error { return nil })))

		state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
		state.intent.Streams = nil
		input, ok := mediaPlanDecodeStreamInputFromCompileState(&state)
		if !ok {
			t.Fatal("decode graph lowerer input was not selected")
		}
		state.inputAttachments = nil
		state.outputAttachments = nil

		lowerer, ok, err := newMediaPlanDecodeStreamLowerer(input)
		if err != nil {
			t.Fatalf("lower decode stream from captured input: %v", err)
		}
		if !ok {
			t.Fatal("decode graph lowerer was not built from captured input")
		}
		graph, ok := lowerer.(mediaPlanStreamGraph)
		if !ok {
			t.Fatalf("lowerer = %T, want mediaPlanStreamGraph", lowerer)
		}
		if graph.copyPackets ||
			len(graph.inputs) != 1 ||
			len(graph.outputs) != 1 ||
			graph.stream.Name != "audio" ||
			graph.sourceDomain != shape.DomainFrame {
			t.Fatalf("decode graph = %+v, want captured frame-domain stream data", graph)
		}
	})
}

func TestBranchComposerGraphLowererConsumesHandoff(t *testing.T) {
	job := From(mixTestAudioSource("mic", 11, 22)).
		Audio().
		Branches(Branch("monitor").To(Sink(SinkFunc("frames", func(context.Context, Message) error { return nil }))))

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	input, ok := mediaPlanBranchComposerInputFromCompileState(&state)
	if !ok {
		t.Fatal("branch-composer graph lowerer input was not selected")
	}

	state.runtime = nil
	state.branchInputAttachment = InputSpec{}
	state.plan = branchComposePlan{}

	lowerer, ok, err := newMediaPlanBranchComposerLowerer(input)
	if err != nil {
		t.Fatalf("lower branch composition from captured input: %v", err)
	}
	if !ok {
		t.Fatal("branch-composer graph lowerer was not built from captured input")
	}
	graph, ok := lowerer.(mediaPlanBranchComposeGraph)
	if !ok {
		t.Fatalf("lowerer = %T, want mediaPlanBranchComposeGraph", lowerer)
	}
	if len(graph.inputs) != 1 ||
		len(graph.plan.Branches) != 1 ||
		graph.plan.Branches[0].Name != "monitor" ||
		len(graph.plan.Destinations) != 1 {
		t.Fatalf("branch-compose graph = inputs:%+v branches:%+v destinations:%+v, want captured handoff data",
			graph.inputs, graph.plan.Branches, graph.plan.Destinations)
	}
}

func TestMediaPlannerConsumesRecipeIRInputFacts(t *testing.T) {
	job := From(
		compositeTestVideoSource("camera", 4, 4, 100, 10, 20),
		mixTestAudioSource("mic", 1),
	).
		Audio(InputName("mic")).
		To(Sink(SinkFunc("audio", func(context.Context, Message) error { return nil })))

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	state.inputAttachments = nil
	stream := state.intent.Streams[0]
	input, inputName := planStreamInputBinding(&state, stream)
	if input.Name != "mic" || inputName != "mic" {
		t.Fatalf("input binding = %+v %q, want mic from IR input facts", input, inputName)
	}
	selected, ok := planSelectedStream(&state, stream)
	if !ok || selected.ID != "mic" || selected.Type != av.MediaAudio {
		t.Fatalf("selected stream = %+v %v, want mic audio from IR input facts", selected, ok)
	}
	branches, _ := planBranchesFromRecipeIR(&state, state.recipe, planOutputsFromRecipeIR(state.recipe.Destinations, nil))
	if len(branches) != 1 || branches[0].Input != "mic" {
		t.Fatalf("planned branches = %+v, want branch bound to mic", branches)
	}
}

func TestCopyPlannerConsumesRecipeIRInputFacts(t *testing.T) {
	job := From(Source("events", shape.Event(), SourceFunc(func(context.Context, source.Push) error { return nil }))).
		To(Sink(SinkFunc("events", func(context.Context, Message) error { return nil })))

	state := recipeCompileStateFromSnapshot(newJobRecipeSnapshot(job), recipeCompileOptions{})
	state.inputAttachments = nil
	branches, decisions := planCopyBranchesFromRecipeIR(&state, state.recipe, planOutputsFromRecipeIR(state.recipe.Destinations, nil))
	if len(branches) != 1 || branches[0].Shape.Domain != shape.DomainEvent {
		t.Fatalf("copy branches = %+v, want event source shape from IR input facts", branches)
	}
	if len(decisions) != 1 || decisions[0].Code != diagnosticEventSource {
		t.Fatalf("copy decisions = %+v, want event source decision", decisions)
	}
}

func TestRecipeIRSnapshotCapturesSinkDestinationKind(t *testing.T) {
	job := From(FileInput("input.ivf", strings.NewReader(""))).
		Video().
		Decode().
		To(Sink(SinkFunc("frames", func(context.Context, Message) error { return nil })))

	snapshot := newJobRecipeSnapshot(job)
	if len(snapshot.recipe.Destinations) != 1 ||
		snapshot.recipe.Destinations[0].Kind != recipeir.DestinationKindSink {
		t.Fatalf("recipe IR destinations = %+v, want sink kind", snapshot.recipe.Destinations)
	}
}

func TestRecipeDestinationShapeUsesIRKind(t *testing.T) {
	frameShape := shape.Frame(av.MediaVideo)
	if err := validateRecipeDestinationShape("build job", "video", "frames", recipeir.DestinationKindSink, fileDestination("frames.ivf", &bytes.Buffer{}), frameShape); err != nil {
		t.Fatalf("sink IR kind should accept frame shape even when concrete fallback looks byte-stream: %v", err)
	}
	if err := validateRecipeDestinationShape("build job", "video", "frames", recipeir.DestinationKindByteStream, destinationSpec{}, frameShape); err == nil {
		t.Fatal("byte-stream IR kind should reject frame shape even when concrete fallback is empty")
	}
}

func TestRecipeIROperationTransformIsTyped(t *testing.T) {
	field, ok := reflect.TypeOf(recipeir.Operation{}).FieldByName("Transform")
	if !ok {
		t.Fatal("recipeir.Operation.Transform missing")
	}
	if field.Type != reflect.TypeOf(recipeir.Transform{}) {
		t.Fatalf("recipeir.Operation.Transform type = %s, want recipeir.Transform", field.Type)
	}
}

func recipeIRTransformForTest(t *testing.T, recipe recipeir.Recipe, kind plan.OperationKind) recipeir.Transform {
	t.Helper()
	for i := range recipe.Streams {
		for j := range recipe.Streams[i].Operations {
			if recipe.Streams[i].Operations[j].Kind == kind {
				return recipe.Streams[i].Operations[j].Transform
			}
		}
	}
	t.Fatalf("operation kind %s not found in recipe %+v", kind, recipe)
	return recipeir.Transform{}
}
