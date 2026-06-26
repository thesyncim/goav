package goav

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/recipeir"
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
			Branch("record").Copy().To(Sink(SinkFunc("late", func(context.Context, Message) error { return nil }))),
		)

	snapshot := newJobRecipeSnapshot(job)
	if len(snapshot.recipe.StreamRules) != 1 {
		t.Fatalf("stream rule count = %d, want 1", len(snapshot.recipe.StreamRules))
	}
	rule := snapshot.recipe.StreamRules[0]
	if rule.MatchDescription != "media=audio" ||
		len(rule.Branches) != 1 ||
		rule.Branches[0].Name != "record" ||
		!reflect.DeepEqual(rule.Branches[0].Destinations, []string{"late"}) {
		t.Fatalf("recipe IR stream rule = %+v", rule)
	}

	state := recipeCompileStateFromSnapshot(snapshot, recipeCompileOptions{})
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
	state.joinAttachment = nil
	if err := validateJoinRecipePass().Apply(&state); err != nil {
		t.Fatalf("validate join from IR facts: %v", err)
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

func TestRecipeCompileEntryPointUsesRecipeIRBoundary(t *testing.T) {
	body, err := os.ReadFile("recipe_compile.go")
	if err != nil {
		t.Fatal(err)
	}
	compileBody := sourceFunctionBody(t, string(body), "compileJobRecipeWithOptions")
	for _, required := range []string{
		"newJobRecipeSnapshot(job)",
		"compileRecipeSnapshotWithOptions",
	} {
		if !strings.Contains(compileBody, required) {
			t.Fatalf("compileJobRecipeWithOptions should call %s", required)
		}
	}
	for _, forbidden := range []string{
		"job.plan()",
		"job.runtimeOrNil()",
		"job.join",
		"job.branchStreams",
		"job.inputs",
		"job.outputs",
		"job.streamRules",
	} {
		if strings.Contains(compileBody, forbidden) {
			t.Fatalf("compileJobRecipeWithOptions still reaches through builder internals with %q", forbidden)
		}
	}
}

func TestJoinPlannerUsesRecipeIRInputFacts(t *testing.T) {
	body, err := os.ReadFile("join_build.go")
	if err != nil {
		t.Fatal(err)
	}
	planBody := sourceFunctionBody(t, string(body), "newJoinPlan")
	if !strings.Contains(planBody, "jobInputStreamSetsFromRecipeIR(state.intent.Inputs, state.inputFacts, state.inputProbes)") {
		t.Fatal("newJoinPlan should derive stream sets from recipe IR input facts")
	}
	if strings.Contains(planBody, "jobInputStreamSets(state.intent.Inputs, state.inputAttachments") {
		t.Fatal("newJoinPlan still derives stream sets from concrete input attachments")
	}
}

func sourceFunctionBody(t *testing.T, source string, name string) string {
	t.Helper()
	start := strings.Index(source, "func "+name+"(")
	if start < 0 {
		t.Fatalf("could not find %s", name)
	}
	open := strings.Index(source[start:], "{")
	if open < 0 {
		t.Fatalf("could not find %s body", name)
	}
	open += start
	depth := 0
	for i := open; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[open : i+1]
			}
		}
	}
	t.Fatalf("could not parse %s body", name)
	return ""
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
