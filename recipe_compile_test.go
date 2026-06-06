package goav

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestRecipeCompileStateDoesNotCarryRecipeBuilders(t *testing.T) {
	stateType := reflect.TypeOf(recipeCompileState{})
	forbidden := map[reflect.Type]string{
		reflect.TypeOf((*Job)(nil)):          "*Job",
		reflect.TypeOf((*TranscodeJob)(nil)): "*TranscodeJob",
		reflect.TypeOf([]streamBuild(nil)):   "[]streamBuild",
	}
	for i := 0; i < stateType.NumField(); i++ {
		field := stateType.Field(i)
		if name, ok := forbidden[field.Type]; ok {
			t.Fatalf("recipeCompileState field %s carries %s; compiler passes should use captured intent attachments", field.Name, name)
		}
	}
}

func TestCompileJobRecipeCarriesIntentAndBuilder(t *testing.T) {
	job := Record(
		FileInput("input.ivf", strings.NewReader("")),
		FileOutput("recording.ivf", io.Discard),
	)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
	if resolved.builder == nil {
		t.Fatal("compileJobRecipe() produced nil builder")
	}
	if resolved.compiler == nil || resolved.migration == nil {
		t.Fatal("compileJobRecipe() did not select a migration graph compiler")
	}
	if !resolved.specReady {
		t.Fatal("compileJobRecipe() did not emit a planned graph spec")
	}
	if resolved.intent.Name != "record" {
		t.Fatalf("intent name = %q, want record", resolved.intent.Name)
	}
	if len(resolved.intent.Inputs) != 1 || resolved.intent.Inputs[0].Name != "input.ivf" {
		t.Fatalf("intent inputs = %+v", resolved.intent.Inputs)
	}
	if len(resolved.intent.Outputs) != 1 || resolved.intent.Outputs[0].Name != "recording.ivf" {
		t.Fatalf("intent outputs = %+v", resolved.intent.Outputs)
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

func TestCompileTranscodeRecipeCarriesIntentAndPlan(t *testing.T) {
	job := Transcode(FileInput("input.ivf", strings.NewReader(""))).
		Video("360p").Resize(640, 360).VP9(600_000).To("web").
		Output("web", FileOutput("web.ivf", io.Discard))

	resolved, err := compileTranscodeRecipe(job)
	if err != nil {
		t.Fatalf("compileTranscodeRecipe() error = %v", err)
	}
	builder, ok := resolved.builder.(*builder)
	if !ok {
		t.Fatalf("resolved builder type = %T, want *builder", resolved.builder)
	}
	if len(builder.transcodes) != 1 {
		t.Fatalf("builder transcodes = %d, want 1", len(builder.transcodes))
	}
	if resolved.compiler == nil || resolved.migration == nil {
		t.Fatal("compileTranscodeRecipe() did not select a migration graph compiler")
	}
	if !resolved.specReady {
		t.Fatal("compileTranscodeRecipe() did not emit a planned graph spec")
	}
	if resolved.intent.Name != "transcode" {
		t.Fatalf("intent name = %q, want transcode", resolved.intent.Name)
	}
	if len(resolved.intent.Streams) != 1 || resolved.intent.Streams[0].Name != "360p" {
		t.Fatalf("intent streams = %+v", resolved.intent.Streams)
	}
	if got := resolved.intent.Streams[0].RouteTo; len(got) != 1 || got[0] != "web" {
		t.Fatalf("intent route targets = %+v, want [web]", got)
	}
	spec, err := resolved.Describe()
	if err != nil {
		t.Fatalf("resolved.Describe() error = %v", err)
	}
	if len(spec.Nodes) == 0 || len(spec.Edges) == 0 {
		t.Fatalf("resolved spec = %+v, want planned graph nodes and edges", spec)
	}
}

func TestRecipeResolvedBuildUsesPlannedCompiler(t *testing.T) {
	job := Record(
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
		FileOutput("recording.ivf", io.Discard),
	)

	resolved, err := compileJobRecipe(job)
	if err != nil {
		t.Fatalf("compileJobRecipe() error = %v", err)
	}
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
