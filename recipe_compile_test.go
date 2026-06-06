package goav

import (
	"io"
	"strings"
	"testing"
)

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
	if resolved.intent.Name != "transcode" {
		t.Fatalf("intent name = %q, want transcode", resolved.intent.Name)
	}
	if len(resolved.intent.Streams) != 1 || resolved.intent.Streams[0].Name != "360p" {
		t.Fatalf("intent streams = %+v", resolved.intent.Streams)
	}
	if got := resolved.intent.Streams[0].RouteTo; len(got) != 1 || got[0] != "web" {
		t.Fatalf("intent route targets = %+v, want [web]", got)
	}
}
