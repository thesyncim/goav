package goav

import (
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

func TestTaskTapsDoNotInferFromGraphNodePrefixes(t *testing.T) {
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "tap-prefix-regression"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(&runtimeTestSource{name: "select-audio"}, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(&runtimeTestStage{name: "decode-audio"}, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(&runtimeTestSink{name: "out"}, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}

	task := &task{graph: graph}
	if taps := task.Taps(); len(taps) != 0 {
		t.Fatalf("Taps() = %+v, want no prefix-inferred taps", taps)
	}
}
