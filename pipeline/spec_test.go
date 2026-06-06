package pipeline

import (
	"strings"
	"testing"
)

func TestSpecTextAndDOT(t *testing.T) {
	spec := Spec{
		Name: "receive",
		Nodes: []NodeSpec{
			{Name: "source", Kind: NodeSource},
			{Name: "decode", Kind: NodeStage},
			{Name: "sink", Kind: NodeSink},
		},
		Edges: []EdgeSpec{
			{
				From:   Node("source"),
				To:     Node("decode"),
				Policy: RouteAll,
			},
			{
				From:   Node("decode"),
				To:     Node("sink"),
				Policy: RouteByStream,
				Label:  "audio",
			},
		},
	}

	text := spec.String()
	if !strings.Contains(text, "pipeline receive") ||
		!strings.Contains(text, "source source") ||
		!strings.Contains(text, "decode -> sink [by_stream:audio]") {
		t.Fatalf("text spec:\n%s", text)
	}

	dot := spec.DOT()
	if !strings.Contains(dot, "digraph \"receive\"") ||
		!strings.Contains(dot, "\"source\" -> \"decode\"") ||
		!strings.Contains(dot, "label=\"by_stream:audio\"") {
		t.Fatalf("dot spec:\n%s", dot)
	}

	mermaid := spec.Mermaid()
	if !strings.Contains(mermaid, "flowchart LR") ||
		!strings.Contains(mermaid, "n0([\"source\\nsource\"])") ||
		!strings.Contains(mermaid, "n1 -- \"by_stream:audio\" --> n2") {
		t.Fatalf("mermaid spec:\n%s", mermaid)
	}
}
