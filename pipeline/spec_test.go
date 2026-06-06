package pipeline

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestSpecTextAndDOT(t *testing.T) {
	spec := Spec{
		Name: "receive",
		Nodes: []NodeSpec{
			{Name: "source", Kind: NodeSource, Detail: "rtp receive"},
			{Name: "decode", Kind: NodeStage, Detail: "packets -> frames"},
			{Name: "sink", Kind: NodeSink},
		},
		Edges: []EdgeSpec{
			{
				From:   NodeRef("source"),
				To:     NodeRef("decode"),
				Policy: RouteAll,
			},
			{
				From:   NodeRef("decode"),
				To:     NodeRef("sink"),
				Policy: RouteByStream,
				Label:  "audio",
			},
		},
	}

	text := spec.String()
	if !strings.Contains(text, "pipeline receive") ||
		!strings.Contains(text, "source source [rtp receive]") ||
		!strings.Contains(text, "decode -> sink [stream=audio]") {
		t.Fatalf("text spec:\n%s", text)
	}

	dot := spec.Render(SpecDOT)
	if !strings.Contains(dot, "digraph \"receive\"") ||
		!strings.Contains(dot, "source\\nsource\\nrtp receive") ||
		!strings.Contains(dot, "\"source\" -> \"decode\"") ||
		!strings.Contains(dot, "label=\"stream=audio\"") {
		t.Fatalf("dot spec:\n%s", dot)
	}

	mermaid := spec.Render(SpecMermaid)
	if !strings.Contains(mermaid, "flowchart LR") ||
		!strings.Contains(mermaid, "n0([\"source\\nsource\\nrtp receive\"])") ||
		!strings.Contains(mermaid, "n1 -- \"stream=audio\" --> n2") {
		t.Fatalf("mermaid spec:\n%s", mermaid)
	}

	var out bytes.Buffer
	if err := spec.Write(&out, SpecMermaid); err != nil {
		t.Fatal(err)
	}
	if out.String() != mermaid {
		t.Fatalf("write mermaid:\n%s\nwant:\n%s", out.String(), mermaid)
	}

	if err := spec.Write(&out, SpecFormat("json")); !errors.Is(err, ErrUnsupportedSpecFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedSpecFormat", err)
	}
}
