package graphrender

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

func TestRenderTextDOTAndMermaid(t *testing.T) {
	spec := pipeline.Spec{
		Name: "receive",
		Nodes: []pipeline.NodeSpec{
			{Name: "source", Kind: pipeline.NodeSource, Detail: "rtp receive"},
			{Name: "decode", Kind: pipeline.NodeStage, Detail: "packets -> frames"},
			{Name: "sink", Kind: pipeline.NodeSink},
		},
		Edges: []pipeline.EdgeSpec{
			{
				From:   pipeline.NodeRef("source"),
				To:     pipeline.NodeRef("decode"),
				Policy: pipeline.RouteAll,
			},
			{
				From:   pipeline.NodeRef("decode"),
				To:     pipeline.NodeRef("sink"),
				Policy: pipeline.RouteByStream,
				Label:  "audio",
			},
		},
	}

	text := Render(spec, Text)
	if !strings.Contains(text, "pipeline receive") ||
		!strings.Contains(text, "source source [rtp receive]") ||
		!strings.Contains(text, "decode -> sink [stream=audio]") {
		t.Fatalf("text spec:\n%s", text)
	}

	dot := Render(spec, DOT)
	if !strings.Contains(dot, "digraph \"receive\"") ||
		!strings.Contains(dot, "source\\nsource\\nrtp receive") ||
		!strings.Contains(dot, "\"source\" -> \"decode\"") ||
		!strings.Contains(dot, "label=\"stream=audio\"") {
		t.Fatalf("dot spec:\n%s", dot)
	}

	mermaid := Render(spec, Mermaid)
	if !strings.Contains(mermaid, "flowchart LR") ||
		!strings.Contains(mermaid, "n0([\"source\\nsource\\nrtp receive\"])") ||
		!strings.Contains(mermaid, "n1 -- \"stream=audio\" --> n2") {
		t.Fatalf("mermaid spec:\n%s", mermaid)
	}

	var out bytes.Buffer
	if err := Write(&out, spec, Mermaid); err != nil {
		t.Fatal(err)
	}
	if out.String() != mermaid {
		t.Fatalf("write mermaid:\n%s\nwant:\n%s", out.String(), mermaid)
	}

	if err := Write(&out, spec, Format("json")); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}
