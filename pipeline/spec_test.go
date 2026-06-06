package pipeline

import (
	"strings"
	"testing"
)

func TestSpecString(t *testing.T) {
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
}
