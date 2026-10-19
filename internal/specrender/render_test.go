package specrender

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

func TestRenderURITextDOTAndMermaid(t *testing.T) {
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

	text, err := RenderURI(spec, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "pipeline receive") ||
		!strings.Contains(text, "source source [rtp receive]") ||
		!strings.Contains(text, "decode -> sink [stream=audio]") {
		t.Fatalf("text spec:\n%s", text)
	}

	dot, err := RenderURI(spec, "goav://graph/dot")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dot, "digraph \"receive\"") ||
		!strings.Contains(dot, "source\\nsource\\nrtp receive") ||
		!strings.Contains(dot, "\"source\" -> \"decode\"") ||
		!strings.Contains(dot, "label=\"stream=audio\"") {
		t.Fatalf("dot spec:\n%s", dot)
	}

	mermaid, err := RenderURI(spec, "goav:graph?format=mermaid")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(mermaid, "flowchart LR") ||
		!strings.Contains(mermaid, "n0([\"source<br>source<br>rtp receive\"])") ||
		!strings.Contains(mermaid, "n1 -- \"stream=audio\" --> n2") {
		t.Fatalf("mermaid spec:\n%s", mermaid)
	}
}

func TestRenderURI(t *testing.T) {
	spec := pipeline.Spec{
		Name: "receive",
		Nodes: []pipeline.NodeSpec{
			{Name: "source", Kind: pipeline.NodeSource, Detail: "rtp receive"},
			{Name: "sink", Kind: pipeline.NodeSink},
		},
		Edges: []pipeline.EdgeSpec{{
			From: pipeline.NodeRef("source"),
			To:   pipeline.NodeRef("sink"),
		}},
	}

	text, err := RenderURI(spec, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "pipeline receive") ||
		!strings.Contains(text, "source -> sink") {
		t.Fatalf("text:\n%s", text)
	}

	mermaid, err := RenderURI(spec, "goav:graph?format=mermaid")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(mermaid, "flowchart LR") {
		t.Fatalf("mermaid:\n%s", mermaid)
	}

	dot, err := RenderURI(spec, "goav://graph/dot")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dot, "digraph \"receive\"") {
		t.Fatalf("dot:\n%s", dot)
	}

	if _, err := RenderURI(spec, "file:///tmp/graph.dot"); !errors.Is(err, ErrUnsupportedURI) {
		t.Fatalf("err = %v, want ErrUnsupportedURI", err)
	}
	if _, err := RenderURI(spec, "goav:graph?format=json"); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestRenderURIFormatsAndErrors(t *testing.T) {
	spec := pipeline.Spec{}
	text, err := RenderURI(spec, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "pipeline goav") {
		t.Fatalf("default text:\n%s", text)
	}

	dot, err := RenderURI(spec, "goav://graph?format=dot")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dot, "digraph \"goav\"") {
		t.Fatalf("query-selected dot:\n%s", dot)
	}

	if _, err := RenderURI(spec, "%"); err == nil {
		t.Fatal("invalid URI succeeded")
	}
	if _, err := RenderURI(spec, "goav://not-graph/dot"); !errors.Is(err, ErrUnsupportedURI) {
		t.Fatalf("non-graph URI err = %v, want ErrUnsupportedURI", err)
	}
	if err := write(io.Discard, spec, format("json")); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("write unsupported err = %v, want ErrUnsupportedFormat", err)
	}
	if err := writeText(failingWriter{}, spec); err == nil {
		t.Fatal("writeText with failing writer succeeded")
	}
}

func TestRenderRouteLabelsAndMissingMermaidNodes(t *testing.T) {
	spec := pipeline.Spec{
		Name: "routes",
		Nodes: []pipeline.NodeSpec{
			{Name: "source", Kind: pipeline.NodeSource},
			{Name: "sink", Kind: pipeline.NodeSink},
		},
		Edges: []pipeline.EdgeSpec{
			{From: "source", To: "sink", Policy: pipeline.RouteByEvent},
			{From: "missing/source", To: "sink", Policy: pipeline.RoutePolicy("tap")},
			{From: "source", To: "missing/sink", Policy: pipeline.RoutePolicy("codec"), Label: "opus"},
		},
	}

	text, err := RenderURI(spec, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"source -> sink [event]", "missing/source -> sink [tap]", "source -> missing/sink [codec=opus]"} {
		if !strings.Contains(text, want) {
			t.Fatalf("text missing %q:\n%s", want, text)
		}
	}

	mermaid, err := RenderURI(spec, "goav://graph/mermaid")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"missing_missing_source", "missing_missing_sink", "-- \"event\" -->", "-- \"tap\" -->", "-- \"codec=opus\" -->"} {
		if !strings.Contains(mermaid, want) {
			t.Fatalf("mermaid missing %q:\n%s", want, mermaid)
		}
	}
}

func TestRenderHelperEdgeCases(t *testing.T) {
	if got, err := parseFormat(""); err != nil || got != textFormat {
		t.Fatalf("parseFormat(empty) = %q, %v; want text", got, err)
	}
	if got := mermaidSafeID(""); got != "node" {
		t.Fatalf("mermaidSafeID(empty) = %q, want node", got)
	}
}

// TestRenderMermaidEscapesLabels pins Mermaid's quoted-label syntax: line
// breaks are <br> and quotes and '#' are entity codes. Mermaid has no
// backslash escapes, so a Go-quoted label would render "\n" literally and
// could end the label early at an escaped quote.
func TestRenderMermaidEscapesLabels(t *testing.T) {
	spec := pipeline.Spec{
		Nodes: []pipeline.NodeSpec{
			{Name: "in", Kind: pipeline.NodeSource, Detail: `file "a.webm" #1`},
			{Name: "out", Kind: pipeline.NodeSink},
		},
		Edges: []pipeline.EdgeSpec{{From: "in", To: "out", Policy: pipeline.RouteByStream, Label: `say "hi"`}},
	}
	mermaid, err := RenderURI(spec, "goav:graph?format=mermaid")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`n0(["in<br>source<br>file #quot;a.webm#quot; #35;1"])`,
		`n0 -- "stream=say #quot;hi#quot;" --> n1`,
	} {
		if !strings.Contains(mermaid, want) {
			t.Fatalf("mermaid missing %q:\n%s", want, mermaid)
		}
	}
	if strings.Contains(mermaid, `\`) {
		t.Fatalf("mermaid contains backslash escapes:\n%s", mermaid)
	}
}
