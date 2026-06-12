package graphrender

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
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
		!strings.Contains(mermaid, "n0([\"source\\nsource\\nrtp receive\"])") ||
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

func TestRenderTaskFlowchartAnnotatesRuntimeBranches(t *testing.T) {
	task := snapshotTask{
		snap: snapshot.Task{
			Spec: pipeline.Spec{
				Name: "live",
				Nodes: []pipeline.NodeSpec{
					{Name: "source", Kind: pipeline.NodeSource},
					{Name: "tap", Kind: pipeline.NodeStage, Detail: "packets"},
					{Name: "preview-copy", Kind: pipeline.NodeStage, Detail: "copy"},
					{Name: "preview-sink", Kind: pipeline.NodeSink},
				},
				Edges: []pipeline.EdgeSpec{
					{From: "source", To: "tap"},
					{From: "tap", To: "preview-copy"},
					{From: "preview-copy", To: "preview-sink"},
				},
			},
			Branches: []snapshot.Branch{{
				Name:  "preview",
				State: lifecycle.BranchAttached,
				Nodes: []pipeline.NodeRef{
					"preview-copy",
					"preview-sink",
				},
			}},
		},
	}

	flowchart, err := RenderTaskFlowchart(task)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flowchart, "flowchart LR") ||
		!strings.Contains(flowchart, "branch=preview (attached)") ||
		!strings.Contains(flowchart, "preview-copy") {
		t.Fatalf("flowchart:\n%s", flowchart)
	}

	text, err := RenderTaskURI(task, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "stage preview-copy [copy\nbranch=preview (attached)]") {
		t.Fatalf("text:\n%s", text)
	}
}

func TestRenderTaskURIHandlesNilAndSnapshotBranchDetails(t *testing.T) {
	text, err := RenderTaskURI(nil, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "pipeline goav") {
		t.Fatalf("nil task text:\n%s", text)
	}

	task := snapshotTask{snap: snapshot.Task{
		Spec: pipeline.Spec{
			Name: "snapshot",
			Nodes: []pipeline.NodeSpec{
				{Name: "shared", Kind: pipeline.NodeStage, Detail: "existing"},
				{Name: "branch-only", Kind: pipeline.NodeStage},
			},
		},
		Branches: []snapshot.Branch{
			{
				Name:  "alpha",
				State: lifecycle.BranchDetached,
				Nodes: []pipeline.NodeRef{"shared"},
			},
			{
				ID:    "zeta",
				Nodes: []pipeline.NodeRef{"shared", "shared", ""},
				Spec: pipeline.Spec{Nodes: []pipeline.NodeSpec{
					{Name: "branch-only"},
					{Name: "shared"},
					{},
				}},
			},
			{},
		},
	}}

	text, err = RenderTaskURI(task, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "stage shared [existing\nbranch=alpha (detached), branch=zeta]") {
		t.Fatalf("shared branch annotations missing or unsorted:\n%s", text)
	}
	if !strings.Contains(text, "stage branch-only [branch=zeta]") {
		t.Fatalf("snapshot branch spec node annotation missing:\n%s", text)
	}
}

func TestRenderHelperEdgeCases(t *testing.T) {
	if got, err := parseFormat(""); err != nil || got != textFormat {
		t.Fatalf("parseFormat(empty) = %q, %v; want text", got, err)
	}
	if got := mermaidSafeID(""); got != "node" {
		t.Fatalf("mermaidSafeID(empty) = %q, want node", got)
	}
	if got := branchRenderLabel(nil); got != "" {
		t.Fatalf("nil branch label = %q, want empty", got)
	}
	if got := branchNodeNames(nil); got != nil {
		t.Fatalf("nil branch nodes = %#v, want nil", got)
	}
	if got := appendNodeDetail("existing", ""); got != "existing" {
		t.Fatalf("append empty annotation = %q, want existing", got)
	}
	if got := appendNodeDetail("", "annotation"); got != "annotation" {
		t.Fatalf("append to empty detail = %q, want annotation", got)
	}

	spec := specFromSnapshot(snapshot.Task{Spec: pipeline.Spec{Name: "plain"}})
	if spec.Name != "plain" {
		t.Fatalf("snapshot without branches = %+v", spec)
	}
}

func TestRenderTaskFlowchartReadsLiveTaskSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, goav.Branch("preview").
		From(goav.PacketTap("pkts")).
		Copy().
		To(goavtest.NewCollector().Sink()))
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Close(ctx)

	flowchart, err := RenderTaskFlowchart(task)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flowchart, "flowchart LR") ||
		!strings.Contains(flowchart, "branch=preview (attached)") {
		t.Fatalf("flowchart:\n%s", flowchart)
	}
}

type snapshotTask struct {
	snap snapshot.Task
}

func (t snapshotTask) Snapshot() snapshot.Task {
	return t.snap
}
