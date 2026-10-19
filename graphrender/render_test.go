package graphrender

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
)

func TestRenderTaskFlowchartAnnotatesRuntimeBranches(t *testing.T) {
	task := snapshotTask{
		snap: goav.TaskSnapshot{
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
			Branches: []goav.BranchSnapshot{{
				Name:  "preview",
				State: goav.BranchAttached,
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

func TestRenderSnapshotFlowchartAnnotatesTaps(t *testing.T) {
	snap := goav.TaskSnapshot{
		Spec: pipeline.Spec{
			Name: "live",
			Nodes: []pipeline.NodeSpec{
				{Name: "source", Kind: pipeline.NodeSource},
				{Name: "select-video", Kind: pipeline.NodeStage, Detail: "select video"},
				{Name: "encode-video", Kind: pipeline.NodeStage},
			},
			Edges: []pipeline.EdgeSpec{
				{From: "source", To: "select-video"},
				{From: "select-video", To: "encode-video"},
			},
		},
		Taps: []goav.TapSnapshot{{
			Name:      "frames",
			Node:      "select-video",
			Domain:    "frame",
			MediaKind: "video",
		}},
	}

	flowchart, err := RenderSnapshotFlowchart(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flowchart, "tap=frames (frame video)") {
		t.Fatalf("flowchart:\n%s", flowchart)
	}

	text, err := RenderSnapshotURI(snap, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "stage select-video [select video\ntap=frames (frame video)]") {
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

	task := snapshotTask{snap: goav.TaskSnapshot{
		Spec: pipeline.Spec{
			Name: "snapshot",
			Nodes: []pipeline.NodeSpec{
				{Name: "shared", Kind: pipeline.NodeStage, Detail: "existing"},
				{Name: "branch-only", Kind: pipeline.NodeStage},
			},
		},
		Branches: []goav.BranchSnapshot{
			{
				Name:  "alpha",
				State: goav.BranchDetached,
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

func TestRenderSnapshotAndBranchHelpers(t *testing.T) {
	snap := goav.TaskSnapshot{
		Spec: pipeline.Spec{
			Name: "live",
			Nodes: []pipeline.NodeSpec{
				{Name: "source", Kind: pipeline.NodeSource},
				{Name: "tap", Kind: pipeline.NodeStage, Detail: "frames"},
				{Name: "archive-sink", Kind: pipeline.NodeSink},
			},
			Edges: []pipeline.EdgeSpec{
				{From: "source", To: "tap"},
				{From: "tap", To: "archive-sink"},
			},
		},
		Branches: []goav.BranchSnapshot{{
			Name:  "archive",
			State: goav.BranchAttached,
			Nodes: []pipeline.NodeRef{"archive-sink"},
		}},
	}

	flowchart, err := RenderSnapshotFlowchart(snap)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flowchart, "flowchart LR") ||
		!strings.Contains(flowchart, "branch=archive (attached)") {
		t.Fatalf("snapshot flowchart:\n%s", flowchart)
	}
	text, err := RenderSnapshotURI(snap, "goav:graph")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "sink archive-sink [branch=archive (attached)]") {
		t.Fatalf("snapshot text:\n%s", text)
	}

	branch := goav.BranchSnapshot{
		ID:    "branch-001",
		Name:  "preview",
		State: goav.BranchDetached,
		Spec: pipeline.Spec{
			Nodes: []pipeline.NodeSpec{
				{Name: "preview-meter", Kind: pipeline.NodeStage, Detail: "meter"},
				{Name: "preview-sink", Kind: pipeline.NodeSink},
			},
			Edges: []pipeline.EdgeSpec{{From: "preview-meter", To: "preview-sink"}},
		},
	}
	branchFlowchart, err := RenderBranchFlowchart(branch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(branchFlowchart, "branch=preview (detached)") ||
		!strings.Contains(branchFlowchart, "preview-meter") {
		t.Fatalf("branch flowchart:\n%s", branchFlowchart)
	}
	if branch.Spec.Nodes[0].Detail != "meter" {
		t.Fatalf("RenderBranchFlowchart mutated branch spec: %+v", branch.Spec.Nodes[0])
	}
	dot, err := RenderBranchURI(branch, "goav://graph/dot")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dot, "digraph \"preview\"") ||
		!strings.Contains(dot, "branch=preview (detached)") {
		t.Fatalf("branch dot:\n%s", dot)
	}

	unnamedDot, err := RenderBranchURI(goav.BranchSnapshot{
		ID: "branch-002",
		Spec: pipeline.Spec{
			Nodes: []pipeline.NodeSpec{{Name: "sink", Kind: pipeline.NodeSink}},
		},
	}, "goav://graph/dot")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unnamedDot, "digraph \"branch-002\"") {
		t.Fatalf("unnamed branch dot:\n%s", unnamedDot)
	}
}

func TestRenderHelperEdgeCases(t *testing.T) {
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

	spec := specFromSnapshot(goav.TaskSnapshot{Spec: pipeline.Spec{Name: "plain"}})
	if spec.Name != "plain" {
		t.Fatalf("snapshot without branches = %+v", spec)
	}
}

func TestRenderTaskFlowchartReadsLiveTaskSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}
	task, err := goavtest.Runtime().Build(ctx, goavtest.Packets(av.CodecOpus, packet).
		Audio().
		Tap("pkts").
		To(goavtest.NewCollector().Sink()))
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx,
		task.Tap("pkts").
			As("preview").
			To(goavtest.NewCollector().Sink()),
	)
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
	snap goav.TaskSnapshot
}

func (t snapshotTask) Snapshot() goav.TaskSnapshot {
	return t.snap
}
