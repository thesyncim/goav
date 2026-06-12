package graphrender_test

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

func ExampleRenderSnapshotFlowchart() {
	snap := snapshot.Task{
		Spec: pipeline.Spec{
			Name: "live",
			Nodes: []pipeline.NodeSpec{
				{Name: "source", Kind: pipeline.NodeSource},
				{Name: "meter", Kind: pipeline.NodeStage, Detail: "rms"},
				{Name: "archive", Kind: pipeline.NodeSink},
			},
			Edges: []pipeline.EdgeSpec{
				{From: "source", To: "meter"},
				{From: "meter", To: "archive"},
			},
		},
		Branches: []snapshot.Branch{{
			Name:  "recording",
			State: lifecycle.BranchAttached,
			Nodes: []pipeline.NodeRef{"meter", "archive"},
		}},
	}

	flowchart, err := graphrender.RenderSnapshotFlowchart(snap)
	fmt.Println("err:", err)
	fmt.Println(strings.HasPrefix(flowchart, "flowchart LR"))
	fmt.Println(strings.Contains(flowchart, "branch=recording (attached)"))

	// Output:
	// err: <nil>
	// true
	// true
}

func ExampleRenderBranchFlowchart() {
	branch := snapshot.Branch{
		Name:  "preview",
		State: lifecycle.BranchAttached,
		Spec: pipeline.Spec{
			Nodes: []pipeline.NodeSpec{
				{Name: "preview-copy", Kind: pipeline.NodeStage, Detail: "copy"},
				{Name: "preview-sink", Kind: pipeline.NodeSink},
			},
			Edges: []pipeline.EdgeSpec{{From: "preview-copy", To: "preview-sink"}},
		},
	}

	flowchart, err := graphrender.RenderBranchFlowchart(branch)
	fmt.Println("err:", err)
	fmt.Println(strings.HasPrefix(flowchart, "flowchart LR"))
	fmt.Println(strings.Contains(flowchart, "branch=preview (attached)"))

	// Output:
	// err: <nil>
	// true
	// true
}
