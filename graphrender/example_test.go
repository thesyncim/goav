package graphrender_test

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav"

	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
)

func ExampleRenderSnapshotFlowchart() {
	snap := goav.TaskSnapshot{
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
		Branches: []goav.BranchSnapshot{{
			Name:  "recording",
			State: goav.BranchAttached,
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
	branch := goav.BranchSnapshot{
		Name:  "preview",
		State: goav.BranchAttached,
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
