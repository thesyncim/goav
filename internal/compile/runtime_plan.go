package compile

import (
	"github.com/thesyncim/goav/pipeline"
)

type PlannedNode struct {
	Ref  pipeline.NodeRef
	Kind pipeline.NodeKind
}

func AddPlannedNode(nodes map[string]PlannedNode, spec *pipeline.Spec, name string, kind pipeline.NodeKind, ref pipeline.NodeRef, detail ...string) error {
	if name == "" {
		return pipeline.ErrUnknownNode
	}
	if _, ok := nodes[name]; ok {
		return pipeline.ErrNodeExists
	}
	nodes[name] = PlannedNode{Ref: ref, Kind: kind}
	spec.Nodes = append(spec.Nodes, pipeline.NodeSpec{Name: name, Kind: kind, Detail: firstDetail(detail)})
	return nil
}

func firstDetail(detail []string) string {
	for i := range detail {
		if detail[i] != "" {
			return detail[i]
		}
	}
	return ""
}

func DescribedNodeDetail(node any) string {
	describer, ok := node.(pipeline.NodeDescriber)
	if !ok {
		return ""
	}
	return describer.DescribeNode().Detail
}
