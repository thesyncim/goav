package main

import (
	"sort"

	"github.com/thesyncim/goav/pipeline"
)

func graphFromSpec(name string, spec pipeline.Spec) graphView {
	view := graphView{
		Name:  name,
		Nodes: make([]nodeView, 0, len(spec.Nodes)),
		Edges: make([]edgeView, 0, len(spec.Edges)),
		Stats: graphStats{
			NodeCount: len(spec.Nodes),
			EdgeCount: len(spec.Edges),
			KindCount: make(map[string]int),
		},
	}
	for _, node := range spec.Nodes {
		kind := string(node.Kind)
		view.Nodes = append(view.Nodes, nodeView{
			Name:   node.Name,
			Kind:   kind,
			Detail: node.Detail,
		})
		view.Stats.KindCount[kind]++
		switch kind {
		case "source":
			view.Stats.Sources = append(view.Stats.Sources, node.Name)
		case "sink":
			view.Stats.Sinks = append(view.Stats.Sinks, node.Name)
		}
	}
	for _, edge := range spec.Edges {
		view.Edges = append(view.Edges, edgeView{
			From: edge.From.String(),
			To:   edge.To.String(),
		})
	}
	sort.Slice(view.Nodes, func(i, j int) bool {
		return view.Nodes[i].Name < view.Nodes[j].Name
	})
	sort.Slice(view.Edges, func(i, j int) bool {
		if view.Edges[i].From != view.Edges[j].From {
			return view.Edges[i].From < view.Edges[j].From
		}
		return view.Edges[i].To < view.Edges[j].To
	})
	sort.Strings(view.Stats.Sources)
	sort.Strings(view.Stats.Sinks)
	return view
}
