package goav

import "github.com/thesyncim/goav/pipeline"

type plannedNode struct {
	pad  pipeline.PadRef
	kind pipeline.NodeKind
}

func (b *builder) Describe() (pipeline.Spec, error) {
	spec := pipeline.Spec{
		Name:     "goav",
		Realtime: b.runtime.realtime,
	}
	if b.hasHighLevelRequests() {
		if b.hasExplicitGraph() {
			return pipeline.Spec{}, ErrUnsupportedBuild
		}
		if b.canBuildRemux() {
			return b.planRemux(spec)
		}
		return pipeline.Spec{}, ErrUnsupportedBuild
	}
	if !b.hasExplicitGraph() {
		return spec, nil
	}
	return b.planExplicitGraph(spec)
}

func (b *builder) planRemux(spec pipeline.Spec) (pipeline.Spec, error) {
	nodes := make(map[string]plannedNode, 1+len(b.outputs))
	sourceName := demuxNodeName(b.inputs[0])
	sourcePad := pipeline.PadRef{Node: sourceName, Pad: "out"}
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourcePad); err != nil {
		return pipeline.Spec{}, err
	}
	for i := range b.outputs {
		stageName := muxNodeName(b.outputs[i], i)
		stagePad := pipeline.PadRef{Node: stageName, Pad: "inout"}
		if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, stagePad); err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   sourcePad,
			To:     stagePad,
			Policy: pipeline.RouteAll,
		})
	}
	return spec, nil
}

func (b *builder) planExplicitGraph(spec pipeline.Spec) (pipeline.Spec, error) {
	if len(b.sources) == 0 {
		return pipeline.Spec{}, ErrUnsupportedBuild
	}

	nodes := make(map[string]plannedNode, len(b.sources)+len(b.stages)+len(b.sinks))
	sourcePads := make([]pipeline.PadRef, len(b.sources))
	stagePads := make([]pipeline.PadRef, len(b.stages))
	sinkPads := make([]pipeline.PadRef, len(b.sinks))

	for i := range b.sources {
		if b.sources[i] == nil {
			return pipeline.Spec{}, ErrNilSource
		}
		name := b.sources[i].Name()
		pad := pipeline.PadRef{Node: name, Pad: "out"}
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeSource, pad); err != nil {
			return pipeline.Spec{}, err
		}
		sourcePads[i] = pad
	}
	for i := range b.stages {
		if b.stages[i] == nil {
			return pipeline.Spec{}, ErrNilStage
		}
		name := b.stages[i].Name()
		pad := pipeline.PadRef{Node: name, Pad: "inout"}
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeStage, pad); err != nil {
			return pipeline.Spec{}, err
		}
		stagePads[i] = pad
	}
	for i := range b.sinks {
		if b.sinks[i] == nil {
			return pipeline.Spec{}, ErrNilSink
		}
		name := b.sinks[i].Name()
		pad := pipeline.PadRef{Node: name, Pad: "in"}
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeSink, pad); err != nil {
			return pipeline.Spec{}, err
		}
		sinkPads[i] = pad
	}

	if len(b.links) != 0 || len(b.routes) != 0 {
		if err := b.planExplicitEdges(nodes, &spec); err != nil {
			return pipeline.Spec{}, err
		}
		return spec, nil
	}

	if len(stagePads) == 0 {
		planLinks(&spec, sourcePads, sinkPads)
		return spec, nil
	}
	planLinks(&spec, sourcePads, stagePads[:1])
	for i := 0; i < len(stagePads)-1; i++ {
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   stagePads[i],
			To:     stagePads[i+1],
			Policy: pipeline.RouteAll,
		})
	}
	planLinks(&spec, stagePads[len(stagePads)-1:], sinkPads)
	return spec, nil
}

func (b *builder) planExplicitEdges(nodes map[string]plannedNode, spec *pipeline.Spec) error {
	for i := range b.links {
		if err := planLink(nodes, spec, b.links[i]); err != nil {
			return err
		}
	}
	for i := range b.routes {
		if err := planRoute(nodes, spec, b.routes[i]); err != nil {
			return err
		}
	}
	return nil
}

func addPlannedNode(nodes map[string]plannedNode, spec *pipeline.Spec, name string, kind pipeline.NodeKind, pad pipeline.PadRef) error {
	if name == "" {
		return pipeline.ErrUnknownNode
	}
	if _, ok := nodes[name]; ok {
		return pipeline.ErrNodeExists
	}
	nodes[name] = plannedNode{pad: pad, kind: kind}
	spec.Nodes = append(spec.Nodes, pipeline.NodeSpec{Name: name, Kind: kind})
	return nil
}

func planLinks(spec *pipeline.Spec, from []pipeline.PadRef, to []pipeline.PadRef) {
	for i := range from {
		for j := range to {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
				From:   from[i],
				To:     to[j],
				Policy: pipeline.RouteAll,
			})
		}
	}
}

func planLink(nodes map[string]plannedNode, spec *pipeline.Spec, link pipeline.Link) error {
	if err := validatePlannedEdge(nodes, link.From, link.To); err != nil {
		return err
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   link.From,
		To:     link.To,
		Policy: pipeline.RouteAll,
	})
	return nil
}

func planRoute(nodes map[string]plannedNode, spec *pipeline.Spec, route pipeline.Route) error {
	if route.Policy == pipeline.RouteByLabel {
		return pipeline.ErrUnsupportedRoute
	}
	from, ok := nodes[route.From.Node]
	if !ok {
		return pipeline.ErrUnknownNode
	}
	if from.kind == pipeline.NodeSink {
		return pipeline.ErrInvalidLink
	}
	for i := range route.To {
		to, ok := nodes[route.To[i].Node]
		if !ok {
			return pipeline.ErrUnknownNode
		}
		if to.kind == pipeline.NodeSource {
			return pipeline.ErrInvalidLink
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   route.From,
			To:     route.To[i],
			Policy: route.Policy,
			Label:  route.Label,
		})
	}
	return nil
}

func validatePlannedEdge(nodes map[string]plannedNode, from pipeline.PadRef, to pipeline.PadRef) error {
	fromNode, ok := nodes[from.Node]
	if !ok {
		return pipeline.ErrUnknownNode
	}
	if fromNode.kind == pipeline.NodeSink {
		return pipeline.ErrInvalidLink
	}
	toNode, ok := nodes[to.Node]
	if !ok {
		return pipeline.ErrUnknownNode
	}
	if toNode.kind == pipeline.NodeSource {
		return pipeline.ErrInvalidLink
	}
	return nil
}
