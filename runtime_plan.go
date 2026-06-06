package goav

import "github.com/thesyncim/goav/pipeline"

type plannedNode struct {
	ref  pipeline.NodeRef
	kind pipeline.NodeKind
}

func (b *builder) Describe() (pipeline.Spec, error) {
	spec := pipeline.Spec{
		Name:     "goav",
		Realtime: b.runtime.realtime,
	}
	compiler, err := b.selectCompiler()
	if err != nil {
		return pipeline.Spec{}, err
	}
	return compiler.describe(b, spec)
}

func (b *builder) planRemux(spec pipeline.Spec) (pipeline.Spec, error) {
	nodes := make(map[string]plannedNode, 1+len(b.outputs))
	sourceName := demuxNodeName(b.inputs[0])
	sourceRef := pipeline.NodeRef(sourceName)
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef); err != nil {
		return pipeline.Spec{}, err
	}
	for i := range b.outputs {
		stageName := muxNodeName(b.outputs[i], i)
		stageRef := pipeline.NodeRef(stageName)
		if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, stageRef); err != nil {
			return pipeline.Spec{}, err
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   sourceRef,
			To:     stageRef,
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
	sourceRefs := make([]pipeline.NodeRef, len(b.sources))
	stageRefs := make([]pipeline.NodeRef, len(b.stages))
	sinkRefs := make([]pipeline.NodeRef, len(b.sinks))

	for i := range b.sources {
		if b.sources[i] == nil {
			return pipeline.Spec{}, ErrNilSource
		}
		name := b.sources[i].Name()
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeSource, ref); err != nil {
			return pipeline.Spec{}, err
		}
		sourceRefs[i] = ref
	}
	for i := range b.stages {
		if b.stages[i] == nil {
			return pipeline.Spec{}, ErrNilStage
		}
		name := b.stages[i].Name()
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeStage, ref); err != nil {
			return pipeline.Spec{}, err
		}
		stageRefs[i] = ref
	}
	for i := range b.sinks {
		if b.sinks[i] == nil {
			return pipeline.Spec{}, ErrNilSink
		}
		name := b.sinks[i].Name()
		ref := pipeline.NodeRef(name)
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeSink, ref); err != nil {
			return pipeline.Spec{}, err
		}
		sinkRefs[i] = ref
	}

	if len(b.links) != 0 || len(b.routes) != 0 {
		if err := b.planExplicitEdges(nodes, &spec); err != nil {
			return pipeline.Spec{}, err
		}
		return spec, nil
	}

	if len(stageRefs) == 0 {
		planLinks(&spec, sourceRefs, sinkRefs)
		return spec, nil
	}
	planLinks(&spec, sourceRefs, stageRefs[:1])
	for i := 0; i < len(stageRefs)-1; i++ {
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   stageRefs[i],
			To:     stageRefs[i+1],
			Policy: pipeline.RouteAll,
		})
	}
	planLinks(&spec, stageRefs[len(stageRefs)-1:], sinkRefs)
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

func addPlannedNode(nodes map[string]plannedNode, spec *pipeline.Spec, name string, kind pipeline.NodeKind, ref pipeline.NodeRef) error {
	if name == "" {
		return pipeline.ErrUnknownNode
	}
	if _, ok := nodes[name]; ok {
		return pipeline.ErrNodeExists
	}
	nodes[name] = plannedNode{ref: ref, kind: kind}
	spec.Nodes = append(spec.Nodes, pipeline.NodeSpec{Name: name, Kind: kind})
	return nil
}

func planLinks(spec *pipeline.Spec, from []pipeline.NodeRef, to []pipeline.NodeRef) {
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
	from, to, err := resolvePlannedEdge(nodes, link.From, link.To)
	if err != nil {
		return err
	}
	spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
		From:   from,
		To:     to,
		Policy: pipeline.RouteAll,
	})
	return nil
}

func planRoute(nodes map[string]plannedNode, spec *pipeline.Spec, route pipeline.Route) error {
	if route.Policy == pipeline.RouteByLabel {
		return pipeline.ErrUnsupportedRoute
	}
	fromRef, err := resolvePlannedNode(nodes, route.From)
	if err != nil {
		return err
	}
	from, ok := nodes[fromRef.String()]
	if !ok {
		return pipeline.ErrUnknownNode
	}
	if from.kind == pipeline.NodeSink {
		return pipeline.ErrInvalidLink
	}
	for i := range route.To {
		toRef, err := resolvePlannedNode(nodes, route.To[i])
		if err != nil {
			return err
		}
		to, ok := nodes[toRef.String()]
		if !ok {
			return pipeline.ErrUnknownNode
		}
		if to.kind == pipeline.NodeSource {
			return pipeline.ErrInvalidLink
		}
		spec.Edges = append(spec.Edges, pipeline.EdgeSpec{
			From:   fromRef,
			To:     toRef,
			Policy: route.Policy,
			Label:  route.Label,
		})
	}
	return nil
}

func validatePlannedEdge(nodes map[string]plannedNode, from pipeline.NodeRef, to pipeline.NodeRef) error {
	fromNode, ok := nodes[from.String()]
	if !ok {
		return pipeline.ErrUnknownNode
	}
	if fromNode.kind == pipeline.NodeSink {
		return pipeline.ErrInvalidLink
	}
	toNode, ok := nodes[to.String()]
	if !ok {
		return pipeline.ErrUnknownNode
	}
	if toNode.kind == pipeline.NodeSource {
		return pipeline.ErrInvalidLink
	}
	return nil
}

func resolvePlannedEdge(nodes map[string]plannedNode, from pipeline.NodeRef, to pipeline.NodeRef) (pipeline.NodeRef, pipeline.NodeRef, error) {
	fromRef, err := resolvePlannedNode(nodes, from)
	if err != nil {
		return "", "", err
	}
	toRef, err := resolvePlannedNode(nodes, to)
	if err != nil {
		return "", "", err
	}
	if err := validatePlannedEdge(nodes, fromRef, toRef); err != nil {
		return "", "", err
	}
	return fromRef, toRef, nil
}

func resolvePlannedNode(nodes map[string]plannedNode, ref pipeline.NodeRef) (pipeline.NodeRef, error) {
	node, ok := nodes[ref.String()]
	if !ok {
		return "", pipeline.ErrUnknownNode
	}
	return node.ref, nil
}
