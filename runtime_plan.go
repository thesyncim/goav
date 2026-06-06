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
	if err := addPlannedNode(nodes, &spec, sourceName, pipeline.NodeSource, sourceRef, inputNodeDetail(b.inputs[0])); err != nil {
		return pipeline.Spec{}, err
	}
	for i := range b.outputs {
		stageName := muxNodeName(b.outputs[i], i)
		stageRef := pipeline.NodeRef(stageName)
		if err := addPlannedNode(nodes, &spec, stageName, pipeline.NodeStage, stageRef, outputNodeDetail(b.outputs[i])); err != nil {
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
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeSource, ref, describedNodeDetail(b.sources[i])); err != nil {
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
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeStage, ref, describedNodeDetail(b.stages[i])); err != nil {
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
		if err := addPlannedNode(nodes, &spec, name, pipeline.NodeSink, ref, describedNodeDetail(b.sinks[i])); err != nil {
			return pipeline.Spec{}, err
		}
		sinkRefs[i] = ref
	}

	if len(b.connections) != 0 {
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
	for i := range b.connections {
		if err := planConnection(nodes, spec, b.connections[i]); err != nil {
			return err
		}
	}
	return nil
}

func addPlannedNode(nodes map[string]plannedNode, spec *pipeline.Spec, name string, kind pipeline.NodeKind, ref pipeline.NodeRef, detail ...string) error {
	if name == "" {
		return pipeline.ErrUnknownNode
	}
	if _, ok := nodes[name]; ok {
		return pipeline.ErrNodeExists
	}
	nodes[name] = plannedNode{ref: ref, kind: kind}
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

func describedNodeDetail(node any) string {
	describer, ok := node.(pipeline.NodeDescriber)
	if !ok {
		return ""
	}
	return describer.DescribeNode().Detail
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

func planConnection(nodes map[string]plannedNode, spec *pipeline.Spec, connection pipeline.Connection) error {
	policy, err := plannedRoutePolicy(connection.Policy)
	if err != nil {
		return pipeline.ErrUnsupportedRoute
	}
	if len(connection.To) == 0 {
		return pipeline.ErrInvalidLink
	}
	fromRef, err := resolvePlannedNode(nodes, pipeline.NodeRef(connection.From))
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
	for i := range connection.To {
		toRef, err := resolvePlannedNode(nodes, pipeline.NodeRef(connection.To[i]))
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
			Policy: policy,
			Label:  connection.Label,
		})
	}
	return nil
}

func plannedRoutePolicy(policy pipeline.RoutePolicy) (pipeline.RoutePolicy, error) {
	switch policy {
	case "", pipeline.RouteAll:
		return pipeline.RouteAll, nil
	case pipeline.RouteByStream, pipeline.RouteByEvent:
		return policy, nil
	default:
		return "", pipeline.ErrUnsupportedRoute
	}
}

func resolvePlannedNode(nodes map[string]plannedNode, ref pipeline.NodeRef) (pipeline.NodeRef, error) {
	node, ok := nodes[ref.String()]
	if !ok {
		return "", pipeline.ErrUnknownNode
	}
	return node.ref, nil
}
