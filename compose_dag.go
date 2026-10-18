package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

// composeNodeKind is what a branch-composition node lowers to.
type composeNodeKind uint8

const (
	composeSelect composeNodeKind = iota
	composeDecode
	composeSharedStep
	composeStep
	composeEncode
	composeSink
	composeMux
	// composeRemuxSink and composeRemuxMux are whole-job packet copy outputs:
	// every source feeds them with all its streams, and no branch selects one.
	composeRemuxSink
	composeRemuxMux
	// Join trees: an arm's input, its event-keeping decode, a planned
	// transform, a tap arm's restamp, the convergence stage, and a join's
	// encoder and destinations.
	composeArmSource
	composeArmDecode
	composeTransform
	composeTapRestamp
	composeJoin
	composeJoinEncode
	composeJoinDestination
)

// composeNode is one planned node of a branch composition: its name and
// describe detail, the nodes it consumes in connect order, and what the
// lowering builds from.
type composeNode struct {
	kind   composeNodeKind
	name   string
	detail string
	inputs []pipeline.NodeRef
	// group indexes the selector group a select or decode node serves.
	group int
	// branch is the branch a step or encode node belongs to; a shared step or
	// decode serves branches.
	branch   int
	branches []int
	// transform is a step's operation; output indexes a sink or mux.
	transform mediaTransform
	output    int
	// join is the join a join-tree node belongs to, arm the arm it serves,
	// and stream the planned stream a transform is built against.
	join   *joinPlan
	arm    int
	stream av.Stream
}

// composeGraph is the one schedule a branch composition is described and
// built from: selects, their decodes, and shared steps per selector group;
// each branch's private steps and encoder; then the outputs. Describe emits
// the nodes in this order with each node's edges in consumer order, and the
// build adds the nodes in the same order and connects each one's inputs as
// it is added, so the built graph is the described one by construction.
type composeGraph struct {
	sources  []pipeline.NodeRef
	groups   []branchComposeSelectorGroup
	branches []branchComposeRoute
	outputs  []branchComposeTargetRoute
	nodes    []composeNode
	// branchOut is the node each branch's outputs consume.
	branchOut []pipeline.NodeRef
	// remux holds the outputs of a whole-job packet copy.
	remux []destinationAttachment
	// external are producers planned before the graph (input sources already
	// in the spec); their edges come first.
	external []pipeline.NodeRef
}

func planComposeGraph(sources []pipeline.NodeRef, branches []branchComposeRoute, outputs []branchComposeTargetRoute) (composeGraph, error) {
	g := composeGraph{external: sources}
	if err := g.planBranches(sources, branches, outputs); err != nil {
		return composeGraph{}, err
	}
	return g, nil
}

// planBranches appends a branch composition fed by sources: per selector
// group a select, its decode, and shared steps; then each branch's private
// steps and encoder; then the outputs. A graph holds one composition.
func (g *composeGraph) planBranches(sources []pipeline.NodeRef, branches []branchComposeRoute, outputs []branchComposeTargetRoute) error {
	g.sources = sources
	g.groups = branchComposeSelectorGroups(branches)
	g.branches = branches
	g.outputs = outputs
	g.branchOut = make([]pipeline.NodeRef, len(branches))
	for i := range g.groups {
		group := g.groups[i]
		selectRef := g.add(composeNode{
			kind:   composeSelect,
			name:   branchComposeInputNodeName(selectNodeName(group.selector), group.input),
			detail: selectNodeDetail(group.selector),
			inputs: sources,
			group:  i,
		})
		decoded := branchComposeDecodedBranchIndices(group.branches, branches)
		for _, b := range group.branches {
			if !branchComposeRouteNeedsDecode(branches[b]) {
				g.branchOut[b] = selectRef
			}
		}
		if len(decoded) == 0 {
			continue
		}
		config, err := branchComposeGroupDecodeConfig(decoded, branches)
		if err != nil {
			return err
		}
		decodeRef := g.add(composeNode{
			kind:     composeDecode,
			name:     branchComposeInputNodeName(decodeNodeName(group.selector), group.input),
			detail:   decodeRequestDetail(decodeRequest{selector: group.selector, config: config}),
			inputs:   []pipeline.NodeRef{selectRef},
			group:    i,
			branches: decoded,
		})
		for _, b := range decoded {
			g.branchOut[b] = decodeRef
		}
		for _, prefix := range branchComposeSharedStepGroups(decoded, branches) {
			if len(prefix.branches) == 0 {
				continue
			}
			transforms, err := branchComposeRouteOperationTransformsForName(branchComposeSharedOperationName(branches[prefix.branches[0]]), prefix.operations)
			if err != nil {
				return err
			}
			upstream := decodeRef
			for _, transform := range transforms {
				upstream = g.add(composeNode{
					kind:      composeSharedStep,
					name:      transform.name,
					detail:    mediaTransformDetail(transform),
					inputs:    []pipeline.NodeRef{upstream},
					branches:  prefix.branches,
					transform: transform,
				})
			}
			for _, b := range prefix.branches {
				g.branchOut[b] = upstream
			}
		}
	}
	for b := range branches {
		transforms, err := branchComposePrivateOperationTransforms(branches[b])
		if err != nil {
			return err
		}
		upstream := g.branchOut[b]
		for _, transform := range transforms {
			upstream = g.add(composeNode{
				kind:      composeStep,
				name:      transform.name,
				detail:    mediaTransformDetail(transform),
				inputs:    []pipeline.NodeRef{upstream},
				branch:    b,
				transform: transform,
			})
		}
		if branchComposeRouteNeedsEncode(branches[b]) {
			upstream = g.add(composeNode{
				kind:   composeEncode,
				name:   encodeNodeName(branches[b].request),
				detail: encodeNodeDetail(branches[b].request),
				inputs: []pipeline.NodeRef{upstream},
				branch: b,
			})
		}
		g.branchOut[b] = upstream
	}
	for i := range outputs {
		inputs := make([]pipeline.NodeRef, 0, len(outputs[i].matches))
		for _, b := range outputs[i].matches {
			inputs = append(inputs, g.branchOut[b])
		}
		if outputs[i].sink != nil {
			g.add(composeNode{
				kind:   composeSink,
				name:   branchComposeTargetSinkNodeName(outputs[i], i),
				detail: describedNodeDetail(outputs[i].sink),
				inputs: inputs,
				output: i,
			})
			continue
		}
		g.add(composeNode{
			kind:   composeMux,
			name:   muxNodeName(outputs[i].target, i),
			detail: outputNodeDetailWithFormat(outputs[i].target, outputs[i].output.Format),
			inputs: inputs,
			output: i,
		})
	}
	return nil
}

// planRemuxGraph plans a whole-job packet copy: each output, in order, fed
// by every source.
func planRemuxGraph(sources []pipeline.NodeRef, outputs []destinationAttachment) composeGraph {
	g := composeGraph{sources: sources, external: sources, remux: outputs}
	for i := range outputs {
		if outputs[i].details.sink {
			g.add(composeNode{
				kind:   composeRemuxSink,
				name:   firstNonEmpty(outputs[i].details.sinkComponentName, outputs[i].details.label, "sink"),
				detail: describedNodeDetail(destinationAttachmentSink(outputs[i])),
				inputs: sources,
				output: i,
			})
			continue
		}
		output := destinationAttachmentOutput(outputs[i])
		g.add(composeNode{
			kind:   composeRemuxMux,
			name:   muxNodeName(output, i),
			detail: outputNodeDetailWithFormat(output, destinationAttachmentGraphFormat(outputs[i])),
			inputs: sources,
			output: i,
		})
	}
	return g
}

func (g *composeGraph) add(node composeNode) pipeline.NodeRef {
	g.nodes = append(g.nodes, node)
	return pipeline.NodeRef(node.name)
}

// emit appends the composition to a planned spec whose source nodes are
// already in place: the nodes in schedule order, then the edges grouped by
// producer in schedule order (sources first), each producer's edges in
// consumer order, which is the order a built graph reports them in.
func (g composeGraph) emit(spec pipeline.Spec, planned map[string]plannedNode) (pipeline.Spec, error) {
	for _, node := range g.nodes {
		kind := pipeline.NodeStage
		switch {
		case node.kind == composeSink || node.kind == composeRemuxSink:
			kind = pipeline.NodeSink
		case node.kind == composeArmSource:
			kind = pipeline.NodeSource
		case node.kind == composeJoinDestination && node.join.destinations[node.output].sink != nil:
			kind = pipeline.NodeSink
		}
		if err := addPlannedNode(planned, &spec, node.name, kind, pipeline.NodeRef(node.name), node.detail); err != nil {
			return pipeline.Spec{}, err
		}
	}
	consumers := make(map[pipeline.NodeRef][]pipeline.NodeRef, len(g.nodes)+len(g.external))
	for _, node := range g.nodes {
		for _, input := range node.inputs {
			consumers[input] = append(consumers[input], pipeline.NodeRef(node.name))
		}
	}
	producers := make([]pipeline.NodeRef, 0, len(g.external)+len(g.nodes))
	producers = append(producers, g.external...)
	for _, node := range g.nodes {
		producers = append(producers, pipeline.NodeRef(node.name))
	}
	for _, from := range producers {
		for _, to := range consumers[from] {
			spec.Edges = append(spec.Edges, pipeline.EdgeSpec{From: from, To: to, Policy: pipeline.RouteAll})
		}
	}
	return spec, nil
}

// composeLowering is what building a composition needs beyond its plan: the
// stream each branch resolved to, the source bounds decoders size from, and
// the per-branch queues the work plan sized; a remux reads every stream each
// source carries.
type composeLowering struct {
	streams       []av.Stream
	sourceStreams [][]av.Stream
	bounds        []decodeBoundsProvider
	branchBuffer  map[string]graphPlanBranchComposeBranchOperation
	realtime      bool
	// admission enables per-arm backpressure on join stages; work sizes a
	// pinned join queue.
	admission bool
	work      workPlan
}

// lower builds the composition into graph: each node in schedule order,
// connected to its inputs as it is added.
func (g composeGraph) lower(ctx context.Context, service *builder, graph pipeline.Graph, in composeLowering) error {
	runtime := service.runtime
	streams := make(map[pipeline.NodeRef]av.Stream, len(g.nodes))
	for _, node := range g.nodes {
		ref := pipeline.NodeRef(node.name)
		switch node.kind {
		case composeSelect:
			group := g.groups[node.group]
			selected := in.streams[group.branches[0]]
			stage := newStreamSelectStage(node.name, selected, group.selector, node.detail)
			if err := addComposeStage(graph, stage, runtime.buffer, node.inputs); err != nil {
				return err
			}
			streams[ref] = selected
		case composeDecode:
			group := g.groups[node.group]
			selected := streams[node.inputs[0]]
			config, err := branchComposeGroupDecodeConfig(node.branches, g.branches)
			if err != nil {
				return err
			}
			bounds := codec.DecodeBounds{}
			if len(in.bounds) != 0 {
				bounds = providerDecodeBoundsForStream(selected, in.bounds)
			}
			drop := branchComposeGroupDropDecodeEvents(node.branches, g.branches)
			stage, err := service.newDecodeStageNamed(ctx, node.name, decodeRequest{selector: group.selector, config: config}, selected, in.realtime, drop, bounds)
			if err != nil {
				return err
			}
			if err := addComposeStage(graph, stage, runtime.buffer, node.inputs); err != nil {
				return err
			}
			streams[ref] = selected
		case composeSharedStep, composeStep:
			buffer := runtime.buffer
			if node.kind == composeStep {
				buffer = in.branchBuffer[g.branches[node.branch].name].buffer
			}
			stage, out, err := service.newBranchComposeStepStageNamed(ctx, node.name, node.transform, streams[node.inputs[0]], in.realtime)
			if err != nil {
				return err
			}
			if err := addComposeStage(graph, stage, buffer, node.inputs); err != nil {
				return err
			}
			streams[ref] = out
		case composeEncode:
			branch := g.branches[node.branch]
			config, _, err := prepareEncodeConfig(streams[node.inputs[0]], branch.request, in.realtime)
			if err != nil {
				return err
			}
			_, encoded, err := service.compileEncodeStageNamed(ctx, graph, node.name, node.inputs[0], branch.request, config, in.branchBuffer[branch.name].buffer)
			if err != nil {
				return err
			}
			streams[ref] = encoded
		case composeSink:
			output := g.outputs[node.output]
			sink := output.sink
			if node.name != sink.Name() {
				sink = namedSink{name: node.name, sink: sink}
			}
			sinkRef, err := graph.AddSink(sink, branchComposeOutputBuffer(runtime.buffer, output.matches, g.branches, in.branchBuffer))
			if err != nil {
				return err
			}
			if err := connectComposeInputs(graph, node.inputs, sinkRef); err != nil {
				return err
			}
		case composeArmSource, composeArmDecode, composeTransform, composeTapRestamp, composeJoin, composeJoinEncode, composeJoinDestination:
			stream, err := lowerJoinNode(ctx, service, graph, node, streams, in)
			if err != nil {
				return err
			}
			streams[ref] = stream
		case composeRemuxSink:
			sink := destinationAttachmentSink(g.remux[node.output])
			if node.name != sink.Name() {
				sink = namedSink{name: node.name, sink: sink}
			}
			sinkRef, err := graph.AddSink(sink, runtime.buffer)
			if err != nil {
				return err
			}
			if err := connectComposeInputs(graph, node.inputs, sinkRef); err != nil {
				return err
			}
		case composeRemuxMux:
			output := g.remux[node.output]
			var muxed []av.Stream
			for i := range g.sources {
				muxed = append(muxed, in.sourceStreams[i]...)
			}
			muxStage, err := service.openMuxDestinationStage(ctx, destinationAttachmentDestination(output), node.output, muxed, destinationAttachmentOpenFormat(output), destinationAttachmentGraphFormat(output))
			if err != nil {
				return err
			}
			stage := pipeline.Stage(muxStage)
			if node.name != muxStage.Name() {
				stage = namedStage{name: node.name, stage: muxStage}
			}
			if err := addComposeStage(graph, stage, runtime.buffer, node.inputs); err != nil {
				return err
			}
		case composeMux:
			output := g.outputs[node.output]
			muxed := make([]av.Stream, 0, len(node.inputs))
			for _, input := range node.inputs {
				muxed = append(muxed, streams[input])
			}
			destination := output.output.Destination
			if destinationSpecEmpty(destination) {
				destination = destinationSpec{output: output.target, format: output.output.Format}
			}
			muxStage, err := service.openMuxDestinationStage(ctx, destination, node.output, muxed, branchComposeTargetOpenFormat(output.output), output.output.Format)
			if err != nil {
				return err
			}
			stage := pipeline.Stage(muxStage)
			if node.name != muxStage.Name() {
				stage = namedStage{name: node.name, stage: muxStage}
			}
			if err := addComposeStage(graph, stage, branchComposeOutputBuffer(runtime.buffer, output.matches, g.branches, in.branchBuffer), node.inputs); err != nil {
				return err
			}
		}
	}
	return nil
}

// addComposeStage adds stage and connects its inputs, closing the stage if
// the graph refuses it.
func addComposeStage(graph pipeline.Graph, stage pipeline.Stage, buffer pipeline.BufferPolicy, inputs []pipeline.NodeRef) error {
	ref, err := graph.AddStage(stage, buffer)
	if err != nil {
		stage.Close()
		return err
	}
	return connectComposeInputs(graph, inputs, ref)
}

func connectComposeInputs(graph pipeline.Graph, inputs []pipeline.NodeRef, to pipeline.NodeRef) error {
	for _, from := range inputs {
		if err := connectRefs(graph, from, to); err != nil {
			return err
		}
	}
	return nil
}

// composeBranchStreams maps the resolved stream groups back to the stream
// each branch reads.
func composeBranchStreams(groups []branchComposeStreamGroup, branches int) []av.Stream {
	streams := make([]av.Stream, branches)
	for i := range groups {
		for _, b := range groups[i].branches {
			streams[b] = groups[i].stream
		}
	}
	return streams
}
