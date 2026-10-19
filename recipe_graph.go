package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// recipeGraph is a value recipe compiled to its DAG: the recipeir.Graph and
// the concrete handles its nodes index — inputs, destinations, and stream
// matches — which stay outside the IR. Every Stream an Output reaches is one
// node: a Stream used twice is one node with several children, so the work
// before a fork is planned once, and a join is one node with several
// parents.
type recipeGraph struct {
	graph   recipeir.Graph
	inputs  []InputSpec
	dests   []Destination
	matches []sourcepkg.StreamMatch
	// roots carries the value root of each root node.
	roots []*streamRoot
	// outputs are the recipe's Outputs in order: the node each routes (a
	// NodeInput for input.To) and its destination nodes in .To order.
	outputs []graphOutput

	// err is the first conflict between copies of one destination handle.
	err error

	byNode  map[*streamNode]recipeir.NodeID
	byInput map[*inputIdentity]recipeir.NodeID
	byDest  map[uint64]recipeir.NodeID
}

type graphOutput struct {
	from  recipeir.NodeID
	dests []recipeir.NodeID
}

// compileGraph compiles Outputs into one recipe DAG. Construction errors on
// a Stream (and a zero Stream or input) surface here, before any planning.
func compileGraph(outs []Output) (*recipeGraph, error) {
	if len(outs) == 0 {
		return nil, publicError(compile.StreamInvalidError("", "a recipe needs at least one Output: route a stream with stream.To(destination)"))
	}
	g := &recipeGraph{
		byNode:  map[*streamNode]recipeir.NodeID{},
		byInput: map[*inputIdentity]recipeir.NodeID{},
		byDest:  map[uint64]recipeir.NodeID{},
	}
	for i := range outs {
		out := outs[i]
		if len(out.dests) == 0 {
			return nil, publicError(compile.StreamInvalidError("", "an Output needs a destination: stream.To(goav.Write(name, writer)) or .To(goav.Sink(sink))"))
		}
		var from recipeir.NodeID
		if out.remux != nil {
			from = g.input(*out.remux)
		} else {
			id, err := g.stream(out.stream.node)
			if err != nil {
				return nil, err
			}
			from = id
		}
		output := graphOutput{from: from}
		for _, dest := range out.dests {
			node := g.destination(dest)
			g.graph.Nodes[node].Parents = append(g.graph.Nodes[node].Parents, from)
			output.dests = append(output.dests, node)
		}
		g.outputs = append(g.outputs, output)
	}
	if g.err != nil {
		return nil, g.err
	}
	return g, nil
}

func (g *recipeGraph) add(node recipeir.Node, root *streamRoot) recipeir.NodeID {
	id := recipeir.NodeID(len(g.graph.Nodes))
	g.graph.Nodes = append(g.graph.Nodes, node)
	g.roots = append(g.roots, root)
	return id
}

func (g *recipeGraph) node(id recipeir.NodeID) recipeir.Node { return g.graph.Nodes[id] }

// input is the source node of one input, shared by every stream of it.
func (g *recipeGraph) input(spec InputSpec) recipeir.NodeID {
	if id, ok := g.byInput[spec.id]; ok && spec.id != nil {
		return id
	}
	index := len(g.inputs)
	g.inputs = append(g.inputs, spec)
	id := g.add(recipeir.Node{Kind: recipeir.NodeInput, Input: index}, nil)
	if spec.id != nil {
		g.byInput[spec.id] = id
	}
	return id
}

// destination is the output node of one Destination handle: streams routed
// to one handle feed one node.
func (g *recipeGraph) destination(dest Destination) recipeir.NodeID {
	key := dest.spec.id
	if id, ok := g.byDest[key]; ok && key != 0 {
		if first := g.dests[g.node(id).Output]; destinationConfigKey(first.spec) != destinationConfigKey(dest.spec) && g.err == nil {
			// Copies of one handle (Destination.With) must agree: they are one output.
			g.err = publicError(compile.DestinationNameClashError(first.spec.label("destination")))
		}
		return id
	}
	index := len(g.dests)
	g.dests = append(g.dests, dest)
	id := g.add(recipeir.Node{Kind: recipeir.NodeOutput, Output: index}, nil)
	if key != 0 {
		g.byDest[key] = id
	}
	return id
}

// stream interns one Stream node and, first, everything it reads.
func (g *recipeGraph) stream(n *streamNode) (recipeir.NodeID, error) {
	if n == nil {
		return 0, publicError(compile.StreamInvalidError("", "the stream is the zero Stream: start from an input (input.Video(), input.Audio()) or a join (goav.Mix(...))"))
	}
	if id, ok := g.byNode[n]; ok {
		return id, nil
	}
	if n.err != nil {
		return 0, n.err
	}
	if n.root == nil {
		if n.parent == nil {
			return 0, publicError(compile.StreamInvalidError("", "the stream has no input"))
		}
		parent, err := g.stream(n.parent)
		if err != nil {
			return 0, err
		}
		id := parent
		for _, op := range splitStreamOp(n.op) {
			id = g.add(recipeir.Node{Kind: recipeir.NodeOp, Parents: []recipeir.NodeID{id}, Op: graphOperation(op)}, nil)
		}
		g.byNode[n] = id
		return id, nil
	}
	id, err := g.root(n.root)
	if err != nil {
		return 0, err
	}
	g.byNode[n] = id
	return id, nil
}

func (g *recipeGraph) root(root *streamRoot) (recipeir.NodeID, error) {
	switch {
	case root.join != nil:
		node := recipeir.Node{Kind: recipeir.NodeJoin, Join: recipeir.Join{
			Kind:  firstNonEmpty(root.join.kind, root.join.name),
			Stage: root.join.stage,
			Sync:  root.join.sync,
		}}
		for _, layer := range root.join.layers {
			stream, x, y, placed := layer.compositeLayer()
			arm, err := g.stream(stream.node)
			if err != nil {
				return 0, err
			}
			node.Parents = append(node.Parents, arm)
			node.Join.At = append(node.Join.At, recipeir.Point{X: x, Y: y, Set: placed})
		}
		return g.add(node, root), nil
	case root.tap != "" || root.track != nil || root.route != nil:
		anchor := recipeir.Anchor{Tap: root.tap, Route: root.route, Input: -1}
		if root.track != nil {
			track := *root.track
			anchor.Track = &track
			anchor.Input = g.node(g.input(root.input)).Input
		}
		return g.add(recipeir.Node{Kind: recipeir.NodeAnchor, Anchor: anchor}, root), nil
	case root.input.id == nil:
		return 0, publicError(compile.StreamInvalidError("", "the stream's input was not constructed: use goav.FileInput, goav.Input, or goav.Source"))
	case root.rule != nil:
		input := g.input(root.input)
		g.matches = append(g.matches, *root.rule)
		g.graph.Rules = append(g.graph.Rules, root.rule.Description())
		return g.add(recipeir.Node{Kind: recipeir.NodeRule, Parents: []recipeir.NodeID{input}, Rule: len(g.matches) - 1}, root), nil
	default:
		input := g.input(root.input)
		config := newStreamSelectConfig(root.media, root.options...)
		return g.add(recipeir.Node{
			Kind:    recipeir.NodeSelect,
			Parents: []recipeir.NodeID{input},
			Input:   g.node(input).Input,
			Select:  compile.StreamSelectFromAV(config.selector),
		}, root), nil
	}
}

// splitStreamOp splits a Do of several stages into one operation per stage:
// each stage is a node of its own.
func splitStreamOp(op streamOp) []streamOp {
	if op.kind != streamOpDo || len(op.stages) < 2 {
		return []streamOp{op}
	}
	out := make([]streamOp, 0, len(op.stages))
	for _, stage := range op.stages {
		one := op
		one.stages = []pipeline.Stage{stage}
		out = append(out, one)
	}
	return out
}

// graphOperation is the recipe IR record of one value operation. A tap's
// domain follows its position, which the planner resolves.
func graphOperation(op streamOp) recipeir.Operation {
	switch op.kind {
	case streamOpDecode:
		return opDecode(codecSpecFromOptions(op.codecOptions...))
	case streamOpEncode:
		return opEncode(op.encode)
	case streamOpResize:
		return opTransform(resizeSpec(op.width, op.height, op.resize...))
	case streamOpResample:
		return opTransform(resampleSpec(op.rate, op.channels, op.audio...))
	case streamOpDo:
		var stage pipeline.Stage
		if len(op.stages) != 0 {
			stage = op.stages[0]
		}
		return opStage(stage)
	case streamOpPlayout:
		return opPlayout(op.playout)
	case streamOpSync:
		return opSync(op.sync)
	case streamOpTap:
		return recipeir.Operation{Kind: plan.OpTap, Component: op.name, Tap: recipeir.Tap{Name: op.name}}
	case streamOpBuffer:
		return recipeir.Operation{Kind: recipeir.OpBuffer, Buffer: op.buffer}
	case streamOpAuto:
		return opAuto(op.policies)
	case streamOpRequire:
		return opRequire(op.spec)
	case streamOpPrefer:
		return opPrefer(op.spec)
	case streamOpShape:
		return opShape(op.spec)
	case streamOpName:
		return recipeir.Operation{Kind: recipeir.OpName, Name: op.name}
	default:
		return recipeir.Operation{}
	}
}

// rootOf is the value root of a root node.
func (g *recipeGraph) rootOf(id recipeir.NodeID) *streamRoot { return g.roots[id] }

// destinationConfigKey is what copies of one destination handle must agree on
// to be one output: its name, address, container, and sink.
func destinationConfigKey(spec destinationSpec) string {
	return destinationIdentityFromFacts(branchDestinationFactsFromSpec(spec.label(""), spec))
}

// compileHandles are the concrete handles a recipe's DAG indexes: its
// inputs, and its destinations by graph slot. They check what only a handle
// knows for the compiler, as its compileHost.
type compileHandles struct {
	runtime *Runtime
	inputs  []InputSpec
	dests   []destinationSpec
}

// handles converts the graph's destination handles; a handle that does not
// convert stays zero, and its facts carry why.
func (g *recipeGraph) handles(rt *Runtime) *compileHandles {
	h := &compileHandles{runtime: rt, inputs: g.inputs, dests: make([]destinationSpec, len(g.dests))}
	for i := range g.dests {
		h.dests[i], _ = destinationSpecFromDestination(g.dests[i])
	}
	return h
}

// compileRequest projects the DAG and the facts of its handles: the one
// thing the compiler reads.
func (g *recipeGraph) compileRequest(h *compileHandles) *compile.Request {
	req := &compile.Request{
		Graph:   recipeir.Graph{Nodes: append([]recipeir.Node(nil), g.graph.Nodes...), Rules: g.graph.Rules},
		Matches: g.matches,
		Host:    h,
	}
	for _, out := range g.outputs {
		req.Outputs = append(req.Outputs, compile.Output{From: out.from, Dests: out.dests})
	}
	sourceNames := graphSourceNodeNames(g.inputs)
	for i, input := range g.inputs {
		req.Inputs = append(req.Inputs, compileInputFromSpec(input, sourceNames[i]))
	}
	for i := range g.dests {
		_, err := destinationSpecFromDestination(g.dests[i])
		facts := compileDestinationFromSpec(h.dests[i])
		facts.Label = g.dests[i].spec.label("")
		if err != nil {
			facts.Invalid = err.Error()
		}
		req.Dests = append(req.Dests, facts)
	}
	return req
}

// compileInputFromSpec is what the compiler knows of one input.
func compileInputFromSpec(input InputSpec, sourceNode string) compile.Input {
	declared, ok := declaredSourceShape(input)
	return compile.Input{
		IR:           recipeIRInputFromSpec(input),
		Name:         input.inputName(input.graphSourceNodeName()),
		SourceNode:   sourceNode,
		SourceDetail: input.graphSourceNodeDetail(),
		Shape:        declared,
		HasShape:     ok,
		Frames:       ok && declared.Domain == shape.DomainFrame,
		Live:         input.realtime || input.input.Realtime,
		Demuxed:      input.provider == nil && input.source == nil,
		EventDomain:  input.sourceEventDomain(),
	}
}

// compileDestinationFromSpec is what the compiler knows of one destination.
func compileDestinationFromSpec(spec destinationSpec) compile.Destination {
	facts := compile.Destination{
		Label:          spec.label(""),
		Group:          destinationIdentityFromFacts(branchDestinationFactsFromSpec("", spec)),
		IR:             recipeIRDestinationFromSpec("", spec),
		Sink:           spec.sink != nil,
		HasOutput:      destinationSpecHasOutput(spec),
		Output:         spec.output,
		Format:         spec.format,
		ResolvedFormat: spec.resolvedFormat,
	}
	if spec.sink != nil {
		facts.SinkName = spec.sink.Name()
		facts.SinkDetail = compile.DescribedNodeDetail(spec.sink)
	}
	return facts
}

// outputs are the destination handles of a recipe, in its order.
func (h *compileHandles) outputs(order []int) []destinationSpec {
	out := make([]destinationSpec, len(order))
	for i, slot := range order {
		out[i] = h.dests[slot]
	}
	return out
}

func (h *compileHandles) ValidateHandles(operation string, order []int, names []string) error {
	if err := validateJobInputs(h.inputs); err != nil {
		return err
	}
	return validateDestinationSpecs(operation, h.outputs(order), names...)
}

func (h *compileHandles) ResolveOutputs(ctx context.Context, order []int, names []string) ([]av.FormatID, error) {
	resolved, err := validateOutputFormatAdapters(ctx, h.runtime, h.outputs(order), names...)
	if err != nil {
		return nil, err
	}
	formats := make([]av.FormatID, len(order))
	for i, slot := range order {
		h.dests[slot] = resolved[i]
		formats[i] = resolved[i].resolvedFormat
	}
	return formats, nil
}

func (h *compileHandles) ProbeInputs(ctx context.Context) ([]format.ProbeResult, error) {
	return validateInputFormatAdapters(ctx, h.runtime, h.inputs)
}
