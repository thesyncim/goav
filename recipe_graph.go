package goav

import (
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
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
	// ops is the value operation each NodeOp came from; roots carries the
	// value root of each root node.
	ops   []streamOp
	roots []*streamRoot
	// outputs are the recipe's Outputs in order: the node each routes (a
	// NodeInput for input.To) and its destination nodes in .To order.
	outputs []graphOutput

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
		return nil, streamInvalidError("", "a recipe needs at least one Output: route a stream with stream.To(destination)")
	}
	g := &recipeGraph{
		byNode:  map[*streamNode]recipeir.NodeID{},
		byInput: map[*inputIdentity]recipeir.NodeID{},
		byDest:  map[uint64]recipeir.NodeID{},
	}
	for i := range outs {
		out := outs[i]
		if len(out.dests) == 0 {
			return nil, streamInvalidError("", "an Output needs a destination: stream.To(goav.Write(name, writer)) or .To(goav.Sink(sink))")
		}
		var from recipeir.NodeID
		if out.remux != nil {
			from = g.input(*out.remux)
		} else {
			if _, err := streamNodes(out.stream); err != nil {
				return nil, err
			}
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
	return g, nil
}

func (g *recipeGraph) add(node recipeir.Node, op streamOp, root *streamRoot) recipeir.NodeID {
	id := recipeir.NodeID(len(g.graph.Nodes))
	g.graph.Nodes = append(g.graph.Nodes, node)
	g.ops = append(g.ops, op)
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
	id := g.add(recipeir.Node{Kind: recipeir.NodeInput, Input: index}, streamOp{}, nil)
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
		return id
	}
	index := len(g.dests)
	g.dests = append(g.dests, dest)
	id := g.add(recipeir.Node{Kind: recipeir.NodeOutput, Output: index}, streamOp{}, nil)
	if key != 0 {
		g.byDest[key] = id
	}
	return id
}

// stream interns one Stream node and, first, everything it reads.
func (g *recipeGraph) stream(n *streamNode) (recipeir.NodeID, error) {
	if n == nil {
		return 0, streamInvalidError("", "the stream is the zero Stream: start from an input (input.Video(), input.Audio()) or a join (goav.Mix(...))")
	}
	if id, ok := g.byNode[n]; ok {
		return id, nil
	}
	if n.err != nil {
		return 0, n.err
	}
	if n.root == nil {
		if n.parent == nil {
			return 0, streamInvalidError("", "the stream has no input")
		}
		parent, err := g.stream(n.parent)
		if err != nil {
			return 0, err
		}
		id := parent
		for _, op := range splitStreamOp(n.op) {
			id = g.add(recipeir.Node{Kind: recipeir.NodeOp, Parents: []recipeir.NodeID{id}, Op: graphOperation(op)}, op, nil)
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
		node := recipeir.Node{Kind: recipeir.NodeJoin, Join: recipeir.JoinNode{
			Kind:  firstNonEmpty(string(root.join.kind), root.join.name),
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
		return g.add(node, streamOp{}, root), nil
	case root.tap != "" || root.track != nil || root.route != nil:
		anchor := recipeir.Anchor{Tap: root.tap, Route: root.route, Input: -1}
		if root.track != nil {
			track := *root.track
			anchor.Track = &track
			anchor.Input = g.node(g.input(root.input)).Input
		}
		return g.add(recipeir.Node{Kind: recipeir.NodeAnchor, Anchor: anchor}, streamOp{}, root), nil
	case root.input.id == nil:
		return 0, streamInvalidError("", "the stream's input was not constructed: use goav.FileInput, goav.Input, or goav.Source")
	case root.rule != nil:
		input := g.input(root.input)
		g.matches = append(g.matches, *root.rule)
		g.graph.Rules = append(g.graph.Rules, root.rule.Description())
		return g.add(recipeir.Node{Kind: recipeir.NodeRule, Parents: []recipeir.NodeID{input}, Rule: len(g.matches) - 1}, streamOp{}, root), nil
	default:
		input := g.input(root.input)
		config := newStreamSelectConfig(root.media, root.options...)
		return g.add(recipeir.Node{
			Kind:    recipeir.NodeSelect,
			Parents: []recipeir.NodeID{input},
			Input:   g.node(input).Input,
			Select:  streamSelectFromAV(config.selector),
		}, streamOp{}, root), nil
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
		return recipeir.Operation{Kind: plan.OpDecode, Decode: codecSpecFromOptions(op.codecOptions...)}
	case streamOpEncode:
		return recipeIROperationFromSpec(operationSpecForEncode(cloneCodecSpec(op.encode)))
	case streamOpResize:
		return recipeIROperationFromSpec(operationSpecForTransform(resizeSpec(op.width, op.height, op.resize...)))
	case streamOpResample:
		return recipeIROperationFromSpec(operationSpecForTransform(resampleSpec(op.rate, op.channels, op.audio...)))
	case streamOpDo:
		var stage pipeline.Stage
		if len(op.stages) != 0 {
			stage = op.stages[0]
		}
		return recipeIROperationFromSpec(operationSpecForStage(stage))
	case streamOpPlayout:
		return recipeIROperationFromSpec(operationSpecForPlayout(op.playout))
	case streamOpSync:
		return recipeIROperationFromSpec(operationSpecForSync(op.sync))
	case streamOpTap:
		return recipeir.Operation{Kind: plan.OpTap, Component: op.name, Tap: recipeir.Tap{Name: op.name}}
	case streamOpBuffer:
		return recipeir.Operation{Kind: recipeir.OpBuffer, Buffer: op.buffer}
	case streamOpAuto:
		return recipeIROperationFromSpec(operationSpecForAutoPolicy(op.policies))
	case streamOpRequire:
		return recipeIROperationFromSpec(operationSpecForRequire(op.spec))
	case streamOpPrefer:
		return recipeIROperationFromSpec(operationSpecForPreference(op.spec))
	case streamOpShape:
		return recipeIROperationFromSpec(operationSpecForShape(op.spec))
	case streamOpName:
		return recipeir.Operation{Kind: recipeir.OpName, Name: op.name}
	default:
		return recipeir.Operation{}
	}
}

// path lists the nodes from a stream's root to it: its select, rule, anchor,
// or join, then its operations in order.
func (g *recipeGraph) path(id recipeir.NodeID) []recipeir.NodeID {
	var nodes []recipeir.NodeID
	for {
		nodes = append(nodes, id)
		node := g.node(id)
		if node.Kind != recipeir.NodeOp {
			break
		}
		id = node.Parents[0]
	}
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	return nodes
}

// rootOf is the value root of a root node.
func (g *recipeGraph) rootOf(id recipeir.NodeID) *streamRoot { return g.roots[id] }

// opOf is the value operation of an operation node.
func (g *recipeGraph) opOf(id recipeir.NodeID) streamOp { return g.ops[id] }

// destinationsOf are the Destination handles of output nodes.
func (g *recipeGraph) destinationsOf(nodes []recipeir.NodeID) []Destination {
	out := make([]Destination, 0, len(nodes))
	for _, id := range nodes {
		out = append(out, g.dests[g.node(id).Output])
	}
	return out
}
