package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

// dag plans a join job as one composeGraph: the join tree, then its
// downstream (planned branches, the downstream transforms and encoder that
// feed the destinations, or the destinations straight off the join).
func (p *joinPlan) dag() (composeGraph, error) {
	var g composeGraph
	joinRef := p.planDAG(&g)
	switch {
	case len(p.branchRoutes) != 0:
		if err := g.planBranches([]pipeline.NodeRef{joinRef}, nil, p.branchRoutes, p.branchTargets); err != nil {
			return composeGraph{}, err
		}
	case p.encode != nil:
		upstream := joinRef
		for i := range p.downstream {
			upstream = g.add(composeNode{
				kind:      composeTransform,
				name:      p.downstream[i].transform.name,
				detail:    mediaTransformDetail(p.downstream[i].transform),
				inputs:    []pipeline.NodeRef{upstream},
				transform: p.downstream[i].transform,
				stream:    p.downstream[i].stream,
				join:      p,
				resolved:  p.late,
			})
		}
		encodeRef := g.add(composeNode{
			kind:   composeJoinEncode,
			name:   encodeNodeName(*p.encode),
			detail: encodeNodeDetail(*p.encode),
			inputs: []pipeline.NodeRef{upstream},
			join:   p,
		})
		p.planDestinationDAG(&g, encodeRef)
	default:
		p.planDestinationDAG(&g, joinRef)
	}
	return g, nil
}

// planDAG appends this join's tree depth first: each arm's chain (its source,
// a nested join, or a tap arm's restamp hung off the tap's node), its decode
// and conversion stage, then the join node, whose convergence edges come
// after every arm's chain. It returns the join node.
func (p *joinPlan) planDAG(g *composeGraph) pipeline.NodeRef {
	armRefs := make([]pipeline.NodeRef, 0, len(p.arms))
	for i := range p.arms {
		arm := &p.arms[i]
		var upstream pipeline.NodeRef
		switch {
		case arm.sub != nil:
			upstream = arm.sub.planDAG(g)
		case arm.tapArm != nil:
			upstream = g.add(composeNode{
				kind:   composeTapRestamp,
				name:   arm.tapArm.node,
				detail: tapArmNodeDetail(arm.tapArm.tap.name),
				inputs: []pipeline.NodeRef{pipeline.NodeRef(arm.tapArm.anchor.node())},
				join:   p,
				arm:    i,
			})
		default:
			upstream = g.add(composeNode{
				kind:   composeArmSource,
				name:   arm.sourceNode,
				detail: arm.input.graphSourceNodeDetail(),
				join:   p,
				arm:    i,
			})
		}
		if arm.bind != nil {
			upstream = g.add(composeNode{
				kind:   composeArmBind,
				name:   arm.bind.node,
				detail: bindArmNodeDetail(arm.bind.selector),
				inputs: []pipeline.NodeRef{upstream},
				join:   p,
				arm:    i,
			})
		}
		if arm.decodeNode != "" {
			upstream = g.add(composeNode{
				kind:   composeArmDecode,
				name:   arm.decodeNode,
				detail: decodeRequestDetail(decodeRequest{selector: av.StreamSelector{Type: arm.stream.Type}}),
				inputs: []pipeline.NodeRef{upstream},
				join:   p,
				arm:    i,
			})
		}
		if arm.stage != nil {
			upstream = g.add(composeNode{
				kind:      composeTransform,
				name:      arm.stage.transform.name,
				detail:    mediaTransformDetail(arm.stage.transform),
				inputs:    []pipeline.NodeRef{upstream},
				transform: arm.stage.transform,
				stream:    arm.stage.stream,
				join:      p,
			})
		}
		if arm.conform != nil {
			node := composeNode{
				kind:   composeArmConform,
				name:   arm.conform.node,
				detail: conformNodeDetail(arm.conform.target),
				inputs: []pipeline.NodeRef{upstream},
				join:   p,
				arm:    i,
			}
			if i > 0 && p.profile.conformFollowsFirstArm {
				node.target = armRefs[0]
			}
			upstream = g.add(node)
		}
		armRefs = append(armRefs, upstream)
	}
	return g.add(composeNode{
		kind:   composeJoin,
		name:   p.name,
		detail: p.joinNodeDetail(),
		inputs: armRefs,
		join:   p,
	})
}

func (p *joinPlan) planDestinationDAG(g *composeGraph, upstream pipeline.NodeRef) {
	for i := range p.destinations {
		detail := outputNodeDetailWithFormat(p.destinations[i].output, destinationGraphFormat(p.destinations[i]))
		if p.destinations[i].sink != nil {
			detail = describedNodeDetail(p.destinations[i].sink)
		}
		g.add(composeNode{
			kind:   composeJoinDestination,
			name:   p.joinDestinationNodeName(i),
			detail: detail,
			inputs: []pipeline.NodeRef{upstream},
			output: i,
			join:   p,
		})
	}
}

// lowerJoinNode builds one join-tree node and returns the stream it emits.
// Arms publish their stream id to the convergence stage, so a node keeps its
// input's stream unless it re-publishes one.
func lowerJoinNode(ctx context.Context, service *builder, graph pipeline.Graph, node composeNode, streams map[pipeline.NodeRef]av.Stream, in composeLowering) (av.Stream, error) {
	p := node.join
	rt := p.runtime
	switch node.kind {
	case composeArmSource:
		arm := p.arms[node.arm]
		source, opened, _, err := arm.input.openGraphSource(ctx, service, node.name)
		if err != nil {
			return av.Stream{}, err
		}
		selector := av.StreamSelector{Type: p.profile.media}
		if arm.bind != nil {
			selector = arm.bind.selector
		}
		stream, err := selectStream(opened, selector)
		if err != nil {
			source.Close()
			return av.Stream{}, err
		}
		if _, err := graph.AddSource(source, rt.buffer); err != nil {
			source.Close()
			return av.Stream{}, err
		}
		return stream, nil
	case composeArmBind:
		// The selected stream republishes under the arm id: two files carry
		// the same track ids.
		stream := streams[node.inputs[0]]
		stage := newArmRestampStage(node.name, node.detail, stream.ID, p.arms[node.arm].stream.ID)
		stream.ID = p.arms[node.arm].stream.ID
		return stream, addComposeStage(graph, stage, rt.buffer, node.inputs)
	case composeArmDecode:
		stream := streams[node.inputs[0]]
		// Arm events must flow through decode (dropInputEvents=false): the
		// join's per-arm EOS bookkeeping and its PTS re-sync on discontinuity
		// read them, and the join consumes them.
		stage, err := service.newDecodeStageNamed(ctx, node.name, decodeRequest{selector: av.StreamSelector{Type: stream.Type}}, stream, rt.realtime, false, codec.DecodeBounds{})
		if err != nil {
			return av.Stream{}, err
		}
		return stream, addComposeStage(graph, stage, rt.buffer, node.inputs)
	case composeArmConform:
		stream := streams[node.inputs[0]]
		first := stream
		if node.target != "" {
			first = streams[node.target]
		}
		target := p.profile.conformTarget(p, first)
		stage := newJoinConformStage(service, node.name, node.detail, target, rt.realtime)
		return conformedStream(stream, target), addComposeStage(graph, stage, rt.buffer, node.inputs)
	case composeTransform:
		stream := node.stream
		if node.resolved {
			stream = streams[node.inputs[0]]
		}
		stage, out, err := service.newMediaTransformStageNamed(ctx, node.name, node.transform, stream, rt.realtime)
		if err != nil {
			return av.Stream{}, err
		}
		out.ID = streams[node.inputs[0]].ID
		return out, addComposeStage(graph, stage, rt.buffer, node.inputs)
	case composeTapRestamp:
		arm := p.arms[node.arm]
		// No source opens: the restamp republishes the tapped media under
		// the tap name, hung off the node the declaring arm built.
		stream := streams[node.inputs[0]]
		if stream.Type == "" {
			stream = arm.stream
		}
		stream.ID = av.StreamID(arm.tapArm.tap.name)
		stage := newArmRestampStage(node.name, node.detail, "", stream.ID)
		return stream, addComposeStage(graph, stage, rt.buffer, node.inputs)
	case composeJoin:
		armIDs := make([]av.StreamID, 0, len(node.inputs))
		seen := make(map[av.StreamID]struct{}, len(node.inputs))
		for _, input := range node.inputs {
			id := streams[input].ID
			if _, dup := seen[id]; dup {
				return av.Stream{}, joinArmError(p.name, string(id), p.name+" arms must have distinct stream ids",
					"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id")
			}
			seen[id] = struct{}{}
			armIDs = append(armIDs, id)
		}
		stage, pinned := p.profile.newStage(p, armIDs)
		if admitting, ok := stage.(interface{ enableAdmission() }); ok && in.admission {
			admitting.enableAdmission()
		}
		buffer := rt.buffer
		if pinned != nil {
			buffer = *pinned
			if !buffer.IsDirect() {
				var err error
				if buffer, err = p.joinNodeBufferPolicy(buffer, in.work); err != nil {
					return av.Stream{}, err
				}
			}
		}
		joined := p.joined
		if p.late {
			joined = p.resolvedJoinedStream(streams, node.inputs)
		}
		return joined, addComposeStage(graph, stage, buffer, node.inputs)
	case composeJoinEncode:
		config := p.encodeConfig
		if p.late {
			// The joined format is known now its arms opened: encode it.
			var err error
			if config, _, err = prepareEncodeConfig(streams[node.inputs[0]], *p.encode, rt.realtime); err != nil {
				return av.Stream{}, err
			}
		}
		_, encoded, err := compileEncodeStage(ctx, service, graph, node.inputs[0], *p.encode, config)
		return encoded, err
	case composeJoinDestination:
		destination := p.destinations[node.output]
		if destination.sink != nil {
			sink := destination.sink
			if node.name != sink.Name() {
				sink = namedSink{name: node.name, sink: sink}
			}
			ref, err := graph.AddSink(sink, rt.buffer)
			if err != nil {
				return av.Stream{}, err
			}
			return av.Stream{}, connectComposeInputs(graph, node.inputs, ref)
		}
		muxStage, err := service.openMuxDestinationStage(ctx, destination, node.output, []av.Stream{streams[node.inputs[0]]}, destinationOpenFormat(destination), destinationGraphFormat(destination))
		if err != nil {
			return av.Stream{}, err
		}
		stage := pipeline.Stage(muxStage)
		if node.name != muxStage.Name() {
			stage = namedStage{name: node.name, stage: muxStage}
		}
		return av.Stream{}, addComposeStage(graph, stage, rt.buffer, node.inputs)
	}
	return av.Stream{}, nil
}

func (p *joinPlan) spec() (pipeline.Spec, error) {
	g, err := p.dag()
	if err != nil {
		return pipeline.Spec{}, err
	}
	spec := pipeline.Spec{Name: "goav-" + p.name, Realtime: runtimeRealtime(p.runtime)}
	return g.emit(spec, make(map[string]plannedNode, len(g.nodes)))
}

// lower builds the planned join: the tree depth first, each arm's input
// opened through the input seam, then the downstream, every node connected
// to its inputs as it is added, so the built graph equals the planned spec.
func (p *joinPlan) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	g, err := p.dag()
	if err != nil {
		return err
	}
	rt := p.runtime
	in := composeLowering{
		realtime: rt.realtime,
		work:     gp.work,
		// Arm backpressure is for offline joins, where holding a fast arm
		// back is lossless and its unbounded lead is the memory risk. A live
		// join keeps buffering: a sparse arm (a slide feed beside a camera)
		// needs the fast arm to lead by its whole period, and holding a live
		// source back would drop its frames at capture.
		admission: !rt.realtime && !p.treeHasTapArm(),
	}
	if len(p.branchRoutes) != 0 {
		groups, err := resolveBranchComposeStreamGroups([]av.Stream{p.joined}, p.branchRoutes)
		if err != nil {
			return err
		}
		in.streams = composeBranchStreams(groups, len(p.branchRoutes))
	}
	return g.lower(ctx, service, graph, in)
}
