package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

// lowerJoinNode builds one join-tree node and returns the stream it emits.
// Arms publish their stream id to the convergence stage, so a node keeps its
// input's stream unless it re-publishes one.
func lowerJoinNode(ctx context.Context, service *builder, graph pipeline.Graph, node composeNode, streams map[pipeline.NodeRef]av.Stream, in composeLowering) (av.Stream, error) {
	p := node.join
	rt := p.runtime
	switch node.kind {
	case composeArmBind:
		// The selected stream republishes under the arm id: two files carry
		// the same track ids.
		arm := p.arms[node.arm]
		stream := streams[node.inputs[0]]
		if opened := in.sourceStreams[arm.input]; len(opened) != 0 {
			selected, err := selectStream(opened, arm.bind.selector)
			if err != nil {
				return av.Stream{}, err
			}
			stream = qualifyStream(selected, in.qualifiers[arm.input])
		}
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
		}
		joined := p.joined
		if p.late {
			joined = p.resolvedJoinedStream(streams, node.inputs)
		}
		return joined, addComposeStage(graph, stage, buffer, node.inputs)
	}
	return av.Stream{}, nil
}
