package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

// lowerJoinNode builds one join-tree node and returns the stream it emits.
// Arms publish their stream id to the convergence stage, so a node keeps its
// input's stream unless it re-publishes one.
func lowerJoinNode(ctx context.Context, service *builder, graph pipeline.Graph, node compile.ComposeNode, streams map[pipeline.NodeRef]av.Stream, in composeLowering) (av.Stream, error) {
	p := node.Join
	rt := service.runtime
	switch node.Kind {
	case compile.ComposeArmBind:
		// The selected stream republishes under the arm id: two files carry
		// the same track ids.
		arm := p.Arms[node.Arm]
		stream := streams[node.Inputs[0]]
		if opened := in.sourceStreams[arm.Input]; len(opened) != 0 {
			selected, err := refuse(compile.SelectStream(opened, arm.Bind.Selector))
			if err != nil {
				return av.Stream{}, err
			}
			stream = qualifyStream(selected, in.qualifiers[arm.Input])
		}
		stage := newArmRestampStage(node.Name, node.Detail, stream.ID, p.Arms[node.Arm].Stream.ID)
		stream.ID = p.Arms[node.Arm].Stream.ID
		return stream, addComposeStage(graph, stage, rt.buffer, node.Inputs)
	case compile.ComposeArmDecode:
		stream := streams[node.Inputs[0]]
		// Arm events must flow through decode (dropInputEvents=false): the
		// join's per-arm EOS bookkeeping and its PTS re-sync on discontinuity
		// read them, and the join consumes them.
		stage, err := service.newDecodeStageNamed(ctx, node.Name, compile.DecodeRequest{Selector: av.StreamSelector{Type: stream.Type}}, stream, rt.realtime, false, codec.DecodeBounds{})
		if err != nil {
			return av.Stream{}, err
		}
		return stream, addComposeStage(graph, stage, rt.buffer, node.Inputs)
	case compile.ComposeArmConform:
		stream := streams[node.Inputs[0]]
		first := stream
		if node.Target != "" {
			first = streams[node.Target]
		}
		target := p.Profile.ConformTarget(p, first)
		stage := newJoinConformStage(service, node.Name, node.Detail, target, rt.realtime)
		return compile.ConformedStream(stream, target), addComposeStage(graph, stage, rt.buffer, node.Inputs)
	case compile.ComposeTransform:
		stream := node.Stream
		if node.Resolved {
			stream = streams[node.Inputs[0]]
		}
		stage, out, err := service.newMediaTransformStageNamed(ctx, node.Name, node.Transform, stream, rt.realtime)
		if err != nil {
			return av.Stream{}, err
		}
		out.ID = streams[node.Inputs[0]].ID
		return out, addComposeStage(graph, stage, rt.buffer, node.Inputs)
	case compile.ComposeJoin:
		armIDs := make([]av.StreamID, 0, len(node.Inputs))
		seen := make(map[av.StreamID]struct{}, len(node.Inputs))
		for _, input := range node.Inputs {
			id := streams[input].ID
			if _, dup := seen[id]; dup {
				return av.Stream{}, publicError(compile.JoinArmError(p.Name, string(id), p.Name+" arms must have distinct stream ids",
					"give each arm a distinct stream id: name inputs/taps differently so no two arms publish the same id"))
			}
			seen[id] = struct{}{}
			armIDs = append(armIDs, id)
		}
		stage, pinned := newJoinStage(p, armIDs, rt)
		if admitting, ok := stage.(interface{ enableAdmission() }); ok && in.admission {
			admitting.enableAdmission()
		}
		buffer := rt.buffer
		if pinned != nil {
			buffer = *pinned
		}
		joined := p.Joined
		if p.Late {
			joined = p.ResolvedJoinedStream(streams, node.Inputs)
		}
		return joined, addComposeStage(graph, stage, buffer, node.Inputs)
	}
	return av.Stream{}, nil
}

// newJoinStage builds a join's convergence stage. A non-nil buffer pins the
// stage's input queue.
func newJoinStage(p *compile.JoinPlan, armIDs []av.StreamID, rt *Runtime) (pipeline.Stage, *pipeline.BufferPolicy) {
	if p.Tree.Custom != nil {
		return p.Tree.Custom.Stage, nil
	}
	switch p.Tree.Kind {
	case compile.JoinMix:
		return newAudioMixStageWithPreallocAndPools(p.Name, armIDs, av.StreamID(p.Name), p.Tree.Sync, joinStagePreallocDepth(rt), runtimeMediaPools(rt)), nil
	case compile.JoinComposite:
		return newVideoCompositeStageWithPreallocAndPools(p.Name, armIDs, av.StreamID(p.Name), p.Layouts, p.Tree.Sync, joinStagePreallocDepth(rt), runtimeMediaPools(rt)), nil
	default:
		// The selector is the control target: an injected SelectActive shares
		// its input queue with the flooding arms, so a lossy queue could evict
		// the control before the worker applies it. The pinned non-lossy
		// DropBlock queue backpressures instead, so the switch is always
		// delivered (the DropBlock control-plane lesson).
		return newSelectorStage(p.Name, armIDs, av.StreamID(p.Name)),
			&pipeline.BufferPolicy{Capacity: compile.SelectBufferCapacity, Drop: pipeline.DropBlock}
	}
}
