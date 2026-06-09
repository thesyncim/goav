package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// Select switches one of N live source-chains through to a single destination —
// the runtime is a passthrough one-of-N switch (active-speaker camera, simulcast
// layer, failover). Unlike Mix/Composite it does NOT decode/encode/resample: the
// active arm's frames or packets are forwarded as-is, only the StreamID rewritten
// to the output. Arms must have distinct stream ids; the first arm is active by
// default and SelectActive switches live through the control plane. This reuses
// the existing Job, so .To/Build/Run are unchanged. (First slice: arms to a Sink.)
func Select(arms ...*jobStreamBuilder) *selectorStream {
	return &selectorStream{arms: arms}
}

// selectBufferCapacity sizes the non-lossy queues on the Select graph. It is small
// — a passthrough switch holds no real backlog — but large enough that an injected
// SelectActive control rides alongside live arm traffic without forcing a stall.
const selectBufferCapacity = 32

type selectorStream struct {
	arms []*jobStreamBuilder
}

type selectSpec struct {
	arms []*jobStreamBuilder
	dest Destination
}

// To delivers the switched stream to a destination and returns a Job, so the
// select runs through the same Build/Run as every other recipe.
func (s *selectorStream) To(dest Destination) *Job {
	job := &Job{name: "select", runtime: Default()}
	if len(s.arms) < 2 {
		job.setErr(&BuildError{Code: "select_inputs", Operation: "build select", Node: "select", Reason: "select requires at least two source arms", Cause: ErrUnsupportedBuild})
		return job
	}
	job.selectJob = &selectSpec{arms: s.arms, dest: dest}
	return job
}

// SelectActive switches a running Select to forward the arm identified by id. It
// is the public control-plane switch: task.Control(ctx, goav.SelectActive("select",
// "cam2")) flips the live output without restarting the task. The event rides the
// selector node's serial worker and is interpreted by selectorStage.Handle.
func SelectActive(node pipeline.NodeRef, id av.StreamID) Control {
	return Deliver(av.Event{Type: av.EventStats, StreamID: id, Reason: selectorActiveReason}).At(node)
}

func (j *Job) buildSelect(ctx context.Context) (Task, error) {
	if j.err != nil {
		return nil, j.err
	}
	rt, ok := j.runtime.(*runtime)
	if !ok {
		return nil, ErrExpertRuntimeRequired
	}
	// Select is driven live by SelectActive through the control plane, which needs a
	// per-node serial worker — i.e. a buffered graph. A direct (zero) runtime buffer
	// would build a direct graph with no injector, so default it to a non-lossy
	// buffered policy. An explicitly configured buffer is honoured as-is.
	graphBuffer := rt.buffer
	if graphBuffer.IsDirect() {
		graphBuffer = pipeline.BufferPolicy{Capacity: selectBufferCapacity, Drop: pipeline.DropBlock}
	}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "goav-select", Buffer: graphBuffer, Realtime: rt.realtime})
	if err != nil {
		return nil, err
	}
	armRefs := make([]string, 0, len(j.selectJob.arms))
	armIDs := make([]av.StreamID, 0, len(j.selectJob.arms))
	seen := make(map[av.StreamID]struct{}, len(j.selectJob.arms))
	service := &builder{runtime: rt}
	for i := range j.selectJob.arms {
		arm := j.selectJob.arms[i]
		if arm == nil || arm.job == nil || len(arm.job.inputs) != 1 {
			graph.Close()
			return nil, &BuildError{Code: "select_arm", Operation: "build select", Node: "select", Reason: "each select arm must be a single-input source chain", Cause: ErrUnsupportedBuild}
		}
		source, streams, _, err := arm.job.inputs[0].openGraphSource(ctx, service, i)
		if err != nil {
			graph.Close()
			return nil, err
		}
		// Select is media-agnostic passthrough: pick the arm's single stream rather
		// than forcing an audio/video type, so a frame or packet arm switches as-is.
		stream, err := selectStream(streams, av.StreamSelector{})
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		id := stream.ID
		if _, dup := seen[id]; dup {
			source.Close()
			graph.Close()
			return nil, &BuildError{Code: "select_arm", Operation: "build select", Node: string(id), Reason: "select arms must have distinct stream ids", Cause: ErrUnsupportedBuild}
		}
		seen[id] = struct{}{}
		srcRef, err := graph.AddSource(source, rt.buffer)
		if err != nil {
			source.Close()
			graph.Close()
			return nil, err
		}
		armRefs = append(armRefs, string(srcRef))
		armIDs = append(armIDs, id)
	}
	// The selector is the control target: an injected SelectActive shares its input
	// queue with the flooding arms, so a lossy queue could evict the control before
	// the worker applies it. A non-lossy DropBlock queue backpressures instead, so
	// the switch is always delivered (the DropBlock control-plane lesson).
	selectRef, err := graph.AddStage(newSelectorStage("select", armIDs, av.StreamID("select")), pipeline.BufferPolicy{Capacity: selectBufferCapacity, Drop: pipeline.DropBlock})
	if err != nil {
		graph.Close()
		return nil, err
	}
	for i := range armRefs {
		if err := graph.Connect(pipeline.Route{From: armRefs[i], To: []string{string(selectRef)}, Policy: pipeline.RouteAll}); err != nil {
			graph.Close()
			return nil, err
		}
	}
	sink := j.selectJob.dest.spec.sink
	if sink == nil {
		graph.Close()
		return nil, &BuildError{Code: "select_destination", Operation: "build select", Node: "select", Reason: "select to a non-Sink destination is not supported yet", Cause: ErrUnsupportedBuild}
	}
	sinkRef, err := graph.AddSink(sink, rt.buffer)
	if err != nil {
		graph.Close()
		return nil, err
	}
	if err := graph.Connect(pipeline.Route{From: string(selectRef), To: []string{string(sinkRef)}, Policy: pipeline.RouteAll}); err != nil {
		graph.Close()
		return nil, err
	}
	return newTask(graph, rt), nil
}
