package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
)

// The lowering turns a recipe's DAG into one composeGraph: the schedule
// Describe emits and Build builds, node for node. Every DAG node lowers once,
// however many branches read it: a node one branch reads takes that branch's
// name, a node several read takes its stream's. The schedule is
//
//   - the sources of the inputs the recipe's streams read, in input order;
//   - per stream root, in branch order: an input's select (shared by every
//     root of one selector), its decode, and the work all its branches
//     share; or a join with its arms, depth first;
//   - each branch's own work, in branch order (work only some branches
//     share runs where the first of them reaches it);
//   - the destinations, in order.
//
// A node's inputs that are not scheduled yet (a join arm fed by a stream's
// node) are scheduled first.

// --- the build ---

// recipeLowerer builds a recipe's planned graph.
type recipeLowerer struct {
	runtime   *Runtime
	recipe    recipeir.Recipe
	joins     map[recipeir.NodeID]*compile.JoinPlan
	joinOrder []recipeir.NodeID
	graph     compile.ComposeGraph
	// inputs and outputs are the handles the plan's nodes index.
	inputs  []InputSpec
	outputs []destinationSpec
}

func (r *recipeLowerer) withRuntime(rt *Runtime) *recipeLowerer {
	out := *r
	out.runtime = rt
	return &out
}

// lower builds the plan into graph: each node in schedule order, connected
// to its inputs as it is added, so the built graph is the described one.
func (r *recipeLowerer) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	rt := service.runtime
	realtime := rt.realtime
	streams := make(map[pipeline.NodeRef]av.Stream, len(r.graph.Nodes))
	sourceStreams := map[int][]av.Stream{}
	qualifiers := map[int]string{}
	sourceInputs := map[pipeline.NodeRef]int{}
	var bounds []decodeBoundsProvider
	buffers := r.branchBuffers()
	bufferOf := func(node compile.ComposeNode) pipeline.BufferPolicy {
		if node.Owner >= 0 {
			if buffer, ok := buffers[node.Owner]; ok && !buffer.IsDirect() {
				return buffer
			}
		}
		return rt.buffer
	}
	in := composeLowering{realtime: realtime, admission: !realtime}
	for _, node := range r.graph.Nodes {
		ref := pipeline.NodeRef(node.Name)
		switch node.Kind {
		case compile.ComposeSource:
			sourceInputs[ref] = node.Input
			input := r.inputs[node.Input]
			build, err := input.openGraphSourceBuild(ctx, service, node.Name, len(r.inputs) > 1)
			if err != nil {
				return err
			}
			if _, err := graph.AddSource(build.source, rt.buffer); err != nil {
				build.source.Close()
				return err
			}
			sourceStreams[node.Input] = append([]av.Stream(nil), build.streams...)
			qualifiers[node.Input] = build.qualifier
			realtime = realtime || build.realtime
			in.realtime = realtime
			if build.bounds != nil {
				bounds = append(bounds, build.bounds)
			}
			if len(build.streams) == 1 {
				streams[ref] = qualifyStream(build.streams[0], build.qualifier)
			}
		case compile.ComposeSelect:
			selected, err := r.selectStream(node, sourceStreams[node.Input], qualifiers[node.Input])
			if err != nil {
				return err
			}
			stage := newStreamSelectStage(node.Name, selected, node.Selector, node.Detail)
			if err := addComposeStage(graph, stage, rt.buffer, node.Inputs); err != nil {
				return err
			}
			streams[ref] = selected
		case compile.ComposeDecode, compile.ComposeStep, compile.ComposeEncode, compile.ComposeSink, compile.ComposeRemuxSink, compile.ComposeMux, compile.ComposeRemuxMux:
			var in []av.Stream
			if node.Kind == compile.ComposeRemuxMux || node.Kind == compile.ComposeRemuxSink {
				for _, from := range node.Inputs {
					input := sourceInputs[from]
					in = append(in, qualifyStreams(sourceStreams[input], qualifiers[input])...)
				}
			} else {
				for _, input := range node.Inputs {
					in = append(in, streams[input])
				}
			}
			decodeBounds := codec.DecodeBounds{}
			if node.Kind == compile.ComposeDecode && len(bounds) != 0 {
				decodeBounds = providerDecodeBoundsForStream(in[0], bounds)
			}
			opened, err := openComposeNode(ctx, service, node, in, r.outputs, realtime, decodeBounds)
			if err != nil {
				return err
			}
			if err := opened.install(graph, bufferOf(node), node.Inputs); err != nil {
				return err
			}
			streams[ref] = opened.stream
		default:
			in.sourceStreams = sourceStreams
			in.qualifiers = qualifiers
			stream, err := lowerJoinNode(ctx, service, graph, node, streams, in)
			if err != nil {
				return err
			}
			streams[ref] = stream
		}
	}
	return nil
}

// openedNode is the component one planned node opened as, and the stream it
// emits.
type openedNode struct {
	stage  pipeline.Stage
	sink   pipeline.Sink
	stream av.Stream
	// owned marks a stage the lowering opened (a decoder, filter, encoder,
	// or muxer), or a media sink whose streams it opened, which a failed
	// build or attach closes; a caller's own stages and sinks stay the
	// caller's.
	owned bool
}

// openComposeNode opens the component a decode, step, encode, sink, or mux
// node lowers to against the streams its inputs carry. The build and an
// attach open every such node through it.
func openComposeNode(ctx context.Context, service *builder, node compile.ComposeNode, in []av.Stream, dests []destinationSpec, realtime bool, bounds codec.DecodeBounds) (openedNode, error) {
	switch node.Kind {
	case compile.ComposeDecode:
		stage, err := service.newDecodeStageNamed(ctx, node.Name, compile.DecodeRequest{Selector: node.Selector, Config: node.Decode}, in[0], realtime, node.DropEvents, bounds)
		if err != nil {
			return openedNode{}, err
		}
		return openedNode{stage: stage, stream: in[0], owned: true}, nil
	case compile.ComposeStep:
		stage, out, err := service.newBranchComposeStepStageNamed(ctx, node.Name, node.Transform, in[0], realtime)
		if err != nil {
			return openedNode{}, err
		}
		return openedNode{stage: stage, stream: out, owned: node.Transform.Stage == nil}, nil
	case compile.ComposeEncode:
		config, _, err := prepareEncodeConfig(in[0], node.Request, realtime)
		if err != nil {
			return openedNode{}, err
		}
		stage, encoded, err := service.newEncodeStageNamed(ctx, node.Name, node.Request, config)
		if err != nil {
			return openedNode{}, err
		}
		return openedNode{stage: stage, stream: encoded, owned: true}, nil
	case compile.ComposeSink, compile.ComposeRemuxSink:
		sink := dests[node.Output].sink
		// A media destination learns its streams and binds the task's
		// control path; once it opened them, goav closes it either way.
		opened, err := openMediaSink(ctx, sink, in, service.runtime.controlFunc())
		if err != nil {
			return openedNode{}, err
		}
		if node.Name != sink.Name() {
			sink = namedSink{name: node.Name, sink: sink}
		}
		return openedNode{sink: sink, owned: opened}, nil
	default:
		destination := dests[node.Output]
		muxStage, err := service.openMuxDestinationStage(ctx, destination, node.Output, in, destinationOpenFormat(destination), destinationGraphFormat(destination))
		if err != nil {
			return openedNode{}, err
		}
		stage := pipeline.Stage(muxStage)
		if node.Name != muxStage.Name() {
			stage = namedStage{name: node.Name, stage: muxStage}
		}
		return openedNode{stage: stage, owned: true}, nil
	}
}

// install adds the opened node to the graph and connects its inputs,
// closing the stage if the graph refuses it.
func (o openedNode) install(graph pipeline.Graph, buffer pipeline.BufferPolicy, inputs []pipeline.NodeRef) error {
	if o.sink == nil {
		return addComposeStage(graph, o.stage, buffer, inputs)
	}
	ref, err := graph.AddSink(o.sink, buffer)
	if err != nil {
		if o.owned {
			o.sink.Close()
		}
		return err
	}
	return connectComposeInputs(graph, inputs, ref)
}

// selectStream is the stream a select picks from its input's streams.
func (r *recipeLowerer) selectStream(node compile.ComposeNode, streams []av.Stream, qualifier string) (av.Stream, error) {
	input := r.inputs[node.Input]
	selected, err := refuse(compile.SelectDecodeStream(streams, node.Selector))
	if inputStartsFrameDomain(input) {
		selected, err = refuse(compile.SelectStream(streams, node.Selector))
	}
	if err != nil {
		return av.Stream{}, err
	}
	return qualifyStream(selected, qualifier), nil
}

// branchBuffers lowers each branch's declared queue to the node policy its
// exclusively owned nodes run behind; an undeclared buffer stays direct and
// inherits the graph's policy.
func (r *recipeLowerer) branchBuffers() map[int]pipeline.BufferPolicy {
	out := map[int]pipeline.BufferPolicy{}
	for i, stream := range r.recipe.Streams {
		out[i] = stream.Buffer.PipelinePolicy()
	}
	return out
}

// composeLowering is what building join nodes needs beyond the plan.
type composeLowering struct {
	realtime bool
	// admission enables per-arm backpressure on join stages.
	admission     bool
	sourceStreams map[int][]av.Stream
	qualifiers    map[int]string
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

// graphConfig runs the graph buffered when the runtime asks for it, when a
// realtime graph decodes or encodes, when a join needs concurrent arms or a
// control-plane queue, when a branch declared a queue, or when a fanout of
// goav-owned encode work can run its branches in parallel.
func (r *recipeLowerer) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	rt := r.runtime
	if rt == nil {
		return pipeline.GraphConfig{}, publicError(compile.RecipeGraphUnsupportedRecipeError("configure graph", r.recipe))
	}
	buffer := rt.buffer
	if buffer.IsDirect() {
		for _, id := range r.joinOrder {
			if pinned := r.joins[id].TreeGraphBuffer(rt.realtime); pinned != nil {
				buffer = *pinned
				break
			}
		}
	}
	if buffer.IsDirect() && rt.realtime && r.realtimeCodecWork() {
		buffer = realtimeRecipeBufferPolicy(buffer)
	}
	declared := false
	for i := range r.recipe.Streams {
		declared = declared || !r.recipe.Streams[i].Buffer.PipelinePolicy().IsDirect()
	}
	if buffer.IsDirect() && (declared || r.runsParallel()) {
		buffer = pipeline.BufferPolicy{Capacity: promotedGraphBufferCapacity, Drop: pipeline.DropBlock}
	}
	return pipeline.GraphConfig{
		Name:             firstNonEmpty(gp.name, "goav"),
		Realtime:         rt.realtime,
		Buffer:           buffer,
		EventCapacity:    rt.eventCapacity,
		CloseWaitTimeout: rt.closeWaitTimeout,
	}, nil
}

// realtimeCodecWork reports a graph that decodes or encodes, so a realtime
// run puts codec work on node workers instead of the source's call.
func (r *recipeLowerer) realtimeCodecWork() bool {
	for _, node := range r.graph.Nodes {
		switch node.Kind {
		case compile.ComposeDecode, compile.ComposeEncode, compile.ComposeArmDecode:
			return true
		}
	}
	return false
}

// runsParallel reports whether a fanout runs its branches on their own
// workers by default: one source fans out to at least two encoders, every
// operation is built in (no custom .Do stage), and every output is a
// goav-owned muxer fed by exactly one branch. Such a graph runs no caller
// code inside a branch and each output still receives one branch's packets
// in order, so concurrency changes only the wall time.
func (r *recipeLowerer) runsParallel() bool {
	if len(r.joins) != 0 {
		return false
	}
	encoding := map[int]int{}
	shared := false
	for _, node := range r.graph.Nodes {
		switch node.Kind {
		case compile.ComposeStep:
			if node.Transform.Stage != nil {
				return false
			}
		case compile.ComposeEncode:
			input := r.streamOfNode(node)
			encoding[input]++
			shared = shared || encoding[input] >= 2
		case compile.ComposeSink:
			return false
		case compile.ComposeMux:
			destination := r.outputs[node.Output]
			if destination.custom != nil || len(node.Inputs) != 1 {
				return false
			}
		}
	}
	return shared
}

// streamOfNode is the recipe input a branch-owned node reads.
func (r *recipeLowerer) streamOfNode(node compile.ComposeNode) int {
	if node.Owner >= 0 {
		return r.recipe.Streams[node.Owner].Input
	}
	return -1
}
