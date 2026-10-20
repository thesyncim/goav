package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/interleave"
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
	inputs    []InputSpec
	outputs   []destinationSpec
	isolation []isolationPlan
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
	execution, err := r.planExecution(gp)
	if err != nil {
		return err
	}
	buffers := execution.buffers
	in := composeLowering{realtime: realtime, admission: !realtime, buffers: buffers}
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
			if _, err := graph.AddSource(build.source, buffers[node.Name]); err != nil {
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
			if err := addComposeStage(graph, stage, buffers[node.Name], node.Inputs); err != nil {
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
			switch node.Kind {
			case compile.ComposeSink, compile.ComposeMux, compile.ComposeRemuxSink, compile.ComposeRemuxMux:
				tx := service.destinationTxs[len(service.destinationTxs)-1]
				tx.node = ref
				for _, op := range gp.work.Operations {
					if op.Node == ref && len(op.Destinations) == 1 {
						tx.id = op.Destinations[0]
						for _, destination := range gp.work.Destinations {
							if destination.ID == tx.id {
								tx.name = destination.Name
							}
						}
					}
				}
			}
			if err := opened.install(graph, buffers[node.Name], node.Inputs); err != nil {
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
	// transferred is set after a successful graph Add during attachment.
	transferred bool
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
		if err := validateMediaSinkStreams(node.Name, dests[node.Output], in, node.Kind == compile.ComposeRemuxSink); err != nil {
			return openedNode{}, err
		}
		// A media destination learns its streams and binds the task's
		// control path; once it opened them, goav closes it either way.
		opened, err := openMediaSink(ctx, sink, in, service.runtime.controlFunc())
		if err != nil {
			return openedNode{}, err
		}
		tx := &destinationTransaction{name: node.Name, node: pipeline.NodeRef(node.Name), requireSuccess: service.requireRunOK}
		service.destinationTxs = append(service.destinationTxs, tx)
		sink = trackedDestinationSink{namedSink: namedSink{name: node.Name, sink: sink}, transaction: tx}
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

// executionBuffers is shared by lowering and inspection. Selects and join
// machinery use the graph queue; branch operations and destinations may
// override it with their declared buffer.
func (r *recipeLowerer) executionBuffers(fallback pipeline.BufferPolicy) map[string]pipeline.BufferPolicy {
	declared := r.branchBuffers()
	out := make(map[string]pipeline.BufferPolicy, len(r.graph.Nodes))
	for _, node := range r.graph.Nodes {
		policy := fallback
		switch node.Kind {
		case compile.ComposeDecode, compile.ComposeStep, compile.ComposeEncode, compile.ComposeSink, compile.ComposeRemuxSink, compile.ComposeMux, compile.ComposeRemuxMux:
			if buffer, ok := declared[node.Owner]; node.Owner >= 0 && ok && !buffer.IsDirect() {
				policy = buffer
			}
		}
		if node.Kind == compile.ComposeJoin {
			policy = joinNodeBuffer(node.Join, policy)
		}
		out[node.Name] = policy
	}
	return out
}

// composeLowering is what building join nodes needs beyond the plan.
type composeLowering struct {
	buffers  map[string]pipeline.BufferPolicy
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
type graphExecutionPlan struct {
	config    pipeline.GraphConfig
	buffers   map[string]pipeline.BufferPolicy
	reason    string
	admission string
}

func (r *recipeLowerer) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	plan, err := r.planExecution(gp)
	return plan.config, err
}

// planExecution is the single selection of runner, queues, and rationale used
// by Describe, Explain and lowering. It does not change execution defaults.
func (r *recipeLowerer) planExecution(gp graphPlan) (graphExecutionPlan, error) {
	rt := r.runtime
	if rt == nil {
		return graphExecutionPlan{}, publicError(compile.RecipeGraphUnsupportedRecipeError("configure graph", r.recipe))
	}
	buffer := rt.buffer
	reason := "runtime_buffer"
	if buffer.IsDirect() {
		reason = r.serialExecutionReason()
		for _, id := range r.joinOrder {
			if pinned := r.joins[id].TreeGraphBuffer(rt.realtime); pinned != nil {
				buffer = *pinned
				reason = "join_requires_workers"
				break
			}
		}
	}
	if buffer.IsDirect() && rt.realtime && r.realtimeCodecWork() {
		buffer = realtimeRecipeBufferPolicy(buffer)
		reason = "realtime_codec_workers"
	}
	declared := false
	for i := range r.recipe.Streams {
		declared = declared || !r.recipe.Streams[i].Buffer.PipelinePolicy().IsDirect()
	}
	if buffer.IsDirect() && (declared || r.runsParallel()) {
		buffer = pipeline.BufferPolicy{Capacity: promotedGraphBufferCapacity, Drop: pipeline.DropBlock}
		reason = "automatic_encode_ladder"
		if declared {
			reason = "declared_branch_buffer"
		}
	}
	config := pipeline.GraphConfig{
		Name:             firstNonEmpty(gp.name, "goav"),
		Realtime:         rt.realtime,
		Buffer:           buffer,
		EventCapacity:    rt.eventCapacity,
		CloseWaitTimeout: rt.closeWaitTimeout,
	}
	return graphExecutionPlan{config: config, buffers: r.executionBuffers(buffer), reason: reason, admission: r.executionAdmission(config)}, nil
}

func (r *recipeLowerer) serialExecutionReason() string {
	for _, node := range r.graph.Nodes {
		switch node.Kind {
		case compile.ComposeStep:
			if node.Transform.Stage != nil {
				return "custom_stage_requires_explicit_buffer"
			}
		case compile.ComposeSink:
			return "custom_sink_requires_explicit_buffer"
		case compile.ComposeMux:
			if r.outputs[node.Output].custom != nil {
				return "custom_destination_requires_explicit_buffer"
			}
			if len(node.Inputs) != 1 {
				return "shared_destination_preserves_order"
			}
		}
	}
	return "serial_no_parallel_ladder"
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

func (r *recipeLowerer) executionAdmission(config pipeline.GraphConfig) string {
	p := interleave.Ordering{Realtime: config.Realtime, Buffered: !config.Buffer.IsDirect()}
	unknown := 0
	for _, n := range r.graph.Nodes {
		if n.Kind == compile.ComposeSource {
			p.Sources++
			input := r.inputs[n.Input]
			switch {
			case len(input.wraps) > 0 || input.provider != nil || input.bytes != nil:
				unknown++
			case input.source == nil && !input.realtime && !input.input.Realtime:
				p.TimedSources++
			}
		}
		if len(n.Inputs) > 1 {
			p.Merges = true
		}
		var stage pipeline.Stage
		if n.Kind == compile.ComposeJoin {
			if n.Join.Tree.Custom != nil {
				stage = n.Join.Tree.Custom.Stage
			} else if n.Join.Tree.Kind == compile.JoinMix || n.Join.Tree.Kind == compile.JoinComposite {
				p.Converging = true
			}
		}
		if n.Kind == compile.ComposeStep && n.Transform.Stage != nil && n.Transform.Stage.Name() == n.Name {
			stage = n.Transform.Stage
		}
		if _, ok := stage.(pipeline.Admitter); ok {
			p.Converging = true
			if _, gate := stage.(interleave.Gate); !gate {
				p.BlockingAdmitter = true
			}
		}
	}
	enabled := p.Enabled()
	p.TimedSources += unknown
	if p.Enabled() != enabled {
		return "runtime"
	}
	if enabled {
		return "timestamp"
	}
	return "arrival"
}
