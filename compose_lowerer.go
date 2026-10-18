package goav

import (
	"context"

	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// composeLowerer lowers every recipe that is not a join: its sources feed one
// composeGraph, whether the recipe is a single stream, a selected or whole-job
// packet copy, several stream chains, or branches. Describe and the build plan
// the same graph from the same routes, so the built graph is the described one.
type composeLowerer struct {
	runtime *runtime
	inputs  []InputSpec
	// describe and build name the operation a source failure reports, and
	// context the recipe it describes.
	describe string
	build    string
	context  intent
	branches []branchComposeRoute
	outputs  []branchComposeTargetRoute
	// remux holds the outputs of a whole-job packet copy; it has no branches.
	remux []destinationAttachment
	// unsupported marks a shape no graph lowers: decoded frames routed to an
	// output that is not a sink, with no encoder.
	unsupported bool
}

func (l composeLowerer) runtimeRef() *runtime {
	return l.runtime
}

func (l composeLowerer) spec() (pipeline.Spec, error) {
	spec := pipeline.Spec{Name: "goav", Realtime: runtimeRealtime(l.runtime)}
	nodes := make(map[string]plannedNode, len(l.inputs)+4*len(l.branches)+len(l.outputs)+len(l.remux))
	sourceRefs, ok, err := mediaPlanSourceSpecs(&spec, nodes, l.inputs)
	if err != nil {
		return pipeline.Spec{}, err
	}
	if !ok || l.unsupported {
		return pipeline.Spec{}, recipeGraphUnsupportedError(l.describe, l.context)
	}
	if l.remux != nil {
		if err := l.validateRemux(); err != nil {
			return pipeline.Spec{}, err
		}
		return planRemuxGraph(sourceRefs, l.remux).emit(spec, nodes)
	}
	return planBranchComposeRoutes(spec, nodes, sourceRefs, l.branches, l.outputs)
}

func (l composeLowerer) lower(ctx context.Context, gp graphPlan, graph pipeline.Graph, service *builder) error {
	if l.unsupported {
		return recipeGraphUnsupportedError(l.build, l.context)
	}
	sources, err := compileMediaPlanSources(ctx, l.runtime, graph, l.inputs, l.build, l.context)
	if err != nil {
		return err
	}
	if l.remux != nil {
		if err := l.validateRemux(); err != nil {
			return err
		}
		return planRemuxGraph(sources.refs, l.remux).lower(ctx, service, graph, composeLowering{sourceStreams: sources.streamGroups})
	}
	groups, err := resolveBranchComposeStreamGroupsForInputs(sources, l.inputs, l.branches)
	if err != nil {
		return err
	}
	buffers, err := composeBranchBuffers(l.branches, gp.work)
	if err != nil {
		return err
	}
	return lowerBranchCompose(ctx, service, graph, sources.refs, groups, sources.bounds, l.branches, l.outputs, buffers, sources.realtime)
}

// validateRemux refuses a whole-job copy of event sources into a container:
// events have no packets to mux.
func (l composeLowerer) validateRemux() error {
	if mediaPlanInputsContainDomain(l.inputs, shape.DomainEvent) && destinationAttachmentsContainMuxDestination(l.remux) {
		return graphPlanInvalidError("event source destination must be a sink", nil)
	}
	return nil
}

// graphConfig runs the graph buffered when the runtime asks for it, when a
// realtime graph decodes or encodes, when a branch declared a queue, or when a
// fanout of goav-owned encode work can run its branches in parallel.
func (l composeLowerer) graphConfig(gp graphPlan) (pipeline.GraphConfig, error) {
	config, err := recipeGraphConfig(l.runtime, gp.name, gp.work, l.needsRealtimeBufferedGraph())
	if err != nil || !config.Buffer.IsDirect() {
		return config, err
	}
	declared := branchComposeDeclaresBuffer(l.branches)
	if !declared && !branchComposeRunsParallel(l.branches, l.outputs, len(l.inputs)) {
		return config, nil
	}
	// A declared branch queue needs a buffered graph, and a fanout of
	// goav-owned encode work earns one: each branch then runs on its own
	// workers instead of in its upstream's call, and the trunk runs behind
	// modest blocking queues.
	buffer, err := bufferPolicyWithShapeBudgets(pipeline.BufferPolicy{Capacity: promotedGraphBufferCapacity, Drop: pipeline.DropBlock}, gp.work)
	if err != nil {
		if declared {
			return config, err
		}
		return config, nil // parallelism is an optimization: stay direct
	}
	config.Buffer = buffer
	return config, nil
}

// needsRealtimeBufferedGraph reports whether a realtime graph decodes or
// encodes, so codec work runs on node workers instead of the source's call.
func (l composeLowerer) needsRealtimeBufferedGraph() bool {
	if l.runtime == nil || !l.runtime.realtime {
		return false
	}
	for i := range l.branches {
		if branchComposeRouteNeedsDecode(l.branches[i]) || branchComposeRouteNeedsEncode(l.branches[i]) {
			return true
		}
	}
	return false
}

// composeBranchBuffers sizes each branch's declared queue from the shapes the
// work plan records for it.
func composeBranchBuffers(branches []branchComposeRoute, work workPlan) (map[string]graphPlanBranchComposeBranchOperation, error) {
	operations := graphPlanOperationsByBranch(work.Operations)
	out := make(map[string]graphPlanBranchComposeBranchOperation, len(branches))
	for i := range branches {
		buffer, err := branchComposeBufferPolicy(branches[i].branch.Buffer, operations[branches[i].name])
		if err != nil {
			return nil, err
		}
		out[branches[i].name] = graphPlanBranchComposeBranchOperation{buffer: buffer}
	}
	return out, nil
}
