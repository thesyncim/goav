package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
)

// graphPlan binds the compiled work plan — the single executable truth built
// once by the compile — to the pipeline spec and the lowerer that executes it.
type graphPlan struct {
	runtime  *Runtime
	name     string
	realtime bool
	nodes    []pipeline.NodeSpec
	edges    []pipeline.EdgeSpec
	work     compile.WorkPlan
	lowerer  *recipeLowerer
}

func (p graphPlan) ready() bool {
	return p.lowerer != nil
}

func (p graphPlan) Describe() (pipeline.Spec, error) {
	if !p.ready() {
		return pipeline.Spec{}, publicError(compile.GraphPlanInvalidError("graph plan is not ready", nil))
	}
	return p.spec(), nil
}

func (p graphPlan) Build(ctx context.Context) (LiveTask, error) {
	if !p.ready() {
		return nil, publicError(compile.GraphPlanInvalidError("graph plan is not ready", nil))
	}
	return buildGraphPlanTask(ctx, p)
}

func (p graphPlan) lower(ctx context.Context, graph pipeline.Graph, service *builder) error {
	if err := validateGraphPlanLowering(p); err != nil {
		return err
	}
	return p.lowerer.lower(ctx, p, graph, service)
}

func (p graphPlan) spec() pipeline.Spec {
	return pipeline.Spec{
		Name:     p.name,
		Realtime: p.realtime,
		Nodes:    append([]pipeline.NodeSpec(nil), p.nodes...),
		Edges:    append([]pipeline.EdgeSpec(nil), p.edges...),
	}
}

func (p graphPlan) workPlan() compile.WorkPlan {
	return compile.CloneWorkPlan(p.work)
}

func newGraphPlan(runtime *Runtime, spec pipeline.Spec, work compile.WorkPlan, lowerer *recipeLowerer) graphPlan {
	return graphPlan{
		runtime:  runtime,
		name:     work.Name,
		realtime: spec.Realtime,
		nodes:    append([]pipeline.NodeSpec(nil), spec.Nodes...),
		edges:    append([]pipeline.EdgeSpec(nil), spec.Edges...),
		work:     work,
		lowerer:  lowerer,
	}
}

func graphPlanWithRuntime(gp graphPlan, rt *Runtime) graphPlan {
	gp.runtime = rt
	if gp.lowerer != nil {
		gp.lowerer = gp.lowerer.withRuntime(rt)
	}
	return gp
}

func validateGraphPlanLowering(gp graphPlan) error {
	if !gp.ready() {
		return publicError(compile.GraphPlanInvalidError("graph plan is not ready", nil))
	}
	if len(gp.nodes) != 0 && len(gp.work.Operations) == 0 {
		return publicError(compile.GraphPlanInvalidError("graph plan has nodes but no ordered operations", []string{
			"nodes=" + strconv.Itoa(len(gp.nodes)),
		}))
	}
	if err := validateGraphPlanEdges(gp); err != nil {
		return err
	}
	for i := range gp.work.Operations {
		operation := gp.work.Operations[i]
		details := []string{
			"operation=" + strconv.Itoa(i),
			"branch=" + operation.Branch,
			"node=" + operation.Node.String(),
			"kind=" + string(operation.Kind),
		}
		if operation.Kind == "" {
			return publicError(compile.GraphPlanInvalidError("graph-plan operation has no kind", details))
		}
		if operation.Branch == "" {
			return publicError(compile.GraphPlanInvalidError("graph-plan operation has no branch", details))
		}
		if graphPlanOperationDestinationsRequired(operation.Kind) && len(operation.Destinations) == 0 {
			return publicError(compile.GraphPlanInvalidError("graph-plan destination operation has no destination refs", details))
		}
	}
	return nil
}

func validateGraphPlanEdges(gp graphPlan) error {
	if len(gp.edges) == 0 {
		return nil
	}
	nodes := make(map[pipeline.NodeRef]struct{}, len(gp.nodes))
	for i := range gp.nodes {
		nodes[pipeline.NodeRef(gp.nodes[i].Name)] = struct{}{}
	}
	for i := range gp.edges {
		edge := gp.edges[i]
		details := []string{
			"edge=" + strconv.Itoa(i),
			"from=" + edge.From.String(),
			"to=" + edge.To.String(),
		}
		if edge.From == "" || edge.To == "" {
			return publicError(compile.GraphPlanInvalidError("graph-plan edge is missing a source or target", details))
		}
		if _, ok := nodes[edge.From]; !ok {
			return publicError(compile.GraphPlanInvalidError("graph-plan edge source is not a planned node", details))
		}
		if _, ok := nodes[edge.To]; !ok {
			return publicError(compile.GraphPlanInvalidError("graph-plan edge target is not a planned node", details))
		}
	}
	return nil
}

func graphPlanOperationDestinationsRequired(kind plan.OperationKind) bool {
	switch kind {
	case plan.OpMux, plan.OpSink, plan.OpWrite:
		return true
	default:
		return false
	}
}
