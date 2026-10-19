package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// attachBranch is one branch an Output attaches to a running task: its name,
// media, anchor, operations, queue, and destinations. A stream rule's
// branches are attach branches too, anchored on each discovered stream.
type attachBranch struct {
	name       string
	media      av.MediaType
	source     branchSourceBinding
	operations []recipeir.Operation
	buffer     flow.BranchBuffer
	dests      []destinationRef
}

// rebranchArg lets an attach branch ride the RebranchArg list
// Attachment.Rebranch accepts, as a replacement.
func (attachBranch) rebranchArg() {}

// Attach adds Outputs fed from the task's taps, tracks, and graph routes to
// the running task.
func (t *task) Attach(ctx context.Context, outs ...Output) (Attachment, error) {
	branches, err := t.attachBranches(outs)
	if err != nil {
		return nil, err
	}
	return t.attach(ctx, branches...)
}

// attachBranches lowers Outputs rooted at the task's taps, tracks, and routes
// onto attach branches. A tap an earlier Output of the group declares is
// known by the domain at its position.
func (t *task) attachBranches(outs []Output) ([]attachBranch, error) {
	invalid := publicError(compile.StreamInvalidError("", "attach Outputs built from task.Tap(name) or task.Track(input, stream), each routed with .To(destination)"))
	if len(outs) == 0 {
		return nil, runtimeBranchInvalidError("no runtime branches to attach", "attach Outputs: task.Attach(ctx, task.Tap(name).To(destination))")
	}
	for i := range outs {
		if outs[i].remux != nil || len(outs[i].dests) == 0 {
			return nil, invalid
		}
	}
	g, err := compileGraph(outs)
	if err != nil {
		return nil, err
	}
	g.bindSchemes(t.runtime)
	taps := map[string]shape.MediaDomain{}
	for _, tap := range t.Taps() {
		taps[tap.Name] = tap.Domain
	}
	handles := g.handles(t.runtime)
	planned, err := refuse(compile.PlanAttach(g.compileRequest(handles), taps))
	if err != nil {
		return nil, err
	}
	var branches []attachBranch
	for i, out := range g.outputs {
		source := g.anchorSource(g.graph.Path(out.from)[0], taps)
		for _, plan := range planned[i] {
			branches = append(branches, handles.attachBranch(plan, source))
		}
	}
	return branches, nil
}

// anchorSource is where an attach branch rooted at anchor reads the task.
func (g *recipeGraph) anchorSource(anchor recipeir.NodeID, taps map[string]shape.MediaDomain) branchSourceBinding {
	root := g.rootOf(anchor)
	switch {
	case root.tap != "":
		return branchSourceBinding{tap: root.tap, tapDomain: taps[root.tap]}
	case root.track != nil:
		return root.input.track(*root.track).branchSource()
	default:
		route := *root.route
		policy := route.Policy
		if policy == "" {
			policy = pipeline.RouteAll
		}
		return branchSourceBinding{from: route.From, policy: policy, label: route.Label}
	}
}

// attachBranch binds a planned branch to the task: its anchor and its
// destination handles.
func (h *compileHandles) attachBranch(plan compile.AttachBranchPlan, source branchSourceBinding) attachBranch {
	branch := attachBranch{
		name:       plan.Name,
		media:      plan.Media,
		source:     source,
		operations: plan.Operations,
		buffer:     plan.Buffer,
	}
	for _, dest := range plan.Dests {
		branch.dests = append(branch.dests, newDirectDestinationRef(dest.Name, h.dests[dest.Slot]))
	}
	return branch
}

// rebranchArgs replaces each Output among args with the branches it attaches
// as, so Rebranch takes the Outputs Attach does.
func (t *task) rebranchArgs(args []RebranchArg) ([]RebranchArg, error) {
	var outs []Output
	mapped := make([]RebranchArg, 0, len(args))
	for _, arg := range args {
		if out, ok := arg.(Output); ok {
			outs = append(outs, out)
			continue
		}
		mapped = append(mapped, arg)
	}
	if len(outs) == 0 {
		return args, nil
	}
	branches, err := t.attachBranches(outs)
	if err != nil {
		return nil, err
	}
	for _, branch := range branches {
		mapped = append(mapped, branch)
	}
	return mapped, nil
}

// rebranchArg lets an Output ride the RebranchArg list
// Attachment.Rebranch accepts, as a replacement.
func (Output) rebranchArg() {}

// --- stream rules ---

// streamRule is one declared dynamic-stream rule: when a discovered stream
// matches, the templated branches attach to it at runtime.
type streamRule struct {
	match    sourcepkg.StreamMatch
	branches []attachBranch
}

// streamRules binds the planned rules to the recipe's destination handles.
func (h *compileHandles) streamRules(plans []compile.StreamRulePlan) []streamRule {
	var rules []streamRule
	for _, plan := range plans {
		rule := streamRule{match: plan.Match}
		for _, branch := range plan.Branches {
			rule.branches = append(rule.branches, h.attachBranch(branch, branchSourceBinding{}))
		}
		rules = append(rules, rule)
	}
	return rules
}

// withStreamAnnouncer decorates the single input of a recipe with OnStream
// rules with a fresh announcer, so rule reactions run on the source's emit
// path. Without rules it returns nil.
func (g *recipeGraph) withStreamAnnouncer() *streamAnnouncer {
	if len(g.matches) == 0 || len(g.inputs) != 1 {
		return nil
	}
	announcer := &streamAnnouncer{}
	input := g.inputs[0]
	input.wraps = append(append([]func(pipeline.Source) pipeline.Source(nil), input.wraps...), announcer.wrap)
	g.inputs[0] = input
	return announcer
}

// installStreamRules binds the recipe's OnStream rules to the built task: the
// rules anchor on the single source node and react on its emit path.
func (r recipeResolved) installStreamRules(built LiveTask, announcer *streamAnnouncer) {
	if announcer == nil || len(r.streamRules) == 0 {
		return
	}
	runtimeTask, ok := built.(*task)
	if !ok || runtimeTask == nil {
		return
	}
	runtimeTask.installStreamRules(announcer, r.rulesSource, r.rulesDomain, r.streamRules)
}

// branchForDiscoveredStream templates one rule branch for a matched stream:
// the branch name is suffixed with the stream id for uniqueness and the
// source binding pins the source+stream anchor (RouteByStream from the source
// node, anchor shape from the announced av.Stream).
func branchForDiscoveredStream(branch attachBranch, sourceNode string, domain shape.MediaDomain, stream av.Stream) attachBranch {
	out := branch
	out.name = branch.name + "-" + string(stream.ID)
	// Each discovered stream gets its own gates: a gate is one graph node.
	out.operations = cloneOperations(branch.operations)
	anchored := stream
	out.source = branchSourceBinding{
		from:         sourceNode,
		policy:       pipeline.RouteByStream,
		label:        string(stream.ID),
		stream:       &anchored,
		streamDomain: domain,
	}
	return out
}

// Tap is the stream at one of the task's taps.
func (t *task) Tap(name string) Stream {
	return Stream{node: &streamNode{root: &streamRoot{tap: name}}}
}

// Track is a stream one of the task's inputs produces.
func (t *task) Track(input InputSpec, stream av.Stream) Stream {
	return Stream{node: &streamNode{root: &streamRoot{input: input, track: &stream}}}
}

// Route is the stream a graph route carries.
func (t *task) Route(route pipeline.Route) Stream {
	return Stream{node: &streamNode{root: &streamRoot{route: &route}}}
}
