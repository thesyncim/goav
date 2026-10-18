package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
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

// RebranchArg lets an attach branch ride the lifecycle.RebranchArg list
// Attachment.Rebranch accepts, as a replacement.
func (attachBranch) RebranchArg() {}

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
	invalid := streamInvalidError("", "attach Outputs built from task.Tap(name) or task.Track(input, stream), each routed with .To(destination)")
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
	taps := map[string]shape.MediaDomain{}
	for _, tap := range t.Taps() {
		taps[tap.Name] = tap.Domain
	}
	names := newStreamBranchNames()
	var branches []attachBranch
	for _, out := range g.outputs {
		nodes := g.path(out.from)
		if g.node(nodes[0]).Kind != recipeir.NodeAnchor {
			return nil, streamInvalidError("", "a running task attaches streams from its taps, tracks, and graph routes: task.Tap(name), task.Track(input, stream), task.Route(route)")
		}
		root := g.rootOf(nodes[0])
		// known reports that the anchor's domain is known now: a tap a pending
		// route branch declares, or a route, is known only once attached.
		frames, known := false, true
		var source branchSourceBinding
		switch {
		case root.tap != "":
			domain := taps[root.tap]
			frames, known = domain == shape.DomainFrame, domain != ""
			source = branchSourceBinding{tap: root.tap, tapDomain: domain}
		case root.track != nil:
			frames = inputStartsFrameDomain(root.input)
			source = root.input.Track(*root.track).branchSource()
		default:
			// A graph route's domain is the graph's: no packet copy is implied.
			frames, known = true, false
			route := *root.route
			policy := route.Policy
			if policy == "" {
				policy = pipeline.RouteAll
			}
			source = branchSourceBinding{from: route.From, policy: policy, label: route.Label}
		}
		path := streamPath{nodes: nodes, dests: out.dests}
		built, err := g.attachBranchesOf([]streamPath{path}, frames, known, names, source, taps)
		if err != nil {
			return nil, err
		}
		branches = append(branches, built...)
	}
	if names.err != nil {
		return nil, names.err
	}
	return branches, nil
}

// attachBranchesOf lowers the paths of one anchor into one attach branch per
// distinct leaf. A packet branch that never decodes copies.
func (g *recipeGraph) attachBranchesOf(paths []streamPath, frames bool, known bool, names *streamBranchNames, source branchSourceBinding, taps map[string]shape.MediaDomain) ([]attachBranch, error) {
	var out []attachBranch
	for _, leaf := range g.groupLeaves(paths) {
		root := g.rootOf(leaf.nodes[0])
		branch := attachBranch{name: names.next(g, leaf), media: root.media, source: source}
		stream := recipeir.Stream{Name: branch.name, Selector: plan.StreamSelect{Type: root.media}, Input: -1}
		ops := leaf.nodes[1:]
		if known && !frames && !g.pathHasOp(ops, streamOpDecode) {
			operation := opCopy()
			operation.Node = -1
			stream.Operations = append(stream.Operations, operation)
		}
		for _, id := range ops {
			operation := g.node(id).Op
			operation.Node = id
			stream.Operations = append(stream.Operations, operation)
		}
		if err := validateBranchOperations(&stream, frames, false, func(recipeir.NodeID) bool { return false }); err != nil {
			return nil, err
		}
		for _, operation := range stream.Operations {
			switch operation.Kind {
			case recipeir.OpBuffer:
				branch.buffer = operation.Buffer
			case recipeir.OpName:
			case plan.OpTap:
				if !known {
					// The attach planner fixes the domain from the live shape.
					operation.Tap.Domain = ""
				}
				if taps != nil {
					taps[operation.Tap.Name] = operation.Tap.Domain
				}
				branch.operations = append(branch.operations, operation)
			default:
				branch.operations = append(branch.operations, operation)
			}
		}
		for i, node := range leaf.dests {
			spec, err := destinationSpecFromDestination(g.dests[g.node(node).Output])
			if err != nil {
				return nil, branchDestinationInvalidError(branch.name, err.Error())
			}
			name := spec.label(fmt.Sprintf("%s-%d", branch.name, i+1))
			branch.dests = append(branch.dests, newDirectDestinationRef(name, spec))
		}
		out = append(out, branch)
	}
	return out, nil
}

// rebranchArgs replaces each Output among args with the branches it attaches
// as, so Rebranch takes the Outputs Attach does.
func (t *task) rebranchArgs(args []lifecycle.RebranchArg) ([]lifecycle.RebranchArg, error) {
	var outs []Output
	mapped := make([]lifecycle.RebranchArg, 0, len(args))
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

// RebranchArg lets an Output ride the lifecycle.RebranchArg list
// Attachment.Rebranch accepts, as a replacement.
func (Output) RebranchArg() {}

// --- stream rules ---

// streamRule is one declared dynamic-stream rule: when a discovered stream
// matches, the templated branches attach to it at runtime.
type streamRule struct {
	match    sourcepkg.StreamMatch
	branches []attachBranch
}

// deriveRule lowers the Outputs of one OnStream root into its rule.
func (s *recipeCompileState) deriveRule(root recipeir.NodeID, paths []streamPath, names *streamBranchNames) error {
	g := s.graph
	node := g.node(root)
	match := g.matches[node.Rule]
	if match.Empty() {
		return streamRuleInvalidError("", "stream rule has no matcher",
			"pass source.MatchMedia(...), source.MatchCodec(...), source.MatchStreamID(...), or source.MatchStream(fn)")
	}
	frames := inputStartsFrameDomain(g.inputs[g.node(node.Parents[0]).Input])
	branches, err := g.attachBranchesOf(paths, frames, true, names, branchSourceBinding{}, nil)
	if err != nil {
		return err
	}
	rule := streamRule{match: match, branches: branches}
	facts := recipeir.StreamRule{MatchDescription: match.Description()}
	for i := range branches {
		facts.RequiresRuntime = facts.RequiresRuntime || recipeIROperationsRequireRuntime(branches[i].operations) || destinationRefsRequireRuntime(branches[i].dests)
		facts.Branches = append(facts.Branches, recipeir.StreamRuleBranch{
			Name:         branches[i].name,
			Media:        branches[i].media,
			Operations:   append([]recipeir.Operation(nil), branches[i].operations...),
			Destinations: branchDestinationNames(branches[i].dests),
			Buffer:       branches[i].buffer,
		})
	}
	s.streamRules = append(s.streamRules, rule)
	s.recipe.StreamRules = append(s.recipe.StreamRules, facts)
	return nil
}

func destinationRefsRequireRuntime(destinations []destinationRef) bool {
	for i := range destinations {
		if destinationSpecRequiresRuntime(destinations[i].dest) {
			return true
		}
	}
	return false
}

// validateStreamRulesForCompile checks the structural prerequisites of
// declared dynamic-stream rules: a single input to anchor on.
func validateStreamRulesForCompile(s *recipeCompileState) error {
	if len(s.streamRules) == 0 {
		return nil
	}
	if inputs := len(s.inputAttachments); inputs != 1 {
		return streamRuleInvalidError("", fmt.Sprintf("stream rules require exactly one input, got %d", inputs),
			"run OnStream Outputs with Outputs of that one input only: input.OnStream(match).To(dst), input.Audio().To(out)")
	}
	return nil
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

func (s *recipeCompileState) ruleSourceNode() string {
	if len(s.streamRules) == 0 || len(s.inputAttachments) == 0 {
		return ""
	}
	return graphSourceNodeNames(s.inputAttachments)[0]
}

func (s *recipeCompileState) ruleSourceDomain() shape.MediaDomain {
	if len(s.streamRules) == 0 || len(s.inputAttachments) == 0 {
		return ""
	}
	return s.inputAttachments[0].sourceEventDomain()
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
