package compile

import (
	"cmp"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// AttachBranchPlan is one branch an attach group or a stream rule plans: its
// name, media, operations, queue, and destinations.
type AttachBranchPlan struct {
	Name       string
	Media      av.MediaType
	Operations []recipeir.Operation
	Buffer     flow.BranchBuffer
	Dests      []attachDestPlan
}

// attachDestPlan is one destination of a planned branch: its slot in the
// request's destinations, and the name the branch routes it by.
type attachDestPlan struct {
	Slot int
	Name string
}

// StreamRulePlan is one planned OnStream rule: its match and the branches
// it attaches for every stream that matches.
type StreamRulePlan struct {
	Match    sourcepkg.StreamMatch
	Branches []AttachBranchPlan
}

// PlanAttach plans the branches each Output of an attach group hangs below
// its anchor on the running task. taps are the task's taps by domain; a tap
// an earlier Output declares is added to it.
func PlanAttach(req *Request, taps map[string]shape.MediaDomain) ([][]AttachBranchPlan, error) {
	names := newStreamBranchNames()
	out := make([][]AttachBranchPlan, len(req.Outputs))
	for i, output := range req.Outputs {
		nodes := req.Graph.Path(output.From)
		anchor := req.Graph.Nodes[nodes[0]]
		if anchor.Kind != recipeir.NodeAnchor {
			return nil, StreamInvalidError("", "a running task attaches streams from its taps, tracks, and graph routes: task.Tap(name), task.Track(input, stream), task.Route(route)")
		}
		// known reports that the anchor's domain is known now: a tap a pending
		// route branch declares, or a route, is known only once attached.
		frames, known := false, true
		switch {
		case anchor.Anchor.Tap != "":
			domain := taps[anchor.Anchor.Tap]
			frames, known = domain == shape.DomainFrame, domain != ""
		case anchor.Anchor.Track != nil:
			frames = req.Inputs[anchor.Anchor.Input].Frames
		default:
			// A graph route's domain is the graph's: no packet copy is implied.
			frames, known = true, false
		}
		path := streamPath{nodes: nodes, dests: output.Dests}
		planned, err := planAttachBranches(req, []streamPath{path}, frames, known, names, taps)
		if err != nil {
			return nil, err
		}
		out[i] = planned
	}
	if names.err != nil {
		return nil, names.err
	}
	return out, nil
}

// planAttachBranches lowers the paths of one anchor into one attach branch
// per distinct leaf. A packet branch that never decodes copies.
func planAttachBranches(req *Request, paths []streamPath, frames bool, known bool, names *streamBranchNames, taps map[string]shape.MediaDomain) ([]AttachBranchPlan, error) {
	var out []AttachBranchPlan
	for _, leaf := range groupLeaves(req.Graph, paths) {
		root := req.Graph.Nodes[leaf.nodes[0]]
		media := av.MediaType("")
		if root.Kind == recipeir.NodeSelect {
			media = root.Select.Type
		}
		branch := AttachBranchPlan{Name: names.next(req, leaf), Media: media}
		stream := recipeir.Stream{Name: branch.Name, Selector: plan.StreamSelect{Type: media}, Input: -1}
		ops := leaf.nodes[1:]
		if known && !frames && !req.Graph.HasOp(ops, plan.OpDecode) {
			operation := OpCopy()
			operation.Node = -1
			stream.Operations = append(stream.Operations, operation)
		}
		for _, id := range ops {
			operation := req.Graph.Nodes[id].Op
			operation.Node = id
			stream.Operations = append(stream.Operations, operation)
		}
		if err := validateBranchOperations(&stream, frames, false, func(recipeir.NodeID) bool { return false }); err != nil {
			return nil, err
		}
		for _, operation := range stream.Operations {
			switch operation.Kind {
			case recipeir.OpBuffer:
				branch.Buffer = operation.Buffer
			case recipeir.OpName:
			case plan.OpTap:
				if !known {
					// The attach planner fixes the domain from the live shape.
					operation.Tap.Domain = ""
				}
				if taps != nil {
					taps[operation.Tap.Name] = operation.Tap.Domain
				}
				branch.Operations = append(branch.Operations, operation)
			default:
				branch.Operations = append(branch.Operations, operation)
			}
		}
		for i, node := range leaf.dests {
			slot := req.Graph.Nodes[node].Output
			dest := req.Dests[slot]
			if dest.Invalid != "" {
				return nil, branchDestinationInvalidError(branch.Name, dest.Invalid)
			}
			name := cmp.Or(dest.Label, fmt.Sprintf("%s-%d", branch.Name, i+1))
			branch.Dests = append(branch.Dests, attachDestPlan{Slot: slot, Name: name})
		}
		out = append(out, branch)
	}
	return out, nil
}

// deriveRule lowers the Outputs of one OnStream root into its rule.
func (s *recipeState) deriveRule(root recipeir.NodeID, paths []streamPath, names *streamBranchNames) error {
	req := s.req
	node := req.Graph.Nodes[root]
	match := req.Matches[node.Rule]
	if match.Empty() {
		return StreamRuleInvalidError("", "stream rule has no matcher",
			"pass source.MatchMedia(...), source.MatchCodec(...), source.MatchStreamID(...), or source.MatchStream(fn)")
	}
	frames := req.Inputs[req.Graph.Nodes[node.Parents[0]].Input].Frames
	branches, err := planAttachBranches(req, paths, frames, true, names, nil)
	if err != nil {
		return err
	}
	facts := recipeir.StreamRule{MatchDescription: match.Description()}
	for i := range branches {
		var dests []string
		for _, dest := range branches[i].Dests {
			dests = append(dests, dest.Name)
		}
		facts.Branches = append(facts.Branches, recipeir.StreamRuleBranch{
			Name:         branches[i].Name,
			Media:        branches[i].Media,
			Operations:   append([]recipeir.Operation(nil), branches[i].Operations...),
			Destinations: dests,
			Buffer:       branches[i].Buffer,
		})
	}
	s.streamRules = append(s.streamRules, StreamRulePlan{Match: match, Branches: branches})
	s.recipe.StreamRules = append(s.recipe.StreamRules, facts)
	return nil
}

// validateStreamRulesForCompile checks the structural prerequisites of
// declared dynamic-stream rules: a single input to anchor on.
func validateStreamRulesForCompile(s *recipeState) error {
	if len(s.streamRules) == 0 {
		return nil
	}
	if inputs := len(s.req.Inputs); inputs != 1 {
		return StreamRuleInvalidError("", fmt.Sprintf("stream rules require exactly one input, got %d", inputs),
			"run OnStream Outputs with Outputs of that one input only: input.OnStream(match).To(dst), input.Audio().To(out)")
	}
	return nil
}

func (s *recipeState) ruleSourceNode() string {
	if len(s.streamRules) == 0 || len(s.req.Inputs) == 0 {
		return ""
	}
	return s.req.Inputs[0].SourceNode
}

func (s *recipeState) ruleSourceDomain() shape.MediaDomain {
	if len(s.streamRules) == 0 || len(s.req.Inputs) == 0 {
		return ""
	}
	return s.req.Inputs[0].EventDomain
}

// AttachGroup is one attach group a running task lowers: its branches are
// the streams of one recipe whose roots are anchors on the task.
type AttachGroup struct {
	Name string
	Env  Env
	// Streams are the group's branches; Frames reports a branch whose
	// anchor carries frames, and Dests indexes each branch's destinations.
	Streams []recipeir.Stream
	Frames  []bool
	Dests   [][]int
	// Destinations are the group's destinations, and Names their names.
	Destinations []Destination
	Names        []string
	// Anchors are the live nodes anchor roots read, and Routes the routes
	// they read them by; Pending are the tap operations that anchor roots
	// on a tap the group itself declares read.
	Anchors map[recipeir.NodeID]pipeline.NodeRef
	Routes  map[recipeir.NodeID]AnchorRoute
	Pending map[recipeir.NodeID]recipeir.NodeID
}

// AttachPlan is a lowered attach group: the nodes it adds to the running
// graph, in schedule order, named under its branches.
type AttachPlan struct {
	Nodes    []ComposeNode
	lowering *lowering
}

// LowerAttach lowers an attach group through the build's lowering: work its
// branches share is one node, a destination several branches route to is
// one node, and every node a branch alone reads is named under the branch.
func LowerAttach(group AttachGroup) (*AttachPlan, error) {
	s := &recipeState{
		operation:              "attach runtime branch",
		req:                    &Request{Env: group.Env},
		env:                    group.Env,
		inserted:               map[string]recipeir.NodeID{},
		outputs:                group.Destinations,
		outputDestinationNames: group.Names,
	}
	s.recipe.Streams = group.Streams
	for i := range group.Streams {
		s.facts = append(s.facts, branchFacts{frames: group.Frames[i], dests: group.Dests[i]})
	}
	l := newRecipeLowering(s)
	l.attach = &attachLowering{name: group.Name, anchors: group.Anchors, routes: group.Routes, pending: group.Pending}
	if err := l.plan(); err != nil {
		return nil, err
	}
	return &AttachPlan{Nodes: l.g.Nodes, lowering: l}, nil
}

// Ref is the planned node of the group's DAG node id.
func (p *AttachPlan) Ref(id recipeir.NodeID) (pipeline.NodeRef, bool) {
	ref, ok := p.lowering.refs[id]
	return ref, ok
}

// ParentRef is the planned node that feeds DAG node id on branch.
func (p *AttachPlan) ParentRef(branch int, id recipeir.NodeID) pipeline.NodeRef {
	return p.lowering.parentRef(branch, id)
}

// Spec is the graph the group adds, as spec describes the running one.
func (p *AttachPlan) Spec(spec pipeline.Spec) (pipeline.Spec, error) {
	return p.lowering.g.emit(spec)
}
