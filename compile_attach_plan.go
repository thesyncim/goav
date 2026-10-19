package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// attachBranchPlan is one branch an attach group or a stream rule plans: its
// name, media, operations, queue, and destinations.
type attachBranchPlan struct {
	name       string
	media      av.MediaType
	operations []recipeir.Operation
	buffer     flow.BranchBuffer
	dests      []attachDestPlan
}

// attachDestPlan is one destination of a planned branch: its slot in the
// request's destinations, and the name the branch routes it by.
type attachDestPlan struct {
	slot int
	name string
}

// streamRulePlan is one planned OnStream rule: its match and the branches
// it attaches for every stream that matches.
type streamRulePlan struct {
	match    sourcepkg.StreamMatch
	branches []attachBranchPlan
}

// planAttach plans the branches each Output of an attach group hangs below
// its anchor on the running task. taps are the task's taps by domain; a tap
// an earlier Output declares is added to it.
func planAttach(req *compileRequest, taps map[string]shape.MediaDomain) ([][]attachBranchPlan, error) {
	names := newStreamBranchNames()
	out := make([][]attachBranchPlan, len(req.Outputs))
	for i, output := range req.Outputs {
		nodes := req.Graph.Path(output.From)
		anchor := req.Graph.Nodes[nodes[0]]
		if anchor.Kind != recipeir.NodeAnchor {
			return nil, streamInvalidError("", "a running task attaches streams from its taps, tracks, and graph routes: task.Tap(name), task.Track(input, stream), task.Route(route)")
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
func planAttachBranches(req *compileRequest, paths []streamPath, frames bool, known bool, names *streamBranchNames, taps map[string]shape.MediaDomain) ([]attachBranchPlan, error) {
	var out []attachBranchPlan
	for _, leaf := range groupLeaves(req.Graph, paths) {
		root := req.Graph.Nodes[leaf.nodes[0]]
		media := av.MediaType("")
		if root.Kind == recipeir.NodeSelect {
			media = root.Select.Type
		}
		branch := attachBranchPlan{name: names.next(req, leaf), media: media}
		stream := recipeir.Stream{Name: branch.name, Selector: plan.StreamSelect{Type: media}, Input: -1}
		ops := leaf.nodes[1:]
		if known && !frames && !req.Graph.HasOp(ops, plan.OpDecode) {
			operation := opCopy()
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
			slot := req.Graph.Nodes[node].Output
			dest := req.Dests[slot]
			if dest.Invalid != "" {
				return nil, branchDestinationInvalidError(branch.name, dest.Invalid)
			}
			name := firstNonEmpty(dest.Label, fmt.Sprintf("%s-%d", branch.name, i+1))
			branch.dests = append(branch.dests, attachDestPlan{slot: slot, name: name})
		}
		out = append(out, branch)
	}
	return out, nil
}

// deriveRule lowers the Outputs of one OnStream root into its rule.
func (s *recipeCompileState) deriveRule(root recipeir.NodeID, paths []streamPath, names *streamBranchNames) error {
	req := s.req
	node := req.Graph.Nodes[root]
	match := req.Matches[node.Rule]
	if match.Empty() {
		return streamRuleInvalidError("", "stream rule has no matcher",
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
		for _, dest := range branches[i].dests {
			dests = append(dests, dest.name)
		}
		facts.Branches = append(facts.Branches, recipeir.StreamRuleBranch{
			Name:         branches[i].name,
			Media:        branches[i].media,
			Operations:   append([]recipeir.Operation(nil), branches[i].operations...),
			Destinations: dests,
			Buffer:       branches[i].buffer,
		})
	}
	s.streamRules = append(s.streamRules, streamRulePlan{match: match, branches: branches})
	s.recipe.StreamRules = append(s.recipe.StreamRules, facts)
	return nil
}

// validateStreamRulesForCompile checks the structural prerequisites of
// declared dynamic-stream rules: a single input to anchor on.
func validateStreamRulesForCompile(s *recipeCompileState) error {
	if len(s.streamRules) == 0 {
		return nil
	}
	if inputs := len(s.req.Inputs); inputs != 1 {
		return streamRuleInvalidError("", fmt.Sprintf("stream rules require exactly one input, got %d", inputs),
			"run OnStream Outputs with Outputs of that one input only: input.OnStream(match).To(dst), input.Audio().To(out)")
	}
	return nil
}

func (s *recipeCompileState) ruleSourceNode() string {
	if len(s.streamRules) == 0 || len(s.req.Inputs) == 0 {
		return ""
	}
	return s.req.Inputs[0].SourceNode
}

func (s *recipeCompileState) ruleSourceDomain() shape.MediaDomain {
	if len(s.streamRules) == 0 || len(s.req.Inputs) == 0 {
		return ""
	}
	return s.req.Inputs[0].EventDomain
}
