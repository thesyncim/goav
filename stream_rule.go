package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// streamRule is one declared dynamic-stream rule: when a discovered stream
// matches, the templated branches are attached to it at runtime.
type streamRule struct {
	match             sourcepkg.StreamMatch
	branches          []BranchSpec
	removeDisposition oldBranchDisposition
}

// OnRemove configures how branches created by an OnStream rule detach when
// the matched stream is removed. Without OnRemove the historical rule default
// drains; OnRemove() selects plain detach, lifecycle.DrainBranch commits, and
// lifecycle.AbortBranch aborts.
func OnRemove(options ...lifecycle.DetachOption) BranchSpec {
	policy := detachPolicyFromOptions(options)
	return BranchSpec{origin: branchSpecOriginOnRemove, removeDisposition: policy.disposition, hasRemoveDisposition: true}
}

// OnStream declares a dynamic-stream rule on the job: when the running task's
// source announces a stream (av.EventStreamAdded carrying the full av.Stream)
// that matches, the branches are attached at runtime — anchored on the source
// node and routed by the discovered stream's id — through the same planner,
// atomic apply, and rollback as Mutable.Attach. Branch names are templated per
// matched stream (suffixed "-<stream id>") so repeated discoveries stay
// unique. On av.EventStreamRemoved the rule's branches for that stream detach
// with drain semantics: their destinations commit and the branch snapshot
// reports lifecycle.DestinationCommitted. Several rules may match one stream;
// each attaches independently. Attach or detach failures surface as
// av.EventAttachError on task watches — never silently. A discovered stream
// matching no rule just surfaces its event, exactly as without rules.
//
// Rules require a single-input job today. Because the task watches its own
// events, each task watch remains independent instead of exposing the shared
// graph channel.
func (j *Job) OnStream(match sourcepkg.StreamMatch, branches ...BranchSpec) *Job {
	if j == nil {
		return j
	}
	rule := streamRule{match: match, removeDisposition: oldBranchDrain}
	for i := range branches {
		branch := branches[i]
		if branch.origin == branchSpecOriginOnRemove || branch.hasRemoveDisposition {
			rule.removeDisposition = branch.removeDisposition
			continue
		}
		rule.branches = append(rule.branches, branch)
	}
	rule.branches = cloneBranchSpecs(rule.branches)
	if err := rule.validate(); err != nil {
		j.setErr(err)
		return j
	}
	j.streamRules = append(j.streamRules, rule)
	return j
}

func (r streamRule) validate() error {
	if r.match.Empty() {
		return streamRuleInvalidError("", "stream rule has no matcher",
			"pass source.MatchMedia(...), source.MatchCodec(...), source.MatchStreamID(...), or source.MatchStream(fn)")
	}
	if len(r.branches) == 0 {
		return streamRuleInvalidError("", "stream rule has no branches",
			"pass one or more goav.Branch(name)...To(destination) values")
	}
	for i := range r.branches {
		branch := r.branches[i]
		if branch.err != nil {
			return branch.err
		}
		if branch.origin != branchSpecOriginBranch {
			return streamRuleInvalidError("", fmt.Sprintf("stream rule branch %d was not constructed with goav.Branch(name)", i+1),
				"pass one or more goav.Branch(name)...To(destination) values")
		}
		if branch.name == "" {
			return streamRuleInvalidError("", fmt.Sprintf("stream rule branch %d has no name", i+1),
				"start each rule branch with goav.Branch(\"name\")")
		}
		if branch.source.from != "" || branch.source.tap != "" {
			return streamRuleInvalidError(branch.name, "stream rule branches must not declare .From(...)",
				"omit .From(...): rule branches anchor on the source node of the discovered stream")
		}
		if len(branch.destinations) == 0 {
			return streamRuleInvalidError(branch.name, "stream rule branch has no destination",
				"finish each rule branch with .To(destination)")
		}
	}
	return nil
}

func cloneBranchSpecs(specs []BranchSpec) []BranchSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]BranchSpec, 0, len(specs))
	for i := range specs {
		spec := specs[i]
		spec.operations = cloneOperationSpecs(spec.operations)
		spec.destinations = cloneDestinationRefs(spec.destinations)
		out = append(out, spec)
	}
	return out
}

func cloneStreamRules(rules []streamRule) []streamRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]streamRule, 0, len(rules))
	for i := range rules {
		out = append(out, streamRule{
			match:             rules[i].match,
			branches:          cloneBranchSpecs(rules[i].branches),
			removeDisposition: rules[i].removeDisposition,
		})
	}
	return out
}

// branchSpecForDiscoveredStream templates one rule branch for a matched
// stream: the branch name is suffixed with the stream id for uniqueness and
// the source binding pins the source+stream anchor (RouteByStream from the
// source node, anchor shape from the announced av.Stream).
func branchSpecForDiscoveredStream(spec BranchSpec, sourceNode string, domain shape.MediaDomain, stream av.Stream) BranchSpec {
	out := spec
	out.name = spec.name + "-" + string(stream.ID)
	out.operations = cloneOperationSpecs(spec.operations)
	out.destinations = cloneDestinationRefs(spec.destinations)
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

// validateStreamRulesPass checks the structural prerequisites of declared
// dynamic-stream rules: a single input to anchor on (the per-rule statics are
// validated by OnStream itself).
func validateStreamRulesPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate stream rules", fn: func(state *recipeCompileState) error {
		if state.streamRuleCount() == 0 {
			return nil
		}
		if state.joinAttachment != nil {
			return streamRuleInvalidError("", "stream rules are not supported on Mix/Composite/Select jobs",
				"declare OnStream rules on single-input goav.From(input) jobs")
		}
		inputs := state.streamRuleInputCount()
		if inputs != 1 {
			return streamRuleInvalidError("", fmt.Sprintf("stream rules require exactly one input, got %d", inputs),
				"declare OnStream rules on single-input goav.From(input) jobs")
		}
		return nil
	}}
}

func (state *recipeCompileState) streamRuleCount() int {
	if state == nil {
		return 0
	}
	if len(state.streamRuleFacts) != 0 {
		return len(state.streamRuleFacts)
	}
	return len(state.streamRules)
}

func (state *recipeCompileState) streamRuleInputCount() int {
	if state == nil {
		return 0
	}
	if state.branchCompositionPresent {
		return 1
	}
	if len(state.inputFacts) != 0 {
		return len(state.inputFacts)
	}
	return len(state.inputAttachments)
}

func explainStreamRuleFacts(rules []recipeir.StreamRule) []plan.Decision {
	if len(rules) == 0 {
		return nil
	}
	out := make([]plan.Decision, 0, len(rules))
	for i := range rules {
		names := make([]string, 0, len(rules[i].Branches))
		destinations := make([]string, 0)
		for j := range rules[i].Branches {
			names = append(names, rules[i].Branches[j].Name+"-<stream>")
			destinations = append(destinations, rules[i].Branches[j].Destinations...)
		}
		match := firstNonEmpty(rules[i].MatchDescription, "none")
		out = append(out, plan.Decision{
			Code:   diagnosticStreamRule,
			Branch: strings.Join(names, "+"),
			Message: fmt.Sprintf("on discovered stream (%s): attach %s to %s per matched stream",
				match, strings.Join(names, ", "), strings.Join(destinations, ", ")),
		})
	}
	return out
}

// sourceEventDomain reports the media domain this input's source produces, so
// discovered-stream anchors carry the right domain (frame-domain custom
// sources announce frame streams; everything else is packet-domain).
func (s InputSpec) sourceEventDomain() shape.MediaDomain {
	if spec, ok := declaredSourceShape(s); ok && spec.Domain != "" {
		return spec.Domain
	}
	return shape.DomainPacket
}

func streamRuleInvalidError(node string, reason string, suggestion string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.StreamRuleInvalid),
		Code:      errcode.StreamRuleInvalid,
		Operation: "build stream rule",
		Node:      firstNonEmpty(node, "rule"),
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			suggestion,
		}),
		Cause: ErrUnsupportedBuild,
	}
}
