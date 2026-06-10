package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/info"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// StreamMatch selects which discovered streams a dynamic-stream rule reacts
// to. Build one with MatchMedia, MatchCodec, MatchStreamID, or MatchStream;
// the zero value matches nothing and is rejected at build.
type StreamMatch struct {
	media av.MediaType
	codec av.CodecID
	id    av.StreamID
	fn    func(av.Stream) bool
	desc  string
}

// MatchMedia matches discovered streams of the given media kind.
func MatchMedia(media av.MediaType) StreamMatch {
	return StreamMatch{media: media, desc: "media=" + string(media)}
}

// MatchCodec matches discovered streams carrying the given codec.
func MatchCodec(id av.CodecID) StreamMatch {
	return StreamMatch{codec: id, desc: "codec=" + string(id)}
}

// MatchStreamID matches the discovered stream with exactly this id.
func MatchStreamID(id av.StreamID) StreamMatch {
	return StreamMatch{id: id, desc: "stream=" + string(id)}
}

// MatchStream matches discovered streams with a custom predicate — the
// escape hatch when the typed matchers are not enough.
func MatchStream(fn func(av.Stream) bool) StreamMatch {
	return StreamMatch{fn: fn, desc: "custom"}
}

func (m StreamMatch) empty() bool {
	return m.media == "" && m.codec == "" && m.id == "" && m.fn == nil
}

func (m StreamMatch) matches(stream av.Stream) bool {
	if m.empty() {
		return false
	}
	if m.media != "" && stream.Type != m.media && stream.Codec.Type != m.media {
		return false
	}
	if m.codec != "" && stream.Codec.ID != m.codec {
		return false
	}
	if m.id != "" && stream.ID != m.id {
		return false
	}
	if m.fn != nil && !m.fn(stream) {
		return false
	}
	return true
}

func (m StreamMatch) description() string {
	if m.desc != "" {
		return m.desc
	}
	return "none"
}

// streamRule is one declared dynamic-stream rule: when a discovered stream
// matches, the templated branches are attached to it at runtime.
type streamRule struct {
	match    StreamMatch
	branches []BranchSpec
}

// OnStream declares a dynamic-stream rule on the job: when the running task's
// source announces a stream (av.EventStreamAdded carrying the full av.Stream)
// that matches, the branches are attached at runtime — anchored on the source
// node and routed by the discovered stream's id — through the same planner,
// atomic apply, and rollback as Task.Attach. Branch names are templated per
// matched stream (suffixed "-<stream id>") so repeated discoveries stay
// unique. On av.EventStreamRemoved the rule's branches for that stream detach
// with drain semantics: their destinations commit and the branch snapshot
// reports info.DestinationCommitted. Several rules may match one stream; each
// attaches independently. Attach or detach failures surface as
// av.EventAttachError on Watch/Events — never silently. A discovered stream
// matching no rule just surfaces its event, exactly as without rules.
//
// Rules require a single-input job today. Because the task watches its own
// events, Events() on a rule-bearing task returns an independent Watch
// subscription per call instead of the shared graph channel.
func (j *Job) OnStream(match StreamMatch, branches ...BranchSpec) *Job {
	if j == nil {
		return j
	}
	rule := streamRule{match: match, branches: cloneBranchSpecs(branches)}
	if err := rule.validate(); err != nil {
		j.setErr(err)
		return j
	}
	j.streamRules = append(j.streamRules, rule)
	return j
}

func (r streamRule) validate() error {
	if r.match.empty() {
		return streamRuleInvalidError("", "stream rule has no matcher",
			"pass goav.MatchMedia(...), goav.MatchCodec(...), goav.MatchStreamID(...), or goav.MatchStream(fn)")
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
			match:    rules[i].match,
			branches: cloneBranchSpecs(rules[i].branches),
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
		if len(state.streamRules) == 0 {
			return nil
		}
		if state.joinAttachment != nil {
			return streamRuleInvalidError("", "stream rules are not supported on Mix/Composite/Select jobs",
				"declare OnStream rules on single-input goav.From(input) jobs")
		}
		inputs := len(state.inputAttachments)
		if state.branchCompositionPresent {
			inputs = 1
		}
		if inputs != 1 {
			return streamRuleInvalidError("", fmt.Sprintf("stream rules require exactly one input, got %d", inputs),
				"declare OnStream rules on single-input goav.From(input) jobs")
		}
		return nil
	}}
}

// explainStreamRules renders the declared rules as plan decisions, so Explain
// lists the conditional branches before any stream appears.
func explainStreamRules(rules []streamRule) []info.Decision {
	if len(rules) == 0 {
		return nil
	}
	out := make([]info.Decision, 0, len(rules))
	for i := range rules {
		names := make([]string, 0, len(rules[i].branches))
		destinations := make([]string, 0)
		for j := range rules[i].branches {
			names = append(names, rules[i].branches[j].name+"-<stream>")
			destinations = append(destinations, branchDestinationNames(rules[i].branches[j].destinations)...)
		}
		out = append(out, info.Decision{
			Code:   "stream_rule",
			Branch: strings.Join(names, "+"),
			Message: fmt.Sprintf("on discovered stream (%s): attach %s to %s per matched stream",
				rules[i].match.description(), strings.Join(names, ", "), strings.Join(destinations, ", ")),
		})
	}
	return out
}

// sourceEventDomain reports the media domain this input's source produces, so
// discovered-stream anchors carry the right domain (frame-domain custom
// sources announce frame streams; everything else is packet-domain).
func (s InputSpec) sourceEventDomain() shape.MediaDomain {
	switch {
	case s.source != nil:
		if spec, ok := customSourceShape(s); ok && spec.Domain != "" {
			return spec.Domain
		}
	case s.provider != nil:
		if domain := s.provider.SourceShape().Domain; domain != "" {
			return domain
		}
	}
	return shape.DomainPacket
}

func streamRuleInvalidError(node string, reason string, suggestion string) error {
	return &BuildError{
		Code:      "stream_rule_invalid",
		Operation: "build stream rule",
		Node:      firstNonEmpty(node, "rule"),
		Reason:    reason,
		Suggestions: []string{
			suggestion,
		},
		Cause: ErrUnsupportedBuild,
	}
}
