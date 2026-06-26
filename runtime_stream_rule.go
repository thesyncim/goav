package goav

import (
	"context"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// streamRuleEventCapacity sizes the rule engine's internal watch buffer.
// Stream announces are rare control-plane events, but the engine may be busy
// attaching while more arrive, so it gets a deeper buffer than a default
// watcher — a shed announce would be a silent non-attach.
const streamRuleEventCapacity = 256

// taskStreamRules is the runtime half of the OnStream grammar: the declared
// rules bound to the built task's source node, plus the bookkeeping that maps
// discovered streams to the attachments their rules created. Everything here
// is cold control plane — the reaction loop consumes a Watch subscription and
// the apply path goes through Mutable.Attach (which takes the attach mutex); the
// media hot path is never touched.
type taskStreamRules struct {
	source string
	domain shape.MediaDomain
	rules  []streamRule

	mu       sync.Mutex
	attached map[av.StreamID][]streamRuleAttachment
}

type streamRuleAttachment struct {
	rule       int
	attachment *runtimeAttachment
}

type streamRuleAttachInput struct {
	attach      runtimeAttachInput
	branchNames string
}

type streamRuleRemoveInput struct {
	streamID    av.StreamID
	attachments []streamRuleRemoveAttachment
}

type streamRuleRemoveAttachment struct {
	detach     runtimeDetachInput
	branchName string
}

// installStreamRules binds the job's declared rules to the built task and
// starts the reaction loop: an internal Watch subscription filtered to stream
// added/removed events. The loop ends when the task closes (the graph closes
// its event stream, the distributor closes every watcher channel).
func (t *task) installStreamRules(sourceNode string, domain shape.MediaDomain, rules []streamRule) {
	if t == nil || len(rules) == 0 {
		return
	}
	t.rules = &taskStreamRules{
		source:   sourceNode,
		domain:   domain,
		rules:    rules,
		attached: make(map[av.StreamID][]streamRuleAttachment),
	}
	events := t.watch.subscribe(
		t.graph.Events(),
		streamRuleEventCapacity,
		[]inspect.EventFilter{inspect.WatchTypes(av.EventStreamAdded, av.EventStreamRemoved)},
	)
	go t.runStreamRules(events.Events())
}

func (t *task) runStreamRules(events <-chan av.Event) {
	for event := range events {
		switch event.Type {
		case av.EventStreamAdded:
			t.handleStreamAdded(event)
		case av.EventStreamRemoved:
			t.handleStreamRemoved(event)
		}
	}
}

// handleStreamAdded reacts to one stream announce: every matching rule's
// branches are templated for the stream and lowered through Mutable.Attach —
// the same plan, atomic apply, and rollback as a manual attach. Failures
// surface as av.EventAttachError on task watches; a failed rule leaves the
// task unchanged and a later re-announce retries.
func (t *task) handleStreamAdded(event av.Event) {
	if event.Stream == nil {
		// No typed payload: nothing declarative to do — the event itself
		// already surfaced to watchers.
		return
	}
	stream := *event.Stream
	if stream.ID == "" {
		stream.ID = event.StreamID
	}
	if stream.ID == "" {
		t.publishStreamRuleError(event.StreamID, "", errStreamRuleStreamID)
		return
	}
	ctx := context.Background()
	for index := range t.rules.rules {
		rule := t.rules.rules[index]
		if !rule.match.Matches(stream) {
			continue
		}
		if t.streamRuleAttached(stream.ID, index) {
			continue
		}
		input, err := t.streamRuleAttachInput(rule, stream)
		if err != nil {
			t.publishStreamRuleError(stream.ID, input.branchNames, err)
			continue
		}
		attachment, err := t.attachRuntimeBranches(ctx, input.attach)
		if err != nil {
			t.publishStreamRuleError(stream.ID, input.branchNames, err)
			continue
		}
		t.trackStreamRuleAttachment(stream.ID, index, attachment)
	}
}

func (t *task) streamRuleAttachInput(rule streamRule, stream av.Stream) (streamRuleAttachInput, error) {
	input := streamRuleAttachInput{branchNames: streamRuleBranchNames(rule)}
	specs, err := t.streamRuleBranchSpecs(rule, stream)
	if err != nil {
		return input, err
	}
	attach, err := runtimeAttachInputFromBranchSpecs(specs)
	if err != nil {
		return input, err
	}
	input.attach = attach
	return input, nil
}

// streamRuleBranchSpecs templates one rule's branches for a matched stream
// and runs each through the shape solver, so .Auto(shape.Allow*) policies on
// late branches insert the same conversions the build-time compile would.
func (t *task) streamRuleBranchSpecs(rule streamRule, stream av.Stream) ([]BranchSpec, error) {
	domain := t.rules.domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	initial := shape.FromStream(stream, domain)
	specs := make([]BranchSpec, 0, len(rule.branches))
	for i := range rule.branches {
		spec := branchSpecForDiscoveredStream(rule.branches[i], t.rules.source, domain, stream)
		if err := validateSyncPolicyForStream("attach stream rule branch", spec.name, stream, spec.operations); err != nil {
			return nil, err
		}
		intent := streamIntent{
			Name: spec.name,
			Select: plan.StreamSelect{
				ID:    stream.ID,
				Type:  stream.Type,
				Codec: stream.Codec.ID,
			},
			Operations: spec.operations,
		}
		solved, _, err := solveOperationSpecShapes("attach runtime branch", t.runtime, intent, initial)
		if err != nil {
			return nil, err
		}
		if solved != nil {
			spec.operations = solved
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// handleStreamRemoved detaches every attachment the rules created for the
// removed stream with drain semantics: destinations commit and the branch
// snapshot reports lifecycle.DestinationCommitted — the same typed outcome as
// Rebranch's DrainOldBranch.
func (t *task) handleStreamRemoved(event av.Event) {
	input := t.streamRuleRemoveInput(event)
	if input.streamID == "" {
		return
	}
	for i := range input.attachments {
		entry := input.attachments[i]
		if entry.detach.runtime == nil {
			continue
		}
		if err := t.detachRuntimeAttachment(context.Background(), entry.detach); err != nil {
			t.publishStreamRuleError(input.streamID, entry.branchName, err)
		}
	}
}

func (t *task) streamRuleRemoveInput(event av.Event) streamRuleRemoveInput {
	input := streamRuleRemoveInput{streamID: event.StreamID}
	if t == nil || t.rules == nil || event.StreamID == "" {
		return input
	}
	rules := t.rules
	rules.mu.Lock()
	attachments := append([]streamRuleAttachment(nil), rules.attached[event.StreamID]...)
	delete(rules.attached, event.StreamID)
	rules.mu.Unlock()
	for i := range attachments {
		entry := attachments[i]
		if entry.attachment == nil {
			continue
		}
		input.attachments = append(input.attachments, streamRuleRemoveAttachment{
			detach:     runtimeDetachInputForRuntimeAttachment(entry.attachment, oldBranchDrain),
			branchName: entry.attachment.Name(),
		})
	}
	return input
}

func (t *task) streamRuleAttached(id av.StreamID, rule int) bool {
	t.rules.mu.Lock()
	defer t.rules.mu.Unlock()
	for _, entry := range t.rules.attached[id] {
		if entry.rule == rule {
			return true
		}
	}
	return false
}

func (t *task) trackStreamRuleAttachment(id av.StreamID, rule int, attachment Attachment) {
	runtimeAttached, ok := attachment.(*runtimeAttachment)
	if !ok {
		return
	}
	t.rules.mu.Lock()
	defer t.rules.mu.Unlock()
	t.rules.attached[id] = append(t.rules.attached[id], streamRuleAttachment{rule: rule, attachment: runtimeAttached})
}

func (t *task) publishStreamRuleError(id av.StreamID, branch string, err error) {
	reason := "stream rule reaction failed"
	if branch != "" {
		reason = "stream rule branch " + branch + " failed"
	}
	t.watch.publish(av.Event{
		Type:     av.EventAttachError,
		StreamID: id,
		Cause:    err,
		Reason:   reason,
	})
}

func streamRuleBranchNames(rule streamRule) string {
	names := ""
	for i := range rule.branches {
		if names != "" {
			names += "+"
		}
		names += rule.branches[i].name
	}
	return names
}

var errStreamRuleStreamID = streamRuleInvalidError("", "discovered stream has no id",
	"set Stream.ID (or Event.StreamID) on av.EventStreamAdded announces")
