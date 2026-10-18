package goav

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// taskStreamRules is the runtime half of the OnStream grammar: the declared
// rules bound to the built task's source node, plus the bookkeeping that maps
// discovered streams to the attachments their rules created. Reactions run on
// the source's emit path (streamAnnouncer) and apply through the same attach
// and detach as task.Attach; the per-message path pays only a kind check.
type taskStreamRules struct {
	source string
	domain shape.MediaDomain
	rules  []streamRule

	matchers []sourcepkg.StreamMatcher
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

// streamAnnouncer runs a task's OnStream rules on the source's own emit path,
// with GStreamer's pad-added semantics: when the source emits a stream_added
// announce, the Emit call returns only after every matching rule's branch is
// attached, so the stream's next message already routes into the branch and
// the branch sees the stream from its first packet. stream_removed detaches
// the same way, after the announce is delivered. The announce itself is
// forwarded first, so Watch observers and the existing routes see it exactly
// as without rules.
//
// The attach runs on the source's goroutine and is bounded by the source's
// context. A rule destination's Open therefore runs on the source's path: it
// should be fast, or hand slow work to its own goroutine.
type streamAnnouncer struct {
	task atomic.Pointer[task]
	// mu serializes reactions for sources that emit from several goroutines,
	// so one announce's check-then-attach never interleaves with another's.
	mu sync.Mutex
}

// bind points the announcer at the built task; announces before bind (none
// in practice: sources start at Run) are forwarded without a reaction.
func (a *streamAnnouncer) bind(t *task) {
	if a != nil {
		a.task.Store(t)
	}
}

// wrap is the input decoration that installs the announcer around the opened
// source. Name and described detail delegate, so the node keeps its identity
// and Describe() ≡ Build() holds; ControllableSource stays visible.
func (a *streamAnnouncer) wrap(source pipeline.Source) pipeline.Source {
	wrapped := announceSource{source: source, announcer: a}
	if _, ok := source.(pipeline.ControllableSource); ok {
		return &announceControllableSource{announceSource: wrapped}
	}
	return &wrapped
}

// react applies the rules to one announce the source just delivered.
func (a *streamAnnouncer) react(ctx context.Context, event *av.Event) {
	t := a.task.Load()
	if t == nil || t.rules == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	switch event.Type {
	case av.EventStreamAdded:
		t.handleStreamAdded(ctx, *event)
	case av.EventStreamRemoved:
		t.handleStreamRemoved(ctx, *event)
	}
}

type announceSource struct {
	source    pipeline.Source
	announcer *streamAnnouncer
}

func (s *announceSource) Name() string { return s.source.Name() }

func (s *announceSource) Close() error { return s.source.Close() }

func (s *announceSource) DescribeNode() pipeline.NodeSpec {
	return describeNamedNode(s.source.Name(), pipeline.NodeSource, s.source)
}

func (s *announceSource) DroppedMessages() uint64 { return droppedMessagesFrom(s.source) }

func (s *announceSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	return s.source.Start(ctx, newAnnounceEmitter(emitter, s.announcer))
}

type announceControllableSource struct {
	announceSource
}

func (s *announceControllableSource) Control(ctx context.Context, msg *pipeline.Message) error {
	return s.source.(pipeline.ControllableSource).Control(ctx, msg)
}

// announceEmitter forwards every message unchanged and, after a stream
// announce is delivered, runs the rule reaction before returning to the
// source. It keeps the DeliveryEmitter capability of the graph's emitter.
type announceEmitter struct {
	next      pipeline.Emitter
	delivery  pipeline.DeliveryEmitter
	announcer *streamAnnouncer
}

func newAnnounceEmitter(next pipeline.Emitter, announcer *streamAnnouncer) *announceEmitter {
	delivery, _ := next.(pipeline.DeliveryEmitter)
	return &announceEmitter{next: next, delivery: delivery, announcer: announcer}
}

func (e *announceEmitter) Emit(ctx context.Context, msg *pipeline.Message) error {
	announce, ok := streamAnnounceOf(msg)
	if err := e.next.Emit(ctx, msg); err != nil || !ok {
		return err
	}
	e.announcer.react(ctx, &announce)
	return nil
}

func (e *announceEmitter) EmitDelivery(ctx context.Context, msg *pipeline.Message) (pipeline.Delivery, error) {
	if e.delivery == nil {
		return pipeline.Delivery{}, e.Emit(ctx, msg)
	}
	announce, ok := streamAnnounceOf(msg)
	delivery, err := e.delivery.EmitDelivery(ctx, msg)
	if err != nil || !ok {
		return delivery, err
	}
	e.announcer.react(ctx, &announce)
	return delivery, nil
}

// streamAnnounceOf copies a stream announce out of msg before it is
// forwarded, so a downstream stage that rewrites the event in place cannot
// change what the rules see.
func streamAnnounceOf(msg *pipeline.Message) (av.Event, bool) {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return av.Event{}, false
	}
	if msg.Event.Type != av.EventStreamAdded && msg.Event.Type != av.EventStreamRemoved {
		return av.Event{}, false
	}
	event := *msg.Event
	if event.Stream != nil {
		stream := *event.Stream
		event.Stream = &stream
	}
	return event, true
}

// installStreamRules binds the job's declared rules to the built task and
// points the source's announcer at it.
func (t *task) installStreamRules(announcer *streamAnnouncer, sourceNode string, domain shape.MediaDomain, rules []streamRule) {
	if t == nil || announcer == nil || len(rules) == 0 {
		return
	}
	started := time.Now()
	matchers := make([]sourcepkg.StreamMatcher, len(rules))
	for i := range rules {
		matchers[i] = rules[i].match.MatcherAt(started)
	}
	t.rules = &taskStreamRules{
		source:   sourceNode,
		domain:   domain,
		rules:    rules,
		matchers: matchers,
		attached: make(map[av.StreamID][]streamRuleAttachment),
	}
	announcer.bind(t)
}

// handleStreamAdded reacts to one stream announce: every matching rule's
// branches are templated for the stream and lowered through task.Attach —
// the same plan, atomic apply, and rollback as a manual attach. Failures
// surface as av.EventAttachError on task watches; a failed rule leaves the
// task unchanged and a later re-announce retries.
func (t *task) handleStreamAdded(ctx context.Context, event av.Event) {
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
	announcedAt := event.At
	if announcedAt.IsZero() {
		announcedAt = time.Now()
	}
	for index := range t.rules.rules {
		rule := t.rules.rules[index]
		if t.streamRuleAttached(stream.ID, index) {
			continue
		}
		if !t.rules.matchStream(index, stream, announcedAt) {
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

func (r *taskStreamRules) matchStream(index int, stream av.Stream, announcedAt time.Time) bool {
	if r == nil || index < 0 {
		return false
	}
	if index < len(r.matchers) {
		return r.matchers[index].MatchesAt(stream, announcedAt)
	}
	if index < len(r.rules) {
		matcher := r.rules[index].match.MatcherAt(announcedAt)
		return matcher.MatchesAt(stream, announcedAt)
	}
	return false
}

func (t *task) streamRuleAttachInput(rule streamRule, stream av.Stream) (streamRuleAttachInput, error) {
	input := streamRuleAttachInput{branchNames: streamRuleBranchNames(rule)}
	branches, err := t.streamRuleAttachBranches(rule, stream)
	if err != nil {
		return input, err
	}
	attach, err := runtimeAttachInputFromBranchInputs(branches, runtimeAttachmentNameFromBranchInputs(branches))
	if err != nil {
		return input, err
	}
	input.attach = attach
	return input, nil
}

// streamRuleAttachBranches templates one rule's branches for a matched stream,
// solves their operation shapes, and captures them as runtime branch inputs
// before any graph mutation.
func (t *task) streamRuleAttachBranches(rule streamRule, stream av.Stream) ([]runtimeAttachBranchInput, error) {
	domain := t.rules.domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	initial := shape.FromStream(stream, domain)
	branches := make([]runtimeAttachBranchInput, 0, len(rule.branches))
	for i := range rule.branches {
		spec := branchForDiscoveredStream(rule.branches[i], t.rules.source, domain, stream)
		if err := validateSyncPolicyForRecipeIROperations("attach stream rule branch", spec.name, stream, spec.operations); err != nil {
			return nil, err
		}
		if err := validatePlayoutPolicyForRecipeIROperations("attach stream rule branch", spec.name, stream, spec.operations); err != nil {
			return nil, err
		}
		planned := recipeir.Stream{
			Name: spec.name,
			Selector: plan.StreamSelect{
				ID:    stream.ID,
				Type:  stream.Type,
				Codec: stream.Codec.ID,
			},
			Operations: spec.operations,
		}
		solved, _, err := solveRecipeIROperationShapes("attach runtime branch", t.runtime, planned, initial)
		if err != nil {
			return nil, err
		}
		if solved != nil {
			spec.operations = solved
		}
		destinations, err := attachBranchDestinations(spec)
		if err != nil {
			return nil, err
		}
		if err := validateAttachBranchSpec(spec, destinations); err != nil {
			return nil, err
		}
		branches = append(branches, runtimeAttachBranchInput{
			recipe:       runtimeBranchRecipeFromBranchSpec(spec),
			destinations: destinations,
		})
	}
	return branches, nil
}

// handleStreamRemoved detaches every attachment the rules created for the
// removed stream with drain semantics: destinations commit and the branch
// snapshot reports lifecycle.DestinationCommitted — the same typed outcome as
// Rebranch's DrainOldBranch.
func (t *task) handleStreamRemoved(ctx context.Context, event av.Event) {
	input := t.streamRuleRemoveInput(event)
	if input.streamID == "" {
		return
	}
	for i := range input.attachments {
		entry := input.attachments[i]
		if entry.detach.runtime == nil {
			continue
		}
		if err := t.detachRuntimeAttachment(ctx, entry.detach); err != nil {
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
