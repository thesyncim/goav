package goav

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// RuntimeBranch describes a branch attached to an already-built task.
type RuntimeBranch struct {
	name   string
	from   string
	stages []pipeline.Stage
	sink   pipeline.Sink
	policy pipeline.RoutePolicy
	label  string
	buffer pipeline.BufferPolicy
	err    error
}

// RuntimeBranchBuilder builds a runtime branch without exposing graph mutation.
type RuntimeBranchBuilder struct {
	branch RuntimeBranch
}

// Attachment is a live runtime branch attached to a task.
type Attachment interface {
	Name() string
	Stop(context.Context) error
}

// Branch starts a runtime branch description. Attach it with Task.Attach.
func Branch(name string) *RuntimeBranchBuilder {
	return &RuntimeBranchBuilder{branch: RuntimeBranch{name: name}}
}

func (b *RuntimeBranchBuilder) From(node string) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	b.branch.from = node
	return b
}

func (b *RuntimeBranchBuilder) FromDecodedAudio(options ...streamOption) *RuntimeBranchBuilder {
	return b.fromDecoded(av.MediaAudio, options...)
}

func (b *RuntimeBranchBuilder) FromDecodedVideo(options ...streamOption) *RuntimeBranchBuilder {
	return b.fromDecoded(av.MediaVideo, options...)
}

func (b *RuntimeBranchBuilder) fromDecoded(media av.MediaType, options ...streamOption) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	b.branch.from = decodeNodeName(newStreamSelector(media, options...))
	return b
}

func (b *RuntimeBranchBuilder) Stream(stream av.StreamID) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	b.branch.policy = pipeline.RouteByStream
	b.branch.label = string(stream)
	return b
}

func (b *RuntimeBranchBuilder) Event(event av.EventType) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	b.branch.policy = pipeline.RouteByEvent
	b.branch.label = string(event)
	return b
}

func (b *RuntimeBranchBuilder) Buffer(policy pipeline.BufferPolicy) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	b.branch.buffer = policy
	return b
}

func (b *RuntimeBranchBuilder) Do(stages ...pipeline.Stage) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	for i := range stages {
		if stages[i] == nil {
			b.setErr(runtimeBranchInvalidError("stage is nil", "pass non-nil pipeline.Stage values or omit Do(...)"))
			return b
		}
		b.branch.stages = append(b.branch.stages, stages[i])
	}
	return b
}

func (b *RuntimeBranchBuilder) To(sink pipeline.Sink) RuntimeBranch {
	if b == nil {
		return RuntimeBranch{err: runtimeBranchInvalidError("branch builder is nil", "start with goav.Branch(name)")}
	}
	if sink == nil {
		b.setErr(runtimeBranchInvalidError("sink is nil", "pass a non-nil pipeline.Sink such as goav.SinkFunc(...)"))
		return b.branch
	}
	b.branch.sink = sink
	return b.branch
}

func (b *RuntimeBranchBuilder) setErr(err error) {
	if b.branch.err == nil {
		b.branch.err = err
	}
}

func (t *task) Attach(ctx context.Context, branch RuntimeBranch) (Attachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRuntimeBranch(branch); err != nil {
		return nil, err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()

	spec := t.graph.Spec()
	if !specHasNode(spec, branch.from) {
		return nil, runtimeBranchAnchorMissingError(branch.from)
	}

	nodeNames, err := runtimeBranchNodeNames(branch, spec)
	if err != nil {
		return nil, err
	}
	refs, routes, err := t.attachRuntimeBranch(branch, nodeNames)
	if err != nil {
		t.rollbackRuntimeBranch(refs)
		return nil, err
	}
	attachment := &runtimeAttachment{
		name:   branch.name,
		owner:  t,
		nodes:  refs,
		routes: routes,
	}
	t.trackAttachmentLocked(attachment)
	return attachment, nil
}

func (t *task) attachRuntimeBranch(branch RuntimeBranch, nodeNames []string) ([]pipeline.NodeRef, []pipeline.Route, error) {
	refs := make([]pipeline.NodeRef, 0, len(nodeNames))
	routes := make([]pipeline.Route, 0, len(nodeNames))
	var previous pipeline.NodeRef
	for i := range branch.stages {
		ref, err := t.graph.AddStage(namedStage{name: nodeNames[i], stage: branch.stages[i]}, branch.buffer)
		if err != nil {
			return refs, routes, runtimeBranchGraphError("add stage", nodeNames[i], err)
		}
		refs = append(refs, ref)
		if i == 0 {
			route := runtimeBranchRoute(pipeline.NodeRef(branch.from), ref, branch)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, runtimeBranchGraphError("connect branch", nodeNames[i], err)
			}
			routes = append(routes, route)
		} else {
			route := routeBetween(previous, ref)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, runtimeBranchGraphError("connect branch stage", nodeNames[i], err)
			}
			routes = append(routes, route)
		}
		previous = ref
	}

	sinkName := nodeNames[len(nodeNames)-1]
	sinkRef, err := t.graph.AddSink(namedSink{name: sinkName, sink: branch.sink}, branch.buffer)
	if err != nil {
		return refs, routes, runtimeBranchGraphError("add sink", sinkName, err)
	}
	refs = append(refs, sinkRef)
	if len(branch.stages) == 0 {
		route := runtimeBranchRoute(pipeline.NodeRef(branch.from), sinkRef, branch)
		if err := t.graph.Connect(route); err != nil {
			return refs, routes, runtimeBranchGraphError("connect branch", sinkName, err)
		}
		routes = append(routes, route)
		return refs, routes, nil
	}
	route := routeBetween(previous, sinkRef)
	if err := t.graph.Connect(route); err != nil {
		return refs, routes, runtimeBranchGraphError("connect branch sink", sinkName, err)
	}
	routes = append(routes, route)
	return refs, routes, nil
}

func (t *task) rollbackRuntimeBranch(nodes []pipeline.NodeRef) {
	for i := len(nodes) - 1; i >= 0; i-- {
		_ = t.graph.Remove(nodes[i])
	}
}

func (t *task) trackAttachmentLocked(attachment *runtimeAttachment) {
	if t.attachments == nil {
		t.attachments = make(map[*runtimeAttachment]struct{})
	}
	t.attachments[attachment] = struct{}{}
}

func (t *task) stopAttachment(ctx context.Context, attachment *runtimeAttachment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.stopAttachmentLocked(ctx, attachment)
}

func (t *task) stopAttachmentLocked(ctx context.Context, attachment *runtimeAttachment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if attachment == nil {
		return nil
	}
	attachment.stopMu.Lock()
	defer attachment.stopMu.Unlock()
	if attachment.stopped {
		delete(t.attachments, attachment)
		return nil
	}
	err := attachment.stopLocked(t.graph)
	attachment.stopped = true
	delete(t.attachments, attachment)
	return err
}

type runtimeAttachment struct {
	name    string
	owner   *task
	nodes   []pipeline.NodeRef
	routes  []pipeline.Route
	stopMu  sync.Mutex
	stopped bool
}

func (a *runtimeAttachment) Name() string {
	if a == nil {
		return ""
	}
	return a.name
}

func (a *runtimeAttachment) Stop(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.owner != nil {
		return a.owner.stopAttachment(ctx, a)
	}
	return nil
}

func (a *runtimeAttachment) stopLocked(graph pipeline.Graph) error {
	var first error
	for i := range a.routes {
		if err := graph.Disconnect(a.routes[i]); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	for i := len(a.nodes) - 1; i >= 0; i-- {
		if err := graph.Remove(a.nodes[i]); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	return first
}

func isStoppedAttachmentError(err error) bool {
	return errors.Is(err, pipeline.ErrInvalidLink) ||
		errors.Is(err, pipeline.ErrUnknownNode) ||
		errors.Is(err, pipeline.ErrClosed)
}

func validateRuntimeBranch(branch RuntimeBranch) error {
	if branch.err != nil {
		return branch.err
	}
	if branch.name == "" {
		return runtimeBranchInvalidError("branch name is empty", "start with goav.Branch(\"name\")")
	}
	if branch.from == "" {
		return runtimeBranchInvalidError("branch source is empty", "call .From(node) with a node from Task.Describe()")
	}
	if branch.sink == nil {
		return runtimeBranchInvalidError("branch sink is missing", "finish the branch with .To(sink)")
	}
	return nil
}

func runtimeBranchNodeNames(branch RuntimeBranch, spec pipeline.Spec) ([]string, error) {
	names := make([]string, 0, len(branch.stages)+1)
	seen := make(map[string]struct{}, len(branch.stages)+1)
	for i := range branch.stages {
		name := runtimeBranchNodeName(branch.name, branch.stages[i].Name(), fmt.Sprintf("stage%d", i+1))
		if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	name := runtimeBranchNodeName(branch.name, branch.sink.Name(), "sink")
	if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
		return nil, err
	}
	return append(names, name), nil
}

func validateRuntimeBranchNodeName(spec pipeline.Spec, seen map[string]struct{}, name string) error {
	if _, ok := seen[name]; ok {
		return runtimeBranchNodeDuplicateError(name)
	}
	seen[name] = struct{}{}
	if specHasNode(spec, name) {
		return runtimeBranchNodeDuplicateError(name)
	}
	return nil
}

func runtimeBranchNodeName(branchName string, localName string, fallback string) string {
	localName = firstNonEmpty(localName, fallback)
	return branchName + "/" + localName
}

func runtimeBranchRoute(from pipeline.NodeRef, to pipeline.NodeRef, branch RuntimeBranch) pipeline.Route {
	policy := branch.policy
	if policy == "" {
		policy = pipeline.RouteAll
	}
	return pipeline.Route{
		From:   from.String(),
		To:     []string{to.String()},
		Policy: policy,
		Label:  branch.label,
	}
}

func routeBetween(from pipeline.NodeRef, to pipeline.NodeRef) pipeline.Route {
	return pipeline.Route{
		From:   from.String(),
		To:     []string{to.String()},
		Policy: pipeline.RouteAll,
	}
}

func specHasNode(spec pipeline.Spec, name string) bool {
	for i := range spec.Nodes {
		if spec.Nodes[i].Name == name {
			return true
		}
	}
	return false
}

func runtimeBranchInvalidError(reason string, suggestion string) error {
	return &BuildError{
		Code:      "runtime_branch_invalid",
		Operation: "attach runtime branch",
		Reason:    reason,
		Suggestions: []string{
			suggestion,
		},
		Cause: ErrUnsupportedBuild,
	}
}

func runtimeBranchAnchorMissingError(node string) error {
	return &BuildError{
		Code:      "runtime_branch_anchor_missing",
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch source node does not exist in the running task graph",
		Suggestions: []string{
			"call task.Describe() and use a node name from the graph spec",
			"use .FromDecodedAudio(...) or .FromDecodedVideo(...) for the common raw decoded frame anchors",
			"attach from a stable decoded-frame node when the branch needs raw frames",
		},
		Cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchNodeDuplicateError(node string) error {
	return &BuildError{
		Code:      "runtime_branch_node_duplicate",
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch node name already exists in the task graph",
		Suggestions: []string{
			"use a unique goav.Branch(name)",
			"use distinct stage and sink names inside repeated runtime branches",
		},
		Cause: pipeline.ErrNodeExists,
	}
}

func runtimeBranchGraphError(operation string, node string, cause error) error {
	return &BuildError{
		Code:      "runtime_branch_graph_error",
		Operation: operation,
		Node:      node,
		Reason:    "runtime graph rejected the branch attachment",
		Suggestions: []string{
			"runtime branches are supported on direct task graphs today",
			"use a direct buffer policy for live runtime branch experiments",
			"build the branch before Run when using buffered execution",
		},
		Cause: cause,
	}
}
