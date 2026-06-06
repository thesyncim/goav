package goav

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

var runtimeAttachmentSeq atomic.Uint64

// RuntimeBranch describes a branch attached to an already-built task.
type RuntimeBranch struct {
	name   string
	from   string
	tap    string
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
	ID() string
	Name() string
	Spec() pipeline.Spec
	Stats() BranchStats
	Close(context.Context) error
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
	b.branch.tap = ""
	return b
}

func (b *RuntimeBranchBuilder) FromTap(name string) *RuntimeBranchBuilder {
	if b == nil {
		return b
	}
	b.branch.tap = name
	b.branch.from = ""
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
	var err error
	branch.from, err = t.resolveRuntimeBranchAnchor(branch, spec)
	if err != nil {
		return nil, err
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
		id:     nextRuntimeAttachmentID(branch.name),
		name:   branch.name,
		owner:  t,
		nodes:  refs,
		routes: routes,
	}
	t.trackAttachmentLocked(attachment)
	return attachment, nil
}

func (t *task) resolveRuntimeBranchAnchor(branch RuntimeBranch, spec pipeline.Spec) (string, error) {
	if branch.tap != "" {
		for _, tap := range t.Taps() {
			if tap.Name == branch.tap {
				return tap.Node.String(), nil
			}
		}
		return "", runtimeBranchTapMissingError(branch.tap, t.Taps())
	}
	if !specHasNode(spec, branch.from) {
		return "", runtimeBranchAnchorMissingError(branch.from)
	}
	return branch.from, nil
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
	id      string
	name    string
	owner   *task
	nodes   []pipeline.NodeRef
	routes  []pipeline.Route
	stopMu  sync.Mutex
	stopped bool
}

func (a *runtimeAttachment) ID() string {
	if a == nil {
		return ""
	}
	return a.id
}

func (a *runtimeAttachment) Name() string {
	if a == nil {
		return ""
	}
	return a.name
}

func (a *runtimeAttachment) Spec() pipeline.Spec {
	if a == nil || a.owner == nil {
		return pipeline.Spec{}
	}
	graph := a.owner.Describe()
	nodes := make(map[string]struct{}, len(a.nodes))
	for i := range a.nodes {
		nodes[a.nodes[i].String()] = struct{}{}
	}
	spec := pipeline.Spec{Name: a.name, Realtime: graph.Realtime}
	for i := range graph.Nodes {
		if _, ok := nodes[graph.Nodes[i].Name]; ok {
			spec.Nodes = append(spec.Nodes, graph.Nodes[i])
		}
	}
	for i := range graph.Edges {
		if _, ok := nodes[graph.Edges[i].To.String()]; ok {
			spec.Edges = append(spec.Edges, graph.Edges[i])
		}
	}
	return spec
}

func (a *runtimeAttachment) Stats() BranchStats {
	if a == nil || a.owner == nil {
		return BranchStats{}
	}
	return a.owner.Stats()
}

func (a *runtimeAttachment) Close(ctx context.Context) error {
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
	if branch.from == "" && branch.tap == "" {
		return runtimeBranchInvalidError("branch source is empty", "call .FromTap(name) with a tap from Task.Taps() or .From(node) with an expert graph node")
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

func nextRuntimeAttachmentID(name string) string {
	seq := runtimeAttachmentSeq.Add(1)
	if name == "" {
		return "attachment-" + strconv.FormatUint(seq, 10)
	}
	return name + "-" + strconv.FormatUint(seq, 10)
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
			"call task.Taps() and use .FromTap(name) for stable media outlets",
			"call task.Describe() and use a node name from the graph spec for expert graph attachments",
			"attach from a stable decoded-frame tap when the branch needs raw frames",
		},
		Cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchTapMissingError(name string, taps []TapInfo) error {
	details := make([]string, 0, len(taps))
	for i := range taps {
		details = append(details, taps[i].Name+": "+string(taps[i].Domain)+" "+string(taps[i].MediaKind))
	}
	return &BuildError{
		Code:      "runtime_branch_tap_missing",
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch source tap does not exist in the running task",
		Details:   details,
		Suggestions: []string{
			"add .Tap(" + strconv.Quote(name) + ") at the point you want to attach",
			"call task.Taps() before attaching runtime branches",
			"use .From(node) only for expert graph-node attachments",
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
