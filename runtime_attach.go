package goav

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/pipeline"
)

var runtimeAttachmentSeq atomic.Uint64

// runtimeBranch is the internal graph mutation plan for a branch attached to an
// already-built task.
type runtimeBranch struct {
	name   string
	from   string
	tap    string
	anchor TapInfo
	steps  []runtimeBranchStep
	sink   pipeline.Sink
	policy pipeline.RoutePolicy
	label  string
	buffer pipeline.BufferPolicy
	err    error
}

type runtimeBranchStep struct {
	stage pipeline.Stage
	tap   string
}

// Attachment is a live runtime branch attached to a task.
type Attachment interface {
	ID() string
	Name() string
	Spec() pipeline.Spec
	Stats() BranchStats
	Close(context.Context) error
}

func (t *task) Attach(ctx context.Context, spec BranchSpec) (Attachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	branch, err := runtimeBranchFromSpec(spec)
	if err != nil {
		return nil, err
	}
	if err := validateRuntimeBranch(branch); err != nil {
		return nil, err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()

	graphSpec := t.graph.Spec()
	branch.from, branch.anchor, err = t.resolveRuntimeBranchAnchor(branch, graphSpec)
	if err != nil {
		return nil, err
	}
	if err := t.validateRuntimeBranchTapsLocked(branch); err != nil {
		return nil, err
	}

	nodeNames, err := runtimeBranchNodeNames(branch, graphSpec)
	if err != nil {
		return nil, err
	}
	refs, routes, taps, err := t.attachRuntimeBranch(branch, nodeNames)
	if err != nil {
		t.rollbackRuntimeBranch(refs)
		return nil, err
	}
	attachment := &runtimeAttachment{
		id:         nextRuntimeAttachmentID(branch.name),
		name:       branch.name,
		owner:      t,
		anchorTap:  branch.tap,
		anchorNode: branch.from,
		nodes:      refs,
		routes:     routes,
		taps:       taps,
	}
	t.trackAttachmentLocked(attachment)
	t.addAttachmentTapsLocked(taps)
	return attachment, nil
}

func runtimeBranchFromSpec(spec BranchSpec) (runtimeBranch, error) {
	if spec.err != nil {
		return runtimeBranch{err: spec.err}, spec.err
	}
	branch := runtimeBranch{
		name:   spec.name,
		from:   spec.from,
		tap:    spec.tap,
		policy: spec.policy,
		label:  spec.label,
		buffer: spec.buffer,
	}
	for i := range spec.steps {
		step := spec.steps[i]
		switch {
		case step.stage != nil:
			branch.steps = append(branch.steps, runtimeBranchStep{stage: step.stage})
		case step.transform.Resize != nil || step.transform.Resample != nil:
			return branch, runtimeBranchInvalidError("runtime branch transforms are not dynamic yet", "attach runtime branches with .Do(stage).To(goav.FrameSink(...)) or plan resize/resample branches before Build")
		case step.tap != "":
			branch.steps = append(branch.steps, runtimeBranchStep{tap: step.tap})
		}
	}
	if codecIntentSet(spec.encode) {
		return branch, runtimeBranchInvalidError("runtime branch encoding is not dynamic yet", "plan encode branches before Build or attach a sink branch from a decoded tap")
	}
	if len(spec.targets) != 1 {
		return branch, runtimeBranchInvalidError("runtime branch needs exactly one sink endpoint", "finish the branch with .To(goav.FrameSink(sink))")
	}
	endpoint := spec.targets[0].endpoint
	if endpoint.sink == nil {
		return branch, runtimeBranchInvalidError("runtime branch target must be a sink endpoint", "use .To(goav.FrameSink(sink)) for runtime branches")
	}
	branch.sink = endpoint.sink
	return branch, nil
}

func (t *task) resolveRuntimeBranchAnchor(branch runtimeBranch, spec pipeline.Spec) (string, TapInfo, error) {
	if branch.tap != "" {
		taps := t.tapsLocked()
		for _, tap := range taps {
			if tap.Name == branch.tap {
				return tap.Node.String(), tap, nil
			}
		}
		return "", TapInfo{}, runtimeBranchTapMissingError(branch.tap, taps)
	}
	if !specHasNode(spec, branch.from) {
		return "", TapInfo{}, runtimeBranchAnchorMissingError(branch.from)
	}
	return branch.from, TapInfo{Node: pipeline.NodeRef(branch.from)}, nil
}

func (t *task) attachRuntimeBranch(branch runtimeBranch, nodeNames []string) ([]pipeline.NodeRef, []pipeline.Route, []TapInfo, error) {
	refs := make([]pipeline.NodeRef, 0, len(nodeNames))
	routes := make([]pipeline.Route, 0, len(nodeNames))
	taps := make([]TapInfo, 0, runtimeBranchTapCount(branch))
	previous := pipeline.NodeRef(branch.from)
	stageIndex := 0
	connectedFromAnchor := false
	for i := range branch.steps {
		step := branch.steps[i]
		if step.tap != "" {
			taps = append(taps, runtimeBranchTapInfo(branch, step.tap, previous))
			continue
		}
		if step.stage == nil {
			continue
		}
		ref, err := t.graph.AddStage(namedStage{name: nodeNames[stageIndex], stage: step.stage}, branch.buffer)
		if err != nil {
			return refs, routes, taps, runtimeBranchGraphError("add stage", nodeNames[stageIndex], err)
		}
		refs = append(refs, ref)
		if !connectedFromAnchor {
			route := runtimeBranchRoute(pipeline.NodeRef(branch.from), ref, branch)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch", nodeNames[stageIndex], err)
			}
			routes = append(routes, route)
			connectedFromAnchor = true
		} else {
			route := routeBetween(previous, ref)
			if err := t.graph.Connect(route); err != nil {
				return refs, routes, taps, runtimeBranchGraphError("connect branch stage", nodeNames[stageIndex], err)
			}
			routes = append(routes, route)
		}
		previous = ref
		stageIndex++
	}

	sinkName := nodeNames[len(nodeNames)-1]
	sinkRef, err := t.graph.AddSink(namedSink{name: sinkName, sink: branch.sink}, branch.buffer)
	if err != nil {
		return refs, routes, taps, runtimeBranchGraphError("add sink", sinkName, err)
	}
	refs = append(refs, sinkRef)
	if !connectedFromAnchor {
		route := runtimeBranchRoute(pipeline.NodeRef(branch.from), sinkRef, branch)
		if err := t.graph.Connect(route); err != nil {
			return refs, routes, taps, runtimeBranchGraphError("connect branch", sinkName, err)
		}
		routes = append(routes, route)
		return refs, routes, taps, nil
	}
	route := routeBetween(previous, sinkRef)
	if err := t.graph.Connect(route); err != nil {
		return refs, routes, taps, runtimeBranchGraphError("connect branch sink", sinkName, err)
	}
	routes = append(routes, route)
	return refs, routes, taps, nil
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

func (t *task) tapsLocked() []TapInfo {
	var base []TapInfo
	if len(t.taps) != 0 {
		base = t.taps
	} else {
		base = inferSpecTaps(t.graph.Spec())
	}
	out := make([]TapInfo, 0, len(base)+len(t.branchTaps))
	seen := make(map[string]struct{}, len(base)+len(t.branchTaps))
	appendTap := func(tap TapInfo) {
		if tap.Name == "" {
			return
		}
		if _, ok := seen[tap.Name]; ok {
			return
		}
		seen[tap.Name] = struct{}{}
		out = append(out, tap)
	}
	for i := range base {
		appendTap(base[i])
	}
	for i := range t.branchTaps {
		appendTap(t.branchTaps[i])
	}
	return out
}

func (t *task) addAttachmentTapsLocked(taps []TapInfo) {
	if len(taps) == 0 {
		return
	}
	t.branchTaps = append(t.branchTaps, taps...)
}

func (t *task) removeAttachmentTapsLocked(attachment *runtimeAttachment) {
	if attachment == nil || len(attachment.taps) == 0 || len(t.branchTaps) == 0 {
		return
	}
	remove := make(map[string]struct{}, len(attachment.taps))
	for i := range attachment.taps {
		remove[attachment.taps[i].Name] = struct{}{}
	}
	out := t.branchTaps[:0]
	for i := range t.branchTaps {
		if _, ok := remove[t.branchTaps[i].Name]; ok {
			continue
		}
		out = append(out, t.branchTaps[i])
	}
	t.branchTaps = out
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
	first := t.stopAttachmentChildrenLocked(ctx, attachment)
	attachment.stopMu.Lock()
	defer attachment.stopMu.Unlock()
	if attachment.stopped {
		delete(t.attachments, attachment)
		return first
	}
	err := attachment.stopLocked(t.graph)
	attachment.stopped = true
	t.removeAttachmentTapsLocked(attachment)
	delete(t.attachments, attachment)
	if first != nil {
		return first
	}
	return err
}

func (t *task) stopAttachmentChildrenLocked(ctx context.Context, attachment *runtimeAttachment) error {
	if attachment == nil {
		return nil
	}
	var first error
	for {
		child := t.childAttachmentLocked(attachment)
		if child == nil {
			return first
		}
		if err := t.stopAttachmentLocked(ctx, child); first == nil && err != nil {
			first = err
		}
	}
}

func (t *task) childAttachmentLocked(parent *runtimeAttachment) *runtimeAttachment {
	if parent == nil {
		return nil
	}
	taps := make(map[string]struct{}, len(parent.taps))
	for i := range parent.taps {
		if parent.taps[i].Name != "" {
			taps[parent.taps[i].Name] = struct{}{}
		}
	}
	nodes := make(map[string]struct{}, len(parent.nodes))
	for i := range parent.nodes {
		nodes[parent.nodes[i].String()] = struct{}{}
	}
	for attachment := range t.attachments {
		if attachment == nil || attachment == parent || attachment.stopped {
			continue
		}
		if _, ok := taps[attachment.anchorTap]; ok && attachment.anchorTap != "" {
			return attachment
		}
		if _, ok := nodes[attachment.anchorNode]; ok && attachment.anchorNode != "" {
			return attachment
		}
	}
	return nil
}

type runtimeAttachment struct {
	id         string
	name       string
	owner      *task
	anchorTap  string
	anchorNode string
	nodes      []pipeline.NodeRef
	routes     []pipeline.Route
	taps       []TapInfo
	stopMu     sync.Mutex
	stopped    bool
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

func validateRuntimeBranch(branch runtimeBranch) error {
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

func runtimeBranchNodeNames(branch runtimeBranch, spec pipeline.Spec) ([]string, error) {
	names := make([]string, 0, runtimeBranchStageCount(branch)+1)
	seen := make(map[string]struct{}, runtimeBranchStageCount(branch)+1)
	stageIndex := 0
	for i := range branch.steps {
		if branch.steps[i].stage == nil {
			continue
		}
		name := runtimeBranchNodeName(branch.name, branch.steps[i].stage.Name(), fmt.Sprintf("stage%d", stageIndex+1))
		if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
			return nil, err
		}
		names = append(names, name)
		stageIndex++
	}
	name := runtimeBranchNodeName(branch.name, branch.sink.Name(), "sink")
	if err := validateRuntimeBranchNodeName(spec, seen, name); err != nil {
		return nil, err
	}
	return append(names, name), nil
}

func runtimeBranchStageCount(branch runtimeBranch) int {
	count := 0
	for i := range branch.steps {
		if branch.steps[i].stage != nil {
			count++
		}
	}
	return count
}

func runtimeBranchTapCount(branch runtimeBranch) int {
	count := 0
	for i := range branch.steps {
		if branch.steps[i].tap != "" {
			count++
		}
	}
	return count
}

func (t *task) validateRuntimeBranchTapsLocked(branch runtimeBranch) error {
	seen := make(map[string]struct{}, runtimeBranchTapCount(branch))
	current := t.tapsLocked()
	existing := make(map[string]struct{}, len(current))
	for i := range current {
		existing[current[i].Name] = struct{}{}
	}
	for i := range branch.steps {
		name := branch.steps[i].tap
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			return runtimeBranchTapDuplicateError(name)
		}
		seen[name] = struct{}{}
		if _, ok := existing[name]; ok {
			return runtimeBranchTapDuplicateError(name)
		}
	}
	return nil
}

func runtimeBranchTapInfo(branch runtimeBranch, name string, node pipeline.NodeRef) TapInfo {
	domain := branch.anchor.Domain
	if domain == "" {
		domain = DomainPacket
	}
	media := branch.anchor.MediaKind
	caps := branch.anchor.Caps
	if caps.Domain == "" {
		caps.Domain = domain
	}
	if caps.MediaKind == "" {
		caps.MediaKind = media
	}
	return TapInfo{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		Caps:      caps,
		Node:      node,
	}
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

func runtimeBranchRoute(from pipeline.NodeRef, to pipeline.NodeRef, branch runtimeBranch) pipeline.Route {
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

func runtimeBranchTapDuplicateError(name string) error {
	return &BuildError{
		Code:      "runtime_branch_tap_duplicate",
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch tap name already exists in the task",
		Suggestions: []string{
			"use a unique tap name for each runtime branch outlet",
			"call task.Taps() before attaching to inspect the current tap names",
		},
		Cause: ErrUnsupportedBuild,
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
