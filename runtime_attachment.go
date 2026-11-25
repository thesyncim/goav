package goav

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func (t *task) rollbackRuntimeBranch(nodes []pipeline.NodeRef) {
	for _, node := range slices.Backward(nodes) {
		_ = t.graph.Remove(node)
	}
}

func (t *task) trackAttachmentLocked(attachment *runtimeAttachment) {
	t.pruneIsolatedLocked()
	if t.attachments == nil {
		t.attachments = make(map[*runtimeAttachment]struct{})
	}
	t.attachments[attachment] = struct{}{}
}

// A timed-out detach can still report a real failure from an in-flight
// handler. Keep that domain until retirement settles, then prune only healthy
// records. Check Failed after Retired so a concurrent last handler cannot be
// mistaken for a healthy completion.
func (t *task) pruneIsolatedLocked() {
	t.isolated = slices.DeleteFunc(t.isolated, func(b *isolatedBranch) bool {
		if b.attachment == nil {
			return false
		}
		if _, attached := t.attachments[b.attachment]; attached {
			return false
		}
		return b.domain.Snapshot().Retired && !b.domain.Failed()
	})
}

func (t *task) tapsLocked() []TapSnapshot {
	base := t.taps
	out := make([]TapSnapshot, 0, len(base)+len(t.branchTaps))
	seen := make(map[string]struct{}, len(base)+len(t.branchTaps))
	appendTap := func(tap TapSnapshot) {
		if tap.Name == "" {
			return
		}
		if _, ok := seen[tap.Name]; ok {
			return
		}
		for _, branch := range t.isolated {
			if branch.attachment != nil {
				if _, attached := t.attachments[branch.attachment]; !attached {
					continue
				}
			}
			if branch.domain.Failed() && slices.Contains(branch.plan.nodes, tap.Node) {
				return
			}
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

func (t *task) addAttachmentTapsLocked(taps []TapSnapshot) {
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

// A commit-intent detach must be able to observe execution failure before
// Run returns. Refuse unsupported graphs before removing children, connecting
// replacements, or changing any destination disposition.
func (t *task) validateAttachmentDisposition(disposition oldBranchDisposition) error {
	if disposition != oldBranchDrain {
		return nil
	}
	if _, ok := t.graph.(pipeline.RunErrorReporter); ok {
		return nil
	}
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchGraphErrorCode),
		Code:      runtimeBranchGraphErrorCode,
		Operation: "drain runtime branch",
		Reason:    "graph does not implement pipeline.RunErrorReporter",
		fixes:     buildErrorFixes([]string{"implement or forward pipeline.RunErrorReporter before requesting DrainBranch"}),
		cause:     pipeline.ErrDynamicGraphUnsupported,
	}
}

func (t *task) stopAttachment(ctx context.Context, input runtimeDetachInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.attachMu.LockContext(ctx); err != nil {
		return err
	}
	defer t.attachMu.Unlock()
	return t.stopAttachmentLocked(ctx, input)
}

func (t *task) stopAttachmentLocked(ctx context.Context, input runtimeDetachInput) error {
	attachment := input.runtime
	disposition := input.disposition
	if attachment == nil {
		return nil
	}
	if err := t.validateAttachmentDisposition(disposition); err != nil {
		return err
	}
	first, stranded := t.stopAttachmentChildrenLocked(ctx, input)
	if stranded {
		// A child that could not stop still reads this branch: it stays
		// attached, and a later detach finishes both.
		return first
	}
	attachment.stopMu.Lock()
	defer attachment.stopMu.Unlock()
	if attachment.stopped {
		delete(t.attachments, attachment)
		waitErr := attachment.waitRetirement(ctx)
		if waitErr == nil && attachment.detachOutcome.Load() == DestinationFinalizing {
			attachment.detachOutcome.Store(DestinationAborted)
		}
		return errors.Join(first, waitErr)
	}
	t.setAttachmentDisposition(attachment, disposition)
	err := attachment.stopLocked(ctx, t.graph)
	if disposition == oldBranchDrain {
		if runErr := t.graph.(pipeline.RunErrorReporter).RunError(); runErr != nil && !isStoppedAttachmentError(runErr) {
			err = errorsJoinUnique(err, runErr)
		}
	}
	attachment.stopped = true
	err = errors.Join(err, attachment.waitRetirement(ctx))
	if err != nil {
		t.setAttachmentDisposition(attachment, oldBranchAbort)
	}
	aborted := t.endAttachmentLifecycle(attachment)
	if aborted {
		disposition = oldBranchAbort
	}
	t.removeAttachmentTapsLocked(attachment)
	delete(t.attachments, attachment)
	t.pruneIsolatedLocked()
	if err == nil {
		switch disposition {
		case oldBranchDrain:
			attachment.detachOutcome.Store(DestinationCommitted)
		case oldBranchAbort:
			attachment.detachOutcome.Store(DestinationAborted)
		}
		t.publishAttachmentDestinationLifecycleEvents(attachment, disposition)
		t.publishBranchLifecycleEvent(av.EventBranchDetached, attachment, disposition)
	} else {
		if errors.Is(err, pipeline.ErrCloseWait) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			attachment.detachOutcome.Store(DestinationFinalizing)
		} else {
			attachment.detachOutcome.Store(DestinationFailed)
		}
		t.publishAttachmentDestinationCommitErrors(attachment, err)
	}
	if first != nil {
		return first
	}
	return err
}

func (t *task) publishAttachmentDestinationLifecycleEvents(attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if attachment == nil {
		return
	}
	var kind av.EventType
	switch disposition {
	case oldBranchDrain:
		kind = av.EventDestinationCommitted
	case oldBranchAbort:
		kind = av.EventDestinationAborted
	default:
		return
	}
	for i := range attachment.destinations {
		if attachment.hasTransaction(attachment.destinations[i].Name) {
			continue
		}
		t.publishDestinationLifecycleEvent(kind, attachment.destinations[i].Name, attachment, nil)
	}
}

func (t *task) publishAttachmentDestinationCommitErrors(attachment *runtimeAttachment, cause error) {
	if attachment == nil || cause == nil {
		return
	}
	for i := range attachment.destinations {
		if attachment.hasTransaction(attachment.destinations[i].Name) {
			continue
		}
		t.publishDestinationLifecycleEvent(av.EventDestinationCommitError, attachment.destinations[i].Name, attachment, cause)
	}
}

// stopAttachmentChildrenLocked stops the attachments anchored on this one's
// taps and nodes. Each child is tried once: one that does not stop (its
// context ended) is never reselected, and stranded reports that one is
// still attached.
func (t *task) stopAttachmentChildrenLocked(ctx context.Context, input runtimeDetachInput) (first error, stranded bool) {
	if input.runtime == nil {
		return nil, false
	}
	tried := map[*runtimeAttachment]bool{}
	for {
		child := t.childAttachmentLocked(input.runtime, tried)
		if child == nil {
			return first, stranded
		}
		tried[child] = true
		childInput := runtimeDetachInputForRuntimeAttachment(child, input.disposition)
		if err := t.stopAttachmentLocked(ctx, childInput); first == nil && err != nil {
			first = err
		}
		if !child.stopped {
			stranded = true
		}
	}
}

func (t *task) publishBranchLifecycleEvent(kind av.EventType, attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if t == nil || attachment == nil {
		return
	}
	reason := "runtime branch attached"
	if kind == av.EventBranchDetached {
		reason = "runtime branch detached"
	}
	event := av.Event{
		Type:   kind,
		Reason: reason,
		Metadata: av.Metadata{
			av.MetadataAttachmentID:   attachment.ID(),
			av.MetadataAttachmentName: attachment.Name(),
		},
	}
	if kind == av.EventBranchDetached {
		event.Metadata[av.MetadataDetachDisposition] = detachDispositionLabel(disposition)
	}
	t.watch.publish(event)
}

func detachDispositionLabel(disposition oldBranchDisposition) string {
	switch disposition {
	case oldBranchDrain:
		return "drain"
	case oldBranchAbort:
		return "abort"
	default:
		return "detach"
	}
}

func (t *task) childAttachmentLocked(parent *runtimeAttachment, skip map[*runtimeAttachment]bool) *runtimeAttachment {
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
		if attachment == nil || attachment == parent || attachment.stopped || skip[attachment] {
			continue
		}
		for _, anchor := range attachment.allAnchorTaps() {
			if _, ok := taps[anchor]; ok && anchor != "" {
				return attachment
			}
		}
		for _, anchor := range attachment.allAnchorNodes() {
			if _, ok := nodes[anchor]; ok && anchor != "" {
				return attachment
			}
		}
	}
	return nil
}

type attachmentPhase uint8

const (
	attachActive attachmentPhase = iota
	attachDraining
	attachAborting
	attachStopped
)

type runtimeAttachment struct {
	id           string
	name         string
	owner        *task
	anchorTap    string
	anchorNode   string
	anchorTaps   []string
	anchorNodes  []string
	nodes        []pipeline.NodeRef
	routes       []pipeline.Route
	taps         []TapSnapshot
	stages       []pipeline.Stage
	destinations []compile.WorkDestination
	transactions []*destinationTransaction
	isolated     []*isolatedBranch
	stopMu       sync.Mutex
	stopped      bool
	// phase is protected by owner.lifecycleMu, including during Close.
	phase attachmentPhase
	// detachOutcome records the DestinationState a disposition detach
	// (DrainBranch/AbortBranch) chose for this branch's destinations;
	// snapshots report it instead of the plain DestinationClosed.
	detachOutcome atomic.Value
	// removedStats holds the final counters of the branch's nodes, taken as
	// detach removed them: removed nodes leave the graph's Stats.
	removedStats atomic.Pointer[map[string]pipeline.NodeStats]
	// removedSpec is the branch's spec as detach found it, so a detached
	// branch reports the same node set from Spec as from Stats.
	removedSpec atomic.Pointer[pipeline.Spec]
}

func (a *runtimeAttachment) allAnchorTaps() []string {
	if a == nil {
		return nil
	}
	if len(a.anchorTaps) == 0 {
		if a.anchorTap == "" {
			return nil
		}
		return []string{a.anchorTap}
	}
	return a.anchorTaps
}

func (a *runtimeAttachment) allAnchorNodes() []string {
	if a == nil {
		return nil
	}
	if len(a.anchorNodes) == 0 {
		if a.anchorNode == "" {
			return nil
		}
		return []string{a.anchorNode}
	}
	return a.anchorNodes
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
	return a.branchSpec()
}

// branchSpec is the branch's own graph: its live nodes, or once detach has
// removed them, the spec it recorded on the way out.
func (a *runtimeAttachment) branchSpec() pipeline.Spec {
	if final := a.removedSpec.Load(); final != nil {
		spec := *final
		spec.Nodes = slices.Clone(spec.Nodes)
		spec.Edges = slices.Clone(spec.Edges)
		return spec
	}
	return a.specFromGraph(a.owner.Spec())
}

func (a *runtimeAttachment) specFromGraph(graph pipeline.Spec) pipeline.Spec {
	if a == nil {
		return pipeline.Spec{}
	}
	nodes := make(map[string]struct{}, len(a.nodes))
	for i := range a.nodes {
		nodes[a.nodes[i].String()] = struct{}{}
	}
	spec := pipeline.Spec{Name: a.name, Realtime: graph.Realtime, Execution: graph.Execution}
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

func (a *runtimeAttachment) Stats() pipeline.GraphStats {
	if a == nil || a.owner == nil {
		return pipeline.GraphStats{}
	}
	if removed := a.removedStats.Load(); removed != nil {
		return branchStatsForNodes(withRemovedStats(pipeline.GraphStats{}, removed), a.nodes)
	}
	return branchStatsForNodes(a.owner.Stats(), a.nodes)
}

func (a *runtimeAttachment) Snapshot() BranchSnapshot {
	if a == nil {
		return BranchSnapshot{}
	}
	stats := pipeline.GraphStats{}
	if a.owner == nil {
		return a.branchSnapshotLocked(stats)
	}
	stats = withRemovedStats(a.owner.Stats(), a.removedStats.Load())
	a.owner.attachMu.Lock()
	defer a.owner.attachMu.Unlock()
	return a.branchSnapshotLocked(stats)
}

func (a *runtimeAttachment) branchSnapshotLocked(taskStats pipeline.GraphStats) BranchSnapshot {
	if a == nil {
		return BranchSnapshot{}
	}
	state := BranchAttached
	destinationState := DestinationOpen
	spec := pipeline.Spec{}
	if a.owner != nil {
		_, destinationState = a.owner.lifecycleStates()
		spec = a.branchSpec()
	}
	if a.stopped {
		taskStats = withRemovedStats(pipeline.GraphStats{}, a.removedStats.Load())
		state = BranchDetached
		destinationState = DestinationClosed
		if outcome, ok := a.detachOutcome.Load().(DestinationState); ok && outcome != "" {
			destinationState = outcome
		}
	}
	destinations := destinationSnapshotsFromWork(a.destinations, destinationState)
	if a.owner != nil {
		destinations = a.owner.destinationSnapshots(a.destinations, a.transactions, destinationState, a.id)
	}
	snapshot := BranchSnapshot{
		ID:           a.id,
		Name:         a.name,
		State:        state,
		AnchorTaps:   append([]string(nil), a.allAnchorTaps()...),
		AnchorNodes:  append([]string(nil), a.allAnchorNodes()...),
		Nodes:        append([]pipeline.NodeRef(nil), a.nodes...),
		Taps:         append([]TapSnapshot(nil), a.taps...),
		Destinations: destinations,
		Spec:         spec,
		Stats:        branchStatsForNodes(taskStats, a.nodes),
	}
	if len(a.isolated) > 0 {
		snapshot.FailurePolicy = IsolateBranch
		snapshot.PendingCleanup, snapshot.Retired, snapshot.Err = a.faultOutcome()
		snapshot.Error = errorText(snapshot.Err)
		if snapshot.Err != nil {
			snapshot.State = BranchFailed
		}
	}
	return snapshot
}

func (a *runtimeAttachment) Pause(ctx context.Context) error {
	return a.setPaused(ctx, true)
}

func (a *runtimeAttachment) Resume(ctx context.Context) error {
	return a.setPaused(ctx, false)
}

func (a *runtimeAttachment) setPaused(ctx context.Context, paused bool) error {
	if a == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	if a.stopped {
		return runtimeBranchInvalidError("pause runtime branch", "branch is already detached")
	}
	if a.owner == nil {
		return nil
	}
	pauser, ok := a.owner.graph.(pipeline.NodePauser)
	if !ok {
		return runtimeBranchInvalidError("pause runtime branch", "this task graph does not support pausing branches")
	}
	var first error
	for i := range a.nodes {
		if err := pauser.SetNodePaused(a.nodes[i], paused); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	return first
}

func (a *runtimeAttachment) Rebranch(ctx context.Context, options ...RebranchArg) (Attachment, error) {
	if a == nil || a.owner == nil {
		return nil, runtimeBranchInvalidError("rebranch runtime branch", "attachment has no owning task")
	}
	options, err := a.owner.rebranchArgs(options)
	if err != nil {
		return nil, err
	}
	input, err := runtimeRebranchInputFromArgs(ctx, options)
	if err != nil {
		return nil, err
	}
	return a.rebranchRuntimeAttachment(ctx, input)
}

func (a *runtimeAttachment) rebranchRuntimeAttachment(ctx context.Context, input runtimeRebranchInput) (Attachment, error) {
	if err := a.owner.validateAttachmentDisposition(input.disposition); err != nil {
		return nil, err
	}
	// Attach the replacements first so they are live before the old branch goes
	// away (no delivery gap); if that fails, leave the old branch untouched.
	next, err := a.owner.attachRuntimeBranches(ctx, input.attach)
	if err != nil {
		return nil, err
	}
	if input.group == nil {
		if err := a.detachReplaced(ctx, input.disposition); err != nil {
			return next, err
		}
		return next, nil
	}
	go a.detachReplacedAtBoundary(input.group, input.disposition)
	return next, nil
}

// detachReplacedAtBoundary waits for the switch boundary — the first
// replacement gate opening — and detaches this branch with the chosen
// disposition. When every gate closes before any opened (teardown reached the
// replacements first), the switch never happened and this branch's own
// teardown finalizes it instead.
func (a *runtimeAttachment) detachReplacedAtBoundary(group *switchGroup, disposition oldBranchDisposition) {
	select {
	case <-group.opened:
	case <-group.abandoned:
		select {
		case <-group.opened:
		default:
			return
		}
	}
	_ = a.detachReplaced(context.Background(), disposition)
}

// detachReplaced detaches this branch as the replaced side of a rebranch:
// drain commits its destinations, abort marks its prepared stages failed so
// closing aborts them, and the default stays a plain detach.
func (a *runtimeAttachment) detachReplaced(ctx context.Context, disposition oldBranchDisposition) error {
	return a.owner.stopAttachment(ctx, runtimeDetachInputForRuntimeAttachment(a, disposition))
}

func (a *runtimeAttachment) applyDetachDisposition(disposition oldBranchDisposition) {
	switch disposition {
	case oldBranchDrain:
		for _, tx := range a.transactions {
			tx.Succeed()
		}
		a.detachOutcome.Store(DestinationFinalizing)
	case oldBranchAbort:
		for _, tx := range a.transactions {
			tx.Fail()
		}
		for _, stage := range a.stages {
			markPipelineStageFailed(stage)
		}
		a.detachOutcome.Store(DestinationFinalizing)
	case oldBranchDetach:
	}
}

func (a *runtimeAttachment) hasTransaction(name string) bool {
	for _, tx := range a.transactions {
		if tx.Name() == name {
			return true
		}
	}
	return false
}

func (a *runtimeAttachment) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.owner != nil {
		return a.owner.stopAttachment(ctx, runtimeDetachInputForRuntimeAttachment(a, oldBranchDetach))
	}
	return nil
}

// stopLocked takes the branch out of the graph without losing its tail: the
// routes into it are cut first, then its nodes are removed upstream first.
// Removing a node delivers what it already queued (pipeline.Graph.Remove), so
// each node drains into downstream nodes that are still attached. An abort
// detach has marked its destinations failed already, so what drains into them
// is discarded with the aborted write. If an upstream removal cannot finish,
// abandon the remaining destinations before any downstream finalizer can run.
func (a *runtimeAttachment) stopLocked(ctx context.Context, graph pipeline.Graph) error {
	var first error
	final := a.specFromGraph(graph.Spec())
	a.removedSpec.Store(&final)
	for i := range a.routes {
		if slices.Contains(a.nodes, pipeline.NodeRef(a.routes[i].From)) {
			continue // leaves with its node
		}
		if err := graph.Disconnect(a.routes[i]); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
			a.owner.setAttachmentDisposition(a, oldBranchAbort)
		}
	}
	removed := make(map[string]pipeline.NodeStats, len(a.nodes))
	for _, v := range upstreamFirst(a.nodes, a.routes) {
		if err := removeBranchNode(ctx, graph, v, removed); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
			a.owner.setAttachmentDisposition(a, oldBranchAbort)
		}
	}
	a.removedStats.Store(&removed)
	return first
}

func (a *runtimeAttachment) waitRetirement(ctx context.Context) error {
	if len(a.isolated) == 0 {
		return nil
	}
	timeout := 30 * time.Second
	if a.owner != nil && a.owner.runtime != nil && a.owner.runtime.closeWaitTimeout != 0 {
		timeout = a.owner.runtime.closeWaitTimeout
	}
	waitCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var outcome error
	for _, branch := range a.isolated {
		err := branch.domain.Wait(waitCtx)
		if err != nil && waitCtx.Err() != nil && ctx.Err() == nil {
			snapshot := branch.domain.Snapshot()
			err = errors.Join(snapshot.CleanupErr, &pipeline.CloseWaitError{Operation: "detach", Node: branch.plan.name, Pending: int64(snapshot.PendingCleanup), Timeout: timeout})
		}
		outcome = errorsJoinUnique(outcome, err)
	}
	return outcome
}

// upstreamFirst orders a branch's nodes so that none comes before a node
// routed into it; ties, and a cycle should one exist, keep the attach order.
func upstreamFirst(nodes []pipeline.NodeRef, routes []pipeline.Route) []pipeline.NodeRef {
	left := slices.Clone(nodes)
	out := make([]pipeline.NodeRef, 0, len(nodes))
	fed := func(node pipeline.NodeRef) bool {
		for _, route := range routes {
			if route.From != node.String() && slices.Contains(left, pipeline.NodeRef(route.From)) && slices.Contains(route.To, node.String()) {
				return true
			}
		}
		return false
	}
	for len(left) > 0 {
		next := slices.IndexFunc(left, func(node pipeline.NodeRef) bool { return !fed(node) })
		if next < 0 {
			next = 0
		}
		out = append(out, left[next])
		left = slices.Delete(left, next, next+1)
	}
	return out
}

func branchStatsForNodes(stats pipeline.GraphStats, nodes []pipeline.NodeRef) pipeline.GraphStats {
	if len(nodes) == 0 || len(stats.Nodes) == 0 {
		return pipeline.GraphStats{}
	}
	out := pipeline.GraphStats{
		DropReasons: make(map[pipeline.DropPolicy]uint64),
		Nodes:       make(map[string]pipeline.NodeStats, len(nodes)),
	}
	for i := range nodes {
		name := nodes[i].String()
		nodeStats, ok := stats.Nodes[name]
		if !ok {
			continue
		}
		out.Nodes[name] = clonePublicNodeStats(nodeStats)
		out.Messages += nodeStats.OutMessages
		out.Packets += nodeStats.OutPackets
		out.Frames += nodeStats.OutFrames
		out.Events += nodeStats.OutEvents
		out.Delivered += nodeStats.InMessages
		out.Dropped += nodeStats.Dropped
		for reason, count := range nodeStats.DropReasons {
			out.DropReasons[reason] += count
		}
		if nodeStats.LastEventPresent {
			out.LastEvent = nodeStats.LastEvent
			out.LastEventPresent = true
		}
	}
	if len(out.DropReasons) == 0 {
		out.DropReasons = nil
	}
	if len(out.Nodes) == 0 {
		out.Nodes = nil
	}
	return out
}

func clonePublicNodeStats(stats pipeline.NodeStats) pipeline.NodeStats {
	cloned := stats
	if stats.DropReasons != nil {
		cloned.DropReasons = make(map[pipeline.DropPolicy]uint64, len(stats.DropReasons))
		maps.Copy(cloned.DropReasons, stats.DropReasons)
	}
	return cloned
}

func isStoppedAttachmentError(err error) bool {
	return errors.Is(err, pipeline.ErrInvalidLink) ||
		errors.Is(err, pipeline.ErrUnknownNode) ||
		errors.Is(err, pipeline.ErrClosed)
}

func destinationSpecHasOutput(dest destinationSpec) bool {
	return dest.output.Name != "" ||
		dest.output.URI != "" ||
		dest.output.Writer != nil ||
		dest.format != "" ||
		dest.resolvedFormat != ""
}

func runtimeBranchStreamMedia(stream av.Stream) av.MediaType {
	if stream.Type != "" {
		return stream.Type
	}
	return stream.Codec.Type
}

func runtimeBranchAnchorShape(anchor TapSnapshot) shape.Spec {
	shape := anchor.Shape
	if shape.Domain == "" {
		shape.Domain = anchor.Domain
	}
	if shape.MediaKind == "" {
		shape.MediaKind = anchor.MediaKind
	}
	return shape
}

func streamFromRuntimeBranchShape(name string, shape shape.Spec) av.Stream {
	stream := av.Stream{
		ID:   av.StreamID(firstNonEmpty(string(shape.StreamID), name, "runtime-branch")),
		Name: firstNonEmpty(name, "runtime-branch"),
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:            shape.Codec,
			Type:          shape.MediaKind,
			SampleRate:    shape.SampleRate,
			Channels:      shape.Channels,
			ChannelLayout: "",
			Width:         shape.Width,
			Height:        shape.Height,
			PixelFormat:   shape.PixelFormat,
			SampleFormat:  shape.SampleFormat,
		},
	}
	if shape.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(shape.SampleRate)
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(shape.SampleRate)}
	}
	return stream
}

func mediaShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := previous
	spec.Domain = shape.DomainFrame
	if stream.Type != "" {
		spec.MediaKind = stream.Type
	}
	if stream.ID != "" {
		spec.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		spec.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		spec.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		spec.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		spec.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		spec.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		spec.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		spec.SampleFormat = stream.Codec.SampleFormat
	}
	return spec
}

func streamPacketShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := mediaShapeFromRuntimeBranchStream(stream, previous)
	spec.Domain = shape.DomainPacket
	return spec
}

func runtimeBranchTap(name string, node pipeline.NodeRef, spec shape.Spec, after plan.OperationKind) TapSnapshot {
	domain := spec.Domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	media := spec.MediaKind
	if spec.Domain == "" {
		spec.Domain = domain
	}
	if spec.MediaKind == "" {
		spec.MediaKind = media
	}
	return TapSnapshot{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		After:     after,
		Shape:     spec,
		Node:      node,
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
