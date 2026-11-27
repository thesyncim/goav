package goav

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

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
	owner        *Task
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
		return
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
