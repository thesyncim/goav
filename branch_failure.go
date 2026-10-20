package goav

import (
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

// BranchFailureError identifies an isolated handler error and unwraps its cause.
type BranchFailureError = pipeline.FaultError

func unsafeIsolationError(reason string, cause error) error {
	if cause == nil {
		cause = pipeline.ErrUnsafeFaultDomain
	}
	return &BuildError{Phase: phaseBuild, Family: errcode.FamilyBranch, Code: "branch_isolation_unsafe", Operation: "isolate branch failures", Reason: reason, fixes: buildErrorFixes([]string{"use FailTask for shared destinations and joins, or give this Output an exclusive downstream destination"}), cause: cause}
}

type isolationPlan struct {
	id, name     string
	nodes        []pipeline.NodeRef
	destinations []string // stable work destination IDs
}

type isolatedBranch struct {
	plan       isolationPlan
	domain     *pipeline.FaultDomain
	spec       pipeline.Spec
	work       []compile.WorkDestination
	txs        []*destinationTransaction
	attachment *runtimeAttachment
}

// exclusiveSuffix computes membership from all reachable terminals. Shared
// ancestors remain task-wide; neither Owner nor display names define safety.
func exclusiveSuffix(nodes []compile.ComposeNode, terminals map[pipeline.NodeRef]bool) ([]pipeline.NodeRef, error) {
	byRef := map[pipeline.NodeRef]int{}
	next := map[pipeline.NodeRef][]pipeline.NodeRef{}
	for i, n := range nodes {
		byRef[pipeline.NodeRef(n.Name)] = i
	}
	for _, n := range nodes {
		for _, in := range n.Inputs {
			next[in] = append(next[in], pipeline.NodeRef(n.Name))
		}
	}
	memo, visiting := map[pipeline.NodeRef]bool{}, map[pipeline.NodeRef]bool{}
	var exclusive func(pipeline.NodeRef) bool
	exclusive = func(ref pipeline.NodeRef) bool {
		if value, ok := memo[ref]; ok {
			return value
		}
		if visiting[ref] {
			return false
		}
		visiting[ref] = true
		children := next[ref]
		value := terminals[ref]
		if len(children) != 0 {
			value = true
			for _, child := range children {
				value = exclusive(child) && value
			}
		}
		visiting[ref] = false
		memo[ref] = value
		return value
	}
	set := map[pipeline.NodeRef]bool{}
	var members []pipeline.NodeRef
	for _, n := range nodes {
		ref := pipeline.NodeRef(n.Name)
		if n.Kind != compile.ComposeSource && exclusive(ref) {
			members = append(members, ref)
			set[ref] = true
		}
	}
	if len(members) == 0 {
		return nil, unsafeIsolationError("the Output has no exclusive failure boundary", nil)
	}
	for ref := range terminals {
		if !set[ref] {
			return nil, unsafeIsolationError("the Output shares a destination or downstream work", nil)
		}
	}
	for _, n := range nodes {
		ref := pipeline.NodeRef(n.Name)
		touches := set[ref]
		for _, in := range n.Inputs {
			touches = touches || set[in]
		}
		if touches && (n.Kind == compile.ComposeJoin || len(n.Inputs) > 1) {
			return nil, unsafeIsolationError("isolation cannot cross a join or a multi-input destination", nil)
		}
		for _, in := range n.Inputs {
			if set[ref] {
				if i, ok := byRef[in]; ok && nodes[i].Kind == compile.ComposeJoin {
					return nil, unsafeIsolationError("isolation cannot border a join", nil)
				}
			}
		}
	}
	return members, nil
}

func (g *recipeGraph) isolationPlans(result compile.Result) ([]isolationPlan, error) {
	var plans []isolationPlan
	for i, out := range g.outputs {
		if out.failure != IsolateBranch {
			continue
		}
		selected := map[int]bool{}
		for _, dest := range out.dests {
			for j, sibling := range g.outputs {
				if j != i && slices.Contains(sibling.dests, dest) {
					return nil, unsafeIsolationError("isolated Outputs cannot share destinations", nil)
				}
			}
			selected[g.node(dest).Output] = true
		}
		terminals := map[pipeline.NodeRef]bool{}
		p := isolationPlan{id: fmt.Sprintf("output-%d", i+1)}
		for _, n := range result.Graph.Nodes {
			switch n.Kind {
			case compile.ComposeSink, compile.ComposeMux, compile.ComposeRemuxSink, compile.ComposeRemuxMux:
				if n.Output >= len(result.OutputSlots) || !selected[result.OutputSlots[n.Output]] {
					continue
				}
				terminals[pipeline.NodeRef(n.Name)] = true
				for _, op := range result.Work.Operations {
					if op.Node == pipeline.NodeRef(n.Name) {
						p.destinations = append(p.destinations, op.Destinations...)
						if p.name == "" {
							p.name = op.Branch
						}
					}
				}
			}
		}
		var err error
		p.nodes, err = exclusiveSuffix(result.Graph.Nodes, terminals)
		if err != nil {
			return nil, err
		}
		p.name = firstNonEmpty(p.name, p.id)
		plans = append(plans, p)
	}
	return plans, nil
}

func (t *task) installIsolation(plans []isolationPlan, txs []*destinationTransaction, work []compile.WorkDestination, attachment *runtimeAttachment) ([]*isolatedBranch, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	graph, ok := t.graph.(pipeline.FaultIsolator)
	if !ok {
		return nil, unsafeIsolationError("graph does not implement isolated failure boundaries", nil)
	}
	var records []*isolatedBranch
	for _, p := range plans {
		domain, err := graph.IsolateFaults(p.id, p.nodes)
		if err != nil {
			return nil, unsafeIsolationError("graph rejected the failure boundary", err)
		}
		record := &isolatedBranch{plan: p, domain: domain, attachment: attachment}
		for _, tx := range txs {
			if slices.Contains(p.nodes, tx.node) {
				tx.mu.Lock()
				tx.domain = domain
				tx.mu.Unlock()
				record.txs = append(record.txs, tx)
			}
		}
		for _, destination := range work {
			if slices.Contains(p.destinations, destination.ID) {
				record.work = append(record.work, destination)
			}
		}
		record.spec = branchSpecForNodes(t.graph.Spec(), p.nodes)
		records = append(records, record)
	}
	return records, nil
}

func branchSpecForNodes(spec pipeline.Spec, nodes []pipeline.NodeRef) pipeline.Spec {
	out := pipeline.Spec{Name: spec.Name, Realtime: spec.Realtime, Execution: spec.Execution}
	for _, n := range spec.Nodes {
		if slices.Contains(nodes, pipeline.NodeRef(n.Name)) {
			out.Nodes = append(out.Nodes, n)
		}
	}
	for _, e := range spec.Edges {
		if slices.Contains(nodes, e.From) && slices.Contains(nodes, e.To) {
			out.Edges = append(out.Edges, e)
		}
	}
	return out
}

func (t *task) isolatedSnapshot(b *isolatedBranch, stats pipeline.GraphStats) BranchSnapshot {
	fault := b.domain.Snapshot()
	state := BranchAttached
	_, destState := t.lifecycleStates()
	if fault.Failed {
		state = BranchFailed
		destState = DestinationFinalizing
	}
	if !fault.Failed && b.attachment != nil && b.attachment.stopped {
		state = BranchDetached
	}
	if b.attachment != nil {
		if _, attached := t.attachments[b.attachment]; !attached {
			// Names may now identify another attachment. Only the old handle's
			// recorded counters belong to this retained failure record.
			stats = withRemovedStats(pipeline.GraphStats{}, b.attachment.removedStats.Load())
		}
	}
	s := BranchSnapshot{ID: b.plan.id, Name: b.plan.name, State: state, FailurePolicy: IsolateBranch, Err: fault.Err, Error: fault.Error, PendingCleanup: fault.PendingCleanup, Retired: fault.Retired, Nodes: fault.Nodes, Spec: clonePipelineSpec(b.spec), Stats: branchStatsForNodes(stats, fault.Nodes)}
	scope := ""
	if b.attachment != nil {
		scope = b.attachment.id
	}
	s.Destinations = t.destinationSnapshots(b.work, b.txs, destState, scope)
	return s
}

func (a *runtimeAttachment) faultOutcome() (int, bool, error) {
	var err error
	pending := 0
	retired := len(a.isolated) > 0
	covered := map[pipeline.NodeRef]bool{}
	for _, b := range a.isolated {
		s := b.domain.Snapshot()
		err = errors.Join(err, s.Err)
		pending += s.PendingCleanup
		retired = retired && s.Retired
	}
	for _, b := range a.isolated {
		for _, node := range b.plan.nodes {
			covered[node] = true
		}
	}
	retired = retired && (len(covered) == len(a.nodes) || a.stopped && a.owner.Stats().PendingCleanup == 0)
	return pending, retired, err
}
