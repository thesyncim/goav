package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
)

// ErrUnsafeFaultDomain reports a boundary with shared egress, a source, a
// converging admission gate, or overlapping membership.
var ErrUnsafeFaultDomain = errors.New("pipeline: unsafe fault domain")

// FaultIsolator is the graph capability for isolating returned handler errors.
// Members must form an exclusive, downstream-closed suffix. A failure stops
// delivery immediately and retires the members asynchronously.
type FaultIsolator interface {
	IsolateFaults(id string, members []NodeRef) (*FaultDomain, error)
}

// FaultError identifies the first handler failure within an isolated domain.
// It never becomes the task's Run error; cleanup failures remain graph errors.
type FaultError struct {
	Domain string
	Node   NodeRef
	Cause  error
}

func (e *FaultError) Error() string {
	return fmt.Sprintf("pipeline: isolated branch %q at %q: %v", e.Domain, e.Node, e.Cause)
}
func (e *FaultError) Unwrap() error { return e.Cause }

type faultContext struct {
	context.Context
	parent context.Context
	cancel context.CancelFunc
}

// FaultDomain is a durable failure record, retained after its nodes retire.
// Its methods are safe concurrently with delivery, retirement, and Close.
type FaultDomain struct {
	id        string
	members   []NodeRef
	failure   atomic.Pointer[FaultError]
	remaining atomic.Int64
	ctx       atomic.Pointer[faultContext]
	done      chan struct{}
	cleanup   atomic.Pointer[domainCleanup]
}
type domainCleanup struct{ err error }

// FaultSnapshot is an immutable view of a failure boundary.
type FaultSnapshot struct {
	ID             string    `json:"id"`
	Nodes          []NodeRef `json:"nodes"`
	Err            error     `json:"-"`
	Error          string    `json:"error,omitempty"`
	Failed         bool      `json:"failed"`
	Retired        bool      `json:"retired"`
	PendingCleanup int       `json:"pendingCleanup,omitempty"`
	CleanupErr     error     `json:"-"`
	CleanupError   string    `json:"cleanupError,omitempty"`
}

// Failed reports whether the boundary rejected a handler error.
func (d *FaultDomain) Failed() bool { return d != nil && d.failure.Load() != nil }

// Snapshot retains the original failure even after resources are reclaimed.
func (d *FaultDomain) Snapshot() FaultSnapshot {
	failure := d.failure.Load()
	s := FaultSnapshot{ID: d.id, Nodes: append([]NodeRef(nil), d.members...), Failed: failure != nil, Retired: d.remaining.Load() == 0}
	if failure != nil {
		s.Err, s.Error = failure, failure.Error()
	}
	if s.Failed {
		s.PendingCleanup = int(d.remaining.Load())
	}
	if cleanup := d.cleanup.Load(); cleanup != nil {
		s.CleanupErr = cleanup.err
		s.CleanupError = cleanup.err.Error()
	}
	return s
}

// Wait waits for every member's finalizer, including asynchronous retirement.
// A caller's context error is local to the wait; actual cleanup failures persist.
func (d *FaultDomain) Wait(ctx context.Context) error {
	select {
	case <-d.done:
		if cleanup := d.cleanup.Load(); cleanup != nil {
			return cleanup.err
		}
		return nil
	case <-ctx.Done():
		if cleanup := d.cleanup.Load(); cleanup != nil {
			return errors.Join(cleanup.err, ctx.Err())
		}
		return ctx.Err()
	}
}

// finalized runs under the owning topology's lock, once per member.
func (d *FaultDomain) finalized(err error) bool {
	if err != nil {
		if previous := d.cleanup.Load(); previous != nil {
			err = errors.Join(previous.err, err)
		}
		d.cleanup.Store(&domainCleanup{err: err})
	}
	if d.remaining.Add(-1) != 0 {
		return false
	}
	if dc := d.ctx.Load(); dc != nil {
		dc.cancel()
	}
	close(d.done)
	return true
}

func (d *FaultDomain) setContext(parent context.Context) {
	if d.Failed() {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	_ = ctx.Done() // allocate on the control plane, never per message
	prior := d.ctx.Swap(&faultContext{Context: ctx, parent: parent, cancel: cancel})
	if prior != nil {
		prior.cancel()
	}
	if d.Failed() {
		cancel()
	}
}

func (b *nodeBase) faulted() bool { return b.domain.Load().Failed() }
func (b *nodeBase) handlerContext(ctx context.Context) (context.Context, func()) {
	if d := b.domain.Load(); d != nil {
		if dc := d.ctx.Load(); dc != nil {
			if reflect.TypeOf(ctx).Comparable() && (ctx == dc.parent || ctx == dc.Context) {
				return dc.Context, nil
			}
			// An explicit per-delivery context keeps its values and deadline.
			// The ordinary Run context uses the preallocated fast path above.
			merged, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(dc.Context, cancel)
			return merged, func() { stop(); cancel() }
		}
	}
	return ctx, nil
}

func (t *topology[P]) startFaultDomains(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.domainParent = ctx
	for _, d := range t.domains {
		d.setContext(ctx)
	}
}

func (t *topology[P]) IsolateFaults(id string, members []NodeRef) (*FaultDomain, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	if id == "" || len(members) == 0 {
		return nil, ErrUnsafeFaultDomain
	}
	for _, d := range t.domains {
		if d.id == id {
			return nil, fmt.Errorf("%w: duplicate domain %q", ErrUnsafeFaultDomain, id)
		}
	}
	set := make(map[int]bool, len(members))
	for _, ref := range members {
		i, ok := t.index[ref.String()]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownNode, ref)
		}
		b := t.nodes[i].base()
		if !b.active || b.kind == nodeSource || b.admit != nil || b.domain.Load() != nil || set[i] {
			return nil, fmt.Errorf("%w: %s", ErrUnsafeFaultDomain, ref)
		}
		set[i] = true
	}
	for i, n := range t.nodes {
		for _, route := range n.base().routes {
			for _, to := range route.to {
				if set[i] && !set[to] || (set[i] || set[to]) && (n.base().admit != nil || t.nodes[to].base().admit != nil) {
					return nil, ErrUnsafeFaultDomain
				}
			}
		}
	}
	d := &FaultDomain{id: id, members: append([]NodeRef(nil), members...), done: make(chan struct{})}
	d.remaining.Store(int64(len(members)))
	parent := t.domainParent
	if parent == nil {
		parent = context.Background()
	}
	d.setContext(parent)
	t.domains = append(t.domains, d)
	for i := range set {
		t.nodes[i].base().domain.Store(d)
	}
	return d, nil
}

// nodeError is called before an error can unwind into a shared producer.
func (t *topology[P]) nodeError(ctx context.Context, b *nodeBase, err error) error {
	if err == nil {
		return nil
	}
	// Shutdown is a graph control operation, not a handler failure boundary.
	if errors.Is(err, ErrClosed) && t.snap.Load() == nil {
		return err
	}
	d := b.domain.Load()
	if d == nil {
		t.fail.report(err)
		return err
	}
	if d.Failed() {
		return nil
	}
	if ctx.Err() != nil {
		t.fail.report(err)
		return err
	}
	failure := &FaultError{Domain: d.id, Node: NodeRef(b.name), Cause: err}
	if !d.failure.CompareAndSwap(nil, failure) {
		return nil
	}
	if dc := d.ctx.Load(); dc != nil {
		dc.cancel()
	}
	t.observe(av.Event{Type: av.EventBranchFailed, Cause: failure, Reason: d.id, Metadata: av.Metadata{"branch": d.id, "node": b.name}}, nil)
	go t.retireFaultDomain(d)
	return nil
}

// Retirement never runs on a handler's stack. Tombstones are already visible
// to old routing snapshots; stopLocked wakes producers parked on full queues.
func (t *topology[P]) retireFaultDomain(d *FaultDomain) {
	t.mu.Lock()
	var nodes []P
	set := map[int]bool{}
	for i, n := range t.nodes {
		b := n.base()
		if b.domain.Load() != d || b.finalized || b.index < 0 {
			continue
		}
		set[i] = true
		delete(t.index, b.name)
		b.active, b.retiring, b.deferred = false, true, true
		b.routes = nil
		if b.kind == nodeSink {
			t.noteSinkMedia(&b.sawMedia)
		}
		_, _ = t.strategy.stopLocked(context.Background(), n)
		nodes = append(nodes, n)
	}
	for _, n := range t.nodes {
		b := n.base()
		routes := b.routes[:0]
		for _, route := range b.routes {
			to := route.to[:0]
			for _, i := range route.to {
				if !set[i] {
					to = append(to, i)
				}
			}
			if len(to) != 0 {
				route.to = to
				routes = append(routes, route)
			}
		}
		b.routes = routes
	}
	t.rebuildLocked()
	t.mu.Unlock()
	// Nodes are installed in dependency order. Waiting upstream first lets
	// emitters release downstream gates before their targets are finalized.
	for _, n := range nodes {
		if err := t.strategy.awaitStopped(context.Background(), n, t.config.CloseWaitTimeout); err != nil {
			t.reap(n)
			continue
		}
		_ = t.finalize(n)
	}
}
