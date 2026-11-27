package goav

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func errorsJoinUnique(first, next error) error {
	if next == nil || errors.Is(first, next) {
		return first
	}
	if joined, ok := next.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			first = errorsJoinUnique(first, child)
		}
		return first
	}
	return errors.Join(first, next)
}

func taskGraphCleanupError(graph pipeline.Graph) error {
	return graph.CleanupError()
}

// closeCallError keeps wait failures separate from resource outcomes. The
// graph ledger already owns every real finalizer error, including reapers.
func closeCallError(err, cleanup error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var out error
		for _, child := range joined.Unwrap() {
			out = errorsJoinUnique(out, closeCallError(child, cleanup))
		}
		return out
	}
	if finalizationContains(cleanup, err) {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, pipeline.ErrCloseWait) {
		return err
	}
	return nil
}

func finalizationContains(ledger, candidate error) bool {
	if joined, ok := ledger.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if finalizationContains(child, candidate) {
				return true
			}
		}
	}
	if node, ok := ledger.(*pipeline.FinalizationError); ok {
		return errors.Is(candidate, node.Cause)
	}
	return false
}

// finalize records the actual resource finalization, including an eventual
// close after the initiating caller's timeout. The ledger outlives the node.
func (t *destinationTransaction) finalize(state DestinationState, err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.state != "" {
		t.mu.Unlock()
		return
	}
	if err != nil {
		state = DestinationFailed
	}
	t.state, t.err = state, err
	var callback func(DestinationState, error)
	if !t.notified && t.onFinalized != nil {
		t.notified = true
		callback = t.onFinalized
	}
	t.mu.Unlock()
	if callback != nil {
		callback(state, err)
	}
}

func (t *destinationTransaction) finalizeFromSinkClose(err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.state != "" {
		t.mu.Unlock()
		return
	}
	state := DestinationClosed
	if err != nil {
		state = DestinationFailed
	} else if !t.failedLocked() && t.decision == txSucceeded {
		state = DestinationCommitted
	}
	t.state, t.err = state, err
	var callback func(DestinationState, error)
	if !t.notified && t.onFinalized != nil {
		t.notified = true
		callback = t.onFinalized
	}
	t.mu.Unlock()
	if callback != nil {
		callback(state, err)
	}
}

func (t *destinationTransaction) outcome() (DestinationState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state, t.err
}

func (t *Task) bindDestination(tx *destinationTransaction, attachment *runtimeAttachment) {
	if tx == nil {
		return
	}
	tx.mu.Lock()
	if reporter, ok := t.graph.(pipeline.RunErrorReporter); ok {
		tx.runError = reporter.RunError
	}
	tx.onFinalized = func(state DestinationState, err error) {
		kind := av.EventDestinationCommitted
		switch state {
		case DestinationFailed:
			kind = av.EventDestinationCommitError
		case DestinationAborted:
			kind = av.EventDestinationAborted
		case DestinationClosed:
			kind = av.EventDestinationClosed
		}
		t.publishDestinationLifecycleEvent(kind, tx.Name(), attachment, err)
	}
	var callback func(DestinationState, error)
	state, err := tx.state, tx.err
	if state != "" && !tx.notified {
		tx.notified = true
		callback = tx.onFinalized
	}
	tx.mu.Unlock()
	if callback != nil {
		callback(state, err)
	}
}

func (t *Task) destinationSnapshots(work []compile.WorkDestination, txs []*destinationTransaction, fallback DestinationState, scope ...string) []DestinationSnapshot {
	out := destinationSnapshotsFromWork(work, fallback)
	for i := range out {
		key := work[i].ID
		if key == "" {
			key = work[i].Name
		}
		if len(scope) != 0 && scope[0] != "" {
			key = scope[0] + "/" + key
		}
		out[i].ID = key
		for _, tx := range txs {
			if tx == nil || tx.id != key {
				continue
			}
			state, err := tx.outcome()
			if state != "" {
				out[i].State = state
			} else if tx.Failed() {
				out[i].State = DestinationFinalizing
			}
			out[i].Err, out[i].Error = err, errorText(err)
			out[i].Open = out[i].State == DestinationOpen
			break
		}
	}
	return out
}

// trackedDestinationSink preserves the sink's graph-visible capabilities while
// recording its real Close result, independently of its siblings.
type trackedDestinationSink struct {
	namedSink
	transaction *destinationTransaction
}

func (s trackedDestinationSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	err := s.namedSink.Handle(ctx, msg)
	if err != nil {
		s.transaction.Fail()
	}
	return err
}

func (s trackedDestinationSink) Close() error {
	err := s.namedSink.Close()
	s.transaction.finalizeFromSinkClose(err)
	return err
}
