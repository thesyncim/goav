package goav

import (
	"errors"
	"sync"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func (t *Task) finishDestinations(runErr error) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	t.finishDestinationsLocked(runErr)
}

func (t *Task) finishDestinationsLocked(runErr error) {
	ok := runErr == nil
	for i := range t.destinations {
		if t.destinations[i] == nil {
			continue
		}
		if ok {
			t.destinations[i].Succeed()
		} else {
			t.destinations[i].Fail()
		}
	}
	// Runtime attachments belong to the same execution outcome until detached.
	for attachment := range t.activeAttachments {
		for _, tx := range attachment.transactions {
			if ok {
				tx.Succeed()
			} else {
				tx.Fail()
			}
		}
	}
}

// Only the graph's own shutdown sentinel is expected after Task.Close. Other
// source/handler failures, including siblings of ErrClosed in a joined error,
// remain execution failures. Caller cancellation is handled separately by Run.
func withoutShutdownError(err error) error {
	if err == nil || err == pipeline.ErrClosed {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var kept []error
		for _, child := range joined.Unwrap() {
			if child = withoutShutdownError(child); child != nil {
				kept = append(kept, child)
			}
		}
		return errors.Join(kept...)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok && wrapped.Unwrap() != nil && withoutShutdownError(wrapped.Unwrap()) == nil {
		return nil
	}
	return err
}

func (t *Task) publishDestinationLifecycleEvent(kind av.EventType, destination string, attachment *runtimeAttachment, cause error) {
	if t == nil || destination == "" {
		return
	}
	reason := "destination finalized"
	switch kind {
	case av.EventDestinationCommitted:
		reason = "destination committed"
	case av.EventDestinationAborted:
		reason = "destination aborted"
	case av.EventDestinationCommitError:
		reason = "destination finalization failed"
	}
	event := av.Event{
		Type:            kind,
		Reason:          reason,
		Cause:           cause,
		DestinationName: destination,
	}
	if attachment != nil {
		event.AttachmentID = attachment.ID()
		event.AttachmentName = attachment.Name()
	}
	t.watch.publish(event)
}

type txDecision uint8

const (
	txUndecided txDecision = iota
	txSucceeded
	txFailed
)

type destinationTransaction struct {
	mu             sync.Mutex
	name           string
	id             string
	node           pipeline.NodeRef
	domain         *pipeline.FaultDomain
	requireSuccess bool
	decision       txDecision
	notified       bool
	state          DestinationState
	err            error
	onFinalized    func(DestinationState, error)
	runError       func() error
}

func (t *destinationTransaction) Name() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.name
}

func (t *destinationTransaction) Succeed() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.state == "" && t.decision == txUndecided {
		t.decision = txSucceeded
	}
	t.mu.Unlock()
}

func (t *destinationTransaction) Fail() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.state == "" {
		t.decision = txFailed
	}
	t.mu.Unlock()
}

func (t *destinationTransaction) ShouldAbort() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failedLocked() || (t.requireSuccess && t.decision != txSucceeded)
}

func (t *destinationTransaction) Failed() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failedLocked()
}

// Called only under mu, on snapshot/finalization paths. The graph reports an
// error before Run finishes waiting for a slow source, so explicit branch
// success cannot commit work after an already-observed execution failure.
func (t *destinationTransaction) failedLocked() bool {
	return t.decision == txFailed || t.domain.Failed() || (t.runError != nil && t.runError() != nil)
}
