package pipeline

import (
	"errors"
	"fmt"
	"time"
)

// ErrCloseWait reports that graph Close or Remove waited too long for a node to
// drain in-flight work.
var ErrCloseWait = errors.New("pipeline: close wait stuck")

// CloseWaitError identifies the node that kept a Close or Remove drain from
// completing before the diagnostic timeout.
type CloseWaitError struct {
	Operation string
	Node      string
	Pending   int64
	Timeout   time.Duration
}

// Error renders the stuck close/remove wait with the operation, node, timeout,
// and any known pending delivery count.
func (e *CloseWaitError) Error() string {
	if e == nil {
		return ""
	}
	if e.Pending > 0 {
		return fmt.Sprintf("pipeline: %s waiting for node %q stuck after %s (%d pending deliveries)", e.Operation, e.Node, e.Timeout, e.Pending)
	}
	return fmt.Sprintf("pipeline: %s waiting for node %q stuck after %s", e.Operation, e.Node, e.Timeout)
}

// Unwrap lets callers match CloseWaitError with errors.Is(err, ErrCloseWait).
func (e *CloseWaitError) Unwrap() error {
	return ErrCloseWait
}

var closeWaitTimeout = 30 * time.Second

func waitCloseDone(operation string, node string, done <-chan struct{}) error {
	if done == nil {
		return nil
	}
	if closeWaitTimeout <= 0 {
		<-done
		return nil
	}
	timer := time.NewTimer(closeWaitTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return &CloseWaitError{Operation: operation, Node: node, Pending: 1, Timeout: closeWaitTimeout}
	}
}
