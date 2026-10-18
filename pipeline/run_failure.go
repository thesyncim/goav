package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// runFailure is the error contract both runners share for one Run: the first
// error a source returns or a node's Handle reports is kept, cancels the run's
// context (when the run has one) so sibling sources and blocked producers stop
// promptly, and is what Run returns — never the context.Canceled it caused.
// Later errors are counted, not dropped: Run wraps the first with the number
// of independent errors that followed it. Echoes are not counted: the same
// error propagating back up a direct chain (errors.Is the first), or the
// context.Canceled/DeadlineExceeded the cancellation produces.
//
// It lives inside its runner and is reset per Run, so arming it allocates
// nothing. The data plane reads it through one atomic load (failed);
// everything else runs only once something has gone wrong.
type runFailure struct {
	failed atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	first  error
	more   int
}

// reset arms the record for a new run; cancel may be nil when the run has no
// context of its own to cancel (a single direct source sees the error from
// Emit synchronously instead).
func (f *runFailure) reset(cancel context.CancelFunc) {
	f.mu.Lock()
	f.cancel = cancel
	f.first = nil
	f.more = 0
	f.failed.Store(false)
	f.mu.Unlock()
}

// report records err (nil is ignored). The first report cancels the run.
func (f *runFailure) report(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	if f.first != nil {
		if !errors.Is(err, f.first) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			f.more++
		}
		f.mu.Unlock()
		return
	}
	f.first = err
	f.failed.Store(true)
	cancel := f.cancel
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// firstErr is the recorded first error, nil while the run is healthy.
func (f *runFailure) firstErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.first
}

// err is the run's result: nil, the first error, or the first error wrapped
// with how many independent errors followed it (errors.Is/As still match it).
func (f *runFailure) err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.first == nil || f.more == 0 {
		return f.first
	}
	return &runErrors{first: f.first, more: f.more}
}

// runErrors is a first error plus the count of errors reported after it.
type runErrors struct {
	first error
	more  int
}

func (e *runErrors) Error() string {
	return fmt.Sprintf("%v (and %d more errors)", e.first, e.more)
}

func (e *runErrors) Unwrap() error { return e.first }
