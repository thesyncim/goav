// Package lifecycle holds the typed lifecycle states a task reports about
// itself, its runtime branches, and its destinations. The states are
// reporting vocabulary only — they are derived from recorded run/close
// progress and introduce no new lifecycle machinery.
//
// These states are also the contract external destination implementations
// are reported through: a custom provider.TransactionalWriter surfaces as
// DestinationOpen while accepting media, DestinationCommitted after a run
// that completed without error (Commit), DestinationAborted after a failed
// run (Abort), and DestinationClosed when closed without a run outcome.
package lifecycle

// TaskState is the typed lifecycle state a snapshot reports for a task.
type TaskState string

const (
	// TaskBuilt reports a task that was built but whose Run has not started.
	TaskBuilt TaskState = "built"
	// TaskRunning reports a task whose Run is in progress.
	TaskRunning TaskState = "running"
	// TaskClosed reports a task whose run completed without error or that was
	// closed before running.
	TaskClosed TaskState = "closed"
	// TaskFailed reports a task whose run ended with a terminal error.
	TaskFailed TaskState = "failed"
)

// BranchState is the typed lifecycle state a snapshot reports for a runtime
// branch. The string values match the states snapshots have always reported,
// so comparisons against the literal strings keep working.
type BranchState string

const (
	// BranchAttached reports a runtime branch that is attached to its task.
	BranchAttached BranchState = "attached"
	// BranchDetached reports a runtime branch that was detached from its task.
	BranchDetached BranchState = "detached"
)

// DestinationState is the typed lifecycle state a snapshot reports for a
// planned task or branch destination.
type DestinationState string

const (
	// DestinationOpen reports a destination that is open and accepting media.
	DestinationOpen DestinationState = "open"
	// DestinationCommitted reports a destination finalized by a run that
	// completed without error.
	DestinationCommitted DestinationState = "committed"
	// DestinationAborted reports a destination abandoned by a run that ended
	// with an error.
	DestinationAborted DestinationState = "aborted"
	// DestinationClosed reports a destination closed without a run outcome,
	// such as a detached branch destination or a task closed before running.
	DestinationClosed DestinationState = "closed"
)
