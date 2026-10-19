// The views a task reports about itself: point-in-time snapshots of the task,
// its runtime branches, its destinations, and its taps, plus the typed
// lifecycle states they carry. The states are reporting vocabulary only: they
// are derived from recorded run/close progress and add no lifecycle
// machinery.

package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// TaskSnapshot is an immutable point-in-time view of a task's graph, taps,
// active runtime branches, and counters, returned by LiveTask.Snapshot.
type TaskSnapshot struct {
	State TaskState `json:"state"`
	// Paused reports whether the task timeline is frozen by LiveTask.Pause. It
	// is orthogonal to State: a paused task still reports its run progress.
	Paused       bool                  `json:"paused,omitempty"`
	Spec         pipeline.Spec         `json:"spec"`
	Stats        pipeline.GraphStats   `json:"stats"`
	Taps         []TapSnapshot         `json:"taps,omitempty"`
	Branches     []BranchSnapshot      `json:"branches,omitempty"`
	Destinations []DestinationSnapshot `json:"destinations,omitempty"`
}

// BranchSnapshot is an immutable point-in-time view of one runtime branch
// attached to a running task, returned by Attachment.Snapshot and listed in
// TaskSnapshot.Branches.
type BranchSnapshot struct {
	ID           string                `json:"id"`
	Name         string                `json:"name"`
	State        BranchState           `json:"state"`
	AnchorTaps   []string              `json:"anchorTaps,omitempty"`
	AnchorNodes  []string              `json:"anchorNodes,omitempty"`
	Nodes        []pipeline.NodeRef    `json:"nodes,omitempty"`
	Taps         []TapSnapshot         `json:"taps,omitempty"`
	Destinations []DestinationSnapshot `json:"destinations,omitempty"`
	Spec         pipeline.Spec         `json:"spec"`
	Stats        pipeline.GraphStats   `json:"stats"`
}

// DestinationSnapshot is an immutable point-in-time view of a planned task or
// branch destination.
type DestinationSnapshot struct {
	Name      string             `json:"name"`
	Operation plan.OperationKind `json:"operation,omitempty"`
	Component string             `json:"component,omitempty"`
	Format    av.FormatID        `json:"format,omitempty"`
	Branches  []string           `json:"branches,omitempty"`
	State     DestinationState   `json:"state"`
	// Open mirrors State == DestinationOpen.
	Open bool `json:"open"`
}

// TapSnapshot describes one named media outlet a task exposes for runtime
// branches, as LiveTask.Taps lists it.
type TapSnapshot struct {
	Name      string             `json:"name"`
	MediaKind av.MediaType       `json:"mediaKind,omitempty"`
	Domain    shape.MediaDomain  `json:"domain,omitempty"`
	After     plan.OperationKind `json:"after,omitempty"`
	Shape     shape.Spec         `json:"shape,omitzero"`
	Node      pipeline.NodeRef   `json:"node,omitempty"`
}

// TaskState is the lifecycle state a TaskSnapshot reports.
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

// BranchState is the lifecycle state a BranchSnapshot reports.
type BranchState string

const (
	// BranchAttached reports a runtime branch that is attached to its task.
	BranchAttached BranchState = "attached"
	// BranchDetached reports a runtime branch that was detached from its task.
	BranchDetached BranchState = "detached"
)

// DestinationState is the lifecycle state a DestinationSnapshot reports. It is
// also the contract external destinations are reported through: a custom
// provider.TransactionalWriter surfaces as DestinationOpen while accepting
// media, DestinationCommitted after a run that completed without error
// (Commit), DestinationAborted after a failed run (Abort), and
// DestinationClosed when closed without a run outcome.
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
