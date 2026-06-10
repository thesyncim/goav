// Package snapshot holds the immutable point-in-time views a task returns
// about itself: the task view, its runtime branches, its destinations, and
// the named taps runtime branches anchor on.
package snapshot

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// Tap describes one named media outlet a task exposes for runtime branches.
type Tap struct {
	Name      string
	MediaKind av.MediaType
	Domain    shape.MediaDomain
	After     plan.OperationKind
	Shape     shape.Spec
	Node      pipeline.NodeRef
}

// Task is an immutable point-in-time view of a task's graph, taps, active
// runtime branches, and counters.
type Task struct {
	State        lifecycle.TaskState
	Spec         pipeline.Spec
	Stats        pipeline.GraphStats
	Taps         []Tap
	Branches     []Branch
	Destinations []Destination
}

// Branch is an immutable point-in-time view of one runtime branch attached
// to a running task.
type Branch struct {
	ID           string
	Name         string
	State        lifecycle.BranchState
	AnchorTaps   []string
	AnchorNodes  []string
	Nodes        []pipeline.NodeRef
	Taps         []Tap
	Destinations []Destination
	Spec         pipeline.Spec
	Stats        pipeline.GraphStats
}

// Destination is an immutable point-in-time view of a planned task or branch
// destination.
type Destination struct {
	Name      string
	Operation plan.OperationKind
	Component string
	Format    av.FormatID
	Branches  []string
	State     lifecycle.DestinationState
	// Open is derived from State and kept for compatibility: it mirrors
	// State == lifecycle.DestinationOpen.
	Open bool
}
