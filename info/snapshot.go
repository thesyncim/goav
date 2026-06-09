package info

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Tap describes one named media outlet a task exposes for runtime branches.
type Tap struct {
	Name      string
	MediaKind av.MediaType
	Domain    shape.MediaDomain
	After     OperationKind
	Shape     shape.Spec
	Node      pipeline.NodeRef
}

// TaskSnapshot is an immutable point-in-time view of a task's graph, taps,
// active runtime branches, and counters.
type TaskSnapshot struct {
	State        TaskState
	Spec         pipeline.Spec
	Stats        pipeline.GraphStats
	Taps         []Tap
	Branches     []BranchSnapshot
	Destinations []DestinationSnapshot
}

// BranchSnapshot is an immutable point-in-time view of one runtime branch
// attached to a running task.
type BranchSnapshot struct {
	ID           string
	Name         string
	State        BranchState
	AnchorTaps   []string
	AnchorNodes  []string
	Nodes        []pipeline.NodeRef
	Taps         []Tap
	Destinations []DestinationSnapshot
	Spec         pipeline.Spec
	Stats        pipeline.GraphStats
}

// DestinationSnapshot is an immutable point-in-time view of a planned task or
// branch destination.
type DestinationSnapshot struct {
	Name      string
	Operation OperationKind
	Component string
	Format    av.FormatID
	Branches  []string
	State     DestinationState
	// Open is derived from State and kept for compatibility: it mirrors
	// State == DestinationOpen.
	Open bool
}
