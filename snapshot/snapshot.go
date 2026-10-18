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
	Name      string             `json:"name"`
	MediaKind av.MediaType       `json:"mediaKind,omitempty"`
	Domain    shape.MediaDomain  `json:"domain,omitempty"`
	After     plan.OperationKind `json:"after,omitempty"`
	Shape     shape.Spec         `json:"shape,omitzero"`
	Node      pipeline.NodeRef   `json:"node,omitempty"`
}

// Task is an immutable point-in-time view of a task's graph, taps, active
// runtime branches, and counters.
type Task struct {
	State lifecycle.TaskState `json:"state"`
	// Paused reports whether the task timeline is frozen by Task.Pause. It is
	// orthogonal to State: a paused task still reports its run progress.
	Paused       bool                `json:"paused,omitempty"`
	Spec         pipeline.Spec       `json:"spec"`
	Stats        pipeline.GraphStats `json:"stats"`
	Taps         []Tap               `json:"taps,omitempty"`
	Branches     []Branch            `json:"branches,omitempty"`
	Destinations []Destination       `json:"destinations,omitempty"`
}

// Branch is an immutable point-in-time view of one runtime branch attached
// to a running task.
type Branch struct {
	ID           string                `json:"id"`
	Name         string                `json:"name"`
	State        lifecycle.BranchState `json:"state"`
	AnchorTaps   []string              `json:"anchorTaps,omitempty"`
	AnchorNodes  []string              `json:"anchorNodes,omitempty"`
	Nodes        []pipeline.NodeRef    `json:"nodes,omitempty"`
	Taps         []Tap                 `json:"taps,omitempty"`
	Destinations []Destination         `json:"destinations,omitempty"`
	Spec         pipeline.Spec         `json:"spec"`
	Stats        pipeline.GraphStats   `json:"stats"`
}

// Destination is an immutable point-in-time view of a planned task or branch
// destination.
type Destination struct {
	Name      string                     `json:"name"`
	Operation plan.OperationKind         `json:"operation,omitempty"`
	Component string                     `json:"component,omitempty"`
	Format    av.FormatID                `json:"format,omitempty"`
	Branches  []string                   `json:"branches,omitempty"`
	State     lifecycle.DestinationState `json:"state"`
	// Open is derived from State and kept for compatibility: it mirrors
	// State == lifecycle.DestinationOpen.
	Open bool `json:"open"`
}
