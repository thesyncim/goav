// Package goav exposes the top-level runtime contracts for composing media
// inputs, chains, taps, branches, destinations, and tasks.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/snapshot"
)

type Packet = av.Packet
type Frame = av.Frame
type Event = av.Event
type Stream = av.Stream

// Runtime is the composition root for applications embedding goav.
type Runtime interface {
	Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error)
}

// GraphBuilder is the handle-based expert graph layer. Most applications should
// start with From and compose chains, taps, branches, destinations, and tasks.
type GraphBuilder interface {
	Source(string, pipeline.Source) GraphNode
	Stage(string, pipeline.Stage) GraphNode
	Sink(string, pipeline.Sink) GraphNode
	Connect(GraphOutlet, ...GraphInlet) GraphBuilder
	Describe() (pipeline.Spec, error)
	Build(context.Context) (Task, error)
}

// Task is a runnable media composition.
type Task interface {
	Describe() pipeline.Spec
	Explain(context.Context) (plan.Report, error)
	Attach(context.Context, ...BranchSpec) (Attachment, error)
	Detach(context.Context, Attachment) error
	Taps() []snapshot.Tap
	// Snapshot returns a point-in-time diagnostic view without exposing graph handles.
	Snapshot() snapshot.Task
	// Control injects an out-of-band control into the running graph, delivered to a
	// target node on its serial worker — the control-plane entry point for live
	// switching (a selector), keyframe requests, and flushes.
	Control(context.Context, Control) error
	Run(context.Context) error
	Events() <-chan av.Event
	// Watch returns an independent, filtered subscription to the task's event
	// stream. Filters AND together; with zero filters every event is delivered.
	// Each watcher owns a buffered channel sized like the task's event buffer:
	// when a watcher falls behind and its buffer fills, new events are dropped
	// for that watcher only — the data plane and other watchers never block on
	// a slow consumer. Watcher channels close when the task closes. Watch and
	// Events drain the same underlying stream, so once Watch is used, subscribe
	// every consumer through Watch (an unfiltered Watch() is the Events
	// equivalent) rather than reading Events directly.
	Watch(filters ...EventFilter) <-chan av.Event
	Stats() pipeline.GraphStats
	Close() error
}
