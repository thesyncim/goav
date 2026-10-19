// Package goav is a pure-Go realtime media grammar: describe media work as
// values, then run it with an explicit runtime.
//
// The grammar is small. An input's Video, Audio, or Stream method selects a
// Stream; operations are methods that return a new Stream (.Decode(),
// .Resize(), .Encode(codec.VP9(...))), so a Stream used twice fans out and the
// work before the fork runs once. Mix, Composite, Select, and Join converge
// Streams into one, Tap names attach points, and .To(destinations...) routes a
// Stream to destinations built by Write, URI, Writer, Custom, or Sink, making
// an Output. A runtime runs Outputs together: rt.Run(ctx, outputs...), or
// rt.Build for a LiveTask with events, snapshots, runtime Attach/Detach, and
// live controls.
//
// New(opts...) builds a bare runtime and returns option/configuration errors.
// Import github.com/thesyncim/goav/bundle when the application wants the
// bundled pure-Go adapters. Build errors are structured: every refusal is a
// *BuildError carrying an errcode.Family, detailed errcode.Code, the failing
// operation, and concrete fixes.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
)

// Task is the minimal runnable media composition accepted by code that only
// needs lifecycle control.
type Task interface {
	// Run drives the graph until every source ends or ctx is done. It returns
	// the first fatal node error; otherwise nil when every source ended, or
	// ctx.Err() when ctx stopped the task. Media does not flow before Run.
	Run(context.Context) error
	// Close releases the task's resources and finalizes destinations; it is
	// idempotent and safe to call while Run is still in flight.
	Close() error
}

// LiveTask is the full task capability set produced by the built-in runtime:
// Runtime.Build returns it for inspection, runtime mutation, controls, and
// event watches. Code that only runs and closes a task can accept Task.
type LiveTask interface {
	Task
	// Explain reports the planned workflow before any resource opens: inputs,
	// branches, destinations, taps, shapes, planner decisions, and warnings.
	Explain(context.Context) (plan.Report, error)
	// Describe returns the structured graph spec — the same nodes and edges
	// Run executes, node for node. Rendering lives outside core (graphrender).
	Describe() pipeline.Spec
	// Taps lists the stable attach points runtime branches can anchor from.
	Taps() []TapSnapshot
	// Snapshot returns a point-in-time diagnostic view without exposing graph handles.
	Snapshot() TaskSnapshot
	// Stats returns per-node counters — packets, frames, drops by reason —
	// readable while the task runs (lock-free atomic reads).
	Stats() pipeline.GraphStats
	// Tap is the stream at one of the task's taps; Outputs built from it are
	// added to the running task with Attach.
	Tap(name string) Stream
	// Track is a stream one of the task's inputs produces or will produce, for
	// Outputs Attach adds before that stream's first media arrives.
	Track(input InputSpec, stream av.Stream) Stream
	// Route is the stream a graph route carries, for Outputs Attach anchors on
	// an expert graph node: task.Route(node.Out().Route()).
	Route(route pipeline.Route) Stream
	// Attach adds Outputs to the running task, each fed from a task.Tap,
	// task.Track, or task.Route stream. One call applies them atomically: a later Output may
	// read a tap an earlier one declares, and a failure rolls the group back.
	Attach(context.Context, ...Output) (Attachment, error)
	// Detach removes a runtime branch and any dependent branches anchored on
	// its taps. By default destinations finalize as closed; DrainBranch or
	// AbortBranch records a commit/abort outcome for the detached branch.
	Detach(context.Context, Attachment, ...DetachOption) error
	// Control injects an out-of-band control into the running graph, delivered to a
	// target node on its serial worker — the control-plane entry point for live
	// switching (a selector), keyframe requests, and flushes.
	Control(context.Context, control.Control) error
	// Pause freezes the task timeline at its current reading: paced sources
	// stall on their next due wait and playout gates hold delivery, coherently,
	// until Resume. Pause pauses TIME, not the data plane — a free-running task
	// (no paced source, no playout gate) would not stall, so Pause refuses it
	// with format.ErrRateUnsupported exactly like control.Rate does. Pausing a
	// paused task is a no-op; pausing before Run is allowed (the timeline is
	// anchored at build); pausing a closed task is refused with
	// control.ErrNotRunning. Rate changes while paused are accepted and take
	// effect on Resume; seek/segment controls are accepted while paused and
	// apply at the source's next read boundary.
	Pause(context.Context) error
	// Resume continues a paused task timeline from its frozen reading at the
	// current rate — no pause gap enters media time. Resuming a running task is
	// a no-op; the refusal contract mirrors Pause.
	Resume(context.Context) error
	// Watch returns an independent, filtered subscription to the task's event
	// stream. Filters AND together; with zero filters every event is delivered.
	// Each subscription owns a buffered channel sized like the task's event
	// buffer: when a watcher falls behind and its buffer fills, new events are
	// dropped for that watcher only — the data plane and other watchers never
	// block on a slow consumer. Subscription channels close when the task closes
	// or Subscription.Close unsubscribes them. Use an unfiltered Watch() when
	// every event should be delivered.
	Watch(filters ...EventFilter) Subscription
}
