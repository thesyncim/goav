// Package goav is a pure-Go realtime media grammar: describe media work once,
// then compile it with an explicit runtime.
//
// The grammar is small. From(input) selects streams (.Audio(), .Video());
// operations are methods on the chain (.Decode(), .Copy(), .Resize(),
// .Encode(codec.VP9(...))); .To(destination) ends a chain at a File, URI,
// Writer, Object, or Sink. Branches fan one stream point out, Mix, Composite,
// and Select converge N arms into one, Tap names attach points, Flow reuses
// operation lists, and Build(ctx) returns a Task — a running graph with
// events, snapshots, runtime Attach/Detach, and live controls.
//
// New(opts...) builds a bare runtime and returns option/configuration errors;
// MustNew(opts...) is the package-level setup shortcut. Import
// github.com/thesyncim/goav/bundle when the application wants the bundled pure-Go
// adapters. Build errors are structured: every refusal is a *BuildError
// carrying an errcode.Family, detailed errcode.Code, the failing operation,
// and concrete fixes.
package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/snapshot"
)

// Runtime is an intentionally opaque composition root for applications
// embedding goav. Build it with New or MustNew, or import
// github.com/thesyncim/goav/bundle for the bundled adapter set. Applications
// pass Runtime handles back to recipe builders; construction and adapter
// registration stay behind the runtime and bundle packages.
type Runtime = runtime

// Task is the minimal runnable media composition accepted by code that only
// needs lifecycle control.
type Task interface {
	// Run drives the graph until every source ends or ctx cancels; it returns
	// the first fatal node error. Media does not flow before Run.
	Run(context.Context) error
	// Close releases the task's resources and finalizes destinations; it is
	// idempotent and safe to call while Run is still in flight.
	Close() error
}

// ContextCloser is the opt-in lifecycle extension implemented by tasks that
// can bound shutdown work with a caller context. Task.Close remains the
// compatibility shortcut and calls CloseContext(context.Background()).
type ContextCloser interface {
	CloseContext(context.Context) error
}

// Explainer reports the planned workflow before any resource opens: inputs,
// branches, destinations, taps, shapes, planner decisions, and warnings.
type Explainer interface {
	Explain(context.Context) (plan.Report, error)
}

// Inspectable exposes graph and runtime state for diagnostics, rendering, and
// branch anchoring. It is deliberately separate from Task so simple runners do
// not need to promise inspection support.
type Inspectable interface {
	// Describe returns the structured graph spec — the same nodes and edges
	// Run executes, node for node. Rendering lives outside core (graphrender).
	Describe() pipeline.Spec
	// Taps lists the stable attach points runtime branches can anchor from.
	Taps() []snapshot.Tap
	// Snapshot returns a point-in-time diagnostic view without exposing graph handles.
	Snapshot() snapshot.Task
	// Stats returns per-node counters — packets, frames, drops by reason —
	// readable while the task runs (lock-free atomic reads).
	Stats() pipeline.GraphStats
}

// Mutable exposes runtime branch mutation.
type Mutable interface {
	// Attach adds late branches to the running graph from typed taps. One call
	// applies all branches atomically: later branches may anchor on taps
	// published by earlier ones, and a failure rolls the whole group back.
	Attach(context.Context, ...BranchSpec) (Attachment, error)
	// Detach removes a runtime branch and any dependent branches anchored on
	// its taps. By default destinations finalize as closed; DrainBranch or
	// AbortBranch records a commit/abort outcome for the detached branch.
	Detach(context.Context, Attachment, ...lifecycle.DetachOption) error
}

// Controllable exposes live out-of-band controls.
type Controllable interface {
	// Control injects an out-of-band control into the running graph, delivered to a
	// target node on its serial worker — the control-plane entry point for live
	// switching (a selector), keyframe requests, and flushes.
	Control(context.Context, control.Control) error
}

// Observable exposes task event streams.
type Observable interface {
	// Events is the single raw event channel. Once Watch is in use, subscribe
	// every consumer through Watch instead — both drain one underlying stream.
	Events() <-chan av.Event
	// Watch returns an independent, filtered subscription to the task's event
	// stream. Filters AND together; with zero filters every event is delivered.
	// Each subscription owns a buffered channel sized like the task's event
	// buffer: when a watcher falls behind and its buffer fills, new events are
	// dropped for that watcher only — the data plane and other watchers never
	// block on a slow consumer. Subscription channels close when the task closes
	// or Subscription.Close unsubscribes them. Watch and Events drain the same
	// underlying stream, so once Watch is used, subscribe every consumer through
	// Watch (an unfiltered Watch() is the Events equivalent) rather than reading
	// Events directly.
	Watch(filters ...inspect.EventFilter) inspect.Subscription
}

// LiveTask is the full task capability set produced by the built-in runtime.
// Accept this only when an API needs inspection, runtime mutation, controls, or
// events; otherwise accept Task.
type LiveTask interface {
	Task
	Explainer
	Inspectable
	Mutable
	Controllable
	Observable
}
