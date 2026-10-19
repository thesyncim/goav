// Media destinations: the optional capabilities of a pipeline.Sink
// destination, bound when its node opens.

package goav

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// streamOpener is a media destination that learns the streams routed to it,
// codec parameters and extradata included, before its first message: an
// RTP or WHIP sender sets up one track per stream.
type streamOpener interface {
	OpenStreams(context.Context, []av.Stream) error
}

// controlUser is a media destination that drives the task's control path:
// a receiver's keyframe request or bandwidth estimate becomes a control on
// the encoder feeding the stream.
type controlUser interface {
	UseControl(func(context.Context, control.Control) error)
}

// shapeAccepter is a media destination that declares what it accepts, in
// preference order; the planner encodes frames to its first codec with a
// registered encoder and refuses packets it cannot take.
type shapeAccepter interface {
	InputShapes() shape.Set
}

// sinkAccepts is the shapes a sink destination declares, or nil.
func sinkAccepts(sink pipeline.Sink) shape.Set {
	if accepter, ok := sink.(shapeAccepter); ok {
		return accepter.InputShapes().Clone()
	}
	return nil
}

// openMediaSink binds the sink's control path and hands it its streams.
// opened reports that the sink holds resources goav must release by closing
// it, whether or not its node joins the graph.
func openMediaSink(ctx context.Context, sink pipeline.Sink, streams []av.Stream, controls func(context.Context, control.Control) error) (opened bool, err error) {
	if user, ok := sink.(controlUser); ok && controls != nil {
		user.UseControl(controls)
	}
	opener, ok := sink.(streamOpener)
	if !ok {
		return false, nil
	}
	if err := opener.OpenStreams(ctx, cloneStreams(streams)); err != nil {
		return false, err
	}
	return true, nil
}

// taskControls is the late-binding seam between media sinks opened at
// lowering time, before the task exists, and the task's control path: the
// build threads one through the runtime clone, and the finished task
// attaches itself, as it does for QoS reports.
type taskControls struct {
	task atomic.Pointer[task]
}

func (c *taskControls) attach(t *task) {
	if c != nil && t != nil {
		c.task.Store(t)
	}
}

func (c *taskControls) control(ctx context.Context, ctrl control.Control) error {
	t := c.task.Load()
	if t == nil {
		return control.ErrNotRunning
	}
	return t.Control(ctx, ctrl)
}

// controlFunc is the runtime's control seam as a bindable func, nil when the
// runtime builds no task (explicit graphs).
func (r *Runtime) controlFunc() func(context.Context, control.Control) error {
	if r == nil || r.controls == nil {
		return nil
	}
	return r.controls.control
}

// validateMediaSinkStreams refuses, with the fix, a stream a packet-only
// media sink does not take whose codec only showed when its input opened (a
// file the planner could not probe for streams), before the sink opens.
func validateMediaSinkStreams(node string, destination destinationSpec, streams []av.Stream, remux bool) error {
	facts := compileDestinationFromSpec(destination)
	if len(facts.Accepts) == 0 || slices.ContainsFunc(facts.Accepts, func(spec shape.Spec) bool {
		return spec.Domain == "" || spec.Domain == shape.DomainFrame
	}) {
		return nil
	}
	if remux {
		return publicError(compile.ValidateSinkRemux("build stream", node, facts.Label, facts, streams, shape.DomainPacket))
	}
	for _, stream := range streams {
		if err := publicError(compile.ValidateSinkShape("build stream", node, facts.Label, facts, shape.FromStream(stream, shape.DomainPacket))); err != nil {
			return err
		}
	}
	return nil
}
