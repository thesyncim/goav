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
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// sinkAccepts is the shapes a sink destination declares, or nil.
func sinkAccepts(sink pipeline.Sink) shape.Set {
	if accepter, ok := sink.(provider.InputShaper); ok {
		return accepter.InputShapes().Clone()
	}
	return nil
}

// openMediaSink binds the sink's control path and hands it its streams.
// opened reports that the sink holds resources goav must release by closing
// it, whether or not its node joins the graph.
func openMediaSink(ctx context.Context, sink pipeline.Sink, streams []av.Stream, controls func(context.Context, control.Control) error) (opened bool, err error) {
	if user, ok := sink.(provider.ControlUser); ok && controls != nil {
		user.UseControl(controls)
	}
	opener, ok := sink.(provider.StreamOpener)
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
	task atomic.Pointer[Task]
}

func (c *taskControls) attach(t *Task) {
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

// liveSink aliases provider.LiveReporter for media destinations that consume
// in real time (RTP out, WHIP, a WebRTC track).
type liveSink = provider.LiveReporter

// destinationsAreLive reports whether a destination consumes in real time.
func destinationsAreLive(dests []Destination) bool {
	for i := range dests {
		if live, ok := dests[i].spec.sink.(liveSink); ok && live.Live() {
			return true
		}
	}
	return false
}
