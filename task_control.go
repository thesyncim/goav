package goav

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
)

// Control injects an out-of-band control into the running task's graph, delivering
// it to the node named by ctrl.Node() on that node's serial worker. It is safe to
// call concurrently with Run: the control rides the target node's normal queue, so
// the node's Handle still sees one message at a time and needs no extra locking.
// Reposition controls (seek, segment) are the exception: sources have no queue,
// so they are handed to each source's Control method synchronously — and once a
// source accepted one, the stale media queued downstream of it is flushed
// (flushSeekBacklog) so a seek repositions promptly instead of draining the
// old position's backlog to sinks first. Rate is task-wide: it scales the
// task's shared timeline, which every paced consumer (clock-aware sources and
// playout gates) sleeps on, and never targets a node.
func (t *task) Control(ctx context.Context, ctrl control.Control) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	msg, err := ctrl.Message()
	if err != nil {
		return err
	}
	if msg == nil || msg.Event == nil {
		return control.ErrNil
	}
	if ctrl.Type() == control.RateType {
		return t.controlRate(ctrl)
	}
	targets, err := t.controlTargets(ctrl)
	if err != nil {
		return err
	}
	deliver, err := t.controlDeliver(ctrl)
	if err != nil {
		return err
	}
	var errs []error
	for _, node := range targets {
		if ctrl.TargetsSources() {
			// Probe, flush, then deliver — see flushSeekBacklog for why this
			// order is the one that never sheds post-seek media.
			if err := t.probeSourceControl(node); err != nil {
				if errors.Is(err, pipeline.ErrDynamicGraphUnsupported) {
					return control.ErrNotRunning
				}
				if errors.Is(err, pipeline.ErrInvalidLink) {
					errs = append(errs, fmt.Errorf("goav: control to %q: source does not accept live control (implement pipeline.ControllableSource): %w", node, err))
					continue
				}
				errs = append(errs, fmt.Errorf("goav: control to %q: %w", node, err))
				continue
			}
			if err := t.flushSeekBacklog(node); err != nil {
				errs = append(errs, fmt.Errorf("goav: flush before %s control to %q: %w", ctrl.Type(), node, err))
			}
		}
		if err := deliver(ctx, node, msg); err != nil {
			if errors.Is(err, pipeline.ErrDynamicGraphUnsupported) {
				return control.ErrNotRunning
			}
			if ctrl.TargetsSources() && errors.Is(err, pipeline.ErrInvalidLink) {
				errs = append(errs, fmt.Errorf("goav: control to %q: source does not accept live control (implement pipeline.ControllableSource): %w", node, err))
				continue
			}
			errs = append(errs, fmt.Errorf("goav: control to %q: %w", node, err))
			continue
		}
	}
	return errors.Join(errs...)
}

// probeSourceControl asks the graph whether a source control would be
// accepted, without delivering one. Graphs without the capability (the direct
// runner) probe as accepted: they also have nothing to flush, so ordering is
// moot and delivery reports the real refusal.
func (t *task) probeSourceControl(node pipeline.NodeRef) error {
	prober, ok := t.graph.(interface {
		CanControlSource(pipeline.NodeRef) error
	})
	if !ok {
		return nil
	}
	return prober.CanControlSource(node)
}

// flushSeekBacklog is the flush half of a reposition control: the media queued
// downstream of a seeking source is backlog from the old position and would
// otherwise drain to the sinks first. The graph's structural flush capability
// (buffered runner only) empties those queues, shedding packets/frames under
// pipeline.DropFlush and preserving queued events; a graph without the
// capability (direct runner) buffers nothing, so there is nothing to flush and
// seek behaves as before.
//
// The probe -> flush -> deliver ordering closes the reposition race by
// construction:
//   - Probe first: a refused control (unknown source, no ControllableSource)
//     must not cost queued media, so nothing is flushed unless delivery would
//     be accepted — CanControlSource runs the same validation delivery runs.
//   - Flush before deliver: the source cannot apply a seek it has not received,
//     so nothing the flush sheds can ever be post-seek media. Flushing after
//     delivery had a real hole: a fast pump could reposition, emit its
//     discontinuity, and have it consumed by the sink before the flush ran —
//     leaving post-seek media alone in the queue for the flush to shed.
//   - What the pump emits between the flush and the seek applying is a bounded
//     pre-seek tail (at most the queue refill), and it precedes the
//     av.EventDiscontinuity the source emits at the new position, so once the
//     discontinuity reaches a sink no pre-seek media follows it (queues are
//     FIFO and the flush never drops events). The flush's stop-at-discontinuity
//     rule remains as a belt for repeated seeks landing close together.
func (t *task) flushSeekBacklog(node pipeline.NodeRef) error {
	flusher, ok := t.graph.(interface {
		FlushDownstream(pipeline.NodeRef) error
	})
	if !ok {
		return nil
	}
	return flusher.FlushDownstream(node)
}

// controlRate applies a task-wide playback-rate change: it re-anchors the
// task's shared timeline, so every paced consumer changes pace together —
// mid-sleep included, playout gates included. It refuses targets (rate has no
// per-node meaning) and shares the timeline-control refusal gate with
// Pause/Resume: tasks with nothing paced to scale (an offline task pumps as
// fast as the graph drains) keep the format.ErrRateUnsupported contract, and
// closed tasks refuse with control.ErrNotRunning.
func (t *task) controlRate(ctrl control.Control) error {
	if ctrl.Node() != "" || ctrl.Tap() != "" {
		return fmt.Errorf("goav: rate is task-wide: it scales the shared task timeline for every paced consumer; send control.Rate without At/AtTap: %w", control.ErrInvalid)
	}
	if err := t.timelineControlRefusal("rate control"); err != nil {
		return err
	}
	return t.timeline.SetRate(ctrl.Rate())
}

// controlDeliver picks the delivery seam for a control. Reposition controls
// target source nodes, which have no queue or serial worker, so they go through
// the graph's SourceInjector. Everything else rides the target node's queue
// through NodeInjector.
func (t *task) controlDeliver(ctrl control.Control) (func(context.Context, pipeline.NodeRef, *pipeline.Message) error, error) {
	if ctrl.TargetsSources() {
		injector, ok := t.graph.(pipeline.SourceInjector)
		if !ok {
			return nil, control.ErrUnsupported
		}
		return injector.InjectSource, nil
	}
	injector, ok := t.graph.(pipeline.NodeInjector)
	if !ok {
		return nil, control.ErrUnsupported
	}
	return injector.Inject, nil
}

func (t *task) controlTargets(ctrl control.Control) ([]pipeline.NodeRef, error) {
	if ctrl.Node() != "" && ctrl.Tap() != "" {
		return nil, fmt.Errorf("goav: control targets both node %q and tap %q; choose exactly one target: %w", ctrl.Node(), ctrl.Tap(), control.ErrAmbiguousTarget)
	}
	if ctrl.Node() != "" {
		return []pipeline.NodeRef{ctrl.Node()}, nil
	}
	if ctrl.Tap() != "" {
		for _, tap := range t.Taps() {
			if tap.Name == ctrl.Tap() {
				return []pipeline.NodeRef{tap.Node}, nil
			}
		}
		return nil, fmt.Errorf("goav: control targets unknown tap %q: %w", ctrl.Tap(), pipeline.ErrUnknownNode)
	}
	if ctrl.Type() == control.KeyframeType || ctrl.Type() == control.BitrateType || ctrl.Type() == control.SelectType {
		targets := t.controlEntryNodes()
		return implicitControlTargets("entry", targets)
	}
	if ctrl.TargetsSources() {
		targets := t.controlSourceNodes()
		return implicitControlTargets("source", targets)
	}
	return nil, fmt.Errorf("goav: control needs a target: use AtTap(tap) or At(node): %w", pipeline.ErrUnknownNode)
}

func implicitControlTargets(kind string, targets []pipeline.NodeRef) ([]pipeline.NodeRef, error) {
	switch len(targets) {
	case 0:
		return nil, pipeline.ErrUnknownNode
	case 1:
		return targets, nil
	default:
		names := make([]string, 0, len(targets))
		for _, target := range targets {
			names = append(names, target.String())
		}
		return nil, fmt.Errorf("goav: control has %d implicit %s targets (%s); use AtTap(tap) or At(node): %w", len(targets), kind, strings.Join(names, ","), control.ErrAmbiguousTarget)
	}
}

func (t *task) controlSourceNodes() []pipeline.NodeRef {
	spec := t.Describe()
	var targets []pipeline.NodeRef
	for _, node := range spec.Nodes {
		if node.Kind == pipeline.NodeSource {
			targets = append(targets, pipeline.NodeRef(node.Name))
		}
	}
	return targets
}

func (t *task) controlEntryNodes() []pipeline.NodeRef {
	spec := t.Describe()
	sources := make(map[pipeline.NodeRef]struct{}, len(spec.Nodes))
	for _, node := range spec.Nodes {
		if node.Kind == pipeline.NodeSource {
			sources[pipeline.NodeRef(node.Name)] = struct{}{}
		}
	}
	seen := make(map[pipeline.NodeRef]struct{})
	var targets []pipeline.NodeRef
	for _, edge := range spec.Edges {
		if _, ok := sources[edge.From]; !ok {
			continue
		}
		if _, ok := seen[edge.To]; ok {
			continue
		}
		seen[edge.To] = struct{}{}
		targets = append(targets, edge.To)
	}
	return targets
}
