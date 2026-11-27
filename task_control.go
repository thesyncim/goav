package goav

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

// Control injects an out-of-band control into the running task's graph, delivering
// it to the node named by ctrl.Node() on that node's serial worker. It is safe to
// call concurrently with Run: the control rides the target node's normal queue, so
// the node's Handle still sees one message at a time and needs no extra locking.
// Reposition controls (seek, segment) are the exception: sources have no queue,
// so they are handed to each source's Control method synchronously — and once a
// source can accept one, stale media in its exclusive downstream queues is
// flushed (flushSeekBacklog) before delivering the control. Shared queues keep
// their admitted tail so another source's media cannot be lost. Rate is task-wide: it scales the
// task's shared timeline, which every paced consumer (clock-aware sources and
// playout gates) sleeps on, and never targets a node.
func (t *Task) Control(ctx context.Context, ctrl control.Control) error {
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
					errs = append(errs, uncontrollableSourceError(ctrl, node, err))
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
				errs = append(errs, uncontrollableSourceError(ctrl, node, err))
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
func (t *Task) probeSourceControl(node pipeline.NodeRef) error {
	prober, ok := t.graph.(pipeline.SourceControlProber)
	if !ok {
		return nil
	}
	return prober.CanControlSource(node)
}

// flushSeekBacklog drains exclusive downstream queues of pre-seek media before
// delivering the seek control. Running probe -> flush -> deliver ensures a
// refused seek never drops media and flush never sheds post-seek frames.
func (t *Task) flushSeekBacklog(node pipeline.NodeRef) error {
	flusher, ok := t.graph.(pipeline.DownstreamFlusher)
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
func (t *Task) controlRate(ctrl control.Control) error {
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
func (t *Task) controlDeliver(ctrl control.Control) (func(context.Context, pipeline.NodeRef, *pipeline.Message) error, error) {
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

func (t *Task) controlTargets(ctrl control.Control) ([]pipeline.NodeRef, error) {
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
	if ctrl.Type() == control.KeyframeType || ctrl.Type() == control.BitrateType {
		if targets := t.encoderNodesProducing(ctrl.StreamID()); len(targets) != 0 {
			return targets, nil
		}
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

func (t *Task) controlSourceNodes() []pipeline.NodeRef {
	spec := t.Spec()
	var targets []pipeline.NodeRef
	for _, node := range spec.Nodes {
		if node.Kind == pipeline.NodeSource {
			targets = append(targets, pipeline.NodeRef(node.Name))
		}
	}
	return targets
}

func (t *Task) controlEntryNodes() []pipeline.NodeRef {
	spec := t.Spec()
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

// encoderNodesProducing finds the encoders that emit stream: an encoded
// rendition (a ladder branch, a simulcast layer) is addressed by its own id
// and reaches only its encoder. A control addressed to a source stream finds
// none here and enters at the graph's entry nodes, reaching every encoder of
// that source.
func (t *Task) encoderNodesProducing(stream av.StreamID) []pipeline.NodeRef {
	if stream == "" {
		return nil
	}
	live := make(map[pipeline.NodeRef]struct{})
	for _, node := range t.Spec().Nodes {
		live[pipeline.NodeRef(node.Name)] = struct{}{}
	}
	var targets []pipeline.NodeRef
	add := func(stage pipeline.Stage) {
		encoder, ok := stage.(*codec.EncoderStage)
		if !ok || encoder.OutputStreamID() != stream {
			return
		}
		if _, ok := live[pipeline.NodeRef(encoder.Name())]; ok {
			targets = append(targets, pipeline.NodeRef(encoder.Name()))
		}
	}
	for _, encoder := range t.encoders {
		add(encoder)
	}
	t.attachMu.Lock()
	for attachment := range t.attachments {
		for _, stage := range attachment.stages {
			add(stage)
		}
	}
	t.attachMu.Unlock()
	return targets
}

// uncontrollableSourceError refuses a source control the source cannot take,
// before anything is flushed, leaving the task running. A seek or segment on
// a source that cannot reposition wraps format.ErrNotSeekable.
func uncontrollableSourceError(ctrl control.Control, node pipeline.NodeRef, err error) error {
	switch ctrl.Type() {
	case control.SeekType, control.SegmentType:
		return &notSeekableError{control: ctrl.Type(), node: node, cause: err}
	}
	return fmt.Errorf("goav: control to %q: source does not accept live control (implement pipeline.ControllableSource): %w", node, err)
}

// notSeekableError is a reposition refused because the source cannot seek.
// It is format.ErrNotSeekable and the graph's refusal (pipeline.ErrInvalidLink).
type notSeekableError struct {
	control control.Type
	node    pipeline.NodeRef
	cause   error
}

func (e *notSeekableError) Error() string {
	return fmt.Sprintf("goav: %s on %q: the source cannot reposition: its input is a pipe or plain io.Reader (read a file or an io.ReadSeeker), its container cannot seek (Matroska, WebM, and MP4 can; IVF and Annex B cannot), or it is a custom source without pipeline.ControllableSource", e.control, e.node)
}

func (e *notSeekableError) Unwrap() []error { return []error{format.ErrNotSeekable, e.cause} }
