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
// Time-axis controls (seek, rate, segment) are the exception: sources have no
// queue, so they are handed to each source's Control method synchronously.
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
		if err := deliver(ctx, node, msg); err != nil {
			if errors.Is(err, pipeline.ErrDynamicGraphUnsupported) {
				return control.ErrNotRunning
			}
			if ctrl.TargetsSources() && errors.Is(err, pipeline.ErrInvalidLink) {
				errs = append(errs, fmt.Errorf("goav: control to %q: source does not accept live control (implement pipeline.ControllableSource): %w", node, err))
				continue
			}
			errs = append(errs, fmt.Errorf("goav: control to %q: %w", node, err))
		}
	}
	return errors.Join(errs...)
}

// controlDeliver picks the delivery seam for a control. Time-axis controls
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
