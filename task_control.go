package goav

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// ControlType classifies an out-of-band control delivered to a running node by
// Task.Control. It mirrors codec.ControlType in spirit: a small, typed vocabulary
// of requests a live graph understands.
type ControlType string

const (
	// ControlKeyframe asks the target node to produce a keyframe. It is delivered
	// as an av.EventKeyframeRequired event on the node's serial worker — the same
	// event a decoder emits from a ControlRequestKeyframe — so an encoder reacts to
	// a live request exactly as it would to one routed through the graph.
	ControlKeyframe ControlType = "keyframe"
	// ControlEvent delivers a caller-supplied av.Event verbatim to the target node.
	// It is the escape hatch for node-interpreted controls (a selector switch, a
	// flush marker) that a stage recognises in its Handle.
	ControlEvent ControlType = "event"
)

var (
	// ErrControlUnsupported is returned by Task.Control when the task's graph
	// cannot inject controls (for example a non-buffered, non-realtime graph that
	// has no per-node serial worker to deliver on).
	ErrControlUnsupported = errors.New("goav: task does not support live control")
	// ErrControlNotRunning is returned by Task.Control when the task is not running,
	// so there is no node worker draining a queue to receive the control.
	ErrControlNotRunning = errors.New("goav: task is not running")
	// ErrNilControl is returned by Task.Control when the control carries no payload.
	ErrNilControl = errors.New("goav: nil control")
)

// Control is an out-of-band request injected into a running task's graph, routed
// to a single target node and processed on that node's serial worker. It is the
// control-plane dual of the data plane: where messages flow edge-to-edge, a
// Control reaches a named node directly to drive live switching (a selector),
// keyframe requests, or flushes — without restarting the task.
//
// A Control is modelled on codec.ControlRequest: a typed request plus the stream
// it concerns. Build one with Keyframe or Deliver and target it with At.
type Control struct {
	// Node is the expert-level target: a node named as it appears in
	// Task.Describe / TapInfo.Node. Normal controls do not need it — an
	// untargeted Keyframe reaches every encoder by itself, and AtTap targets by
	// tap name.
	Node pipeline.NodeRef
	// Tap targets the control at a named tap (resolved through Task.Taps), so
	// callers reason in grammar vocabulary instead of graph node names.
	Tap string
	// Type selects how the control is lowered into a pipeline message.
	Type ControlType
	// StreamID scopes a keyframe request to one stream. Empty targets the node's
	// whole input.
	StreamID av.StreamID
	// Reason is optional human context carried on the lowered event.
	Reason string
	// Event is the verbatim event delivered when Type is ControlEvent. Ignored
	// otherwise.
	Event av.Event
}

// At returns a copy of the control targeting a graph node directly. This is the
// expert form — normal callers use an untargeted Keyframe or AtTap.
func (c Control) At(node pipeline.NodeRef) Control {
	c.Node = node
	return c
}

// AtTap returns a copy of the control targeting the named tap's point in the
// graph. The tap name is the one given to Tap/FrameTap/PacketTap and reported by
// Task.Taps — no node names involved.
func (c Control) AtTap(name string) Control {
	c.Tap = name
	return c
}

// Keyframe builds a keyframe-request control for the given stream. Untargeted,
// it enters the graph at the source boundary and rides the data path downstream
// — the same route a transport's loss feedback takes — so every live encoder for
// that stream sees it without the caller naming any node. Narrow it with AtTap
// or (expert) At.
func Keyframe(stream av.StreamID) Control {
	return Control{Type: ControlKeyframe, StreamID: stream}
}

// Deliver builds a control that hands a caller-supplied event to the target
// verbatim. The receiving stage interprets it in Handle — this is how a stage
// that exposes a custom switch (a selector) is driven live. Target it with
// AtTap or (expert) At.
func Deliver(event av.Event) Control {
	return Control{Type: ControlEvent, Event: event}
}

// message lowers the control into the pipeline message delivered to the node.
func (c Control) message() (*pipeline.Message, error) {
	switch c.Type {
	case ControlKeyframe:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventKeyframeRequired,
			StreamID: c.StreamID,
			Reason:   c.Reason,
		}}, nil
	case ControlEvent:
		event := c.Event
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}, nil
	default:
		return nil, fmt.Errorf("goav: unknown control type %q", c.Type)
	}
}

// Control injects an out-of-band control into the running task's graph, delivering
// it to the node named by control.Node on that node's serial worker. It is safe to
// call concurrently with Run: the control rides the target node's normal queue, so
// the node's Handle still sees one message at a time and needs no extra locking —
// the injection is race-safe by construction.
//
// Control returns ErrControlUnsupported when the task graph has no per-node worker
// to deliver on (a direct, non-buffered graph), ErrControlNotRunning before Run
// has started a node worker, and pipeline.ErrUnknownNode for an unknown target.
func (t *task) Control(ctx context.Context, control Control) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	targets, err := t.controlTargets(control)
	if err != nil {
		return err
	}
	msg, err := control.message()
	if err != nil {
		return err
	}
	if msg == nil || msg.Event == nil {
		return ErrNilControl
	}
	injector, ok := t.graph.(pipeline.NodeInjector)
	if !ok {
		return ErrControlUnsupported
	}
	var errs []error
	for _, node := range targets {
		if err := injector.Inject(ctx, node, msg); err != nil {
			if errors.Is(err, pipeline.ErrDynamicGraphUnsupported) {
				return ErrControlNotRunning
			}
			errs = append(errs, fmt.Errorf("goav: control to %q: %w", node, err))
		}
	}
	return errors.Join(errs...)
}

// controlTargets resolves where a control is delivered, in grammar terms first:
// an explicit node wins; a tap name resolves through Taps(); an untargeted
// keyframe broadcasts to the graph's entry row (the nodes fed directly by
// sources) so it rides the data path to every downstream encoder. Anything else
// untargeted is an error — the caller must say where.
func (t *task) controlTargets(control Control) ([]pipeline.NodeRef, error) {
	if control.Node != "" {
		return []pipeline.NodeRef{control.Node}, nil
	}
	if control.Tap != "" {
		for _, tap := range t.Taps() {
			if tap.Name == control.Tap {
				return []pipeline.NodeRef{tap.Node}, nil
			}
		}
		return nil, fmt.Errorf("goav: control targets unknown tap %q: %w", control.Tap, pipeline.ErrUnknownNode)
	}
	if control.Type == ControlKeyframe {
		targets := t.controlEntryNodes()
		if len(targets) == 0 {
			return nil, pipeline.ErrUnknownNode
		}
		return targets, nil
	}
	return nil, fmt.Errorf("goav: control needs a target: use AtTap(tap) or At(node): %w", pipeline.ErrUnknownNode)
}

// controlEntryNodes returns the graph's entry row: every node fed directly by a
// source, deduplicated in spec order. An event injected there flows downstream
// through the normal data path — the same route transport loss feedback takes.
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
