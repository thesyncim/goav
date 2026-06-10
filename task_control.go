package goav

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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
	// ControlBitrate asks the encoders on the control's path to retarget their
	// bitrate live. It lowers to an av.EventBitrateChanged event carrying the
	// rate in Event.Metadata (av.MetadataBitrate, bits per second) and rides the
	// same route as ControlKeyframe: untargeted it enters at the graph's source
	// boundary and flows the data path downstream to every encoder.
	ControlBitrate ControlType = "bitrate"
	// ControlSelect switches a live Select to the input named by StreamID. Like a
	// keyframe request it needs no target: untargeted, it enters at the graph's
	// source boundary and rides the data path to the selector, which consumes it.
	ControlSelect ControlType = "select"
	// ControlEvent delivers a caller-supplied av.Event verbatim to the target node.
	// It is the escape hatch for node-interpreted controls (a selector switch, a
	// flush marker) that a stage recognises in its Handle.
	ControlEvent ControlType = "event"
	// ControlSeek asks the task's sources to reposition. Unlike the other
	// controls it does not ride a stage queue: it is handed to each source's
	// Control method (pipeline.ControllableSource) as an av.EventSeek, because
	// sources have no inbound queue — they run a Start loop. Untargeted, it
	// broadcasts to every source node.
	ControlSeek ControlType = "seek"
	// ControlRate asks the task's sources to change playback rate. It rides
	// the same source-control seam as ControlSeek (each source's Control
	// method, as an av.EventRate) and broadcasts to every source node when
	// untargeted.
	ControlRate ControlType = "rate"
	// ControlSegment asks the task's sources to play one [start, end) window
	// and then end naturally. It rides the same source-control seam as
	// ControlSeek (each source's Control method, as an av.EventSegment) and
	// broadcasts to every source node when untargeted.
	ControlSegment ControlType = "segment"
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
	// Task.Describe / snapshot.Tap.Node. Normal controls do not need it — an
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
	// Position is the target media position for ControlSeek, or the inclusive
	// window start for ControlSegment, measured from the start of the media.
	// Ignored otherwise.
	Position time.Duration
	// End is the exclusive window end for ControlSegment, measured from the
	// start of the media. Ignored otherwise.
	End time.Duration
	// Rate is the requested playback rate for ControlRate (1 = realtime).
	// Ignored otherwise.
	Rate float64
	// Bitrate is the requested encoder rate in bits per second for
	// ControlBitrate. Ignored otherwise.
	Bitrate int
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

// SetBitrate builds a control that retargets live encoders to bitsPerSecond.
// It is the encoder-control dual of Keyframe and takes the same route:
// untargeted, it enters the graph at the source boundary and rides the data
// path downstream — through stream-routed branches — so every live encoder on
// the stream's path applies the new rate without the caller naming any node.
// Narrow it to one encoder with AtTap or (expert) At.
//
// The control lowers to an av.EventBitrateChanged event whose rate rides
// Event.Metadata under av.MetadataBitrate. An encoder whose backend supports
// live rate control (libvpx vpx_codec_enc_config_set, libopus
// OPUS_SET_BITRATE) applies it from the next encoded frame; one that cannot
// returns an error through the task's error path instead of ignoring the
// request. A non-positive rate fails Task.Control immediately.
func SetBitrate(stream av.StreamID, bitsPerSecond int) Control {
	return Control{Type: ControlBitrate, StreamID: stream, Bitrate: bitsPerSecond}
}

// Deliver builds a control that hands a caller-supplied event to the target
// verbatim. The receiving stage interprets it in Handle — this is how a stage
// that exposes a custom switch (a selector) is driven live. Target it with
// AtTap or (expert) At.
func Deliver(event av.Event) Control {
	return Control{Type: ControlEvent, Event: event}
}

// Seek builds a control that asks the task's sources to reposition to pos,
// measured from the start of the media. Untargeted, it broadcasts to every
// source node; each source that implements pipeline.ControllableSource has its
// Control method called with an av.EventSeek whose Timestamp carries pos in a
// nanosecond timebase. Errors are collected per source, so a source that cannot
// reposition (it does not implement live control) reports clearly without
// stopping a seekable sibling. Narrow it to one source with At (expert).
//
// After repositioning, the source emits av.EventDiscontinuity before the first
// message at the new position (the ControllableSource contract), which is the
// signal downstream decoders already reset on — Seek adds no flush machinery.
// Rate and Segment ride the same source-control seam.
func Seek(pos time.Duration) Control {
	return Control{Type: ControlSeek, Position: pos}
}

// Rate builds a control that asks the task's sources to change playback rate:
// 1.0 is realtime, 2.0 double speed, 0.5 half speed. Only positive rates are
// valid — reverse playback is out of scope — and Task.Control rejects r <= 0
// (or a non-finite rate) with a clear error before delivering anything. It
// rides the seam Seek established: untargeted, it broadcasts to every source
// node; each source implementing pipeline.ControllableSource has its Control
// method called with an av.EventRate whose rate rides Event.Metadata under
// av.MetadataRate (read it with av.EventRateValue). Errors are collected per
// source, so a source that cannot change rate (it does not implement live
// control) reports clearly without stopping an adjustable sibling. Narrow it
// to one source with At (expert).
//
// Contract: a rate change is a pacing change, not a reposition. The source
// keeps delivering from its current position at the new pace and emits
// av.EventDiscontinuity only if applying the rate makes it reposition — a pure
// pacing change does NOT discontinue, so downstream decoder state stays valid.
func Rate(r float64) Control {
	return Control{Type: ControlRate, Rate: r}
}

// Segment builds a control that asks the task's sources to play the window
// [start, end) — start inclusive, end exclusive, both measured from the start
// of the media — and then end naturally. Task.Control rejects windows that are
// not 0 <= start < end with a clear error before delivering anything. It rides
// the seam Seek established: untargeted, it broadcasts to every source node;
// each source implementing pipeline.ControllableSource has its Control method
// called with an av.EventSegment whose start rides Event.Timestamp (like Seek)
// and whose end rides Event.Metadata under av.MetadataSegmentEnd (read it with
// av.EventSegmentEnd). Errors are collected per source, so a source that
// cannot play a window reports clearly without stopping a capable sibling.
// Narrow it to one source with At (expert).
//
// Contract: a segment behaves like a Seek to start followed by natural end of
// stream at end. The source emits av.EventDiscontinuity before the first
// message at start (the repositioning half of the ControllableSource contract)
// and, on reaching end, ends exactly as at the end of the media — it emits
// av.EventEndOfStream and its Start loop returns — so destinations finalize
// the same way they do on a natural end of input. That is what makes
// trim-to-file segment export work: a From(file) task given Segment(a, b)
// plays the window and commits its destinations when the window completes.
func Segment(start, end time.Duration) Control {
	return Control{Type: ControlSegment, Position: start, End: end}
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
	case ControlBitrate:
		if c.Bitrate <= 0 {
			return nil, fmt.Errorf("goav: SetBitrate needs a positive rate in bits per second, got %d", c.Bitrate)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventBitrateChanged,
			StreamID: c.StreamID,
			Reason:   c.Reason,
			Metadata: codec.BitrateMetadata(c.Bitrate),
		}}, nil
	case ControlSelect:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventStats,
			StreamID: c.StreamID,
			Reason:   selectorActiveReason,
		}}, nil
	case ControlSeek:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:      av.EventSeek,
			StreamID:  c.StreamID,
			Reason:    c.Reason,
			Timestamp: av.Timestamp{Value: int64(c.Position), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}, nil
	case ControlRate:
		if !(c.Rate > 0) || math.IsInf(c.Rate, 1) {
			return nil, fmt.Errorf("goav: Rate needs a positive, finite playback rate (reverse playback is not supported), got %v", c.Rate)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventRate,
			StreamID: c.StreamID,
			Reason:   c.Reason,
			Metadata: av.RateMetadata(c.Rate),
		}}, nil
	case ControlSegment:
		if c.Position < 0 || c.End <= c.Position {
			return nil, fmt.Errorf("goav: Segment needs 0 <= start < end, got [%v, %v)", c.Position, c.End)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:      av.EventSegment,
			StreamID:  c.StreamID,
			Reason:    c.Reason,
			Timestamp: av.Timestamp{Value: int64(c.Position), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
			Metadata:  av.SegmentEndMetadata(c.End),
		}}, nil
	case ControlEvent:
		event := c.Event
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}, nil
	default:
		return nil, fmt.Errorf("goav: unknown control type %q", c.Type)
	}
}

// targetsSources reports whether the control is delivered to SOURCE nodes
// through their Control method (pipeline.ControllableSource) instead of riding
// a stage queue: the time-axis controls (seek, rate, segment) steer a source's
// Start loop, and sources have no inbound queue.
func (c Control) targetsSources() bool {
	switch c.Type {
	case ControlSeek, ControlRate, ControlSegment:
		return true
	default:
		return false
	}
}

// Control injects an out-of-band control into the running task's graph, delivering
// it to the node named by control.Node on that node's serial worker. It is safe to
// call concurrently with Run: the control rides the target node's normal queue, so
// the node's Handle still sees one message at a time and needs no extra locking —
// the injection is race-safe by construction. The time-axis controls
// (ControlSeek, ControlRate, ControlSegment) are the exception: sources have no
// queue, so they are handed to each source's Control method
// (pipeline.ControllableSource) synchronously instead.
//
// Control returns ErrControlUnsupported when the task graph has no per-node worker
// to deliver on (a direct, non-buffered graph — except the time-axis controls,
// which a direct graph delivers too), ErrControlNotRunning before Run has started
// a node worker, and pipeline.ErrUnknownNode for an unknown target.
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
	deliver, err := t.controlDeliver(control)
	if err != nil {
		return err
	}
	var errs []error
	for _, node := range targets {
		if err := deliver(ctx, node, msg); err != nil {
			if errors.Is(err, pipeline.ErrDynamicGraphUnsupported) {
				return ErrControlNotRunning
			}
			if control.targetsSources() && errors.Is(err, pipeline.ErrInvalidLink) {
				errs = append(errs, fmt.Errorf("goav: control to %q: source does not accept live control (implement pipeline.ControllableSource): %w", node, err))
				continue
			}
			errs = append(errs, fmt.Errorf("goav: control to %q: %w", node, err))
		}
	}
	return errors.Join(errs...)
}

// controlDeliver picks the delivery seam for a control. The time-axis controls
// (seek, rate, segment) target SOURCE nodes, which have no queue or serial
// worker, so they go through the graph's SourceInjector (the source's Control
// method, called synchronously — see pipeline.ControllableSource for the
// contract). Everything else rides the target node's queue through NodeInjector.
func (t *task) controlDeliver(control Control) (func(context.Context, pipeline.NodeRef, *pipeline.Message) error, error) {
	if control.targetsSources() {
		injector, ok := t.graph.(pipeline.SourceInjector)
		if !ok {
			return nil, ErrControlUnsupported
		}
		return injector.InjectSource, nil
	}
	injector, ok := t.graph.(pipeline.NodeInjector)
	if !ok {
		return nil, ErrControlUnsupported
	}
	return injector.Inject, nil
}

// controlTargets resolves where a control is delivered, in grammar terms first:
// an explicit node wins; a tap name resolves through Taps(); an untargeted
// keyframe or bitrate retarget broadcasts to the graph's entry row (the nodes
// fed directly by sources) so it rides the data path to every downstream
// encoder; an untargeted time-axis control (seek, rate, segment) broadcasts to
// every source node. Anything else untargeted is an error — the caller must
// say where.
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
	if control.Type == ControlKeyframe || control.Type == ControlBitrate || control.Type == ControlSelect {
		targets := t.controlEntryNodes()
		if len(targets) == 0 {
			return nil, pipeline.ErrUnknownNode
		}
		return targets, nil
	}
	if control.targetsSources() {
		targets := t.controlSourceNodes()
		if len(targets) == 0 {
			return nil, pipeline.ErrUnknownNode
		}
		return targets, nil
	}
	return nil, fmt.Errorf("goav: control needs a target: use AtTap(tap) or At(node): %w", pipeline.ErrUnknownNode)
}

// controlSourceNodes returns every source node in the graph, in spec order.
// These are the targets the time-axis controls broadcast to — controls for
// sources are handed to the source implementation itself, not enqueued.
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
