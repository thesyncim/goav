// Package control defines the in-process live task control vocabulary; for
// exposing those controls over a socket, see package ctlserver.
package control

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

const selectorActiveReason = "select.active"

// Type classifies an out-of-band control delivered to a running node.
type Type string

const (
	// KeyframeType asks the target node to produce a keyframe.
	KeyframeType Type = "keyframe"
	// BitrateType asks encoders on the control's path to retarget bitrate live.
	BitrateType Type = "bitrate"
	// SelectType switches a live Select to the input named by StreamID.
	SelectType Type = "select"
	// EventType delivers a caller-supplied av.Event verbatim to the target node.
	EventType Type = "event"
	// SeekType asks task sources to reposition.
	SeekType Type = "seek"
	// RateType asks task sources to change playback rate.
	RateType Type = "rate"
	// SegmentType asks task sources to play one [start, end) window.
	SegmentType Type = "segment"
)

var (
	// ErrUnsupported is returned when a task graph cannot inject controls.
	ErrUnsupported = errors.New("goav: task does not support live control")
	// ErrNotRunning is returned when the task is not running.
	ErrNotRunning = errors.New("goav: task is not running")
	// ErrNil is returned when a control carries no payload.
	ErrNil = errors.New("goav: nil control")
	// ErrInvalid is returned when a control constructor receives an invalid payload.
	ErrInvalid = errors.New("goav: invalid control")
	// ErrAmbiguousTarget is returned when a control does not name exactly one
	// target and the task cannot infer one safely.
	ErrAmbiguousTarget = errors.New("goav: control target is ambiguous")
)

// Control is an out-of-band request injected into a running task's graph.
type Control struct {
	// Node is the expert-level target: a graph node name.
	node pipeline.NodeRef
	// Tap targets the control at a named tap.
	tap string
	// Type selects how the control is lowered into a pipeline message.
	typ Type
	// StreamID scopes a keyframe, bitrate, or select control to one stream.
	streamID av.StreamID
	// Reason is optional human context carried on the lowered event.
	reason string
	// Event is the verbatim event delivered when Type is EventType.
	event av.Event
	// Position is the target media position for SeekType, or segment start.
	position time.Duration
	// End is the exclusive segment end.
	end time.Duration
	// Rate is the requested playback rate for RateType.
	rate float64
	// Bitrate is the requested encoder rate in bits per second.
	bitrate int
}

// At returns a copy of the control targeting a graph node directly.
func (c Control) At(node pipeline.NodeRef) Control {
	c.node = node
	return c
}

// AtTap returns a copy of the control targeting the named tap's point.
func (c Control) AtTap(name string) Control {
	c.tap = name
	return c
}

// WithReason returns a copy of the control carrying human context.
func (c Control) WithReason(reason string) Control {
	c.reason = reason
	return c
}

// Node returns the expert graph-node target, if any.
func (c Control) Node() pipeline.NodeRef {
	return c.node
}

// Tap returns the tap target, if any.
func (c Control) Tap() string {
	return c.tap
}

// Type returns the control verb.
func (c Control) Type() Type {
	return c.typ
}

// StreamID returns the stream scoped by the control, if any.
func (c Control) StreamID() av.StreamID {
	return c.streamID
}

// Reason returns optional human context carried on the lowered event.
func (c Control) Reason() string {
	return c.reason
}

// Event returns the event payload for EventType controls.
func (c Control) Event() av.Event {
	return c.event
}

// Position returns the seek position or segment start.
func (c Control) Position() time.Duration {
	return c.position
}

// End returns the exclusive segment end.
func (c Control) End() time.Duration {
	return c.end
}

// Rate returns the playback rate requested by a RateType control.
func (c Control) Rate() float64 {
	return c.rate
}

// Bitrate returns the encoder rate requested by a BitrateType control.
func (c Control) Bitrate() int {
	return c.bitrate
}

// Keyframe builds a keyframe-request control for the given stream.
func Keyframe(stream av.StreamID) Control {
	return Control{typ: KeyframeType, streamID: stream}
}

// SetBitrate builds a control that retargets live encoders to bitsPerSecond.
func SetBitrate(stream av.StreamID, bitsPerSecond int) (Control, error) {
	if bitsPerSecond <= 0 {
		return Control{}, invalidControl("SetBitrate needs a positive rate in bits per second, got %d", bitsPerSecond)
	}
	return Control{typ: BitrateType, streamID: stream, bitrate: bitsPerSecond}, nil
}

// SelectActive switches a running Select to forward the arm identified by id.
func SelectActive(id av.StreamID) Control {
	return Control{typ: SelectType, streamID: id}
}

// Deliver builds a control that hands a caller-supplied event to the target.
func Deliver(event av.Event) Control {
	return Control{typ: EventType, event: event}
}

// Seek builds a control that asks task sources to reposition to pos.
func Seek(pos time.Duration) Control {
	return Control{typ: SeekType, position: pos}
}

// Rate builds a control that asks task sources to change playback rate.
func Rate(r float64) (Control, error) {
	if !(r > 0) || math.IsInf(r, 0) {
		return Control{}, invalidControl("Rate needs a positive, finite playback rate (reverse playback is not supported), got %v", r)
	}
	return Control{typ: RateType, rate: r}, nil
}

// Segment builds a control that asks task sources to play [start, end).
func Segment(start, end time.Duration) (Control, error) {
	if start < 0 || end <= start {
		return Control{}, invalidControl("Segment needs 0 <= start < end, got [%v, %v)", start, end)
	}
	return Control{typ: SegmentType, position: start, end: end}, nil
}

// Must unwraps a control constructor in tests and static configurations. It
// panics if err is non-nil; production code should handle the error instead.
func Must(ctrl Control, err error) Control {
	if err != nil {
		panic(err)
	}
	return ctrl
}

func invalidControl(format string, args ...any) error {
	return fmt.Errorf("goav: "+format+": %w", append(args, ErrInvalid)...)
}

// Message lowers the control into the pipeline message delivered to a node.
func (c Control) Message() (*pipeline.Message, error) {
	if c.typ == "" {
		return nil, ErrNil
	}
	switch c.typ {
	case KeyframeType:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventKeyframeRequired,
			StreamID: c.streamID,
			Reason:   c.reason,
		}}, nil
	case BitrateType:
		if c.bitrate <= 0 {
			return nil, invalidControl("SetBitrate needs a positive rate in bits per second, got %d", c.bitrate)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventBitrateChanged,
			StreamID: c.streamID,
			Reason:   c.reason,
			Metadata: codec.BitrateMetadata(c.bitrate),
		}}, nil
	case SelectType:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventStats,
			StreamID: c.streamID,
			Reason:   selectorActiveReason,
		}}, nil
	case SeekType:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:      av.EventSeek,
			StreamID:  c.streamID,
			Reason:    c.reason,
			Timestamp: av.Timestamp{Value: int64(c.position), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}, nil
	case RateType:
		if !(c.rate > 0) || math.IsInf(c.rate, 0) {
			return nil, invalidControl("Rate needs a positive, finite playback rate (reverse playback is not supported), got %v", c.rate)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventRate,
			StreamID: c.streamID,
			Reason:   c.reason,
			Metadata: av.RateMetadata(c.rate),
		}}, nil
	case SegmentType:
		if c.position < 0 || c.end <= c.position {
			return nil, invalidControl("Segment needs 0 <= start < end, got [%v, %v)", c.position, c.end)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:      av.EventSegment,
			StreamID:  c.streamID,
			Reason:    c.reason,
			Timestamp: av.Timestamp{Value: int64(c.position), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
			Metadata:  av.SegmentEndMetadata(c.end),
		}}, nil
	case EventType:
		event := c.event
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}, nil
	default:
		return nil, invalidControl("unknown control type %q", c.typ)
	}
}

// TargetsSources reports whether the control is delivered through source
// Control methods instead of node queues.
func (c Control) TargetsSources() bool {
	switch c.typ {
	case SeekType, RateType, SegmentType:
		return true
	default:
		return false
	}
}
