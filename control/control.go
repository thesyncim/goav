// Package control defines the opt-in live task control vocabulary.
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
)

// Control is an out-of-band request injected into a running task's graph.
type Control struct {
	// Node is the expert-level target: a graph node name.
	Node pipeline.NodeRef
	// Tap targets the control at a named tap.
	Tap string
	// Type selects how the control is lowered into a pipeline message.
	Type Type
	// StreamID scopes a keyframe, bitrate, or select control to one stream.
	StreamID av.StreamID
	// Reason is optional human context carried on the lowered event.
	Reason string
	// Event is the verbatim event delivered when Type is EventType.
	Event av.Event
	// Position is the target media position for SeekType, or segment start.
	Position time.Duration
	// End is the exclusive segment end.
	End time.Duration
	// Rate is the requested playback rate for RateType.
	Rate float64
	// Bitrate is the requested encoder rate in bits per second.
	Bitrate int
}

// At returns a copy of the control targeting a graph node directly.
func (c Control) At(node pipeline.NodeRef) Control {
	c.Node = node
	return c
}

// AtTap returns a copy of the control targeting the named tap's point.
func (c Control) AtTap(name string) Control {
	c.Tap = name
	return c
}

// Keyframe builds a keyframe-request control for the given stream.
func Keyframe(stream av.StreamID) Control {
	return Control{Type: KeyframeType, StreamID: stream}
}

// SetBitrate builds a control that retargets live encoders to bitsPerSecond.
func SetBitrate(stream av.StreamID, bitsPerSecond int) Control {
	return Control{Type: BitrateType, StreamID: stream, Bitrate: bitsPerSecond}
}

// SelectActive switches a running Select to forward the arm identified by id.
func SelectActive(id av.StreamID) Control {
	return Control{Type: SelectType, StreamID: id}
}

// Deliver builds a control that hands a caller-supplied event to the target.
func Deliver(event av.Event) Control {
	return Control{Type: EventType, Event: event}
}

// Seek builds a control that asks task sources to reposition to pos.
func Seek(pos time.Duration) Control {
	return Control{Type: SeekType, Position: pos}
}

// Rate builds a control that asks task sources to change playback rate.
func Rate(r float64) Control {
	return Control{Type: RateType, Rate: r}
}

// Segment builds a control that asks task sources to play [start, end).
func Segment(start, end time.Duration) Control {
	return Control{Type: SegmentType, Position: start, End: end}
}

// Message lowers the control into the pipeline message delivered to a node.
func (c Control) Message() (*pipeline.Message, error) {
	switch c.Type {
	case KeyframeType:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventKeyframeRequired,
			StreamID: c.StreamID,
			Reason:   c.Reason,
		}}, nil
	case BitrateType:
		if c.Bitrate <= 0 {
			return nil, fmt.Errorf("goav: SetBitrate needs a positive rate in bits per second, got %d", c.Bitrate)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventBitrateChanged,
			StreamID: c.StreamID,
			Reason:   c.Reason,
			Metadata: codec.BitrateMetadata(c.Bitrate),
		}}, nil
	case SelectType:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventStats,
			StreamID: c.StreamID,
			Reason:   selectorActiveReason,
		}}, nil
	case SeekType:
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:      av.EventSeek,
			StreamID:  c.StreamID,
			Reason:    c.Reason,
			Timestamp: av.Timestamp{Value: int64(c.Position), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		}}, nil
	case RateType:
		if !(c.Rate > 0) || math.IsInf(c.Rate, 1) {
			return nil, fmt.Errorf("goav: Rate needs a positive, finite playback rate (reverse playback is not supported), got %v", c.Rate)
		}
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
			Type:     av.EventRate,
			StreamID: c.StreamID,
			Reason:   c.Reason,
			Metadata: av.RateMetadata(c.Rate),
		}}, nil
	case SegmentType:
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
	case EventType:
		event := c.Event
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}, nil
	default:
		return nil, fmt.Errorf("goav: unknown control type %q", c.Type)
	}
}

// TargetsSources reports whether the control is delivered through source
// Control methods instead of node queues.
func (c Control) TargetsSources() bool {
	switch c.Type {
	case SeekType, RateType, SegmentType:
		return true
	default:
		return false
	}
}
