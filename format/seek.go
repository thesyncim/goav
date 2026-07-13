package format

import (
	"context"
	"errors"
	"time"
)

// Seeker is an optional Demuxer capability, discovered by type assertion like
// every capability in this codebase: a demuxer that can reposition its input
// to a media time. Seek repositions the demuxer so the next ReadInto resumes
// at the nearest independently-decodable point at or before position
// (keyframe-aligned where the container indexes keyframes); when position
// precedes the first indexed point the demuxer repositions to the earliest
// available data. Position is measured from the start of the media.
//
// Seek is never called concurrently with ReadInto: DemuxSource records a
// requested reposition in Control and applies it from its pump loop between
// reads, so implementations need no locking against their own read path.
//
// A demuxer whose input cannot reposition (its reader is not an io.Seeker)
// reports an error wrapping ErrNotSeekable instead of pretending.
type Seeker interface {
	Seek(ctx context.Context, position time.Duration) error
}

// ErrNotSeekable reports a reposition requested on input that cannot seek (a
// pipe, a network stream, a plain io.Reader). Demuxers wrap it from Seek so
// callers can tell "this input cannot seek" from a failed seek on seekable
// input.
var ErrNotSeekable = errors.New("format: input is not seekable")

// ErrRateUnsupported reports a playback-rate control sent to a task with no
// paced delivery to scale: an offline (non-realtime) task pumps as fast as
// the graph drains, so control.Rate is rejected honestly instead of
// pretending to change speed. Task.Control wraps it when the task's shared
// timeline has no paced consumer (no realtime demux pacer, no clock-aware
// source).
var ErrRateUnsupported = errors.New("format: offline demux pump runs unpaced; rate control needs a realtime (paced) task")
