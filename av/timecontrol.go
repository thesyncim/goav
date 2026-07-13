package av

import (
	"math"
	"strconv"
	"time"
)

// This file is the one payload seam for the time-axis control events. EventSeek
// carries its position on Event.Timestamp; EventRate and EventSegment each need
// one more value, which rides Event.Metadata through the typed build/parse pairs
// below — the same wire-form discipline as codec.BitrateMetadata/EventBitrate
// for EventBitrateChanged — so control producers and sources never hand-roll
// metadata strings.

// MetadataRate is the Event.Metadata key carrying a requested playback rate as
// a decimal string (1 = realtime). It is the payload of EventRate events —
// the wire form of task-wide rate controls; build it with RateMetadata and
// read it with EventRateValue.
const MetadataRate = "rate"

// MetadataSegmentEnd is the Event.Metadata key carrying a segment's exclusive
// end as decimal nanoseconds from the start of the media. It is the second
// payload of EventSegment events — the start rides Event.Timestamp — built
// with SegmentEndMetadata and read with EventSegmentEnd.
const MetadataSegmentEnd = "segment_end_ns"

// RateMetadata builds the event metadata that carries a playback-rate change:
// MetadataRate holding the rate as a decimal string. It is the encoding half
// of EventRate, so control producers and sources agree on the wire form.
func RateMetadata(rate float64) Metadata {
	return Metadata{MetadataRate: strconv.FormatFloat(rate, 'g', -1, 64)}
}

// EventRateValue extracts the requested playback rate from an EventRate
// event. It reports false when the event carries no positive, finite decimal
// MetadataRate entry, so a consumer can reject a malformed rate change
// explicitly instead of applying garbage.
func EventRateValue(event *Event) (float64, bool) {
	if event == nil {
		return 0, false
	}
	value, err := strconv.ParseFloat(event.Metadata[MetadataRate], 64)
	if err != nil || !(value > 0) || math.IsInf(value, 1) {
		// !(value > 0) also rejects NaN, which no comparison with 0 catches.
		return 0, false
	}
	return value, true
}

// SegmentEndMetadata builds the event metadata that carries a segment's
// exclusive end bound: MetadataSegmentEnd holding the position as decimal
// nanoseconds from the start of the media. It is the encoding half of
// EventSegment's end (the start rides Event.Timestamp like EventSeek).
func SegmentEndMetadata(end time.Duration) Metadata {
	return Metadata{MetadataSegmentEnd: strconv.FormatInt(int64(end), 10)}
}

// EventSegmentEnd extracts the exclusive end bound from an EventSegment
// event. It reports false when the event carries no positive decimal
// MetadataSegmentEnd entry, so a source can reject a malformed segment
// explicitly instead of playing garbage bounds.
func EventSegmentEnd(event *Event) (time.Duration, bool) {
	if event == nil {
		return 0, false
	}
	value, err := strconv.ParseInt(event.Metadata[MetadataSegmentEnd], 10, 64)
	if err != nil || value <= 0 {
		return 0, false
	}
	return time.Duration(value), true
}
