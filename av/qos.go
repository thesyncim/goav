package av

import (
	"strconv"
	"time"
)

// This file is the one payload seam for EventQoS reports, following the
// build/parse discipline of timecontrol.go and codec.BitrateMetadata: the
// lateness and the delivered-or-dropped disposition ride Event.Metadata
// through the typed pair below, so report producers and QoS policies never
// hand-roll metadata strings. The keys stay unexported — the pair is the wire
// contract.

// metadataQoSLateness carries how far past due the reported message was, as
// decimal nanoseconds. metadataQoSDropped carries whether the message was shed
// ("true") or delivered late ("false").
const (
	metadataQoSLateness = "qos_lateness_ns"
	metadataQoSDropped  = "qos_dropped"
)

// QoSMetadata builds the event metadata that carries one QoS report: how far
// past due the message was at admit time and whether it was shed (dropped) or
// delivered late. It is the encoding half of EventQoS.
func QoSMetadata(lateness time.Duration, dropped bool) Metadata {
	return Metadata{
		metadataQoSLateness: strconv.FormatInt(int64(lateness), 10),
		metadataQoSDropped:  strconv.FormatBool(dropped),
	}
}

// EventQoSReport extracts the lateness and shed disposition from an EventQoS
// event. It reports ok=false when the event carries no positive decimal
// lateness payload — policy-refusal EventQoS events (Cause set, no payload)
// and malformed reports fail here, so a QoS policy never reacts to them.
func EventQoSReport(event *Event) (lateness time.Duration, dropped bool, ok bool) {
	if event == nil {
		return 0, false, false
	}
	value, err := strconv.ParseInt(event.Metadata[metadataQoSLateness], 10, 64)
	if err != nil || value <= 0 {
		return 0, false, false
	}
	dropped, err = strconv.ParseBool(event.Metadata[metadataQoSDropped])
	if err != nil {
		return 0, false, false
	}
	return time.Duration(value), dropped, true
}
