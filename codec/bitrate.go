package codec

import (
	"strconv"

	"github.com/thesyncim/goav/av"
)

// BitrateMetadata builds the event metadata that carries a live encoder
// bitrate retarget: av.MetadataBitrate holding the rate as a decimal string in
// bits per second. It is the encoding half of EventBitrate, so producers and
// encoders agree on the wire form of av.EventBitrateChanged.
func BitrateMetadata(bitsPerSecond int) av.Metadata {
	return av.Metadata{av.MetadataBitrate: strconv.Itoa(bitsPerSecond)}
}

// EventBitrate extracts the requested bitrate in bits per second from an
// av.EventBitrateChanged event. It reports false when the event carries no
// positive decimal av.MetadataBitrate entry, so an encoder can reject a
// malformed retarget explicitly instead of applying garbage.
func EventBitrate(event *av.Event) (int, bool) {
	if event == nil {
		return 0, false
	}
	value, err := strconv.Atoi(event.Metadata[av.MetadataBitrate])
	if err != nil || value <= 0 {
		return 0, false
	}
	return value, true
}
