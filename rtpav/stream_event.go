package rtpav

import "github.com/thesyncim/goav/av"

func eventMatchesStream(stream av.Stream, event *av.Event) bool {
	if event == nil {
		return false
	}
	return event.StreamID == "" || stream.ID == "" || event.StreamID == stream.ID
}

func applyCodecChangedEvent(stream *av.Stream, codec av.CodecID, event *av.Event) bool {
	if stream == nil || event == nil || event.Type != av.EventCodecChanged {
		return false
	}
	if !eventMatchesStream(*stream, event) {
		return false
	}
	if event.Codec != nil && event.Codec.ID != "" && event.Codec.ID != codec {
		return true
	}
	if event.Stream != nil && event.Stream.Codec.ID != "" && event.Stream.Codec.ID != codec {
		return true
	}
	if event.Stream != nil {
		next := *event.Stream
		if next.ID == "" {
			next.ID = stream.ID
		}
		if next.Type == "" {
			next.Type = stream.Type
		}
		if next.Codec.ID == "" {
			next.Codec = stream.Codec
		}
		if event.Codec != nil {
			next.Codec = *event.Codec
		}
		if next.TimeBase == (av.TimeBase{}) {
			next.TimeBase = stream.TimeBase
		}
		if next.Epoch == 0 && event.Epoch != 0 {
			next.Epoch = event.Epoch
		}
		*stream = next
		return true
	}
	if event.StreamID != "" {
		stream.ID = event.StreamID
	}
	if event.Epoch != 0 {
		stream.Epoch = event.Epoch
	}
	if event.Codec != nil {
		stream.Codec = *event.Codec
		if event.Codec.Type != "" {
			stream.Type = event.Codec.Type
		}
	}
	return true
}
