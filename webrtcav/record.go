package webrtcav

import (
	"maps"

	"github.com/thesyncim/goav/av"
)

// Reader/observer boundaries own independent configuration values. Packet
// maps are already immutable, but event memory may be reused by the reader.
func cloneReadEvent(event *av.Event) *av.Event {
	if event == nil {
		return nil
	}
	out := *event
	out.Metadata = maps.Clone(event.Metadata)
	if event.Stream != nil {
		stream := cloneReadStream(*event.Stream)
		out.Stream = &stream
	}
	if event.Codec != nil {
		codec := cloneReadCodec(*event.Codec)
		out.Codec = &codec
	}
	return &out
}
func cloneReadStream(stream av.Stream) av.Stream {
	stream.Metadata = maps.Clone(stream.Metadata)
	stream.Codec = cloneReadCodec(stream.Codec)
	return stream
}
func cloneReadCodec(codec av.CodecParameters) av.CodecParameters {
	codec.Attributes = maps.Clone(codec.Attributes)
	codec.ExtraData = av.Buffer{Bytes: append([]byte(nil), codec.ExtraData.Bytes...)}
	return codec
}
