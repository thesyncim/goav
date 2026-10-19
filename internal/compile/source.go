package compile

import (
	"cmp"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
)

func NormalizeCustomSourceShape(name string, spec shape.Spec) shape.Spec {
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	if spec.StreamID == "" {
		spec.StreamID = av.StreamID(cmp.Or(name, string(spec.MediaKind), "stream"))
	}
	return spec
}

// fillStreamCodecParameters overlays shape-derived codec parameters onto a
// declared source stream: the declared stream stays authoritative, and the
// source shape fills only facts the stream left open.
func FillStreamCodecParameters(stream *av.Stream, spec codec.Spec) {
	if spec.ID != "" && stream.Codec.ID != "" && spec.ID != stream.Codec.ID {
		return
	}
	params := spec.Parameters
	if stream.Codec.ID == "" {
		stream.Codec.ID = firstCodecID(spec.ID, params.ID)
	}
	if stream.Codec.SampleRate == 0 {
		stream.Codec.SampleRate = params.SampleRate
	}
	if stream.Codec.Channels == 0 {
		stream.Codec.Channels = params.Channels
	}
	if stream.Codec.SampleFormat == "" {
		stream.Codec.SampleFormat = params.SampleFormat
	}
	if stream.Codec.ChannelLayout == "" {
		stream.Codec.ChannelLayout = params.ChannelLayout
	}
	if stream.Codec.Width == 0 {
		stream.Codec.Width = params.Width
	}
	if stream.Codec.Height == 0 {
		stream.Codec.Height = params.Height
	}
	if stream.Codec.PixelFormat == "" {
		stream.Codec.PixelFormat = params.PixelFormat
	}
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = params.ClockRate
	}
}

// firstCodecID returns the first non-empty codec id.
func firstCodecID(ids ...av.CodecID) av.CodecID {
	for _, id := range ids {
		if id != "" {
			return id
		}
	}
	return ""
}
