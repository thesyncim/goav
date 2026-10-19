package flv

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// Register installs the FLV demuxer and muxer for av.FormatFLV. The default
// prober already detects FLV (its signature and the .flv extension), so no
// prober is registered here.
func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDemuxerDescriptor(Descriptor(), DemuxerFactory{})
	registry.RegisterMuxerDescriptor(Descriptor(), MuxerFactory{})
}

// Descriptor declares what FLV carries: audio and video in the codecs the
// legacy and enhanced tag headers name.
func Descriptor() format.Descriptor {
	return format.Descriptor{
		Format: av.FormatFLV,
		Media:  []av.MediaType{av.MediaAudio, av.MediaVideo},
		Codecs: []av.CodecID{
			av.CodecH264,
			av.CodecHEVC,
			av.CodecAV1,
			av.CodecVP9,
			av.CodecVP8,
			av.CodecAAC,
			av.CodecOpus,
			av.CodecFLAC,
		},
		MinStreams: 1,
		Metadata: av.Metadata{
			"summary": "FLV reads and writes H.264 and AAC tags, and Enhanced RTMP (E-FLV) HEVC, AV1, VP9, VP8, Opus, FLAC, and multitrack",
		},
	}
}
