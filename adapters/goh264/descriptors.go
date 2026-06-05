package goh264

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func Descriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:           av.CodecH264,
		Name:         "H264",
		Type:         av.MediaVideo,
		Modes:        []codec.Mode{codec.ModeDecode},
		Realtime:     true,
		Experimental: true,
		Capabilities: codec.Capabilities{
			CodecID:      av.CodecH264,
			Type:         av.MediaVideo,
			Decode:       true,
			Realtime:     true,
			PixelFormats: []string{"i420"},
			RTPPayloads:  []string{"video/h264"},
			BuildTags:    []string{"goav_goh264"},
			Experimental: true,
		},
		Backend: codec.Backend{
			Name:    "goh264",
			Module:  "github.com/thesyncim/goh264",
			Package: "github.com/thesyncim/goav/adapters/goh264",
			Status:  "planned-build-tagged",
		},
	}
}

func Register(registry *codec.SimpleRegistry) {
	registry.RegisterDescriptor(Descriptor())
}
