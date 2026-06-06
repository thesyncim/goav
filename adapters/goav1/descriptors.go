package goav1

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

func Descriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:           av.CodecAV1,
		Name:         "AV1",
		Type:         av.MediaVideo,
		Modes:        []codec.Mode{codec.ModeDecode},
		Realtime:     true,
		Experimental: true,
		Capabilities: codec.Capabilities{
			CodecID:      av.CodecAV1,
			Type:         av.MediaVideo,
			Decode:       true,
			Realtime:     true,
			PixelFormats: []string{av.PixelFormatI420, av.PixelFormatGray8},
			RTPPayloads:  []string{"video/av1"},
			BuildTags:    []string{"goav_goav1"},
			Experimental: true,
		},
		Backend: codec.Backend{
			Name:    "goav1",
			Module:  "github.com/thesyncim/goav1",
			Package: "github.com/thesyncim/goav/adapters/goav1",
			Status:  "planned-build-tagged",
		},
	}
}
