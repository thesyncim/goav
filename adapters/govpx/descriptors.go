package govpx

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

const backendName = "govpx"

func Descriptors() []codec.Descriptor {
	return []codec.Descriptor{
		descriptor(av.CodecVP8, "VP8", []string{"i420"}, []string{"video/vp8"}),
		descriptor(av.CodecVP9, "VP9", []string{"i420"}, []string{"video/vp9"}),
	}
}

func Register(registry *codec.SimpleRegistry) {
	descriptors := Descriptors()
	for i := range descriptors {
		registry.RegisterDescriptor(descriptors[i])
	}
}

func descriptor(id av.CodecID, name string, pixelFormats []string, rtpPayloads []string) codec.Descriptor {
	return codec.Descriptor{
		ID:           id,
		Name:         name,
		Type:         av.MediaVideo,
		Modes:        []codec.Mode{codec.ModeDecode, codec.ModeEncode},
		Realtime:     true,
		Experimental: true,
		Capabilities: codec.Capabilities{
			CodecID:      id,
			Type:         av.MediaVideo,
			Decode:       true,
			Encode:       true,
			Realtime:     true,
			PixelFormats: pixelFormats,
			RTPPayloads:  rtpPayloads,
			Experimental: true,
		},
		Backend: codec.Backend{
			Name:    backendName,
			Module:  "github.com/thesyncim/govpx",
			Package: "github.com/thesyncim/goav/adapters/govpx",
			Status:  "planned",
		},
	}
}
