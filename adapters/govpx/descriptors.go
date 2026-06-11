package govpx

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

const backendName = "govpx"

func Descriptors() []codec.Descriptor {
	return []codec.Descriptor{
		descriptor(av.CodecVP8, "VP8", []string{av.PixelFormatI420}, []string{av.MIMEVP8}),
		descriptor(av.CodecVP9, "VP9", []string{av.PixelFormatI420}, []string{av.MIMEVP9}),
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
			PixelFormats: pixelFormats,
			RTPPayloads:  rtpPayloads,
		},
		Backend: codec.Backend{
			Name:    backendName,
			Module:  "github.com/thesyncim/govpx",
			Package: "github.com/thesyncim/goav/adapters/govpx",
			Status:  "active",
		},
	}
}
