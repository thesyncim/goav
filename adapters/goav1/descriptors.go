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
			PixelFormats: []string{
				av.PixelFormatI420,
				av.PixelFormatYUV420P,
				av.PixelFormatI422,
				av.PixelFormatYUV422P,
				av.PixelFormatI444,
				av.PixelFormatYUV444P,
				av.PixelFormatGray8,
			},
			RTPPayloads: []string{av.MIMEAV1},
		},
		Backend: codec.Backend{
			Name:    "goav1",
			Module:  "github.com/thesyncim/goav1",
			Package: "github.com/thesyncim/goav/adapters/goav1",
			Status:  "active",
		},
	}
}

func EncoderDescriptor() codec.Descriptor {
	return codec.Descriptor{
		ID:           av.CodecAV1,
		Name:         "AV1",
		Type:         av.MediaVideo,
		Modes:        []codec.Mode{codec.ModeEncode},
		Realtime:     true,
		Experimental: true,
		Capabilities: codec.Capabilities{
			PixelFormats: []string{
				av.PixelFormatI420,
				av.PixelFormatYUV420P,
			},
			RTPPayloads: []string{av.MIMEAV1},
		},
		Backend: codec.Backend{
			Name:    "goav1-encoder",
			Module:  "github.com/thesyncim/goav1",
			Package: "github.com/thesyncim/goav/adapters/goav1",
			Status:  "active",
		},
	}
}

func Descriptors() []codec.Descriptor {
	return []codec.Descriptor{Descriptor(), EncoderDescriptor()}
}
