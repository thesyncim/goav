package matroska

import (
	"encoding/binary"

	"github.com/thesyncim/goav/av"
)

const (
	codecIDOpus = "A_OPUS"
	codecIDMS   = "A_MS/ACM"
	codecIDVP8  = "V_VP8"
	codecIDVP9  = "V_VP9"
	codecIDAV1  = "V_AV1"
	codecIDH264 = "V_MPEG4/ISO/AVC"
	codecIDH265 = "V_MPEGH/ISO/HEVC"
)

func matroskaCodecID(codec Codec) (string, error) {
	switch codec {
	case CodecOpus:
		return codecIDOpus, nil
	case CodecPCMU, CodecPCMA:
		return codecIDMS, nil
	case CodecVP8:
		return codecIDVP8, nil
	case CodecVP9:
		return codecIDVP9, nil
	case CodecAV1:
		return codecIDAV1, nil
	case CodecH264:
		return codecIDH264, nil
	case CodecH265:
		return codecIDH265, nil
	default:
		return "", ErrUnsupportedCodec
	}
}

func codecFromMatroskaID(id string, private []byte) Codec {
	switch id {
	case codecIDOpus:
		return CodecOpus
	case codecIDVP8:
		return CodecVP8
	case codecIDVP9:
		return CodecVP9
	case codecIDAV1:
		return CodecAV1
	case codecIDH264:
		return CodecH264
	case codecIDH265:
		return CodecH265
	case codecIDMS:
		if len(private) >= 2 {
			switch binary.LittleEndian.Uint16(private[:2]) {
			case 0x0007:
				return CodecPCMU
			case 0x0006:
				return CodecPCMA
			}
		}
	}
	return CodecUnknown
}

func codecFromAV(id av.CodecID) Codec {
	switch id {
	case av.CodecOpus:
		return CodecOpus
	case av.CodecVP8:
		return CodecVP8
	case av.CodecVP9:
		return CodecVP9
	case av.CodecAV1:
		return CodecAV1
	case av.CodecH264:
		return CodecH264
	case av.CodecPCM:
		return CodecUnknown
	default:
		return CodecUnknown
	}
}

func codecToAV(codec Codec) av.CodecID {
	switch codec {
	case CodecOpus:
		return av.CodecOpus
	case CodecVP8:
		return av.CodecVP8
	case CodecVP9:
		return av.CodecVP9
	case CodecAV1:
		return av.CodecAV1
	case CodecH264:
		return av.CodecH264
	case CodecPCMU, CodecPCMA:
		return av.CodecPCM
	default:
		return av.CodecUnknown
	}
}

func defaultCodecPrivate(track Track, scratch *[18]byte) []byte {
	switch track.Codec {
	case CodecPCMU, CodecPCMA:
		tag := uint16(0x0007)
		if track.Codec == CodecPCMA {
			tag = 0x0006
		}
		channels := uint16(track.Audio.Channels)
		if channels == 0 {
			channels = 1
		}
		sampleRate := uint32(track.Audio.SampleRate)
		if sampleRate == 0 {
			sampleRate = 8000
		}
		binary.LittleEndian.PutUint16(scratch[0:2], tag)
		binary.LittleEndian.PutUint16(scratch[2:4], channels)
		binary.LittleEndian.PutUint32(scratch[4:8], sampleRate)
		binary.LittleEndian.PutUint32(scratch[8:12], sampleRate*uint32(channels))
		binary.LittleEndian.PutUint16(scratch[12:14], channels)
		binary.LittleEndian.PutUint16(scratch[14:16], 8)
		binary.LittleEndian.PutUint16(scratch[16:18], 0)
		return scratch[:]
	default:
		return nil
	}
}
