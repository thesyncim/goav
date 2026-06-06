package webm

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/matroska"
)

type Codec = matroska.Codec

const (
	CodecOpus = matroska.CodecOpus
	CodecVP8  = matroska.CodecVP8
	CodecVP9  = matroska.CodecVP9
	CodecAV1  = matroska.CodecAV1
)

type TrackType = matroska.TrackType

const (
	TrackAudio = matroska.TrackAudio
	TrackVideo = matroska.TrackVideo
)

type AudioConfig = matroska.AudioConfig
type VideoConfig = matroska.VideoConfig
type VideoColourConfig = matroska.VideoColourConfig
type VideoMasteringMetadataConfig = matroska.VideoMasteringMetadataConfig
type VideoProjectionConfig = matroska.VideoProjectionConfig
type Track = matroska.Track
type Packet = matroska.Packet
type LacedPacket = matroska.LacedPacket

type LacingMode = matroska.LacingMode

const (
	LacingAuto  = matroska.LacingAuto
	LacingXiph  = matroska.LacingXiph
	LacingEBML  = matroska.LacingEBML
	LacingFixed = matroska.LacingFixed
)

type MuxerOptions struct {
	MuxingApp            string
	WritingApp           string
	TimecodeScaleNS      int64
	ClusterMaxDurationNS int64
	Streaming            bool
}

type DemuxerOptions = matroska.DemuxerOptions

func matroskaOptions(opts MuxerOptions) matroska.MuxerOptions {
	return matroska.MuxerOptions{
		DocType:              "webm",
		DocTypeVersion:       4,
		DocTypeReadVersion:   2,
		MuxingApp:            opts.MuxingApp,
		WritingApp:           opts.WritingApp,
		TimecodeScaleNS:      opts.TimecodeScaleNS,
		ClusterMaxDurationNS: opts.ClusterMaxDurationNS,
		Streaming:            opts.Streaming,
	}
}

func validateTrack(track Track) error {
	switch track.Codec {
	case matroska.CodecOpus:
		if track.Type != matroska.TrackAudio {
			return ErrUnsupportedWebMCodec
		}
	case matroska.CodecVP8, matroska.CodecVP9, matroska.CodecAV1:
		if track.Type != matroska.TrackVideo {
			return ErrUnsupportedWebMCodec
		}
	default:
		return ErrUnsupportedWebMCodec
	}
	return nil
}

func codecFromAV(id av.CodecID) matroska.Codec {
	switch id {
	case av.CodecOpus:
		return matroska.CodecOpus
	case av.CodecVP8:
		return matroska.CodecVP8
	case av.CodecVP9:
		return matroska.CodecVP9
	case av.CodecAV1:
		return matroska.CodecAV1
	default:
		return matroska.CodecUnknown
	}
}

func codecToAV(codec matroska.Codec) av.CodecID {
	switch codec {
	case matroska.CodecOpus:
		return av.CodecOpus
	case matroska.CodecVP8:
		return av.CodecVP8
	case matroska.CodecVP9:
		return av.CodecVP9
	case matroska.CodecAV1:
		return av.CodecAV1
	default:
		return av.CodecUnknown
	}
}
