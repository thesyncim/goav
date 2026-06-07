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
type ContentEncoding = matroska.ContentEncoding
type ContentCompression = matroska.ContentCompression
type ContentEncryption = matroska.ContentEncryption
type ContentEncAESSettings = matroska.ContentEncAESSettings
type ContentEncryptionKey = matroska.ContentEncryptionKey
type SegmentInfo = matroska.SegmentInfo
type Track = matroska.Track
type Packet = matroska.Packet
type LacedPacket = matroska.LacedPacket
type CuePoint = matroska.CuePoint
type CueTrackPosition = matroska.CueTrackPosition
type CueReference = matroska.CueReference
type SeekEntry = matroska.SeekEntry
type UnknownElement = matroska.UnknownElement

type LacingMode = matroska.LacingMode

const (
	LacingAuto  = matroska.LacingAuto
	LacingXiph  = matroska.LacingXiph
	LacingEBML  = matroska.LacingEBML
	LacingFixed = matroska.LacingFixed
)

type CuePolicy = matroska.CuePolicy

const (
	CuePolicyDefault    = matroska.CuePolicyDefault
	CuePolicyKeyframes  = matroska.CuePolicyKeyframes
	CuePolicyAllPackets = matroska.CuePolicyAllPackets
	CuePolicyNone       = matroska.CuePolicyNone
)

const (
	ContentEncodingScopeBlock   = matroska.ContentEncodingScopeBlock
	ContentEncodingScopePrivate = matroska.ContentEncodingScopePrivate
	ContentEncodingScopeNext    = matroska.ContentEncodingScopeNext

	ContentEncodingTypeCompression = matroska.ContentEncodingTypeCompression
	ContentEncodingTypeEncryption  = matroska.ContentEncodingTypeEncryption

	ContentCompAlgoZlib            = matroska.ContentCompAlgoZlib
	ContentCompAlgoBzlib           = matroska.ContentCompAlgoBzlib
	ContentCompAlgoLZO1X           = matroska.ContentCompAlgoLZO1X
	ContentCompAlgoHeaderStripping = matroska.ContentCompAlgoHeaderStripping

	ContentEncAlgoNotEncrypted = matroska.ContentEncAlgoNotEncrypted
	ContentEncAlgoDES          = matroska.ContentEncAlgoDES
	ContentEncAlgoTripleDES    = matroska.ContentEncAlgoTripleDES
	ContentEncAlgoTwofish      = matroska.ContentEncAlgoTwofish
	ContentEncAlgoBlowfish     = matroska.ContentEncAlgoBlowfish
	ContentEncAlgoAES          = matroska.ContentEncAlgoAES

	ContentEncAESCipherModeCTR = matroska.ContentEncAESCipherModeCTR
	ContentEncAESCipherModeCBC = matroska.ContentEncAESCipherModeCBC

	ContentSigAlgoNotSigned = matroska.ContentSigAlgoNotSigned
	ContentSigAlgoRSA       = matroska.ContentSigAlgoRSA

	ContentSigHashAlgoNotSigned = matroska.ContentSigHashAlgoNotSigned
	ContentSigHashAlgoSHA1      = matroska.ContentSigHashAlgoSHA1
	ContentSigHashAlgoMD5       = matroska.ContentSigHashAlgoMD5
)

type MuxerOptions struct {
	MuxingApp                  string
	WritingApp                 string
	Info                       SegmentInfo
	TimecodeScaleNS            int64
	ClusterMaxDurationNS       int64
	Streaming                  bool
	CuePolicy                  CuePolicy
	UnknownSegmentElements     []UnknownElement
	UnknownTracksElements      []UnknownElement
	ContentEncryptionKeys      []ContentEncryptionKey
	ContentEncryptionInitialIV []byte
}

type DemuxerOptions = matroska.DemuxerOptions

func matroskaOptions(opts MuxerOptions) matroska.MuxerOptions {
	cuePolicy := opts.CuePolicy
	if cuePolicy == CuePolicyDefault {
		cuePolicy = CuePolicyKeyframes
	}
	return matroska.MuxerOptions{
		DocType:                    "webm",
		DocTypeVersion:             4,
		DocTypeReadVersion:         2,
		MuxingApp:                  opts.MuxingApp,
		WritingApp:                 opts.WritingApp,
		Info:                       opts.Info,
		TimecodeScaleNS:            opts.TimecodeScaleNS,
		ClusterMaxDurationNS:       opts.ClusterMaxDurationNS,
		Streaming:                  opts.Streaming,
		CuePolicy:                  cuePolicy,
		UnknownSegmentElements:     opts.UnknownSegmentElements,
		UnknownTracksElements:      opts.UnknownTracksElements,
		ContentEncryptionKeys:      opts.ContentEncryptionKeys,
		ContentEncryptionInitialIV: opts.ContentEncryptionInitialIV,
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
		if err := validateVideoMetadata(track.Video); err != nil {
			return err
		}
	default:
		return ErrUnsupportedWebMCodec
	}
	if err := validateCodecPrivate(track); err != nil {
		return err
	}
	if err := validateContentEncodings(track.ContentEncodings); err != nil {
		return err
	}
	return nil
}

func validateVideoMetadata(video VideoConfig) error {
	if video.DisplayUnit != 0 {
		return ErrUnsupportedWebMTrackMetadata
	}
	return nil
}

func validateCodecPrivate(track Track) error {
	switch track.Codec {
	case CodecVP8:
		if len(track.CodecPrivate) != 0 {
			return ErrUnsupportedWebMCodecPrivate
		}
	case CodecVP9:
		if len(track.CodecPrivate) != 0 {
			return validateVP9CodecPrivate(track.CodecPrivate)
		}
	}
	return nil
}

const (
	vp9CodecPrivateFeatureProfile           = 1
	vp9CodecPrivateFeatureLevel             = 2
	vp9CodecPrivateFeatureBitDepth          = 3
	vp9CodecPrivateFeatureChromaSubsampling = 4
	vp9CodecPrivateFeatureReservedMask      = 0x80
)

func validateVP9CodecPrivate(private []byte) error {
	var seen [vp9CodecPrivateFeatureChromaSubsampling + 1]bool
	for len(private) != 0 {
		if len(private) < 2 {
			return ErrUnsupportedWebMCodecPrivate
		}
		idByte := private[0]
		if idByte&vp9CodecPrivateFeatureReservedMask != 0 {
			return ErrUnsupportedWebMCodecPrivate
		}
		id := int(idByte)
		length := int(private[1])
		private = private[2:]
		if id < vp9CodecPrivateFeatureProfile ||
			id > vp9CodecPrivateFeatureChromaSubsampling ||
			seen[id] ||
			length != 1 ||
			len(private) < length {
			return ErrUnsupportedWebMCodecPrivate
		}
		value := private[0]
		private = private[length:]
		seen[id] = true
		switch id {
		case vp9CodecPrivateFeatureProfile:
			if value > 3 {
				return ErrUnsupportedWebMCodecPrivate
			}
		case vp9CodecPrivateFeatureLevel:
			if !validVP9CodecPrivateLevel(value) {
				return ErrUnsupportedWebMCodecPrivate
			}
		case vp9CodecPrivateFeatureBitDepth:
			if value != 8 && value != 10 && value != 12 {
				return ErrUnsupportedWebMCodecPrivate
			}
		case vp9CodecPrivateFeatureChromaSubsampling:
			if value > 3 {
				return ErrUnsupportedWebMCodecPrivate
			}
		}
	}
	return nil
}

func validVP9CodecPrivateLevel(value byte) bool {
	switch value {
	case 10, 11, 20, 21, 30, 31, 40, 41, 50, 51, 52, 60, 61, 62:
		return true
	default:
		return false
	}
}

func validateContentEncodings(encodings []ContentEncoding) error {
	for i := range encodings {
		if err := validateContentEncoding(encodings[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateContentEncoding(encoding ContentEncoding) error {
	scope := encoding.Scope
	if scope == 0 {
		scope = ContentEncodingScopeBlock
	}
	if scope != ContentEncodingScopeBlock {
		return ErrUnsupportedWebMContentEncoding
	}
	if encoding.Type != ContentEncodingTypeEncryption ||
		!encoding.EncryptionSet ||
		encoding.CompressionSet {
		return ErrUnsupportedWebMContentEncoding
	}
	return validateContentEncryption(encoding.Encryption)
}

func validateContentEncryption(encryption ContentEncryption) error {
	if encryption.Algorithm != ContentEncAlgoAES ||
		!encryption.AESSettingsSet ||
		encryption.AESSettings.CipherMode != ContentEncAESCipherModeCTR ||
		len(encryption.Signature) != 0 ||
		len(encryption.SignatureKeyID) != 0 ||
		encryption.SignatureAlgorithm != ContentSigAlgoNotSigned ||
		encryption.SignatureHashAlgorithm != ContentSigHashAlgoNotSigned {
		return ErrUnsupportedWebMContentEncoding
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
