package matroska

import "github.com/thesyncim/goav/container/ebml"

const (
	idEBML               ebml.ID = 0x1a45dfa3
	idEBMLVersion        ebml.ID = 0x4286
	idEBMLReadVersion    ebml.ID = 0x42f7
	idEBMLMaxIDLength    ebml.ID = 0x42f2
	idEBMLMaxSizeLength  ebml.ID = 0x42f3
	idDocType            ebml.ID = 0x4282
	idDocTypeVersion     ebml.ID = 0x4287
	idDocTypeReadVersion ebml.ID = 0x4285
	idCRC32              ebml.ID = 0xbf
	idVoid               ebml.ID = 0xec

	idSegment        ebml.ID = 0x18538067
	idSeekHead       ebml.ID = 0x114d9b74
	idSeek           ebml.ID = 0x4dbb
	idSeekID         ebml.ID = 0x53ab
	idSeekPosition   ebml.ID = 0x53ac
	idInfo           ebml.ID = 0x1549a966
	idTimestampScale ebml.ID = 0x2ad7b1
	idDuration       ebml.ID = 0x4489
	idMuxingApp      ebml.ID = 0x4d80
	idWritingApp     ebml.ID = 0x5741

	idTracks               ebml.ID = 0x1654ae6b
	idTrackEntry           ebml.ID = 0xae
	idTrackNumber          ebml.ID = 0xd7
	idTrackUID             ebml.ID = 0x73c5
	idTrackType            ebml.ID = 0x83
	idFlagEnabled          ebml.ID = 0xb9
	idFlagDefault          ebml.ID = 0x88
	idFlagForced           ebml.ID = 0x55aa
	idFlagHearingImpaired  ebml.ID = 0x55ab
	idFlagVisualImpaired   ebml.ID = 0x55ac
	idFlagTextDescriptions ebml.ID = 0x55ad
	idFlagOriginal         ebml.ID = 0x55ae
	idFlagCommentary       ebml.ID = 0x55af
	idFlagLacing           ebml.ID = 0x9c
	idName                 ebml.ID = 0x536e
	idLanguage             ebml.ID = 0x22b59c
	idLanguageBCP          ebml.ID = 0x22b59d
	idCodecID              ebml.ID = 0x86
	idCodecName            ebml.ID = 0x258688
	idCodecPrivate         ebml.ID = 0x63a2
	idCodecDelay           ebml.ID = 0x56aa
	idSeekPreRoll          ebml.ID = 0x56bb
	idVideo                ebml.ID = 0xe0
	idStereoMode           ebml.ID = 0x53b8
	idAlphaMode            ebml.ID = 0x53c0
	idPixelWidth           ebml.ID = 0xb0
	idPixelHeight          ebml.ID = 0xba
	idPixelCropBottom      ebml.ID = 0x54aa
	idPixelCropTop         ebml.ID = 0x54bb
	idPixelCropLeft        ebml.ID = 0x54cc
	idPixelCropRight       ebml.ID = 0x54dd
	idDisplayWidth         ebml.ID = 0x54b0
	idDisplayHeight        ebml.ID = 0x54ba
	idDisplayUnit          ebml.ID = 0x54b2
	idColour               ebml.ID = 0x55b0
	idMatrixCoefficients   ebml.ID = 0x55b1
	idBitsPerChannel       ebml.ID = 0x55b2
	idChromaSubsampleHorz  ebml.ID = 0x55b3
	idChromaSubsampleVert  ebml.ID = 0x55b4
	idCbSubsampleHorz      ebml.ID = 0x55b5
	idCbSubsampleVert      ebml.ID = 0x55b6
	idChromaSitingHorz     ebml.ID = 0x55b7
	idChromaSitingVert     ebml.ID = 0x55b8
	idColourRange          ebml.ID = 0x55b9
	idTransferChar         ebml.ID = 0x55ba
	idPrimaries            ebml.ID = 0x55bb
	idMaxCLL               ebml.ID = 0x55bc
	idMaxFALL              ebml.ID = 0x55bd
	idMasteringMetadata    ebml.ID = 0x55d0
	idPrimaryRX            ebml.ID = 0x55d1
	idPrimaryRY            ebml.ID = 0x55d2
	idPrimaryGX            ebml.ID = 0x55d3
	idPrimaryGY            ebml.ID = 0x55d4
	idPrimaryBX            ebml.ID = 0x55d5
	idPrimaryBY            ebml.ID = 0x55d6
	idWhitePointX          ebml.ID = 0x55d7
	idWhitePointY          ebml.ID = 0x55d8
	idLuminanceMax         ebml.ID = 0x55d9
	idLuminanceMin         ebml.ID = 0x55da
	idProjection           ebml.ID = 0x7670
	idProjectionType       ebml.ID = 0x7671
	idProjectionPrivate    ebml.ID = 0x7672
	idProjectionPoseYaw    ebml.ID = 0x7673
	idProjectionPosePitch  ebml.ID = 0x7674
	idProjectionPoseRoll   ebml.ID = 0x7675
	idAudio                ebml.ID = 0xe1
	idSamplingFreq         ebml.ID = 0xb5
	idOutputFreq           ebml.ID = 0x78b5
	idChannels             ebml.ID = 0x9f
	idBitDepth             ebml.ID = 0x6264
	idDefaultDur           ebml.ID = 0x23e383
	idDefaultDecodedDur    ebml.ID = 0x234e7a

	idCluster       ebml.ID = 0x1f43b675
	idTimestamp     ebml.ID = 0xe7
	idSimpleBlock   ebml.ID = 0xa3
	idBlockGroup    ebml.ID = 0xa0
	idBlock         ebml.ID = 0xa1
	idBlockDuration ebml.ID = 0x9b
	idReferenceBlk  ebml.ID = 0xfb
	idDiscardPad    ebml.ID = 0x75a2

	idCues               ebml.ID = 0x1c53bb6b
	idCuePoint           ebml.ID = 0xbb
	idCueTime            ebml.ID = 0xb3
	idCueTrackPositions  ebml.ID = 0xb7
	idCueTrack           ebml.ID = 0xf7
	idCueClusterPosition ebml.ID = 0xf1
	idCueRelativePos     ebml.ID = 0xf0
)

const (
	matroskaTrackVideo = 1
	matroskaTrackAudio = 2

	simpleBlockKeyframe    = 0x80
	simpleBlockInvisible   = 0x08
	simpleBlockLacingXiph  = 0x02
	simpleBlockLacingEBML  = 0x04
	simpleBlockLacingFixed = 0x06
	simpleBlockLacingMask  = 0x06
	simpleBlockDiscardable = 0x01
)
