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

	idTracks       ebml.ID = 0x1654ae6b
	idTrackEntry   ebml.ID = 0xae
	idTrackNumber  ebml.ID = 0xd7
	idTrackUID     ebml.ID = 0x73c5
	idTrackType    ebml.ID = 0x83
	idFlagEnabled  ebml.ID = 0xb9
	idFlagDefault  ebml.ID = 0x88
	idName         ebml.ID = 0x536e
	idLanguage     ebml.ID = 0x22b59c
	idCodecID      ebml.ID = 0x86
	idCodecPrivate ebml.ID = 0x63a2
	idVideo        ebml.ID = 0xe0
	idPixelWidth   ebml.ID = 0xb0
	idPixelHeight  ebml.ID = 0xba
	idAudio        ebml.ID = 0xe1
	idSamplingFreq ebml.ID = 0xb5
	idChannels     ebml.ID = 0x9f
	idBitDepth     ebml.ID = 0x6264
	idDefaultDur   ebml.ID = 0x23e383

	idCluster       ebml.ID = 0x1f43b675
	idTimestamp     ebml.ID = 0xe7
	idSimpleBlock   ebml.ID = 0xa3
	idBlockGroup    ebml.ID = 0xa0
	idBlock         ebml.ID = 0xa1
	idBlockDuration ebml.ID = 0x9b
	idReferenceBlk  ebml.ID = 0xfb

	idCues               ebml.ID = 0x1c53bb6b
	idCuePoint           ebml.ID = 0xbb
	idCueTime            ebml.ID = 0xb3
	idCueTrackPositions  ebml.ID = 0xb7
	idCueTrack           ebml.ID = 0xf7
	idCueClusterPosition ebml.ID = 0xf1
)

const (
	matroskaTrackVideo = 1
	matroskaTrackAudio = 2

	simpleBlockKeyframe    = 0x80
	simpleBlockInvisible   = 0x08
	simpleBlockLacingMask  = 0x06
	simpleBlockDiscardable = 0x01
)
