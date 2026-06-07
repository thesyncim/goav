package matroska

import "time"

type Codec int

const (
	CodecUnknown Codec = iota
	CodecOpus
	CodecPCMU
	CodecPCMA
	CodecVP8
	CodecVP9
	CodecAV1
	CodecH264
	CodecH265
)

type TrackType int

const (
	TrackUnknown TrackType = iota
	TrackAudio
	TrackVideo
)

type AudioConfig struct {
	SampleRate       int
	OutputSampleRate int
	Channels         int
	BitDepth         int
}

type VideoConfig struct {
	Width              int
	Height             int
	FlagInterlaced     int
	FlagInterlacedSet  bool
	FieldOrder         int
	FieldOrderSet      bool
	StereoMode         int
	StereoModeSet      bool
	AlphaMode          int
	AlphaModeSet       bool
	PixelCropBottom    int
	PixelCropTop       int
	PixelCropLeft      int
	PixelCropRight     int
	DisplayWidth       int
	DisplayHeight      int
	DisplayUnit        int
	AspectRatioType    int
	AspectRatioTypeSet bool
	Colour             VideoColourConfig
	Projection         VideoProjectionConfig
}

type VideoColourConfig struct {
	MatrixCoefficients         int
	MatrixCoefficientsSet      bool
	BitsPerChannel             int
	BitsPerChannelSet          bool
	ChromaSubsamplingHorz      int
	ChromaSubsamplingHorzSet   bool
	ChromaSubsamplingVert      int
	ChromaSubsamplingVertSet   bool
	CbSubsamplingHorz          int
	CbSubsamplingHorzSet       bool
	CbSubsamplingVert          int
	CbSubsamplingVertSet       bool
	ChromaSitingHorz           int
	ChromaSitingHorzSet        bool
	ChromaSitingVert           int
	ChromaSitingVertSet        bool
	Range                      int
	RangeSet                   bool
	TransferCharacteristics    int
	TransferCharacteristicsSet bool
	Primaries                  int
	PrimariesSet               bool
	MaxCLL                     int
	MaxCLLSet                  bool
	MaxFALL                    int
	MaxFALLSet                 bool
	MasteringMetadata          VideoMasteringMetadataConfig
}

type VideoMasteringMetadataConfig struct {
	PrimaryRChromaticityX      float64
	PrimaryRChromaticityXSet   bool
	PrimaryRChromaticityY      float64
	PrimaryRChromaticityYSet   bool
	PrimaryGChromaticityX      float64
	PrimaryGChromaticityXSet   bool
	PrimaryGChromaticityY      float64
	PrimaryGChromaticityYSet   bool
	PrimaryBChromaticityX      float64
	PrimaryBChromaticityXSet   bool
	PrimaryBChromaticityY      float64
	PrimaryBChromaticityYSet   bool
	WhitePointChromaticityX    float64
	WhitePointChromaticityXSet bool
	WhitePointChromaticityY    float64
	WhitePointChromaticityYSet bool
	LuminanceMax               float64
	LuminanceMaxSet            bool
	LuminanceMin               float64
	LuminanceMinSet            bool
}

type VideoProjectionConfig struct {
	Set       bool
	Type      int
	Private   []byte
	PoseYaw   float64
	PosePitch float64
	PoseRoll  float64
}

type Track struct {
	ID                            uint32
	UID                           uint64
	Type                          TrackType
	Codec                         Codec
	Name                          string
	Language                      string
	LanguageBCP47                 string
	CodecName                     string
	TimebaseNum                   int64
	TimebaseDen                   int64
	DefaultDurationNS             int64
	DefaultDecodedFieldDurationNS int64
	MinCache                      uint64
	MinCacheSet                   bool
	MaxCache                      uint64
	MaxCacheSet                   bool
	CodecDecodeAll                bool
	CodecDecodeAllSet             bool
	CodecDelayNS                  int64
	SeekPreRollNS                 int64
	FlagEnabled                   bool
	FlagEnabledSet                bool
	FlagDefault                   bool
	FlagDefaultSet                bool
	FlagForced                    bool
	FlagForcedSet                 bool
	FlagHearingImpaired           bool
	FlagHearingImpairedSet        bool
	FlagVisualImpaired            bool
	FlagVisualImpairedSet         bool
	FlagTextDescriptions          bool
	FlagTextDescriptionsSet       bool
	FlagOriginal                  bool
	FlagOriginalSet               bool
	FlagCommentary                bool
	FlagCommentarySet             bool
	FlagLacing                    bool
	FlagLacingSet                 bool
	MaxBlockAdditionID            uint64
	BlockAdditionMappings         []BlockAdditionMapping
	TrackOverlays                 []uint64
	TrackTranslates               []TrackTranslate
	ContentEncodings              []ContentEncoding
	Audio                         AudioConfig
	Video                         VideoConfig

	CodecPrivate []byte
}

type BlockAdditionMapping struct {
	IDValue   uint64
	Name      string
	Type      uint64
	ExtraData []byte
}

type TrackTranslate struct {
	TrackID     []byte
	Codec       uint64
	EditionUIDs []uint64
}

type ContentEncoding struct {
	Order          uint64
	Scope          uint64
	Type           uint64
	Compression    ContentCompression
	CompressionSet bool
	Encryption     ContentEncryption
	EncryptionSet  bool
}

type ContentCompression struct {
	Algorithm uint64
	Settings  []byte
}

type ContentEncryption struct {
	Algorithm              uint64
	KeyID                  []byte
	AESSettings            ContentEncAESSettings
	AESSettingsSet         bool
	Signature              []byte
	SignatureKeyID         []byte
	SignatureAlgorithm     uint64
	SignatureHashAlgorithm uint64
}

type ContentEncAESSettings struct {
	CipherMode uint64
}

type ContentEncryptionKey struct {
	KeyID []byte
	Key   []byte
}

const (
	ContentEncodingScopeBlock   uint64 = 1
	ContentEncodingScopePrivate uint64 = 2
	ContentEncodingScopeNext    uint64 = 4

	ContentEncodingTypeCompression uint64 = 0
	ContentEncodingTypeEncryption  uint64 = 1

	ContentCompAlgoZlib            uint64 = 0
	ContentCompAlgoBzlib           uint64 = 1
	ContentCompAlgoLZO1X           uint64 = 2
	ContentCompAlgoHeaderStripping uint64 = 3

	ContentEncAlgoNotEncrypted uint64 = 0
	ContentEncAlgoDES          uint64 = 1
	ContentEncAlgoTripleDES    uint64 = 2
	ContentEncAlgoTwofish      uint64 = 3
	ContentEncAlgoBlowfish     uint64 = 4
	ContentEncAlgoAES          uint64 = 5

	ContentEncAESCipherModeCTR uint64 = 1
	ContentEncAESCipherModeCBC uint64 = 2

	ContentSigAlgoNotSigned uint64 = 0
	ContentSigAlgoRSA       uint64 = 1

	ContentSigHashAlgoNotSigned uint64 = 0
	ContentSigHashAlgoSHA1      uint64 = 1
	ContentSigHashAlgoMD5       uint64 = 2
)

type Packet struct {
	TrackID                     uint32
	TimeNS                      int64
	DurationNS                  int64
	ReferenceBlockTimeNS        []int64
	ReferencePriority           uint64
	DiscardPaddingNS            int64
	CodecState                  []byte
	BlockAdditions              []BlockAddition
	Keyframe                    bool
	Invisible                   bool
	Discardable                 bool
	Data                        []byte
	ContentEncryptionPartitions []uint32
}

type BlockAddition struct {
	ID   uint64
	Data []byte
}

// LacingMode selects how multiple frames are packed into one laced block.
type LacingMode int

const (
	// LacingAuto uses fixed-size lacing for equal-sized frames and EBML lacing
	// otherwise.
	LacingAuto LacingMode = iota
	// LacingXiph writes Xiph lacing.
	LacingXiph
	// LacingEBML writes EBML lacing.
	LacingEBML
	// LacingFixed writes fixed-size lacing and requires equal-sized frames.
	LacingFixed
)

// LacedPacket writes multiple frames for one track into a single laced block.
// Frame timestamps are derived by demuxers from FrameDurationNS when set, or
// from the track default duration otherwise.
type LacedPacket struct {
	TrackID              uint32
	TimeNS               int64
	FrameDurationNS      int64
	ReferenceBlockTimeNS []int64
	ReferencePriority    uint64
	DiscardPaddingNS     int64
	CodecState           []byte
	BlockAdditions       []BlockAddition
	Keyframe             bool
	Invisible            bool
	Discardable          bool
	Lacing               LacingMode
	Frames               [][]byte
}

type CuePoint struct {
	TrackID             uint32
	TimeNS              int64
	ClusterPosition     uint64
	RelativePosition    uint64
	RelativePositionSet bool
	DurationNS          int64
	DurationSet         bool
	BlockNumber         uint64
	BlockNumberSet      bool
	CodecStatePosition  uint64
	CodecStateSet       bool
	References          []CueReference
	Positions           []CueTrackPosition
}

type CueTrackPosition struct {
	TrackID             uint32
	ClusterPosition     uint64
	RelativePosition    uint64
	RelativePositionSet bool
	DurationNS          int64
	DurationSet         bool
	BlockNumber         uint64
	BlockNumberSet      bool
	CodecStatePosition  uint64
	CodecStateSet       bool
	References          []CueReference
}

type CueReference struct {
	TimeNS             int64
	ClusterPosition    uint64
	BlockNumber        uint64
	BlockNumberSet     bool
	CodecStatePosition uint64
	CodecStateSet      bool
}

// CuePolicy controls which packets are indexed in seekable output.
type CuePolicy int

const (
	// CuePolicyDefault indexes audio packets and keyframe video packets.
	CuePolicyDefault CuePolicy = iota
	// CuePolicyKeyframes indexes only packets marked as keyframes.
	CuePolicyKeyframes
	// CuePolicyAllPackets indexes every written packet or laced block.
	CuePolicyAllPackets
	// CuePolicyNone disables cue collection and Cues writing.
	CuePolicyNone
)

type SeekEntry struct {
	ID       uint64
	Position uint64
}

type SegmentInfo struct {
	SegmentUUID     []byte
	SegmentFilename string
	PrevUUID        []byte
	PrevFilename    string
	NextUUID        []byte
	NextFilename    string
	DurationNS      int64
	DurationSet     bool
	Title           string
	DateUTC         time.Time
	DateUTCSet      bool
	MuxingApp       string
	WritingApp      string
}

type Attachment struct {
	UID         uint64
	Filename    string
	MIMEType    string
	Description string
	Data        []byte
}

type ChapterEdition struct {
	UID      uint64
	Hidden   bool
	Default  bool
	Ordered  bool
	Chapters []Chapter
}

type Chapter struct {
	UID        uint64
	StringUID  string
	StartNS    int64
	EndNS      int64
	EndSet     bool
	Hidden     bool
	Enabled    bool
	EnabledSet bool
	TrackUIDs  []uint64
	Displays   []ChapterDisplay
	Children   []Chapter
}

type ChapterDisplay struct {
	String        string
	Language      string
	LanguageBCP47 string
	Country       string
}

type Tag struct {
	Target TagTarget
	Simple []SimpleTag
}

type TagTarget struct {
	TypeValue      uint64
	Type           string
	TrackUIDs      []uint64
	EditionUIDs    []uint64
	ChapterUIDs    []uint64
	AttachmentUIDs []uint64
}

type SimpleTag struct {
	Name          string
	Language      string
	LanguageBCP47 string
	Default       bool
	DefaultSet    bool
	String        string
	StringSet     bool
	Binary        []byte
	Children      []SimpleTag
}

func (p *Packet) Reset() {
	data := p.Data[:0]
	references := p.ReferenceBlockTimeNS[:0]
	codecState := p.CodecState[:0]
	additions := p.BlockAdditions[:0]
	partitions := p.ContentEncryptionPartitions[:0]
	*p = Packet{
		Data:                        data,
		ReferenceBlockTimeNS:        references,
		CodecState:                  codecState,
		BlockAdditions:              additions,
		ContentEncryptionPartitions: partitions,
	}
}

type MuxerOptions struct {
	DocType                    string
	DocTypeVersion             uint64
	DocTypeReadVersion         uint64
	MuxingApp                  string
	WritingApp                 string
	Info                       SegmentInfo
	Attachments                []Attachment
	Chapters                   []ChapterEdition
	Tags                       []Tag
	TimecodeScaleNS            int64
	ClusterMaxDurationNS       int64
	Streaming                  bool
	CueCapacity                int
	CuePolicy                  CuePolicy
	ContentEncryptionKeys      []ContentEncryptionKey
	ContentEncryptionInitialIV []byte
}

type DemuxerOptions struct {
	MaxElementSize        uint64
	MaxLaceFrames         int
	MaxLacePayload        int
	ContentEncryptionKeys []ContentEncryptionKey
}

const (
	defaultDocType              = "matroska"
	defaultDocTypeVersion       = 4
	defaultDocTypeReadVersion   = 2
	defaultMuxingApp            = "goav"
	defaultWritingApp           = "goav"
	defaultTimecodeScaleNS      = int64(time.Millisecond)
	defaultClusterMaxDurationNS = int64(5 * time.Second)
	defaultMaxLaceFrames        = 256
	defaultMaxLacePayload       = 1 << 20
)

func normalizeMuxerOptions(opts MuxerOptions) MuxerOptions {
	if opts.DocType == "" {
		opts.DocType = defaultDocType
	}
	if opts.DocTypeVersion == 0 {
		opts.DocTypeVersion = defaultDocTypeVersion
	}
	if opts.DocTypeReadVersion == 0 {
		opts.DocTypeReadVersion = defaultDocTypeReadVersion
	}
	if opts.MuxingApp == "" {
		opts.MuxingApp = defaultMuxingApp
	}
	if opts.WritingApp == "" {
		opts.WritingApp = defaultWritingApp
	}
	if opts.TimecodeScaleNS <= 0 {
		opts.TimecodeScaleNS = defaultTimecodeScaleNS
	}
	if opts.ClusterMaxDurationNS <= 0 {
		opts.ClusterMaxDurationNS = defaultClusterMaxDurationNS
	}
	return opts
}

func validateCuePolicy(policy CuePolicy) error {
	switch policy {
	case CuePolicyDefault, CuePolicyKeyframes, CuePolicyAllPackets, CuePolicyNone:
		return nil
	default:
		return ErrInvalidData
	}
}
