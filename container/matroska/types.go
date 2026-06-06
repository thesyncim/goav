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
	Width           int
	Height          int
	StereoMode      int
	StereoModeSet   bool
	AlphaMode       int
	AlphaModeSet    bool
	PixelCropBottom int
	PixelCropTop    int
	PixelCropLeft   int
	PixelCropRight  int
	DisplayWidth    int
	DisplayHeight   int
	DisplayUnit     int
	Colour          VideoColourConfig
	Projection      VideoProjectionConfig
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
	Audio                         AudioConfig
	Video                         VideoConfig

	CodecPrivate []byte
}

type Packet struct {
	TrackID              uint32
	TimeNS               int64
	DurationNS           int64
	ReferenceBlockTimeNS []int64
	DiscardPaddingNS     int64
	Keyframe             bool
	Invisible            bool
	Discardable          bool
	Data                 []byte
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
// Frame timestamps are derived by demuxers from the track default duration.
type LacedPacket struct {
	TrackID     uint32
	TimeNS      int64
	Keyframe    bool
	Invisible   bool
	Discardable bool
	Lacing      LacingMode
	Frames      [][]byte
}

type CuePoint struct {
	TrackID             uint32
	TimeNS              int64
	ClusterPosition     uint64
	RelativePosition    uint64
	RelativePositionSet bool
}

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
	Title           string
	DateUTC         time.Time
	DateUTCSet      bool
	MuxingApp       string
	WritingApp      string
}

func (p *Packet) Reset() {
	data := p.Data[:0]
	references := p.ReferenceBlockTimeNS[:0]
	*p = Packet{Data: data, ReferenceBlockTimeNS: references}
}

type MuxerOptions struct {
	DocType              string
	DocTypeVersion       uint64
	DocTypeReadVersion   uint64
	MuxingApp            string
	WritingApp           string
	Info                 SegmentInfo
	TimecodeScaleNS      int64
	ClusterMaxDurationNS int64
	Streaming            bool
	CueCapacity          int
}

type DemuxerOptions struct {
	MaxElementSize uint64
	MaxLaceFrames  int
	MaxLacePayload int
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
