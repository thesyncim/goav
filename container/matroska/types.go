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
	SampleRate int
	Channels   int
	BitDepth   int
}

type VideoConfig struct {
	Width  int
	Height int
}

type Track struct {
	ID                uint32
	Type              TrackType
	Codec             Codec
	Name              string
	Language          string
	TimebaseNum       int64
	TimebaseDen       int64
	DefaultDurationNS int64
	CodecDelayNS      int64
	SeekPreRollNS     int64
	Audio             AudioConfig
	Video             VideoConfig

	CodecPrivate []byte
}

type Packet struct {
	TrackID              uint32
	TimeNS               int64
	DurationNS           int64
	ReferenceBlockTimeNS []int64
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
