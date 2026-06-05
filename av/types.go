package av

import "time"

type MediaType string

const (
	MediaUnknown MediaType = ""
	MediaAudio   MediaType = "audio"
	MediaVideo   MediaType = "video"
	MediaData    MediaType = "data"
)

type CodecID string

const (
	CodecUnknown CodecID = ""
	CodecOpus    CodecID = "opus"
	CodecVP8     CodecID = "vp8"
	CodecVP9     CodecID = "vp9"
	CodecH264    CodecID = "h264"
	CodecAV1     CodecID = "av1"
	CodecPCM     CodecID = "pcm"
)

type FormatID string

const (
	FormatUnknown  FormatID = ""
	FormatWebRTC   FormatID = "webrtc"
	FormatRTP      FormatID = "rtp"
	FormatFLV      FormatID = "flv"
	FormatOgg      FormatID = "ogg"
	FormatIVF      FormatID = "ivf"
	FormatAnnexB   FormatID = "annexb"
	FormatMatroska FormatID = "matroska"
	FormatMP4      FormatID = "mp4"
)

type ProtocolID string

const (
	ProtocolUnknown ProtocolID = ""
	ProtocolFile    ProtocolID = "file"
	ProtocolRTMP    ProtocolID = "rtmp"
	ProtocolRTP     ProtocolID = "rtp"
	ProtocolWebRTC  ProtocolID = "webrtc"
)

type StreamID string
type Epoch uint64
type Metadata map[string]string

type TimeBase struct {
	Num int64
	Den int64
}

type Timestamp struct {
	Value int64
	Base  TimeBase
}

type Duration struct {
	Value int64
	Base  TimeBase
}

type BufferOwnership string

const (
	// BufferBorrowed is valid only until the producer's next Read, Decode,
	// Encode, Filter, or Depacketize call unless the producer documents a
	// longer lifetime.
	BufferBorrowed BufferOwnership = "borrowed"
	// BufferOwned belongs to the caller and may be returned to Owner when the
	// caller is done with it.
	BufferOwned BufferOwnership = "owned"
	// BufferImmutable may be shared and must never be mutated by consumers.
	BufferImmutable BufferOwnership = "immutable"
)

type BufferOwner interface {
	ReleaseBuffer(*Buffer)
}

type Buffer struct {
	Bytes     []byte
	Ownership BufferOwnership
	Owner     BufferOwner
}

func (b *Buffer) Reset() {
	b.Bytes = b.Bytes[:0]
	b.Ownership = ""
	b.Owner = nil
}

type CodecParameters struct {
	ID            CodecID
	Type          MediaType
	Profile       string
	Level         string
	ClockRate     uint32
	SampleRate    int
	Channels      int
	ChannelLayout string
	Width         int
	Height        int
	PixelFormat   string
	SampleFormat  string
	ExtraData     Buffer
	Attributes    Metadata
}

type Stream struct {
	ID       StreamID
	Index    int
	Type     MediaType
	Codec    CodecParameters
	TimeBase TimeBase
	Language string
	Name     string
	Epoch    Epoch
	Metadata Metadata
}

type StreamSelector struct {
	ID    StreamID
	Index int
	Type  MediaType
	Codec CodecID
	Name  string
}

type VideoGeometry struct {
	Width  int
	Height int
}

type AudioGeometry struct {
	SampleRate    int
	Channels      int
	ChannelLayout string
}

type Packet struct {
	StreamID      StreamID
	CodecEpoch    Epoch
	Payload       Buffer
	PTS           Timestamp
	DTS           Timestamp
	Duration      Duration
	Keyframe      bool
	Corrupt       bool
	Discontinuous bool
	LossBefore    bool
	Metadata      Metadata
}

func (p *Packet) Reset() {
	p.StreamID = ""
	p.CodecEpoch = 0
	p.Payload.Reset()
	p.PTS = Timestamp{}
	p.DTS = Timestamp{}
	p.Duration = Duration{}
	p.Keyframe = false
	p.Corrupt = false
	p.Discontinuous = false
	p.LossBefore = false
	p.Metadata = nil
}

type Plane struct {
	Buffer Buffer
	Stride int
	Offset int
}

func (p *Plane) Reset() {
	p.Buffer.Reset()
	p.Stride = 0
	p.Offset = 0
}

type VideoFrame struct {
	Width       int
	Height      int
	PixelFormat string
}

type AudioFrame struct {
	SampleRate    int
	Channels      int
	ChannelLayout string
	SampleFormat  string
	Samples       int
}

type Frame struct {
	StreamID   StreamID
	CodecEpoch Epoch
	Type       MediaType
	PTS        Timestamp
	Duration   Duration
	Video      *VideoFrame
	Audio      *AudioFrame
	Planes     []Plane
	Metadata   Metadata
}

func (f *Frame) Reset() {
	f.StreamID = ""
	f.CodecEpoch = 0
	f.Type = ""
	f.PTS = Timestamp{}
	f.Duration = Duration{}
	f.Video = nil
	f.Audio = nil
	for i := range f.Planes {
		f.Planes[i].Reset()
	}
	f.Planes = f.Planes[:0]
	f.Metadata = nil
}

type EventType string

const (
	EventStreamAdded      EventType = "stream_added"
	EventStreamRemoved    EventType = "stream_removed"
	EventCodecChanged     EventType = "codec_changed"
	EventDiscontinuity    EventType = "discontinuity"
	EventPacketLoss       EventType = "packet_loss"
	EventKeyframeRequired EventType = "keyframe_required"
	EventBackpressure     EventType = "backpressure"
	EventStats            EventType = "stats"
	EventEndOfStream      EventType = "end_of_stream"
)

type Event struct {
	Type      EventType
	StreamID  StreamID
	Epoch     Epoch
	At        time.Time
	Timestamp Timestamp
	Stream    *Stream
	Codec     *CodecParameters
	Cause     error
	Reason    string
	Metadata  Metadata
}

func (e *Event) Reset() {
	e.Type = ""
	e.StreamID = ""
	e.Epoch = 0
	e.At = time.Time{}
	e.Timestamp = Timestamp{}
	e.Stream = nil
	e.Codec = nil
	e.Cause = nil
	e.Reason = ""
	e.Metadata = nil
}

func RTPTimeBase(clockRate uint32) TimeBase {
	if clockRate == 0 {
		return TimeBase{}
	}
	return TimeBase{Num: 1, Den: int64(clockRate)}
}

func RTPToTimestamp(value uint32, clockRate uint32) Timestamp {
	return Timestamp{Value: int64(value), Base: RTPTimeBase(clockRate)}
}

func SamplesDuration(samples int, sampleRate int) Duration {
	if sampleRate <= 0 {
		return Duration{}
	}
	return Duration{Value: int64(samples), Base: TimeBase{Num: 1, Den: int64(sampleRate)}}
}
