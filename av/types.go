package av

import "time"

type MediaType string

const (
	MediaUnknown  MediaType = ""
	MediaAudio    MediaType = "audio"
	MediaVideo    MediaType = "video"
	MediaSubtitle MediaType = "subtitle"
	MediaData     MediaType = "data"
)

type CodecID string

const (
	CodecUnknown  CodecID = ""
	CodecOpus     CodecID = "opus"
	CodecVorbis   CodecID = "vorbis"
	CodecFLAC     CodecID = "flac"
	CodecAAC      CodecID = "aac"
	CodecVP8      CodecID = "vp8"
	CodecVP9      CodecID = "vp9"
	CodecH264     CodecID = "h264"
	CodecAV1      CodecID = "av1"
	CodecPCM      CodecID = "pcm"
	CodecTextUTF8 CodecID = "text_utf8"
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
	FormatWebM     FormatID = "webm"
	FormatMP4      FormatID = "mp4"
)

type ProtocolID string

const (
	ProtocolUnknown ProtocolID = ""
	ProtocolFile    ProtocolID = "file"
	ProtocolRTP     ProtocolID = "rtp"
	ProtocolWebRTC  ProtocolID = "webrtc"
	ProtocolCustom  ProtocolID = "custom"
)

type StreamID string
type Epoch uint64
type Metadata map[string]string

const MetadataMediaType = "media_type"

const (
	SampleFormatS16    = "s16"
	SampleFormatF32    = "f32"
	PixelFormatGray8   = "gray8"
	PixelFormatI420    = "i420"
	PixelFormatYUV420P = "yuv420p"
	PixelFormatI422    = "i422"
	PixelFormatYUV422P = "yuv422p"
	PixelFormatI444    = "i444"
	PixelFormatYUV444P = "yuv444p"
)

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

// Valid reports whether the timebase can convert values.
func (b TimeBase) Valid() bool {
	return b.Num > 0 && b.Den > 0
}

// ToDuration converts a value in this timebase to a standard Go duration.
func (b TimeBase) ToDuration(value int64) (time.Duration, bool) {
	scaled, ok := RescaleValue(value, b, TimeBase{Num: 1, Den: int64(time.Second)})
	if !ok {
		return 0, false
	}
	return time.Duration(scaled), true
}

// FromDuration converts a standard Go duration to a value in this timebase.
func (b TimeBase) FromDuration(duration time.Duration) (int64, bool) {
	return RescaleValue(int64(duration), TimeBase{Num: 1, Den: int64(time.Second)}, b)
}

// Rescale converts the timestamp to another timebase.
func (t Timestamp) Rescale(base TimeBase) (Timestamp, bool) {
	value, ok := RescaleValue(t.Value, t.Base, base)
	if !ok {
		return Timestamp{}, false
	}
	return Timestamp{Value: value, Base: base}, true
}

// ToDuration converts the timestamp value to elapsed duration from zero.
func (t Timestamp) ToDuration() (time.Duration, bool) {
	return t.Base.ToDuration(t.Value)
}

// Sub returns t-previous in t's timebase.
func (t Timestamp) Sub(previous Timestamp) (Duration, bool) {
	value, ok := RescaleValue(previous.Value, previous.Base, t.Base)
	if !ok {
		return Duration{}, false
	}
	delta, ok := checkedSub(t.Value, value)
	if !ok {
		return Duration{}, false
	}
	return Duration{Value: delta, Base: t.Base}, true
}

// Rescale converts the duration to another timebase.
func (d Duration) Rescale(base TimeBase) (Duration, bool) {
	value, ok := RescaleValue(d.Value, d.Base, base)
	if !ok {
		return Duration{}, false
	}
	return Duration{Value: value, Base: base}, true
}

// ToDuration converts the media duration to a standard Go duration.
func (d Duration) ToDuration() (time.Duration, bool) {
	return d.Base.ToDuration(d.Value)
}

// Compare compares d and other after converting other into d's timebase.
func (d Duration) Compare(other Duration) (int, bool) {
	value, ok := RescaleValue(other.Value, other.Base, d.Base)
	if !ok {
		return 0, false
	}
	switch {
	case d.Value < value:
		return -1, true
	case d.Value > value:
		return 1, true
	default:
		return 0, true
	}
}

// TimestampFromStdDuration converts elapsed time from zero into a timestamp.
func TimestampFromStdDuration(duration time.Duration, base TimeBase) (Timestamp, bool) {
	value, ok := base.FromDuration(duration)
	if !ok {
		return Timestamp{}, false
	}
	return Timestamp{Value: value, Base: base}, true
}

// DurationFromStdDuration converts a standard Go duration into a media duration.
func DurationFromStdDuration(duration time.Duration, base TimeBase) (Duration, bool) {
	value, ok := base.FromDuration(duration)
	if !ok {
		return Duration{}, false
	}
	return Duration{Value: value, Base: base}, true
}

// RescaleValue converts value from one rational timebase to another.
func RescaleValue(value int64, from TimeBase, to TimeBase) (int64, bool) {
	if !from.Valid() || !to.Valid() {
		return 0, false
	}
	if value == 0 {
		return 0, true
	}
	negative := value < 0
	if negative {
		if value == minInt64 {
			return 0, false
		}
		value = -value
	}

	a := value
	b := from.Num
	c := to.Den
	d := from.Den
	e := to.Num
	reducePair(&a, &d)
	reducePair(&a, &e)
	reducePair(&b, &d)
	reducePair(&b, &e)
	reducePair(&c, &d)
	reducePair(&c, &e)

	numerator, ok := checkedMul(a, b)
	if !ok {
		return 0, false
	}
	numerator, ok = checkedMul(numerator, c)
	if !ok {
		return 0, false
	}
	value = numerator / d / e
	if negative {
		value = -value
	}
	return value, true
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
	ID       StreamID
	Index    int
	UseIndex bool
	Type     MediaType
	Codec    CodecID
	Name     string
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
	StreamID StreamID
	// Type is the typed media kind so drop/route decisions stay on the hot path
	// without a Metadata map lookup. Producers should set it; Metadata is the
	// cold-path fallback.
	Type          MediaType
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
	p.Type = ""
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

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
)

func reducePair(a *int64, b *int64) {
	gcd := gcdInt64(*a, *b)
	if gcd <= 1 {
		return
	}
	*a /= gcd
	*b /= gcd
}

func gcdInt64(a int64, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

func checkedMul(a int64, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > maxInt64/b {
		return 0, false
	}
	return a * b, true
}

func checkedSub(a int64, b int64) (int64, bool) {
	if b > 0 && a < minInt64+b {
		return 0, false
	}
	if b < 0 && a > maxInt64+b {
		return 0, false
	}
	return a - b, true
}
