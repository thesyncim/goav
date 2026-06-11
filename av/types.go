package av

import "time"

// MediaType is the kind of media a stream, packet, or frame carries.
type MediaType string

// The media kinds. MediaUnknown (the zero value) means "any/unspecified" in
// selectors.
const (
	MediaUnknown  MediaType = ""
	MediaAudio    MediaType = "audio"
	MediaVideo    MediaType = "video"
	MediaSubtitle MediaType = "subtitle"
	MediaData     MediaType = "data"
)

// CodecID names a codec. The type is an open string: external adapters
// introduce their own ids through the same registries and recipes as the
// well-known ones below.
type CodecID string

// Well-known codec ids.
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

// FormatID names a container or transport framing format. The type is an
// open string: external format adapters introduce their own ids.
type FormatID string

// Well-known format ids.
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

// ProtocolID names how an input or output is reached: a file-like reader, a
// live transport, or an application-pushed custom source.
type ProtocolID string

// Well-known protocol ids.
const (
	ProtocolUnknown ProtocolID = ""
	ProtocolFile    ProtocolID = "file"
	ProtocolRTP     ProtocolID = "rtp"
	ProtocolWebRTC  ProtocolID = "webrtc"
	ProtocolCustom  ProtocolID = "custom"
)

// StreamID identifies one media stream within a task; packets, frames, and
// events carry it so routing never inspects payloads.
type StreamID string

// Epoch is a monotonic configuration generation: it increments when a
// stream's codec parameters change mid-run, so consumers can tell which
// configuration a packet or frame belongs to.
type Epoch uint64

// Metadata is the open string-to-string side channel on streams, packets,
// frames, events, inputs, and destinations — cold-path facts only; typed
// fields carry everything the hot path reads.
type Metadata map[string]string

// MetadataMediaType is the Metadata key carrying a media kind as a string,
// the cold-path fallback when a typed MediaType field is unavailable.
const MetadataMediaType = "media_type"

// MetadataBitrate is the Event.Metadata key carrying a requested encoder
// bitrate as a decimal string in bits per second. It is the payload of
// EventBitrateChanged events.
const MetadataBitrate = "bitrate_bps"

// Sample and pixel format names used in CodecParameters, AudioFrame, and
// VideoFrame. Open strings: adapters may introduce more.
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

// TimeBase is the rational unit (Num/Den seconds) timestamp and duration
// values are measured in — 1/48000 for 48kHz audio, 1/90000 for RTP video.
type TimeBase struct {
	Num int64
	Den int64
}

// Timestamp is a media position: Value counted in Base units. Convert with
// Rescale/ToDuration instead of mixing bases arithmetically.
type Timestamp struct {
	Value int64
	Base  TimeBase
}

// Duration is a media time span: Value counted in Base units.
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

// BufferOwnership declares who may write a Buffer's bytes and how long they
// stay valid. The pipeline's buffered execution reads this declaration to
// decide whether a payload may be shared by reference across branches: only
// BufferImmutable is shareable; everything else counts as mutable and is
// copied into branch-owned backing (or refused) before queueing, so a consumer
// that mutates its delivered bytes can never corrupt a sibling branch.
type BufferOwnership string

const (
	// BufferBorrowed bytes still belong to the producer and are valid only
	// until the producer's next Read, Decode, Encode, Filter, or Depacketize
	// call unless the producer documents a longer lifetime. Consumers must not
	// retain the bytes past that window; buffered execution copies borrowed
	// payloads before queueing them.
	BufferBorrowed BufferOwnership = "borrowed"
	// BufferOwned bytes belong to the receiver, which may mutate them and may
	// return them to Owner when done. Because an owned payload has exactly one
	// writer, buffered execution copies it per branch before fanning it out.
	BufferOwned BufferOwnership = "owned"
	// BufferImmutable bytes may be shared by reference indefinitely and must
	// never be written by anyone — producer included — once published. This is
	// the only ownership buffered execution forwards without copying (unless
	// the branch demands defensive copies via flow.CopyAlways). The
	// declaration is trusted, not checked: a producer that marks bytes
	// immutable and then writes them breaks every consumer sharing the
	// backing array.
	BufferImmutable BufferOwnership = "immutable"
)

// BufferOwner receives a Buffer back when the pipeline is done with it, the
// recycling seam for pooled payloads.
type BufferOwner interface {
	ReleaseBuffer(*Buffer)
}

// Buffer is one byte payload plus the ownership declaration consumers and the
// pipeline copy machinery rely on. See BufferOwnership for who may mutate the
// bytes and when the pipeline copies them.
type Buffer struct {
	Bytes     []byte
	Ownership BufferOwnership
	Owner     BufferOwner
}

// Reset clears the buffer for reuse, keeping the backing array.
func (b *Buffer) Reset() {
	b.Bytes = b.Bytes[:0]
	b.Ownership = ""
	b.Owner = nil
}

// CodecParameters are the codec-level facts a stream declares: identity,
// timing, audio/video geometry, formats, and codec-specific extradata.
// Decoders are configured from these; the planner reads them as shape facts.
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

// Stream describes one media stream an input carries: identity, kind, codec
// parameters, timing, and labels. Probers and source providers produce
// streams; selectors match against them.
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

// StreamSelector matches streams by any combination of id, probe index
// (UseIndex gates Index), media kind, codec, and name. Zero fields match
// anything.
type StreamSelector struct {
	ID       StreamID
	Index    int
	UseIndex bool
	Type     MediaType
	Codec    CodecID
	Name     string
}

// VideoGeometry is a width/height pair, the video conversion target shape.
type VideoGeometry struct {
	Width  int
	Height int
}

// AudioGeometry is an audio layout (rate, channels, channel layout), the
// audio conversion target shape.
type AudioGeometry struct {
	SampleRate    int
	Channels      int
	ChannelLayout string
}

// Packet is one encoded media unit: the payload bytes plus the stream,
// timing, keyframe, and integrity facts routing and muxing read. The Payload
// Buffer's ownership declaration governs who may mutate the bytes.
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

// Reset clears the packet for reuse, keeping allocated capacity.
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

// Plane is one raw-media plane: its bytes, the row stride, and the offset of
// the first sample within the buffer.
type Plane struct {
	Buffer Buffer
	Stride int
	Offset int
}

// Reset clears the plane for reuse, keeping the backing array.
func (p *Plane) Reset() {
	p.Buffer.Reset()
	p.Stride = 0
	p.Offset = 0
}

// VideoFrame is the video-specific header of a Frame: geometry and pixel
// format of the planes.
type VideoFrame struct {
	Width       int
	Height      int
	PixelFormat string
}

// AudioFrame is the audio-specific header of a Frame: layout, sample format,
// and the per-channel sample count of the planes.
type AudioFrame struct {
	SampleRate    int
	Channels      int
	ChannelLayout string
	SampleFormat  string
	Samples       int
}

// Frame is one decoded media unit: plane data plus exactly one of the Video
// or Audio headers describing it. Plane buffers carry their own ownership
// declarations.
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

// Reset clears the frame for reuse, keeping allocated plane capacity.
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

// EventType names an out-of-band signal riding the data path alongside
// packets and frames.
type EventType string

// The event vocabulary. Each constant documents who emits it and what
// consumers do with it.
const (
	// EventStreamAdded announces a stream the source discovered after it
	// opened: Event.Stream carries the full av.Stream and Event.StreamID its
	// id. Sources emit it mid-run, before the new stream's media, so the
	// runtime can attach branches to the stream without a rebuild.
	EventStreamAdded EventType = "stream_added"
	// EventStreamRemoved announces that the stream named by Event.StreamID
	// ended; branches attached to it can be detached.
	EventStreamRemoved EventType = "stream_removed"
	// EventAttachError reports a failed runtime reaction to a discovered
	// stream: a dynamic-stream rule branch that could not be attached or
	// detached. Cause carries the failure, Reason names the rule branch, and
	// StreamID is the discovered stream.
	EventAttachError EventType = "attach_error"
	// EventCodecChanged announces that a stream's codec parameters changed
	// mid-run; Event.Codec carries the new parameters and decode chains apply
	// their CodecChangePolicy.
	EventCodecChanged EventType = "codec_changed"
	// EventDiscontinuity marks a timeline break (a seek landed, a receiver
	// reset): downstream decoders reset and consumers re-sync on it.
	EventDiscontinuity EventType = "discontinuity"
	// EventPacketLoss reports transport loss detected before the marked
	// position, typically from an RTP receiver's sequence tracking.
	EventPacketLoss EventType = "packet_loss"
	// EventKeyframeRequired asks the encoders on its path to produce a
	// keyframe (sync point) as soon as possible.
	EventKeyframeRequired EventType = "keyframe_required"
	// EventBackpressure reports sustained downstream pressure, surfaced so
	// adaptive sources can slow down.
	EventBackpressure EventType = "backpressure"
	// EventStats carries periodic diagnostic counters on Event.Metadata.
	EventStats EventType = "stats"
	// EventEndOfStream marks the natural end of the stream named by
	// Event.StreamID: downstream nodes flush and destinations finalize.
	EventEndOfStream EventType = "end_of_stream"
	// EventSeek asks a source to reposition to the media position carried in
	// Event.Timestamp (measured from the start of the media). It is delivered
	// out-of-band to a source's control seam, not routed downstream; the source
	// acknowledges by emitting EventDiscontinuity before the first message at
	// the new position.
	EventSeek EventType = "seek"
	// EventRate asks a source to change its playback rate (1 = realtime,
	// 2 = double speed, 0.5 = half; positive only). The rate rides
	// Event.Metadata under MetadataRate — build it with RateMetadata, read it
	// with EventRateValue. Like EventSeek it is delivered to a source's
	// control seam, not routed downstream. A rate change is pure pacing: the
	// source keeps delivering from its current position and must NOT emit
	// EventDiscontinuity unless applying the rate makes it reposition.
	EventRate EventType = "rate"
	// EventSegment asks a source to play one window [start, end) and then end
	// its stream naturally. The start rides Event.Timestamp exactly like
	// EventSeek; the exclusive end rides Event.Metadata under
	// MetadataSegmentEnd — build it with SegmentEndMetadata, read it with
	// EventSegmentEnd. The source acknowledges like a seek to start
	// (EventDiscontinuity before the first message at start) and, on reaching
	// end, ends exactly as at the end of the media (EventEndOfStream, Start
	// loop returns) so downstream destinations finalize naturally.
	EventSegment EventType = "segment"
	// EventBitrateChanged asks the encoders on its path to retarget their
	// bitrate without restarting the stream. The requested rate rides
	// Event.Metadata under MetadataBitrate as a decimal string in bits per
	// second. An encoder that cannot apply a live retarget must return an
	// error from HandleEvent so the failure is visible, never silent.
	EventBitrateChanged EventType = "bitrate_changed"
)

// Event is one out-of-band signal: its type, the stream it concerns, and the
// typed payload fields the type documents (Stream, Codec, Timestamp, Cause,
// Metadata). Events ride the data path in order with the media.
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

// Reset clears the event for reuse.
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

// RTPTimeBase returns the timebase of an RTP clock rate (1/clockRate
// seconds), or the zero TimeBase when the rate is unknown.
func RTPTimeBase(clockRate uint32) TimeBase {
	if clockRate == 0 {
		return TimeBase{}
	}
	return TimeBase{Num: 1, Den: int64(clockRate)}
}

// RTPToTimestamp wraps a raw RTP timestamp value in its clock-rate timebase.
func RTPToTimestamp(value uint32, clockRate uint32) Timestamp {
	return Timestamp{Value: int64(value), Base: RTPTimeBase(clockRate)}
}

// SamplesDuration returns the media duration of a sample count at a sample
// rate (samples/sampleRate seconds), or the zero Duration when the rate is
// unknown.
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
