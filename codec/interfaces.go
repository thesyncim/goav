package codec

import (
	"context"
	"time"

	"github.com/thesyncim/goav/av"
)

// Mode says which direction a codec implementation works in.
type Mode string

// The codec modes.
const (
	ModeDecode Mode = "decode"
	ModeEncode Mode = "encode"
)

// Descriptor identifies a codec implementation and what it can do: identity,
// media type, supported modes, and concrete capabilities. Registries key on
// it, and the planner's compatibility checks read it before anything is
// allocated or opened.
type Descriptor struct {
	ID           av.CodecID
	Name         string
	Type         av.MediaType
	Modes        []Mode
	Profiles     []string
	Realtime     bool
	Experimental bool
	Capabilities Capabilities
	Backend      Backend
}

// Capabilities carries concrete media compatibility details. Codec identity,
// media type, supported modes, realtime status, and experimental status live on
// Descriptor so each fact has one owner.
type Capabilities struct {
	SampleFormats []string
	PixelFormats  []string
	RTPPayloads   []string
	BuildTags     []string
}

// Backend names the implementation behind an adapter (the Go module and
// package), reported in Explain so requirements are traceable.
type Backend struct {
	Name    string
	Module  string
	Package string
	Status  string
}

// ResiliencePolicy says how a decoder treats damaged or lossy input: whether
// loss is tolerated, audio concealed, damaged video dropped, keyframes
// requested for recovery, and how long to wait for reordered input.
type ResiliencePolicy struct {
	AcceptLoss       bool
	ConcealAudio     bool
	DropDamagedVideo bool
	RequestKeyframes bool
	MaxReorderDelay  time.Duration
}

// DecodeBounds caps a decoder's per-call output and retained memory so
// allocation stays provable up front; zero fields take the adapter defaults
// through WithDefaults.
type DecodeBounds struct {
	// MaxFramesPerInput bounds decoded frames emitted from one DecodeInto call.
	MaxFramesPerInput int
	// MaxEventsPerInput bounds events emitted from one DecodeInto call.
	MaxEventsPerInput int
	// MaxRequestsPerInput bounds control requests emitted from one DecodeInto call.
	MaxRequestsPerInput int
	// MaxPayloadBytes bounds one encoded input payload.
	MaxPayloadBytes int
	// MaxRetainedBytes bounds data a decoder may keep across DecodeInto calls,
	// such as a partial realtime video fragment.
	MaxRetainedBytes int
	// MaxWidth and MaxHeight bound video output geometry when known before open.
	MaxWidth  int
	MaxHeight int
}

// WithDefaults fills every unset (zero) bound from defaults and returns the
// result.
func (b DecodeBounds) WithDefaults(defaults DecodeBounds) DecodeBounds {
	if b.MaxFramesPerInput <= 0 {
		b.MaxFramesPerInput = defaults.MaxFramesPerInput
	}
	if b.MaxEventsPerInput <= 0 {
		b.MaxEventsPerInput = defaults.MaxEventsPerInput
	}
	if b.MaxRequestsPerInput <= 0 {
		b.MaxRequestsPerInput = defaults.MaxRequestsPerInput
	}
	if b.MaxPayloadBytes <= 0 {
		b.MaxPayloadBytes = defaults.MaxPayloadBytes
	}
	if b.MaxRetainedBytes <= 0 {
		b.MaxRetainedBytes = defaults.MaxRetainedBytes
	}
	if b.MaxWidth <= 0 {
		b.MaxWidth = defaults.MaxWidth
	}
	if b.MaxHeight <= 0 {
		b.MaxHeight = defaults.MaxHeight
	}
	return b
}

// CodecSettings carries the grouped common encoder/decoder settings (tier 1)
// plus adapter-specific string settings and the raw Control escape hatch.
// Recipes populate it through codec options (Bitrate, FPS, Profile, Setting,
// Control, ...); adapters read it at open.
type CodecSettings struct {
	// Bitrate requests an encoder bitrate in bits per second. Zero lets the
	// encoder choose its default bitrate.
	Bitrate int `goavctl:"bitrate,rate" usage:"[bitrate=<rate>]" help:"encoder bitrate; accepts 1200k, 2M, or integer bits per second"`
	// Framerate requests an encoder frame cadence. Zero lets the encoder infer
	// cadence from input frames.
	Framerate av.Duration `goavctl:"fps,fps" usage:"[fps=<n|n/d>]" help:"encoder frame cadence; accepts 30, 29.97, or 30000/1001"`
	// KeyframeInterval requests a keyframe cadence in encoded frames. Zero lets
	// the encoder choose its default cadence.
	KeyframeInterval int `goavctl:"keyframe_interval,positive" usage:"[keyframe_interval=<frames>]" help:"keyframe cadence in encoded frames"`
	// Profile requests an encoder profile. Empty lets the encoder choose its
	// default or preserve the selected input profile when copying compatible
	// stream metadata.
	Profile string `goavctl:"profile" usage:"[profile=<name>]" help:"codec profile requested from the adapter"`
	// Level requests an encoder level. Empty lets the encoder choose its default
	// or preserve the selected input level when copying compatible stream
	// metadata.
	Level string `goavctl:"level" usage:"[level=<name>]" help:"codec conformance level requested from the adapter"`
	// Channels, SampleRate, ClockRate, ChannelLayout set the encoder output audio
	// format (zero = derive from the input stream). ChannelsSet/SampleRateSet
	// record that the value was requested explicitly, so a requested zero is
	// validated rather than treated as unset.
	Channels      int    `goavctl:"channels,positive" usage:"[channels=<n>]" help:"encoder output audio channel count"`
	SampleRate    int    `goavctl:"sample_rate,positive" usage:"[sample_rate=<hz>]" help:"encoder output audio sample rate"`
	ClockRate     uint32 `goavctl:"clock_rate,positive" usage:"[clock_rate=<hz>]" help:"RTP clock rate"`
	ChannelLayout string `goavctl:"channel_layout" usage:"[channel_layout=<layout>]" help:"encoder output audio channel layout"`
	ChannelsSet   bool   `goavctl:"-"`
	SampleRateSet bool   `goavctl:"-"`
	// Custom carries adapter-specific string settings that should not become
	// common fields. CLI and control-plane frontends can pass native knobs here
	// while external adapters keep ownership of validation and interpretation.
	Custom av.Metadata `goavctl:"custom,unknown" usage:"[native_key=value...]" help:"adapter-owned settings not claimed by typed codec fields"`
	// Control is the raw escape hatch: at encoder/decoder open the adapter invokes
	// it with its *concrete* native encoder/decoder (or, for construction-only
	// knobs, its options builder — the adapter documents which), so the caller can
	// type-assert to the real type and apply anything the library exposes. Nothing
	// is ever unreachable. A non-nil error from Control fails the open. This single
	// callback replaces a separate typed-config blob — it is strictly more capable.
	Control func(any) error `goavctl:"-"`
}

// DecodeConfig is everything a decoder needs at open: the stream's declared
// parameters, latency/resilience policy, output bounds, and settings.
type DecodeConfig struct {
	Stream     av.Stream
	Realtime   bool
	LowLatency bool
	Resilience ResiliencePolicy
	Bounds     DecodeBounds
	Settings   CodecSettings
	// OpaqueState carries adapter-specific, caller-owned state. Adapter
	// packages must document the concrete type before reading it.
	OpaqueState any
}

// EncodeConfig is everything an encoder needs at open: the input stream, the
// target codec parameters, latency mode, and settings.
type EncodeConfig struct {
	Stream     av.Stream
	Parameters av.CodecParameters
	Realtime   bool
	LowLatency bool
	Settings   CodecSettings
}

// DecodeResult is the caller-owned output buffer DecodeInto/FlushInto append
// to: decoded frames, emitted events, and control requests. Reusing one
// result across calls keeps decode allocation-free.
type DecodeResult struct {
	Frames   []av.Frame
	Events   []av.Event
	Requests []ControlRequest
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *DecodeResult) Reset() {
	for i := range r.Frames {
		r.Frames[i].Reset()
	}
	for i := range r.Events {
		r.Events[i].Reset()
	}
	for i := range r.Requests {
		r.Requests[i].Reset()
	}
	r.Frames = r.Frames[:0]
	r.Events = r.Events[:0]
	r.Requests = r.Requests[:0]
}

// EncodeResult is the caller-owned output buffer EncodeInto/FlushInto append
// to: encoded packets and emitted events. Reusing one result across calls
// keeps encode allocation-free.
type EncodeResult struct {
	Packets []av.Packet
	Events  []av.Event
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *EncodeResult) Reset() {
	for i := range r.Packets {
		r.Packets[i].Reset()
	}
	for i := range r.Events {
		r.Events[i].Reset()
	}
	r.Packets = r.Packets[:0]
	r.Events = r.Events[:0]
}

// ControlRequest is an upstream ask a decoder emits while decoding — most
// commonly a keyframe request after unrecoverable loss.
type ControlRequest struct {
	Type     ControlType
	StreamID av.StreamID
	Reason   string
}

// Reset clears the request for reuse.
func (r *ControlRequest) Reset() {
	r.Type = ""
	r.StreamID = ""
	r.Reason = ""
}

// ControlType names a decoder's upstream ask.
type ControlType string

// ControlRequestKeyframe asks the producer for a keyframe so decoding can
// resynchronize.
const (
	ControlRequestKeyframe ControlType = "request_keyframe"
)

// Decoder turns packets into frames through caller-owned result buffers.
// DecodeInto may emit zero or more frames per packet; FlushInto drains
// buffered output at end of stream; HandleEvent reacts to in-band events
// (discontinuities reset state).
type Decoder interface {
	Descriptor() Descriptor
	Open(context.Context, DecodeConfig) error
	DecodeInto(context.Context, *av.Packet, *DecodeResult) error
	FlushInto(context.Context, *DecodeResult) error
	HandleEvent(context.Context, *av.Event) error
	Close() error
}

// Encoder turns frames into packets through caller-owned result buffers.
// FlushInto drains buffered output at end of stream; HandleEvent applies live
// controls (EventKeyframeRequired, EventBitrateChanged) — an encoder that
// cannot apply one must return an error, never ignore it.
type Encoder interface {
	Descriptor() Descriptor
	Open(context.Context, EncodeConfig) error
	EncodeInto(context.Context, *av.Frame, *EncodeResult) error
	FlushInto(context.Context, *EncodeResult) error
	HandleEvent(context.Context, *av.Event) error
	Close() error
}

// DecoderFactory opens decoders for a registered descriptor; registries hold
// factories so nothing allocates until a chain actually decodes.
type DecoderFactory interface {
	NewDecoder(context.Context, DecodeConfig) (Decoder, error)
}

// DecodeStateFactory is an optional extension for decoder factories that can
// provision adapter-specific state for high-level runtimes. Low-level callers
// may still pass DecodeConfig.OpaqueState directly when they need exact control.
type DecodeStateFactory interface {
	NewDecodeState(context.Context, DecodeConfig) (any, error)
}

// EncoderFactory opens encoders for a registered descriptor; registries hold
// factories so nothing allocates until a chain actually encodes.
type EncoderFactory interface {
	NewEncoder(context.Context, EncodeConfig) (Encoder, error)
}
