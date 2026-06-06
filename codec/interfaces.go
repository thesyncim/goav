package codec

import (
	"context"
	"time"

	"github.com/thesyncim/goav/av"
)

type Mode string

const (
	ModeDecode Mode = "decode"
	ModeEncode Mode = "encode"
)

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

type Capabilities struct {
	CodecID       av.CodecID
	Type          av.MediaType
	Decode        bool
	Encode        bool
	Realtime      bool
	SampleFormats []string
	PixelFormats  []string
	RTPPayloads   []string
	BuildTags     []string
	Experimental  bool
}

type Backend struct {
	Name    string
	Module  string
	Package string
	Status  string
}

var PlannedBackends = []Backend{
	{Name: "gopus", Module: "github.com/thesyncim/gopus", Status: "planned"},
	{Name: "govpx", Module: "github.com/thesyncim/govpx", Status: "planned"},
	{Name: "goh264", Module: "github.com/thesyncim/goh264", Status: "planned"},
	{Name: "goav1", Module: "github.com/thesyncim/goav1", Status: "planned"},
}

type ResiliencePolicy struct {
	AcceptLoss       bool
	ConcealAudio     bool
	DropDamagedVideo bool
	RequestKeyframes bool
	MaxReorderDelay  time.Duration
}

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

type DecodeConfig struct {
	Stream     av.Stream
	Realtime   bool
	LowLatency bool
	Resilience ResiliencePolicy
	Bounds     DecodeBounds
	// OpaqueState carries adapter-specific, caller-owned state. Adapter
	// packages must document the concrete type before reading it.
	OpaqueState any
}

type EncodeConfig struct {
	Stream     av.Stream
	Parameters av.CodecParameters
	Realtime   bool
	LowLatency bool
	Bitrate    int
	Framerate  av.Duration
	Opaque     map[string]any
}

type DecodeResult struct {
	Frames   []av.Frame
	Events   []av.Event
	Requests []ControlRequest
}

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

type EncodeResult struct {
	Packets []av.Packet
	Events  []av.Event
}

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

type ControlRequest struct {
	Type     ControlType
	StreamID av.StreamID
	Reason   string
}

func (r *ControlRequest) Reset() {
	r.Type = ""
	r.StreamID = ""
	r.Reason = ""
}

type ControlType string

const (
	ControlRequestKeyframe ControlType = "request_keyframe"
	ControlResetDecoder    ControlType = "reset_decoder"
	ControlDropUntilSync   ControlType = "drop_until_sync"
)

type Decoder interface {
	Descriptor() Descriptor
	Open(context.Context, DecodeConfig) error
	DecodeInto(context.Context, *av.Packet, *DecodeResult) error
	FlushInto(context.Context, *DecodeResult) error
	HandleEvent(context.Context, *av.Event) error
	Close() error
}

type Encoder interface {
	Descriptor() Descriptor
	Open(context.Context, EncodeConfig) error
	EncodeInto(context.Context, *av.Frame, *EncodeResult) error
	FlushInto(context.Context, *EncodeResult) error
	HandleEvent(context.Context, *av.Event) error
	Close() error
}

type DecoderFactory interface {
	NewDecoder(context.Context, DecodeConfig) (Decoder, error)
}

// DecodeStateFactory is an optional extension for decoder factories that can
// provision adapter-specific state for high-level runtimes. Low-level callers
// may still pass DecodeConfig.OpaqueState directly when they need exact control.
type DecodeStateFactory interface {
	NewDecodeState(context.Context, DecodeConfig) (any, error)
}

type EncoderFactory interface {
	NewEncoder(context.Context, EncodeConfig) (Encoder, error)
}
