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
	Backend      Backend
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

type DecodeConfig struct {
	Stream      av.Stream
	Realtime    bool
	LowLatency  bool
	Resilience  ResiliencePolicy
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

type EncodeResult struct {
	Packets []av.Packet
	Events  []av.Event
}

type ControlRequest struct {
	Type     ControlType
	StreamID av.StreamID
	Reason   string
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
	Decode(context.Context, av.Packet) (DecodeResult, error)
	Flush(context.Context) (DecodeResult, error)
	HandleEvent(context.Context, av.Event) error
	Close() error
}

type Encoder interface {
	Descriptor() Descriptor
	Open(context.Context, EncodeConfig) error
	Encode(context.Context, av.Frame) (EncodeResult, error)
	Flush(context.Context) (EncodeResult, error)
	HandleEvent(context.Context, av.Event) error
	Close() error
}

type DecoderFactory interface {
	NewDecoder(context.Context, DecodeConfig) (Decoder, error)
}

type EncoderFactory interface {
	NewEncoder(context.Context, EncodeConfig) (Encoder, error)
}

type Registry interface {
	Descriptors() []Descriptor
	Find(av.CodecID, Mode) ([]Descriptor, error)
	DecoderFactory(av.CodecID) (DecoderFactory, error)
	EncoderFactory(av.CodecID) (EncoderFactory, error)
}
