package filter

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
)

var (
	ErrNotFound             = errors.New("filter: not found")
	ErrNilFilter            = errors.New("filter: nil filter")
	ErrResultFull           = errors.New("filter: result capacity full")
	ErrUnsupportedFormat    = errors.New("filter: unsupported format")
	ErrOutputBufferTooSmall = errors.New("filter: output buffer too small")
)

const (
	FactoryResize   = "resize"
	FactoryResample = "resample"
)

type Descriptor struct {
	Name      string
	Input     av.MediaType
	Output    av.MediaType
	Realtime  bool
	Stateless bool
	Metadata  av.Metadata
}

type FrameFilter interface {
	Descriptor() Descriptor
	Open(context.Context, Config) error
	FilterInto(context.Context, *av.Frame, *Result) error
	FlushInto(context.Context, *Result) error
	HandleEvent(context.Context, *av.Event) error
	Close() error
}

type Result struct {
	Frames []av.Frame
	Events []av.Event
}

func (r *Result) Reset() {
	for i := range r.Frames {
		r.Frames[i].Reset()
	}
	for i := range r.Events {
		r.Events[i].Reset()
	}
	r.Frames = r.Frames[:0]
	r.Events = r.Events[:0]
}

type Config struct {
	Stream   av.Stream
	Realtime bool
	Video    *ResizeConfig
	Audio    *ResampleConfig
	Opaque   map[string]any
}

type ResizeConfig struct {
	Width       int
	Height      int
	PixelFormat string
	Mode        ResizeMode
}

type ResizeMode string

const (
	ResizeExact       ResizeMode = "exact"
	ResizeFit         ResizeMode = "fit"
	ResizeFill        ResizeMode = "fill"
	ResizePassthrough ResizeMode = "passthrough"
)

type ResampleConfig struct {
	SampleRate    int
	Channels      int
	ChannelLayout string
	SampleFormat  string
}

type Factory interface {
	NewFilter(context.Context, Config) (FrameFilter, error)
}
