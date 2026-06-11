package filter

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
)

// Filter sentinels, matched with errors.Is.
var (
	// ErrNotFound reports a filter name with no registered factory.
	ErrNotFound = errors.New("filter: not found")
	// ErrNilFilter reports a nil filter where one is required.
	ErrNilFilter = errors.New("filter: nil filter")
	// ErrResultFull reports a result slice at capacity.
	ErrResultFull = errors.New("filter: result capacity full")
	// ErrUnsupportedFormat reports a pixel or sample format the filter cannot
	// consume or produce.
	ErrUnsupportedFormat = errors.New("filter: unsupported format")
	// ErrOutputBufferTooSmall reports caller-owned output planes without the
	// capacity the conversion needs.
	ErrOutputBufferTooSmall = errors.New("filter: output buffer too small")
)

// The well-known filter factory names recipes resolve transforms through.
const (
	FactoryResize   = "resize"
	FactoryResample = "resample"
)

// Descriptor declares a filter's identity and capabilities — media kinds,
// formats, resize modes, realtime/stateless flags — so transform validation
// and the shape solver can select adapters before anything opens.
type Descriptor struct {
	Name          string
	Input         av.MediaType
	Output        av.MediaType
	PixelFormats  []string
	SampleFormats []string
	ResizeModes   []ResizeMode
	Realtime      bool
	Stateless     bool
	Metadata      av.Metadata
}

// FrameFilter is one frame transform (resize, resample, format convert):
// FilterInto converts frames into caller-owned results, FlushInto drains any
// buffered tail, HandleEvent reacts to in-band events.
type FrameFilter interface {
	Descriptor() Descriptor
	Open(context.Context, Config) error
	FilterInto(context.Context, *av.Frame, *Result) error
	FlushInto(context.Context, *Result) error
	HandleEvent(context.Context, *av.Event) error
	Close() error
}

// Result is the caller-owned output buffer FilterInto/FlushInto append to,
// keeping filtering allocation-free.
type Result struct {
	Frames []av.Frame
	Events []av.Event
}

// Reset clears the result for reuse, keeping allocated capacity.
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

// Config is everything a filter needs at open: the input stream, pacing, and
// exactly one of the Video or Audio conversion targets.
type Config struct {
	Stream   av.Stream
	Realtime bool
	Video    *ResizeConfig
	Audio    *ResampleConfig
	Opaque   map[string]any
}

// ResizeConfig is a video conversion target: output geometry, optional pixel
// format, and the scaling mode.
type ResizeConfig struct {
	Width       int
	Height      int
	PixelFormat string
	Mode        ResizeMode
}

// ResizeMode says how a resize maps input geometry onto the target.
type ResizeMode string

// The resize modes: exact stretches to the target, fit letterboxes inside
// it, fill crops to cover it, passthrough forwards unchanged.
const (
	ResizeExact       ResizeMode = "exact"
	ResizeFit         ResizeMode = "fit"
	ResizeFill        ResizeMode = "fill"
	ResizePassthrough ResizeMode = "passthrough"
)

// ResampleConfig is an audio conversion target: output rate, channels, and
// optional layout and sample format.
type ResampleConfig struct {
	SampleRate    int
	Channels      int
	ChannelLayout string
	SampleFormat  string
}

// Factory opens filters for a registered descriptor; registries hold
// factories so nothing opens during planning.
type Factory interface {
	NewFilter(context.Context, Config) (FrameFilter, error)
}
