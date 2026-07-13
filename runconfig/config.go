// Package runconfig owns goav runtime construction options.
//
// The root goav package stays focused on recipe grammar. Import this package
// when an application needs to configure a bare runtime, register custom
// codecs, formats, or filters, change buffering, or inject clocks.
package runconfig

import (
	"errors"
	"fmt"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Config is the mutable runtime construction state passed to Option values.
// Options may replace registries or update graph policy, but NewConfig
// validates the final config before the root package publishes a concrete
// Runtime.
type Config struct {
	Codecs           *codec.SimpleRegistry
	Filters          *filter.SimpleRegistry
	Formats          *format.SimpleRegistry
	Buffer           pipeline.BufferPolicy
	Realtime         bool
	Clock            av.Clock
	EventCapacity    int
	CloseWaitTimeout time.Duration
	MediaPools       bool
	ShapeDeltas      []ShapeDeltaContributor
	QoSPolicy        func(av.Event) []control.Control
}

// ShapeDeltaRequest is passed to a runtime-registered shape solver extension
// when a chain's .Auto(...) policy is active and the next operation needs a
// shape the current stream does not satisfy.
type ShapeDeltaRequest struct {
	Actual     shape.Spec
	Expected   shape.Spec
	Preference shape.Spec
	Realtime   bool
}

// ShapeDeltaPlan is one custom solver insertion. Stage must be a fresh
// pipeline.Stage for this planning call; stages that implement shape.Contract
// teach the planner the output shape after insertion.
type ShapeDeltaPlan struct {
	Component string
	Detail    string
	Needed    shape.Policy
	Stage     pipeline.Stage
}

// ShapeDeltaContributor teaches one runtime how to plan a custom .Auto(...)
// conversion class. Return planned=false when this contributor does not apply
// to the request.
type ShapeDeltaContributor interface {
	ShapeDelta(ShapeDeltaRequest) (ShapeDeltaPlan, bool, error)
}

// ShapeDeltaFunc adapts a function into a ShapeDeltaContributor.
type ShapeDeltaFunc func(ShapeDeltaRequest) (ShapeDeltaPlan, bool, error)

// ShapeDelta calls f(request).
func (f ShapeDeltaFunc) ShapeDelta(request ShapeDeltaRequest) (ShapeDeltaPlan, bool, error) {
	if f == nil {
		return ShapeDeltaPlan{}, false, nil
	}
	return f(request)
}

// Option configures a runtime under construction: registries (codecs, formats,
// filters), pacing (WithRealtime, WithClock), graph policy
// (WithBufferPolicy, WithEventCapacity, WithCloseWaitTimeout), and optional
// runtime-owned scratch pooling. Invalid options return an error from goav.New
// instead of silently no-oping.
type Option func(*Config) error

// NewConfig builds the default bare runtime configuration, applies options in
// order, and validates the result.
func NewConfig(options ...Option) (*Config, error) {
	config := DefaultConfig()
	for i, option := range options {
		if option == nil {
			return nil, optionError(i, "option is nil", nil)
		}
		if err := option(config); err != nil {
			return nil, optionError(i, "option rejected config", err)
		}
	}
	if err := Validate(config); err != nil {
		return nil, err
	}
	return config, nil
}

// DefaultConfig returns a new bare runtime configuration: per-runtime
// registries with no adapters beyond content sniffing and realtime pacing on.
func DefaultConfig() *Config {
	formats := format.NewRegistry()
	formats.RegisterProber(format.DefaultProber())
	return &Config{
		Codecs:   codec.NewRegistry(),
		Filters:  filter.NewRegistry(),
		Formats:  formats,
		Realtime: true,
	}
}

// Validate reports invalid runtime construction state.
func Validate(config *Config) error {
	switch {
	case config == nil:
		return errors.New("goav: runtime config is nil")
	case config.Codecs == nil:
		return errors.New("goav: runtime config has nil codec registry")
	case config.Filters == nil:
		return errors.New("goav: runtime config has nil filter registry")
	case config.Formats == nil:
		return errors.New("goav: runtime config has nil format registry")
	case config.EventCapacity < 0:
		return fmt.Errorf("goav: runtime event capacity must be non-negative: %d", config.EventCapacity)
	case config.CloseWaitTimeout < 0:
		return fmt.Errorf("goav: runtime close wait timeout must be non-negative: %s", config.CloseWaitTimeout)
	default:
		return nil
	}
}

func optionError(index int, reason string, cause error) error {
	if cause != nil {
		return fmt.Errorf("goav: runtime option %d invalid: %s: %w", index, reason, cause)
	}
	return fmt.Errorf("goav: runtime option %d invalid: %s", index, reason)
}

// WithCodecAdapter registers a whole codec bundle at once: the callback
// receives the runtime's codec registry. Use WithDecoder/WithEncoder for the
// direct single-value form.
func WithCodecAdapter(register func(*codec.SimpleRegistry)) Option {
	return func(config *Config) error {
		if register == nil {
			return errors.New("codec registry callback is nil")
		}
		register(config.Codecs)
		return nil
	}
}

// WithCodecDescriptor registers a codec descriptor without an implementation,
// so capability checks (Explain, validation) recognize the codec even when
// encode/decode factories come from elsewhere.
func WithCodecDescriptor(desc codec.Descriptor) Option {
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDescriptor(desc)
	})
}

// WithDecoder adds one decoder factory by descriptor, passing the
// implementation directly instead of touching a registry.
func WithDecoder(desc codec.Descriptor, factory codec.DecoderFactory) Option {
	if factory == nil {
		return func(*Config) error {
			return errors.New("decoder factory is nil")
		}
	}
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterDecoder(desc, factory)
	})
}

// WithEncoder adds one encoder factory by descriptor, passing the
// implementation directly instead of touching a registry.
func WithEncoder(desc codec.Descriptor, factory codec.EncoderFactory) Option {
	if factory == nil {
		return func(*Config) error {
			return errors.New("encoder factory is nil")
		}
	}
	return WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		registry.RegisterEncoder(desc, factory)
	})
}

// WithFilter adds one filter factory by descriptor, mirroring WithDecoder and
// WithEncoder.
func WithFilter(desc filter.Descriptor, factory filter.Factory) Option {
	if factory == nil {
		return func(*Config) error {
			return errors.New("filter factory is nil")
		}
	}
	return WithFilterAdapter(func(registry *filter.SimpleRegistry) {
		registry.RegisterFactory(desc, factory)
	})
}

// WithMuxer adds one muxer factory for a container format.
func WithMuxer(id av.FormatID, factory format.MuxerFactory) Option {
	if factory == nil {
		return func(*Config) error {
			return errors.New("muxer factory is nil")
		}
	}
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(id, factory)
	})
}

// WithDemuxer adds one demuxer factory for a container format.
func WithDemuxer(id av.FormatID, factory format.DemuxerFactory) Option {
	if factory == nil {
		return func(*Config) error {
			return errors.New("demuxer factory is nil")
		}
	}
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterDemuxer(id, factory)
	})
}

// WithProber adds one format prober for content sniffing.
func WithProber(prober format.Prober) Option {
	if prober == nil {
		return func(*Config) error {
			return errors.New("format prober is nil")
		}
	}
	return WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterProber(prober)
	})
}

// WithFormatAdapter registers a whole container-format bundle at once: the
// callback receives the runtime's format registry.
func WithFormatAdapter(register func(*format.SimpleRegistry)) Option {
	return func(config *Config) error {
		if register == nil {
			return errors.New("format registry callback is nil")
		}
		register(config.Formats)
		return nil
	}
}

// WithFilterAdapter registers a whole filter bundle at once: the callback
// receives the runtime's filter registry.
func WithFilterAdapter(register func(*filter.SimpleRegistry)) Option {
	return func(config *Config) error {
		if register == nil {
			return errors.New("filter registry callback is nil")
		}
		register(config.Filters)
		return nil
	}
}

// WithBufferPolicy sets the default node buffer policy for graphs this runtime
// builds. Branch-local .Buffer(flow....) declarations override it per branch.
func WithBufferPolicy(policy pipeline.BufferPolicy) Option {
	return func(config *Config) error {
		config.Buffer = policy
		return nil
	}
}

// WithEventCapacity sets the buffered capacity of the task event stream and of
// each Watch subscriber's channel.
func WithEventCapacity(capacity int) Option {
	return func(config *Config) error {
		if capacity < 0 {
			return fmt.Errorf("event capacity must be non-negative: %d", capacity)
		}
		config.EventCapacity = capacity
		return nil
	}
}

// WithCloseWaitTimeout sets the diagnostic timeout graph Close and Remove use
// while waiting for in-flight handlers to drain. A zero timeout keeps the
// pipeline default.
func WithCloseWaitTimeout(timeout time.Duration) Option {
	return func(config *Config) error {
		if timeout < 0 {
			return fmt.Errorf("close wait timeout must be non-negative: %s", timeout)
		}
		config.CloseWaitTimeout = timeout
		return nil
	}
}

// WithMediaPools enables runtime-scoped scratch pools for cold attach/detach
// churn. Pools are owned by one Runtime, never shared globally, and only
// recycle internal media scratch whose lifetime the runtime controls.
func WithMediaPools(enabled bool) Option {
	return func(config *Config) error {
		config.MediaPools = enabled
		return nil
	}
}

// WithShapeDelta registers one per-runtime shape-solver extension. The
// contributor is consulted only when .Auto(...) is active; use
// shape.AllowCustom("name") in the policy returned by the contributor and in
// caller recipes to make a custom class explicit.
func WithShapeDelta(contributor ShapeDeltaContributor) Option {
	return func(config *Config) error {
		if contributor == nil {
			return errors.New("shape delta contributor is nil")
		}
		config.ShapeDeltas = append(config.ShapeDeltas, contributor)
		return nil
	}
}

// WithQoSPolicy sets the opt-in, per-runtime QoS policy: every task this
// runtime builds feeds each av.EventQoS report (read it with
// av.EventQoSReport) to the policy on the task's cold event-delivery path and
// issues the returned controls through the task's own Control seam — exactly
// what an application could do by hand with Watch + Control, packaged. A
// control the task refuses is published back through Watch as an av.EventQoS
// carrying the error on Cause. Reports describe delivery lateness at the
// task's gates and queues, not network conditions; transports keep their own
// congestion control.
func WithQoSPolicy(policy func(av.Event) []control.Control) Option {
	return func(config *Config) error {
		if policy == nil {
			return errors.New("qos policy is nil")
		}
		config.QoSPolicy = policy
		return nil
	}
}

// WithRealtime selects pacing: true (the default) delivers file media when its
// media time is due on the runtime clock; false pumps at full speed.
func WithRealtime(realtime bool) Option {
	return func(config *Config) error {
		config.Realtime = realtime
		return nil
	}
}

// WithClock sets the time source the runtime's realtime pacing uses. Nil or
// unset defaults to av.MonotonicClock().
func WithClock(clock av.Clock) Option {
	return func(config *Config) error {
		config.Clock = clock
		return nil
	}
}
