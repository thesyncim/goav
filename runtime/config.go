// Package runtime owns goav runtime construction options.
//
// The root goav package stays focused on recipe grammar. Import this package
// when an application needs to configure a bare runtime, register custom
// codecs, formats, or filters, change buffering, or inject clocks.
package runtime

import (
	"errors"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

// Config is the mutable runtime construction state passed to Option values.
// Options may replace registries or update graph policy, but NewConfig
// validates the final config before the root package publishes a concrete
// Runtime.
type Config struct {
	Codecs        *codec.SimpleRegistry
	Filters       *filter.SimpleRegistry
	Formats       *format.SimpleRegistry
	Buffer        pipeline.BufferPolicy
	Realtime      bool
	Clock         av.Clock
	EventCapacity int
}

// Option configures a runtime under construction: registries (codecs, formats,
// filters), pacing (WithRealtime, WithClock), and graph policy
// (WithBufferPolicy, WithEventCapacity). Invalid options return an error from
// goav.New instead of silently no-oping.
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
