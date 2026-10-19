package codec

import (
	"fmt"
	"reflect"

	"github.com/thesyncim/goav/av"
)

// Audio channel-count shorthands.
const (
	Mono   = 1
	Stereo = 2
)

// Option configures encoder/decoder settings. Options live in the codec package
// (not the goav root) so the grammar stays small; goav's codec constructors
// (Opus, VP9, …) accept them.
type Option func(*CodecSettings)

// Bitrate sets the target encoder bitrate in bits per second.
func Bitrate(bitsPerSecond int) Option {
	return func(s *CodecSettings) { s.Bitrate = bitsPerSecond }
}

// FPS sets the encode frame cadence. den defaults to 1 (so FPS(30) is 30/1).
func FPS(num int, den ...int) Option {
	return func(s *CodecSettings) {
		rateDen := 1
		if len(den) != 0 {
			rateDen = den[0]
		}
		if num <= 0 || rateDen <= 0 {
			s.Framerate = av.Duration{Value: -1}
			return
		}
		s.Framerate = av.Duration{Value: int64(rateDen), Base: av.TimeBase{Num: 1, Den: int64(num)}}
	}
}

// KeyframeInterval sets the keyframe cadence in encoded frames.
func KeyframeInterval(frames int) Option {
	return func(s *CodecSettings) { s.KeyframeInterval = frames }
}

// Profile requests a codec profile by its codec-specific name (VP9 "0",
// H264 "baseline"); the adapter validates it at open and fails on values its
// backend cannot honor. Empty leaves the adapter default.
func Profile(profile string) Option {
	return func(s *CodecSettings) { s.Profile = profile }
}

// Level requests a codec conformance level by its codec-specific name
// (H264 "3.1"); the adapter validates it at open and fails on values its
// backend cannot honor. Empty leaves the adapter default.
func Level(level string) Option {
	return func(s *CodecSettings) { s.Level = level }
}

// Backend makes one backend serve the codec, by its name (videotoolbox,
// openh264, goav1), instead of the runtime's preference order. A backend
// that is not registered, or cannot meet the request, is refused with the
// backends that can; selection never falls back on its own.
func Backend(name string) Option {
	return func(s *CodecSettings) { s.Backend = name }
}

// BitDepth requests the coded sample bit depth (8, 10, 12); selection keeps
// to the backends that declare it.
func BitDepth(bits int) Option {
	return func(s *CodecSettings) { s.BitDepth = bits }
}

// Setting records one adapter-specific string setting. Common knobs should use
// the typed options above; Setting is for native/custom adapters that own their
// own validation vocabulary.
func Setting(key string, value string) Option {
	return func(s *CodecSettings) {
		if key == "" {
			return
		}
		if s.Custom == nil {
			s.Custom = av.Metadata{}
		}
		s.Custom[key] = value
	}
}

// Channels sets the encoder output channel count (Mono/Stereo).
func Channels(channels int) Option {
	return func(s *CodecSettings) {
		s.Channels = channels
		s.ChannelsSet = true
		s.ChannelLayout = ""
		switch channels {
		case Mono:
			s.ChannelLayout = "mono"
		case Stereo:
			s.ChannelLayout = "stereo"
		}
	}
}

// SampleRate sets the encoder output sample rate in Hz.
func SampleRate(sampleRate int) Option {
	return func(s *CodecSettings) {
		s.SampleRate = sampleRate
		s.ClockRate = uint32(sampleRate)
		s.SampleRateSet = true
	}
}

// ClockRate sets the RTP clock rate in Hz.
func ClockRate(clockRate uint32) Option {
	return func(s *CodecSettings) { s.ClockRate = clockRate }
}

// Control is the raw escape hatch: when the codec opens, the adapter calls fn
// with its concrete native encoder or decoder (or the construction config it
// documents), so host code applies anything the backend exposes. T is that
// native type, inferred from fn:
//
//	codec.Opus(codec.Control(func(e *gopus.Encoder) error { return e.SetComplexity(10) }))
//
// A native value that is not a T fails the open, naming both types, as does
// an error from fn.
func Control[T any](fn func(T) error) Option {
	return func(s *CodecSettings) {
		if fn == nil {
			return
		}
		s.Control = func(native any) error {
			v, ok := native.(T)
			if !ok {
				return fmt.Errorf("codec: Control wants %s, the adapter offers %T", reflect.TypeFor[T](), native)
			}
			return fn(v)
		}
	}
}
