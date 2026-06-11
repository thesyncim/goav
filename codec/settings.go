package codec

import "github.com/thesyncim/goav/av"

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

// Channels sets the encoder output channel count (Mono/Stereo).
func Channels(channels int) Option {
	return func(s *CodecSettings) {
		s.Channels = channels
		s.ChannelsSet = true
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

// Control is the raw escape hatch: at encoder/decoder open the adapter invokes fn
// with its concrete native encoder/decoder, so you can type-assert to the real
// type and apply anything the underlying library exposes. A non-nil error fails
// the open. This single callback covers every codec-specific knob the common
// typed settings do not.
func Control(fn func(any) error) Option {
	return func(s *CodecSettings) {
		if fn != nil {
			s.Control = fn
		}
	}
}
