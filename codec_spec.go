// Codec spec handling: merging, cloning, and encode value validation.

package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// codecSpecFromOptions builds a spec carrying only the Settings configured by
// decode options (decode does not set output caps).
func codecSpecFromOptions(options ...codec.Option) codec.Spec {
	var spec codec.Spec
	for i := range options {
		if options[i] != nil {
			options[i](&spec.Settings)
		}
	}
	return spec
}

func mergeCodecSettings(base codec.CodecSettings, override codec.CodecSettings) codec.CodecSettings {
	if override.Bitrate != 0 {
		base.Bitrate = override.Bitrate
	}
	if override.Framerate != (av.Duration{}) {
		base.Framerate = override.Framerate
	}
	if override.KeyframeInterval != 0 {
		base.KeyframeInterval = override.KeyframeInterval
	}
	if override.Profile != "" {
		base.Profile = override.Profile
	}
	if override.Level != "" {
		base.Level = override.Level
	}
	if override.ChannelsSet {
		base.Channels = override.Channels
		base.ChannelLayout = override.ChannelLayout
		base.ChannelsSet = true
	}
	if override.SampleRateSet {
		base.SampleRate = override.SampleRate
		base.SampleRateSet = true
	}
	if override.ClockRate != 0 {
		base.ClockRate = override.ClockRate
	}
	if override.Control != nil {
		base.Control = override.Control
	}
	return base
}

func mergeDecodeCodecSpec(base codec.Spec, override codec.Spec) codec.Spec {
	if override.ID != "" {
		base.ID = override.ID
	}
	if override.Type != "" {
		base.Type = override.Type
	}
	base.Parameters = mergeCodecParameters(base.Parameters, override.Parameters)
	base.Settings = mergeCodecSettings(base.Settings, override.Settings)
	return base
}
