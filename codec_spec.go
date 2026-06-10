// Codec spec handling: merging, cloning, and encode and codec-change value validation.

package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// codecSpecFromOptions builds a spec carrying only the Settings configured by
// decode options (decode does not set output caps).
func codecSpecFromOptions(options ...codec.Option) codec.CodecSpec {
	var spec codec.CodecSpec
	for i := range options {
		if options[i] != nil {
			options[i](&spec.Settings)
		}
	}
	return spec
}

func cloneCodecSpec(spec codec.CodecSpec) codec.CodecSpec {
	spec.Parameters.Attributes = cloneMetadata(spec.Parameters.Attributes)
	spec.Parameters.ExtraData = cloneBuffer(spec.Parameters.ExtraData)
	spec.Settings = cloneCodecSettings(spec.Settings)
	return spec
}

func cloneCodecSettings(settings codec.CodecSettings) codec.CodecSettings {
	// Config (Tier 2) and Control (Tier 3) are reference values owned by the
	// caller; copy the reference, not the target.
	return settings
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

func mergeDecodeCodecSpec(base codec.CodecSpec, override codec.CodecSpec) codec.CodecSpec {
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

func codecSpecHasParameters(spec codec.CodecSpec) bool {
	parameters := spec.Parameters
	return parameters.ID != "" ||
		parameters.Type != "" ||
		parameters.Profile != "" ||
		parameters.Level != "" ||
		parameters.ClockRate != 0 ||
		parameters.SampleRate != 0 ||
		parameters.Channels != 0 ||
		parameters.ChannelLayout != "" ||
		parameters.Width != 0 ||
		parameters.Height != 0 ||
		parameters.PixelFormat != "" ||
		parameters.SampleFormat != "" ||
		len(parameters.ExtraData.Bytes) != 0 ||
		len(parameters.Attributes) != 0
}

func encodeConfigFromSpec(spec codec.CodecSpec) codec.EncodeConfig {
	parameters := spec.Parameters
	if spec.ID == av.CodecOpus {
		if !spec.Settings.SampleRateSet {
			parameters.SampleRate = 0
			parameters.ClockRate = 0
		}
		if !spec.Settings.ChannelsSet {
			parameters.Channels = 0
			parameters.ChannelLayout = ""
		}
	}
	return codec.EncodeConfig{
		Parameters: parameters,
		Settings:   cloneCodecSettings(spec.Settings),
	}
}

func cloneEncodeConfig(config codec.EncodeConfig) codec.EncodeConfig {
	config.Stream.Codec.Attributes = cloneMetadata(config.Stream.Codec.Attributes)
	config.Stream.Codec.ExtraData = cloneBuffer(config.Stream.Codec.ExtraData)
	config.Stream.Metadata = cloneMetadata(config.Stream.Metadata)
	config.Parameters.Attributes = cloneMetadata(config.Parameters.Attributes)
	config.Parameters.ExtraData = cloneBuffer(config.Parameters.ExtraData)
	config.Settings = cloneCodecSettings(config.Settings)
	return config
}

func cloneBuffer(buffer av.Buffer) av.Buffer {
	buffer.Bytes = append([]byte(nil), buffer.Bytes...)
	return buffer
}

func validateRecipeEncode(spec codec.CodecSpec, operation string, node string) error {
	if spec.Auto {
		return &BuildError{
			Code:      "encode_auto_unresolved",
			Operation: operation,
			Node:      node,
			Reason:    "automatic codec selection is not implemented for stream recipes yet",
			Suggestions: []string{
				"choose an explicit recipe encoder with .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...))",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if spec.Copy {
		return nil
	}
	if spec.ID == "" {
		return nil
	}
	switch spec.ID {
	case av.CodecOpus, av.CodecVP8, av.CodecVP9:
		return validateRecipeEncodeValues(spec, operation, node)
	case av.CodecH264, av.CodecAV1:
		return &BuildError{
			Code:      "encode_work_in_progress",
			Operation: operation,
			Node:      node,
			Reason:    string(spec.ID) + " recipe encoding is work in progress; recipe encode branches currently support opus, vp8, and vp9",
			Suggestions: []string{
				"decode the stream with .To(goav.Sink(...))",
				"use .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) for recipe encode branches",
				"use the expert builder with an explicit codec.EncodeConfig when testing an experimental encoder",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return validateRecipeEncodeValues(spec, operation, node)
	}
}

func validateRecipeEncodeValues(spec codec.CodecSpec, operation string, node string) error {
	switch {
	case spec.Settings.Bitrate < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode bitrate must be non-negative",
			Details: []string{
				fmt.Sprintf("bitrate=%d", spec.Settings.Bitrate),
			},
			Suggestions: []string{
				"pass a positive value to codec.Bitrate(...)",
				"omit codec.Bitrate(...) when the encoder should choose its default",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.Framerate.Value < 0 || spec.Settings.Framerate.Base.Num < 0 || spec.Settings.Framerate.Base.Den < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode FPS must be positive",
			Details: []string{
				fmt.Sprintf("fps_duration=%d/%d/%d", spec.Settings.Framerate.Value, spec.Settings.Framerate.Base.Num, spec.Settings.Framerate.Base.Den),
			},
			Suggestions: []string{
				"pass a positive value to goav.FPS(...)",
				"omit goav.FPS(...) when the encoder should infer frame cadence",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.KeyframeInterval < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode keyframe interval must be non-negative",
			Details: []string{
				fmt.Sprintf("keyframe_interval=%d", spec.Settings.KeyframeInterval),
			},
			Suggestions: []string{
				"pass a positive value to goav.KeyframeInterval(...)",
				"omit goav.KeyframeInterval(...) when the encoder should choose its default cadence",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.SampleRateSet && spec.Parameters.SampleRate <= 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "explicit encode sample rate must be positive",
			Details: []string{
				fmt.Sprintf("sample_rate=%d", spec.Parameters.SampleRate),
			},
			Suggestions: []string{
				"use codec.SampleRate(rate) with a positive rate",
				"omit codec.SampleRate(...) to use the selected stream rate",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.ChannelsSet && spec.Parameters.Channels <= 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "explicit encode channel count must be positive",
			Details: []string{
				fmt.Sprintf("channels=%d", spec.Parameters.Channels),
			},
			Suggestions: []string{
				"use codec.Channels(codec.Mono), codec.Channels(codec.Stereo), or another positive channel count",
				"omit codec.Channels(...) to use the selected stream channel count",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return nil
	}
}

func validateCodecChangePolicy(operation string, node string, policy CodecChangePolicy) error {
	if !codecChangePolicySet(policy) || policy == RealtimeCodecChangePolicy() {
		return nil
	}
	return &BuildError{
		Code:      "codec_change_policy_unsupported",
		Operation: operation,
		Node:      node,
		Reason:    "custom codec-change policies are not implemented yet",
		Details: []string{
			"supported: " + codecChangePolicyDetail(RealtimeCodecChangePolicy()),
			"requested: " + codecChangePolicyDetail(policy),
		},
		Suggestions: []string{
			"use goav.RealtimeCodecChangePolicy() for today's live receive behavior",
			"use packet-preserving goav.From(input).Copy().To(output) when codec changes should stay encoded",
			"rebuild the job when a live stream switches to a different decoder codec",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func codecChangePolicySet(policy CodecChangePolicy) bool {
	return policy.RebindCompatible || policy.RequestKeyframe || policy.DropUntilSync || policy.FailOnDifferentCodec
}

func codecChangePolicyDetail(policy CodecChangePolicy) string {
	if !codecChangePolicySet(policy) {
		return "codec-change=default"
	}
	parts := make([]string, 0, 4)
	if policy.RebindCompatible {
		parts = append(parts, "rebind-compatible")
	}
	if policy.RequestKeyframe {
		parts = append(parts, "request-keyframe")
	}
	if policy.DropUntilSync {
		parts = append(parts, "drop-until-sync")
	}
	if policy.FailOnDifferentCodec {
		parts = append(parts, "fail-different-codec")
	}
	if len(parts) == 0 {
		return "codec-change=custom"
	}
	return "codec-change=" + strings.Join(parts, ",")
}
