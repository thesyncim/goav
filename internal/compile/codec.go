package compile

import (
	"bytes"
	"fmt"
	"maps"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/shape"
)

func CloneCodecSpec(spec codec.Spec) codec.Spec {
	spec.Parameters.Attributes = CloneMetadata(spec.Parameters.Attributes)
	spec.Parameters.ExtraData = CloneBuffer(spec.Parameters.ExtraData)
	spec.Settings = CloneCodecSettings(spec.Settings)
	return spec
}

func CodecSpecEqual(a, b codec.Spec) bool {
	return a.ID == b.ID &&
		a.Type == b.Type &&
		CodecParametersEqual(a.Parameters, b.Parameters) &&
		codecSettingsEqual(a.Settings, b.Settings) &&
		a.Copy == b.Copy &&
		a.Auto == b.Auto
}

func CodecParametersEqual(a, b av.CodecParameters) bool {
	return a.ID == b.ID &&
		a.Type == b.Type &&
		a.Profile == b.Profile &&
		a.Level == b.Level &&
		a.ClockRate == b.ClockRate &&
		a.SampleRate == b.SampleRate &&
		a.Channels == b.Channels &&
		a.ChannelLayout == b.ChannelLayout &&
		a.Width == b.Width &&
		a.Height == b.Height &&
		a.PixelFormat == b.PixelFormat &&
		a.SampleFormat == b.SampleFormat &&
		bufferEqual(a.ExtraData, b.ExtraData) &&
		metadataEqual(a.Attributes, b.Attributes)
}

func codecSettingsEqual(a, b codec.CodecSettings) bool {
	return a.Bitrate == b.Bitrate &&
		a.Framerate == b.Framerate &&
		a.KeyframeInterval == b.KeyframeInterval &&
		a.Profile == b.Profile &&
		a.Level == b.Level &&
		a.Channels == b.Channels &&
		a.SampleRate == b.SampleRate &&
		a.ClockRate == b.ClockRate &&
		a.ChannelLayout == b.ChannelLayout &&
		a.ChannelsSet == b.ChannelsSet &&
		a.SampleRateSet == b.SampleRateSet &&
		a.Control == nil &&
		b.Control == nil
}

func bufferEqual(a, b av.Buffer) bool {
	return a.Owner == nil &&
		b.Owner == nil &&
		bytesEqual(a.Bytes, b.Bytes)
}

func bytesEqual(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}

func metadataEqual(a, b av.Metadata) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func CloneCodecSettings(settings codec.CodecSettings) codec.CodecSettings {
	// Config (Tier 2) and control.Control (Tier 3) are reference values owned by the
	// caller; copy the reference, not the target.
	return settings
}

func CodecSpecHasParameters(spec codec.Spec) bool {
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

func encodeConfigFromSpec(spec codec.Spec) codec.EncodeConfig {
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
		Settings:   CloneCodecSettings(spec.Settings),
	}
}

func codecSpecFromStream(stream av.Stream) codec.Spec {
	parameters := stream.Codec
	id := parameters.ID
	media := FirstNonEmptyMedia(parameters.Type, stream.Type, CodecMedia(id))
	if parameters.Type == "" {
		parameters.Type = media
	}
	return CloneCodecSpec(codec.Spec{
		ID:         id,
		Type:       media,
		Parameters: parameters,
	})
}

func CloneBuffer(buffer av.Buffer) av.Buffer {
	buffer.Bytes = append([]byte(nil), buffer.Bytes...)
	return buffer
}

func ValidateRecipeEncode(spec codec.Spec, operation string, node string) error {
	if spec.Auto {
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.EncodeAutoUnresolvedCode),
			Code:      builderr.EncodeAutoUnresolvedCode,
			Operation: operation,
			Node:      node,
			Reason:    "automatic codec selection is not implemented for stream recipes yet",
			Fixes: builderr.Fixes([]string{
				"choose an explicit recipe encoder with " + builderr.EncodeHint(spec.Type),
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	}
	if spec.Copy {
		return nil
	}
	if spec.ID == "" {
		return nil
	}
	return validateRecipeEncodeValues(spec, operation, node)
}

func validateRecipeEncodeValues(spec codec.Spec, operation string, node string) error {
	switch {
	case spec.Settings.Bitrate < 0:
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.EncodeParameterInvalidCode),
			Code:      builderr.EncodeParameterInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "encode bitrate must be non-negative",
			Details:   builderr.Details(builderr.KV("bitrate", fmt.Sprintf("%d", spec.Settings.Bitrate))),
			Fixes: builderr.Fixes([]string{
				"pass a positive value to codec.Bitrate(...)",
				"omit codec.Bitrate(...) when the encoder should choose its default",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	case spec.Settings.Framerate.Value < 0 || spec.Settings.Framerate.Base.Num < 0 || spec.Settings.Framerate.Base.Den < 0:
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.EncodeParameterInvalidCode),
			Code:      builderr.EncodeParameterInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "encode FPS must be positive",
			Details:   builderr.Details(builderr.KV("fps_duration", fmt.Sprintf("%d/%d/%d", spec.Settings.Framerate.Value, spec.Settings.Framerate.Base.Num, spec.Settings.Framerate.Base.Den))),
			Fixes: builderr.Fixes([]string{
				"pass a positive value to codec.FPS(...)",
				"omit codec.FPS(...) when the encoder should infer frame cadence",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	case spec.Settings.KeyframeInterval < 0:
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.EncodeParameterInvalidCode),
			Code:      builderr.EncodeParameterInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "encode keyframe interval must be non-negative",
			Details:   builderr.Details(builderr.KV("keyframe_interval", fmt.Sprintf("%d", spec.Settings.KeyframeInterval))),
			Fixes: builderr.Fixes([]string{
				"pass a positive value to codec.KeyframeInterval(...)",
				"omit codec.KeyframeInterval(...) when the encoder should choose its default cadence",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	case spec.Settings.SampleRateSet && spec.Parameters.SampleRate <= 0:
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.EncodeParameterInvalidCode),
			Code:      builderr.EncodeParameterInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "explicit encode sample rate must be positive",
			Details:   builderr.Details(builderr.KV("sample_rate", fmt.Sprintf("%d", spec.Parameters.SampleRate))),
			Fixes: builderr.Fixes([]string{
				"use codec.SampleRate(rate) with a positive rate",
				"omit codec.SampleRate(...) to use the selected stream rate",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	case spec.Settings.ChannelsSet && spec.Parameters.Channels <= 0:
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.EncodeParameterInvalidCode),
			Code:      builderr.EncodeParameterInvalidCode,
			Operation: operation,
			Node:      node,
			Reason:    "explicit encode channel count must be positive",
			Details:   builderr.Details(builderr.KV("channels", fmt.Sprintf("%d", spec.Parameters.Channels))),
			Fixes: builderr.Fixes([]string{
				"use codec.Channels(codec.Mono), codec.Channels(codec.Stereo), or another positive channel count",
				"omit codec.Channels(...) to use the selected stream channel count",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	default:
		return nil
	}
}

// MediaShapeFromCodecSpec is the shape.Spec view of a codec spec. It stays in
// goav (not the shape package) because it crosses codec.Spec and the codecMedia/
// firstNonEmptyMedia helpers that live in the goav root.
func MediaShapeFromCodecSpec(spec codec.Spec, domain shape.MediaDomain) shape.Spec {
	out := shape.FromCodecParameters(spec.Parameters)
	out.Domain = domain
	if out.MediaKind == "" {
		out.MediaKind = FirstNonEmptyMedia(spec.Type, CodecMedia(spec.ID))
	}
	if out.Codec == "" {
		out.Codec = spec.ID
	}
	return out
}

// codecSpecInputShapes is the shape.Set view of a codec spec's input. It is a
// goav function (not a method) because codec.Spec's data lives in the codec
// package and shape.Set is reached through the goav-only codecMedia helper.
func codecSpecInputShapes(spec codec.Spec) shape.Set {
	if spec.Copy {
		return shape.Set{shape.New(shape.Domain(shape.DomainPacket))}
	}
	media := FirstNonEmptyMedia(spec.Type, spec.Parameters.Type, CodecMedia(spec.ID))
	if spec.ID == "" && !spec.Auto && media == "" {
		return nil
	}
	return shape.Set{shape.Frame(media)}
}

func codecSpecOutputShapes(spec codec.Spec, input shape.Spec) shape.Set {
	if spec.Copy {
		input.Domain = shape.DomainPacket
		return shape.Set{input}
	}
	out := input
	out.Domain = shape.DomainPacket
	out = shape.Merge(out, MediaShapeFromCodecSpec(spec, shape.DomainPacket))
	return shape.Set{out}
}

func CloneMetadata(metadata av.Metadata) av.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(av.Metadata, len(metadata))
	maps.Copy(cloned, metadata)
	return cloned
}
