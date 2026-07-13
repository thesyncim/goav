package codecargs

import (
	"errors"
	"sort"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/argbind"
)

// Arg is one codec option key/value pair from a CLI-like grammar.
type Arg struct {
	Key   string
	Value string
}

// Error describes one invalid codec option while leaving callers free to wrap
// it in their own CLI or structured error shape.
type Error struct {
	Field       string
	Value       string
	Message     string
	Suggestions []string
	Cause       error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

var codecSettingsType = argbind.TypeOf[codec.CodecSettings]()

// ArgsUsage renders the reflected encoder settings usage fragment.
func ArgsUsage() string {
	return argbind.ArgsUsage(codecSettingsType)
}

// SettingsFields reports the reflected encoder setting fields used by CLI and
// control-plane help.
func SettingsFields() []argbind.Field {
	return argbind.OrderedFields(argbind.Fields(codecSettingsType))
}

// ParseOptions maps reflected codec settings into one codec.Option value.
// Unmatched keys are preserved as adapter-owned custom settings.
func ParseOptions(args []Arg) ([]codec.Option, error) {
	seenKeys := make(map[string]struct{}, len(args))
	bindArgs := make([]string, 0, len(args))
	for _, arg := range args {
		key := strings.ToLower(strings.TrimSpace(arg.Key))
		value := strings.TrimSpace(arg.Value)
		if key != "" {
			if _, ok := seenKeys[key]; ok {
				return nil, optionError(key, value, "encoder option "+key+" was provided more than once", []string{"keep only one " + key + "=... value"}, nil)
			}
			seenKeys[key] = struct{}{}
		}
		if err := validateReservedOrAlias(key, value); err != nil {
			return nil, err
		}
		if key == "" || key == "codec" || key == "media" {
			continue
		}
		bindArgs = append(bindArgs, key+"="+value)
	}
	result, err := argbind.Bind(argbind.Context{
		Name:                 "codec settings",
		Operation:            "parse encoder options",
		ArgsType:             codecSettingsType,
		Usage:                strings.TrimSpace("encode codec=<id> media=<kind> " + ArgsUsage()),
		Suggestions:          []string{"run `goav ctl help attach`"},
		UnknownMetadataField: "custom",
	}, bindArgs)
	if err != nil {
		return nil, codecBindError(err)
	}
	settings := result.Value.(codec.CodecSettings)
	normalizeSettings(&settings, result.Seen)
	return []codec.Option{func(target *codec.CodecSettings) { *target = settings }}, nil
}

// ParseOptionsMap parses a map of codec options in stable key order.
func ParseOptionsMap(args map[string]string) ([]codec.Option, error) {
	keys := make([]string, 0, len(args))
	for key := range args {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]Arg, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, Arg{Key: key, Value: args[key]})
	}
	return ParseOptions(ordered)
}

// BuildSpec builds the standard typed codec specs before falling back to the
// generic custom-codec constructor.
func BuildSpec(id av.CodecID, media av.MediaType, options ...codec.Option) codec.Spec {
	switch id {
	case av.CodecAV1:
		return codec.AV1(options...)
	case av.CodecVP9:
		return codec.VP9(options...)
	case av.CodecVP8:
		return codec.VP8(options...)
	case av.CodecH264:
		return codec.H264(options...)
	case av.CodecOpus:
		return codec.Opus(options...)
	default:
		return codec.Codec(id, media, options...)
	}
}

func validateReservedOrAlias(key string, value string) error {
	switch key {
	case "", "codec", "media":
		return nil
	case "id":
		return optionError("id", value, "id duplicates codec", []string{"use codec=<id>"}, nil)
	case "type":
		return optionError("type", value, "type duplicates media", []string{"use media=<audio|video|subtitle>"}, nil)
	case "rate":
		return optionError("rate", value, "rate is ambiguous for encoder settings", []string{"use bitrate=<rate> for bitrate", "use sample_rate=<hz> for audio sample rate"}, nil)
	case "bitrate_bps":
		return optionError("bitrate_bps", value, "bitrate_bps duplicates bitrate", []string{"use bitrate=<rate>"}, nil)
	case "framerate":
		return optionError("framerate", value, "framerate duplicates fps", []string{"use fps=<n|n/d>"}, nil)
	case "samplerate":
		return optionError("samplerate", value, "samplerate duplicates sample_rate", []string{"use sample_rate=<hz>"}, nil)
	case "ch":
		return optionError("ch", value, "ch duplicates channels", []string{"use channels=<n>"}, nil)
	case "clockrate":
		return optionError("clockrate", value, "clockrate duplicates clock_rate", []string{"use clock_rate=<hz>"}, nil)
	case "keyint", "gop":
		return optionError(key, value, key+" duplicates keyframe_interval", []string{"use keyframe_interval=<frames>"}, nil)
	default:
		return nil
	}
}

func normalizeSettings(settings *codec.CodecSettings, seen map[string]struct{}) {
	if _, ok := seen["channels"]; ok {
		settings.ChannelsSet = true
		if settings.ChannelLayout == "" {
			switch settings.Channels {
			case codec.Mono:
				settings.ChannelLayout = "mono"
			case codec.Stereo:
				settings.ChannelLayout = "stereo"
			}
		}
	}
	if _, ok := seen["sample_rate"]; ok {
		settings.SampleRateSet = true
		if _, clockSet := seen["clock_rate"]; !clockSet {
			settings.ClockRate = uint32(settings.SampleRate)
		}
	}
}

func codecBindError(err error) error {
	var bindErr *argbind.Error
	if errors.As(err, &bindErr) {
		message := bindErr.Message
		switch bindErr.Node {
		case "bitrate":
			message = "encoder bitrate must be like 300k, 2M, or integer bits per second"
		case "fps":
			message = "fps must be a positive integer, decimal, or fraction"
		case "keyframe_interval":
			message = "keyframe_interval must be a positive integer"
		case "channels":
			message = "channels must be a positive integer"
		case "sample_rate":
			message = "sample_rate must be a positive integer"
		case "clock_rate":
			message = "clock_rate must be a positive uint32"
		}
		return optionError(bindErr.Node, bindValue(bindErr.Details), message, bindErr.Suggestions, err)
	}
	return err
}

func bindValue(details []string) string {
	for _, detail := range details {
		if value, ok := strings.CutPrefix(detail, "value="); ok {
			return value
		}
	}
	return ""
}

func optionError(field string, value string, message string, suggestions []string, cause error) error {
	if cause != nil {
		message = message + ": " + cause.Error()
	}
	return &Error{
		Field:       field,
		Value:       value,
		Message:     message,
		Suggestions: append([]string(nil), suggestions...),
		Cause:       cause,
	}
}
