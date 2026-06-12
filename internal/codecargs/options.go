package codecargs

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/cliargs"
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
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// ParseOptions maps canonical codec option keys into codec.Option values.
// Unknown keys are preserved as adapter-owned custom settings.
func ParseOptions(args []Arg) ([]codec.Option, error) {
	options := make([]codec.Option, 0, len(args))
	var clockRateOptions []codec.Option
	for _, arg := range args {
		key := strings.ToLower(strings.TrimSpace(arg.Key))
		value := strings.TrimSpace(arg.Value)
		switch key {
		case "", "codec", "media":
			continue
		case "id":
			return nil, optionError("id", value, "id duplicates codec", []string{"use codec=<id>"}, nil)
		case "type":
			return nil, optionError("type", value, "type duplicates media", []string{"use media=<audio|video|subtitle>"}, nil)
		case "bitrate":
			bitrate, err := cliargs.ParseRate(value)
			if err != nil {
				return nil, optionError("bitrate", value, "encoder bitrate must be like 300k, 2M, or integer bits per second", []string{"use bitrate=900k"}, err)
			}
			options = append(options, codec.Bitrate(bitrate))
		case "fps":
			fps, err := cliargs.ParseFPS(value)
			if err != nil {
				return nil, optionError("fps", value, "fps must be a positive integer, decimal, or fraction", []string{"use fps=30", "use fps=29.97", "use fps=30000/1001"}, err)
			}
			options = append(options, codec.FPS(fps.Num, fps.Den))
		case "framerate":
			return nil, optionError("framerate", value, "framerate duplicates fps", []string{"use fps=<n|n/d>"}, nil)
		case "keyframe_interval":
			frames, err := parsePositiveInt("keyframe_interval", value)
			if err != nil {
				return nil, optionError("keyframe_interval", value, "keyframe_interval must be a positive integer", []string{"use keyframe_interval=60"}, err)
			}
			options = append(options, codec.KeyframeInterval(frames))
		case "profile":
			options = append(options, codec.Profile(value))
		case "level":
			options = append(options, codec.Level(value))
		case "channels":
			channels, err := parsePositiveInt("channels", value)
			if err != nil {
				return nil, optionError("channels", value, "channels must be a positive integer", []string{"use channels=1", "use channels=2"}, err)
			}
			options = append(options, codec.Channels(channels))
		case "ch":
			return nil, optionError("ch", value, "ch duplicates channels", []string{"use channels=<n>"}, nil)
		case "sample_rate":
			sampleRate, err := parsePositiveInt("sample_rate", value)
			if err != nil {
				return nil, optionError("sample_rate", value, "sample_rate must be a positive integer", []string{"use sample_rate=48000"}, err)
			}
			options = append(options, codec.SampleRate(sampleRate))
		case "clock_rate":
			clockRate, err := parsePositiveUint32("clock_rate", value)
			if err != nil {
				return nil, optionError("clock_rate", value, "clock_rate must be a positive uint32", []string{"use clock_rate=90000"}, err)
			}
			clockRateOptions = append(clockRateOptions, codec.ClockRate(clockRate))
		case "rate":
			return nil, optionError("rate", value, "rate is ambiguous for encoder settings", []string{"use bitrate=<rate> for bitrate", "use sample_rate=<hz> for audio sample rate"}, nil)
		case "bitrate_bps":
			return nil, optionError("bitrate_bps", value, "bitrate_bps duplicates bitrate", []string{"use bitrate=<rate>"}, nil)
		case "samplerate":
			return nil, optionError("samplerate", value, "samplerate duplicates sample_rate", []string{"use sample_rate=<hz>"}, nil)
		case "clockrate":
			return nil, optionError("clockrate", value, "clockrate duplicates clock_rate", []string{"use clock_rate=<hz>"}, nil)
		case "keyint", "gop":
			return nil, optionError(key, value, key+" duplicates keyframe_interval", []string{"use keyframe_interval=<frames>"}, nil)
		default:
			options = append(options, codec.Setting(key, value))
		}
	}
	return append(options, clockRateOptions...), nil
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
func BuildSpec(id av.CodecID, media av.MediaType, options ...codec.Option) codec.CodecSpec {
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

func optionError(field string, value string, message string, suggestions []string, cause error) error {
	if cause != nil {
		message = message + ": " + cause.Error()
	}
	return &Error{
		Field:       field,
		Value:       value,
		Message:     message,
		Suggestions: append([]string(nil), suggestions...),
	}
}

func parsePositiveInt(name string, value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return parsed, nil
}

func parsePositiveUint32(name string, value string) (uint32, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return uint32(parsed), nil
}
