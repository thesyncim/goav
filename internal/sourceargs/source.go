package sourceargs

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/cliargs"
)

// Arg is one generated source option key/value pair from a CLI-like grammar.
type Arg struct {
	Key   string
	Value string
}

// GeneratedVideo is the canonical CLI representation of a generated video
// source.
type GeneratedVideo struct {
	Name        string
	Width       int
	Height      int
	FPS         cliargs.FPS
	Frames      int
	Realtime    bool
	PixelFormat string
	Pattern     string
}

// Error describes one invalid generated source option while leaving callers
// free to wrap it in their own CLI or structured error shape.
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

// ParseGeneratedVideo maps the canonical testsrc video fields into a generated
// source. The only accepted step shape is:
//
//	testsrc video width=<px> height=<px> fps=<n[/d]|decimal> frames=<n>|duration=<d>
func ParseGeneratedVideo(sourceName string, args []Arg) (GeneratedVideo, error) {
	sourceName = strings.ToLower(strings.TrimSpace(sourceName))
	if sourceName != "testsrc" {
		switch sourceName {
		case "videosrc", "testvideo":
			return GeneratedVideo{}, optionError("source", sourceName, sourceName+" duplicates testsrc", []string{"use testsrc video"})
		default:
			return GeneratedVideo{}, optionError("source", sourceName, "unsupported source "+sourceName, []string{"use testsrc video"})
		}
	}

	source := GeneratedVideo{
		Name:        "testsrc",
		Width:       1280,
		Height:      720,
		FPS:         cliargs.FPS{Num: 30, Den: 1},
		Frames:      90,
		Realtime:    true,
		PixelFormat: av.PixelFormatI420,
		Pattern:     "gradient",
	}
	seen := make(map[string]string, 8)
	mediaKind := ""
	var duration time.Duration
	var framesSet bool
	var durationSet bool

	for _, arg := range args {
		key := strings.ToLower(strings.TrimSpace(arg.Key))
		value := strings.TrimSpace(arg.Value)
		switch key {
		case "":
			if value == "" {
				continue
			}
			if !strings.EqualFold(value, "video") {
				return GeneratedVideo{}, optionError("testsrc", value, "testsrc source kind must be video", []string{"use testsrc video"})
			}
			if mediaKind != "" {
				return GeneratedVideo{}, optionError("testsrc", value, "testsrc source kind was provided more than once", []string{"keep only one video argument"})
			}
			mediaKind = "video"
		case "name":
			if err := markSeen(seen, "name", value); err != nil {
				return GeneratedVideo{}, err
			}
			if value == "" {
				return GeneratedVideo{}, optionError("name", value, "source name cannot be empty", []string{"use name=fixture"})
			}
			source.Name = value
		case "width":
			if err := markSeen(seen, "width", value); err != nil {
				return GeneratedVideo{}, err
			}
			parsed, err := parsePositiveInt("width", value)
			if err != nil {
				return GeneratedVideo{}, optionError("width", value, "width must be a positive integer", []string{"use width=1280"})
			}
			source.Width = parsed
		case "height":
			if err := markSeen(seen, "height", value); err != nil {
				return GeneratedVideo{}, err
			}
			parsed, err := parsePositiveInt("height", value)
			if err != nil {
				return GeneratedVideo{}, optionError("height", value, "height must be a positive integer", []string{"use height=720"})
			}
			source.Height = parsed
		case "fps":
			if err := markSeen(seen, "fps", value); err != nil {
				return GeneratedVideo{}, err
			}
			fps, err := cliargs.ParseFPS(value)
			if err != nil {
				return GeneratedVideo{}, optionError("fps", value, "fps must be a positive integer, decimal, or fraction", []string{"use fps=30", "use fps=30000/1001"})
			}
			source.FPS = fps
		case "frames":
			if err := markSeen(seen, "frames", value); err != nil {
				return GeneratedVideo{}, err
			}
			parsed, err := parsePositiveInt("frames", value)
			if err != nil {
				return GeneratedVideo{}, optionError("frames", value, "frames must be a positive integer", []string{"use frames=90"})
			}
			source.Frames = parsed
			framesSet = true
		case "duration":
			if err := markSeen(seen, "duration", value); err != nil {
				return GeneratedVideo{}, err
			}
			parsed, err := time.ParseDuration(value)
			if err != nil || parsed <= 0 {
				return GeneratedVideo{}, optionError("duration", value, "duration must be a positive Go duration", []string{"use duration=3s"})
			}
			duration = parsed
			durationSet = true
		case "realtime":
			if err := markSeen(seen, "realtime", value); err != nil {
				return GeneratedVideo{}, err
			}
			realtime, err := parseBool(value)
			if err != nil {
				return GeneratedVideo{}, optionError("realtime", value, "realtime must be true or false", []string{"use realtime=true", "use realtime=false"})
			}
			source.Realtime = realtime
		case "format":
			if err := markSeen(seen, "format", value); err != nil {
				return GeneratedVideo{}, err
			}
			format, err := parsePixelFormat(value)
			if err != nil {
				return GeneratedVideo{}, err
			}
			source.PixelFormat = format
		case "pattern":
			if err := markSeen(seen, "pattern", value); err != nil {
				return GeneratedVideo{}, err
			}
			pattern, err := parsePattern(value)
			if err != nil {
				return GeneratedVideo{}, err
			}
			source.Pattern = pattern
		case "w":
			return GeneratedVideo{}, optionError("w", value, "w duplicates width", []string{"use width=<px>"})
		case "h":
			return GeneratedVideo{}, optionError("h", value, "h duplicates height", []string{"use height=<px>"})
		case "size":
			return GeneratedVideo{}, optionError("size", value, "size duplicates width and height", []string{"use width=<px> height=<px>"})
		case "framerate":
			return GeneratedVideo{}, optionError("framerate", value, "framerate duplicates fps", []string{"use fps=<n|n/d>"})
		case "live":
			return GeneratedVideo{}, optionError("live", value, "live duplicates realtime", []string{"use realtime=<bool>"})
		case "pixel_format", "pix_fmt":
			return GeneratedVideo{}, optionError(key, value, key+" duplicates format", []string{"use format=i420"})
		default:
			return GeneratedVideo{}, optionError(key, value, "unknown testsrc option "+key, []string{"use width=<px>", "use height=<px>", "use fps=<n|n/d>"})
		}
	}

	if mediaKind == "" {
		return GeneratedVideo{}, optionError("testsrc", "", "testsrc needs source kind video", []string{"use testsrc video"})
	}
	if framesSet && durationSet {
		return GeneratedVideo{}, optionError("duration", "", "frames and duration both bound the generated source", []string{"use frames=<n>", "use duration=<d>"})
	}
	if durationSet {
		source.Frames = framesForDuration(duration, source.FPS)
	}
	return source, nil
}

func markSeen(seen map[string]string, key string, value string) error {
	if previous, ok := seen[key]; ok {
		return optionError(key, value, key+" was provided more than once", []string{"keep only one " + key + "=... value", "previous=" + previous})
	}
	seen[key] = value
	return nil
}

func parsePositiveInt(name string, value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return parsed, nil
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("boolean value must be true or false")
	}
}

func parsePixelFormat(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case av.PixelFormatI420, av.PixelFormatYUV420P:
		return av.PixelFormatI420, nil
	default:
		return "", optionError("format", value, "testsrc currently generates i420/yuv420p", []string{"use format=i420"})
	}
}

func parsePattern(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "gradient":
		return "gradient", nil
	case "bars", "solid":
		return strings.ToLower(strings.TrimSpace(value)), nil
	default:
		return "", optionError("pattern", value, "pattern must be gradient, bars, or solid", []string{"use pattern=bars"})
	}
}

func framesForDuration(duration time.Duration, fps cliargs.FPS) int {
	if duration <= 0 {
		return 0
	}
	frames := math.Ceil(duration.Seconds() * float64(fps.Num) / float64(fps.Den))
	return max(int(frames), 1)
}

func optionError(field string, value string, message string, suggestions []string) error {
	return &Error{
		Field:       field,
		Value:       value,
		Message:     message,
		Suggestions: append([]string(nil), suggestions...),
	}
}
