package transformargs

import (
	"strconv"
	"strings"
)

// Arg is one transform option key/value pair from a CLI-like grammar.
type Arg struct {
	Key   string
	Value string
}

// Resize is the canonical CLI representation of a resize transform.
type Resize struct {
	Width  int
	Height int
}

// Resample is the canonical CLI representation of an audio resample transform.
type Resample struct {
	SampleRate int
	Channels   int
}

// Error describes one invalid transform option while leaving callers free to
// wrap it in their own CLI or structured error shape.
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

// ParseResize maps canonical resize fields into a Resize.
func ParseResize(args []Arg) (Resize, error) {
	var resize Resize
	seen := make(map[string]string, 2)
	for _, arg := range args {
		key := strings.ToLower(strings.TrimSpace(arg.Key))
		value := strings.TrimSpace(arg.Value)
		switch key {
		case "":
			if value == "" {
				continue
			}
			return Resize{}, optionError("resize", value, "resize dimensions must be written as width=<px> height=<px>", []string{"use width=854 height=480"})
		case "width":
			if err := markSeen(seen, "width", value); err != nil {
				return Resize{}, err
			}
			parsed, err := parsePositiveInt("width", value)
			if err != nil {
				return Resize{}, optionError("width", value, "width must be a positive integer", []string{"use width=854"})
			}
			resize.Width = parsed
		case "height":
			if err := markSeen(seen, "height", value); err != nil {
				return Resize{}, err
			}
			parsed, err := parsePositiveInt("height", value)
			if err != nil {
				return Resize{}, optionError("height", value, "height must be a positive integer", []string{"use height=480"})
			}
			resize.Height = parsed
		case "w":
			return Resize{}, optionError("w", value, "w duplicates width", []string{"use width=<px>"})
		case "h":
			return Resize{}, optionError("h", value, "h duplicates height", []string{"use height=<px>"})
		case "size":
			return Resize{}, optionError("size", value, "size duplicates width and height", []string{"use width=<px> height=<px>"})
		default:
			return Resize{}, optionError(key, value, "unknown resize option "+key, []string{"use width=<px>", "use height=<px>"})
		}
	}
	if resize.Width <= 0 || resize.Height <= 0 {
		return Resize{}, optionError("resize", "", "resize needs width=<px> height=<px>", []string{"use width=854 height=480"})
	}
	return resize, nil
}

// ParseResample maps canonical resample fields into a Resample.
func ParseResample(args []Arg) (Resample, error) {
	var resample Resample
	seen := make(map[string]string, 2)
	for _, arg := range args {
		key := strings.ToLower(strings.TrimSpace(arg.Key))
		value := strings.TrimSpace(arg.Value)
		switch key {
		case "":
			if value == "" {
				continue
			}
			return Resample{}, optionError("resample", value, "resample settings must be written as sample_rate=<hz> channels=<n>", []string{"use sample_rate=48000 channels=2"})
		case "sample_rate":
			if err := markSeen(seen, "sample_rate", value); err != nil {
				return Resample{}, err
			}
			parsed, err := parsePositiveInt("sample_rate", value)
			if err != nil {
				return Resample{}, optionError("sample_rate", value, "sample_rate must be a positive integer", []string{"use sample_rate=48000"})
			}
			resample.SampleRate = parsed
		case "channels":
			if err := markSeen(seen, "channels", value); err != nil {
				return Resample{}, err
			}
			parsed, err := parsePositiveInt("channels", value)
			if err != nil {
				return Resample{}, optionError("channels", value, "channels must be a positive integer", []string{"use channels=1", "use channels=2"})
			}
			resample.Channels = parsed
		case "rate":
			return Resample{}, optionError("rate", value, "rate duplicates sample_rate", []string{"use sample_rate=<hz>"})
		case "ch":
			return Resample{}, optionError("ch", value, "ch duplicates channels", []string{"use channels=<n>"})
		default:
			return Resample{}, optionError(key, value, "unknown resample option "+key, []string{"use sample_rate=<hz>", "use channels=<n>"})
		}
	}
	if resample.SampleRate <= 0 || resample.Channels <= 0 {
		return Resample{}, optionError("resample", "", "resample needs sample_rate=<hz> channels=<n>", []string{"use sample_rate=48000 channels=2"})
	}
	return resample, nil
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
		return 0, optionError(name, value, name+" must be positive", nil)
	}
	return parsed, nil
}

func optionError(field string, value string, message string, suggestions []string) error {
	return &Error{
		Field:       field,
		Value:       value,
		Message:     message,
		Suggestions: append([]string(nil), suggestions...),
	}
}
