package fileargs

import (
	"sort"
	"strings"

	"github.com/thesyncim/goav/av"
)

// Arg is one file-sink option key/value pair from a CLI-like grammar.
type Arg struct {
	Key   string
	Value string
}

// FileSink is the canonical CLI representation of a writer-backed filesink.
type FileSink struct {
	Location string
	Format   av.FormatID
}

// Error describes one invalid file-sink option while leaving callers free to
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

// ParseFileSink maps canonical file-sink fields into a FileSink. It accepts
// only location and format so every CLI surface exposes one spelling.
func ParseFileSink(args []Arg) (FileSink, error) {
	var sink FileSink
	seen := make(map[string]string, 2)
	for _, arg := range args {
		key := strings.ToLower(strings.TrimSpace(arg.Key))
		value := strings.TrimSpace(arg.Value)
		switch key {
		case "":
			if value == "" {
				continue
			}
			return FileSink{}, optionError("location", value, "filesink location must be written as location=<path>", []string{"use location=" + value})
		case "location":
			if err := markSeen(seen, "location", value); err != nil {
				return FileSink{}, err
			}
			sink.Location = value
		case "format":
			if err := markSeen(seen, "format", value); err != nil {
				return FileSink{}, err
			}
			sink.Format = av.FormatID(strings.ToLower(value))
		case "path", "file":
			return FileSink{}, optionError(key, value, key+" duplicates location", []string{"use location=<path>"})
		case "container":
			return FileSink{}, optionError("container", value, "container duplicates format", []string{"use format=<id>"})
		default:
			return FileSink{}, optionError(key, value, "unknown filesink option "+key, []string{"use location=<path>", "use format=<id>"})
		}
	}
	if sink.Location == "" {
		return FileSink{}, optionError("location", "", "filesink needs location=<path>", []string{"use location=out.webm"})
	}
	return sink, nil
}

// ParseFileSinkMap parses a map of file-sink options in stable key order.
func ParseFileSinkMap(args map[string]string) (FileSink, error) {
	keys := make([]string, 0, len(args))
	for key := range args {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]Arg, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, Arg{Key: key, Value: args[key]})
	}
	return ParseFileSink(ordered)
}

func markSeen(seen map[string]string, key string, value string) error {
	if previous, ok := seen[key]; ok {
		return optionError(key, value, "filesink "+key+" was provided more than once", []string{"keep only one " + key + "=... value", "previous=" + previous})
	}
	seen[key] = value
	return nil
}

func optionError(field string, value string, message string, suggestions []string) error {
	return &Error{
		Field:       field,
		Value:       value,
		Message:     message,
		Suggestions: append([]string(nil), suggestions...),
	}
}
