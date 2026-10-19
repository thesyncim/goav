// Package builderr is the refusal vocabulary goav's root package and its
// compiler share: typed details, fixes, phases, the unsupported-build cause,
// and Refusal, the compiler's refusal value. The root package converts every
// Refusal into the public goav.BuildError before a caller sees it.
package builderr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
)

// Detail is one typed machine-readable fact attached to a refusal. Callers
// read details through BuildError.Detail and BuildError.DetailLines.
type Detail struct {
	Key   string
	Value any
}

func (d Detail) String() string {
	if d.Key == "" {
		return fmt.Sprint(d.Value)
	}
	return d.Key + "=" + fmt.Sprint(d.Value)
}

// Fix is one concrete way to repair a refusal.
type Fix struct {
	Message string
}

func (f Fix) String() string {
	return f.Message
}

// The lifecycle phases a refusal names.
const (
	PhaseBuild = "build"
	PhaseOpen  = "open"
	PhaseRun   = "run"
	PhaseClose = "close"
)

// ErrUnsupportedBuild is the cause behind every build-shape refusal: the
// declared recipe or graph cannot be lowered as written.
var ErrUnsupportedBuild = errors.New("goav: unsupported builder graph")

// Refusal is a refusal the compiler raises. It carries exactly what a
// goav.BuildError carries; the root package converts it at its public
// boundary, so no caller ever sees a Refusal.
type Refusal struct {
	Phase     string
	Family    errcode.Family
	Code      errcode.Code
	Operation string
	Node      string
	Reason    string
	Details   []Detail
	Fixes     []Fix
	Cause     error
}

// Error renders the refusal as goav.BuildError does.
func (r *Refusal) Error() string {
	if r == nil {
		return ""
	}
	return Render(r.Operation, r.Node, r.Reason, Lines(r.Details), FixLines(r.Fixes))
}

// Unwrap exposes the cause to errors.Is.
func (r *Refusal) Unwrap() error {
	if r == nil {
		return nil
	}
	return r.Cause
}

// Render is the one refusal text shape: "goav: cannot <operation> for
// <node>: <reason>" followed by Details and Suggestions lines.
func Render(operation, node, reason string, details, suggestions []string) string {
	var out strings.Builder
	out.WriteString("goav")
	if operation != "" {
		out.WriteString(": cannot ")
		out.WriteString(operation)
	} else {
		out.WriteString(": build failed")
	}
	if node != "" {
		out.WriteString(" for ")
		out.WriteString(node)
	}
	if reason != "" {
		out.WriteString(": ")
		out.WriteString(reason)
	}
	if len(details) != 0 {
		out.WriteString("\nDetails:")
		for i := range details {
			out.WriteString("\n  - ")
			out.WriteString(details[i])
		}
	}
	if len(suggestions) != 0 {
		out.WriteString("\nSuggestions:")
		for i := range suggestions {
			out.WriteString("\n  - ")
			out.WriteString(suggestions[i])
		}
	}
	return out.String()
}

// KV builds one typed detail from an explicit key and value: the value keeps
// its real type and is read back through BuildError.Detail(key).
func KV(key string, value any) Detail {
	return Detail{Key: key, Value: value}
}

// Note builds one keyless detail: a machine-readable note with no key.
func Note(message string) Detail {
	return Detail{Value: message}
}

// Details collects typed details, dropping empties.
func Details(details ...Detail) []Detail {
	if len(details) == 0 {
		return nil
	}
	out := make([]Detail, 0, len(details))
	for _, d := range details {
		if d.Key == "" && (d.Value == nil || d.Value == "") {
			continue
		}
		out = append(out, d)
	}
	return out
}

// DetailLines converts dynamically built "key=value" (or plain) lines into
// typed details, splitting each at the first '='. Literal detail lists use
// KV and Note instead.
func DetailLines(lines []string) []Detail {
	if len(lines) == 0 {
		return nil
	}
	out := make([]Detail, 0, len(lines))
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		key, value, ok := strings.Cut(lines[i], "=")
		if !ok || key == "" {
			out = append(out, Detail{Value: lines[i]})
			continue
		}
		out = append(out, Detail{Key: key, Value: value})
	}
	return out
}

// Fixes builds the fixes of a refusal from their messages, dropping empties.
func Fixes(lines []string) []Fix {
	if len(lines) == 0 {
		return nil
	}
	out := make([]Fix, 0, len(lines))
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		out = append(out, Fix{Message: lines[i]})
	}
	return out
}

// Lines renders details as the lines Error prints.
func Lines(details []Detail) []string {
	if len(details) == 0 {
		return nil
	}
	out := make([]string, 0, len(details))
	for i := range details {
		if details[i].Key == "" && details[i].Value == nil {
			continue
		}
		out = append(out, details[i].String())
	}
	return out
}

// FixLines renders fixes as the suggestion lines Error prints.
func FixLines(fixes []Fix) []string {
	if len(fixes) == 0 {
		return nil
	}
	out := make([]string, 0, len(fixes))
	for i := range fixes {
		if fixes[i].Message == "" {
			continue
		}
		out = append(out, fixes[i].String())
	}
	return out
}

// EncodeHint is the encoder phrase refusals suggest: the bundled recipe
// encoders for the stream's media, so a video refusal never offers an audio
// codec, and every encoder when the media is unknown.
func EncodeHint(media av.MediaType) string {
	switch media {
	case av.MediaAudio:
		return ".Encode(codec.Opus(...))"
	case av.MediaVideo:
		return ".Encode(codec.VP8(...)), .Encode(codec.VP9(...)), or .Encode(codec.AV1(...))"
	default:
		return ".Encode(codec.Opus(...)) for audio or .Encode(codec.VP8(...)), .Encode(codec.VP9(...)), or .Encode(codec.AV1(...)) for video"
	}
}
