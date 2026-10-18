// BuildError: the one structured refusal goav raises.

package goav

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
)

// buildErrorDetail is one typed machine-readable fact attached to a
// BuildError. Callers read details through BuildError.Detail and
// BuildError.DetailLines so the root API does not expose implementation DTOs.
type buildErrorDetail struct {
	Key   string
	Value any
}

func (d buildErrorDetail) String() string {
	if d.Key == "" {
		return fmt.Sprint(d.Value)
	}
	return d.Key + "=" + fmt.Sprint(d.Value)
}

// buildErrorFix is one concrete way to repair a BuildError.
type buildErrorFix struct {
	Message string
}

func (f buildErrorFix) String() string {
	return f.Message
}

const (
	phaseBuild = "build"
	phaseOpen  = "open"
	phaseRun   = "run"
	phaseClose = "close"
)

// BuildError is the one structured refusal goav raises from build,
// validation, attach, and explain paths. Family identifies the stable
// application branch key, Code identifies the detailed diagnostic leaf (see the
// errcode package), Phase says when in the lifecycle it happened,
// Operation/Node say where, Reason says why, Detail exposes typed
// machine-readable facts, and DetailLines and FixLines expose rendered details
// and repair actions. Match build-shape refusals through Family and Code;
// Unwrap preserves low-level causes for errors.Is where a pipeline or runtime
// sentinel is still part of that lower-level contract.
type BuildError struct {
	Phase     string
	Family    errcode.Family
	Code      errcode.Code
	Operation string
	Node      string
	Reason    string
	fields    []buildErrorDetail
	fixes     []buildErrorFix
	cause     error
}

// Error renders the one goav error shape: "goav: cannot <operation> for
// <node>: <reason>" followed by Details and Suggestions lines.
func (e *BuildError) Error() string {
	if e == nil {
		return ""
	}
	var out strings.Builder
	out.WriteString("goav")
	if e.Operation != "" {
		out.WriteString(": cannot ")
		out.WriteString(e.Operation)
	} else {
		out.WriteString(": build failed")
	}
	if e.Node != "" {
		out.WriteString(" for ")
		out.WriteString(e.Node)
	}
	if e.Reason != "" {
		out.WriteString(": ")
		out.WriteString(e.Reason)
	}
	details := e.detailLines()
	if len(details) != 0 {
		out.WriteString("\nDetails:")
		for i := range details {
			out.WriteString("\n  - ")
			out.WriteString(details[i])
		}
	}
	suggestions := e.suggestionLines()
	if len(suggestions) != 0 {
		out.WriteString("\nSuggestions:")
		for i := range suggestions {
			out.WriteString("\n  - ")
			out.WriteString(suggestions[i])
		}
	}
	return out.String()
}

// Detail returns the detail named key as a T, reporting false when the error
// carries no such detail or it is not a T:
//
//	format, ok := buildErr.Detail[av.FormatID]("format")
func (e *BuildError) Detail[T any](key string) (T, bool) {
	var zero T
	if e == nil || key == "" {
		return zero, false
	}
	for i := range e.fields {
		if e.fields[i].Key == key {
			v, ok := e.fields[i].Value.(T)
			return v, ok
		}
	}
	return zero, false
}

// DetailLines returns the human-readable detail lines rendered in Error.
func (e *BuildError) DetailLines() []string {
	return append([]string(nil), e.detailLines()...)
}

// FixLines returns the human-readable fix lines rendered in Error.
func (e *BuildError) FixLines() []string {
	return append([]string(nil), e.suggestionLines()...)
}

// Unwrap exposes the underlying cause to errors.Is.
func (e *BuildError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// MarshalJSON renders the BuildError as a stable machine-readable document:
// phase, family, code, operation, node, reason, typed key/value details, and
// concrete fixes. It is the JSON contract consumed by tooling and pinned by the
// golden error tests; Detail values keep their real type.
func (e *BuildError) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	type detailJSON struct {
		Key   string `json:"key,omitempty"`
		Value any    `json:"value,omitempty"`
	}
	doc := struct {
		Phase     string         `json:"phase,omitempty"`
		Family    errcode.Family `json:"family,omitempty"`
		Code      errcode.Code   `json:"code,omitempty"`
		Operation string         `json:"operation,omitempty"`
		Node      string         `json:"node,omitempty"`
		Reason    string         `json:"reason,omitempty"`
		Details   []detailJSON   `json:"details,omitempty"`
		Fixes     []string       `json:"fixes,omitempty"`
	}{
		Phase:     e.Phase,
		Family:    e.Family,
		Code:      e.Code,
		Operation: e.Operation,
		Node:      e.Node,
		Reason:    e.Reason,
		Fixes:     e.suggestionLines(),
	}
	for i := range e.fields {
		doc.Details = append(doc.Details, detailJSON{Key: e.fields[i].Key, Value: e.fields[i].Value})
	}
	return json.Marshal(doc)
}

func (e *BuildError) detailLines() []string {
	if e == nil {
		return nil
	}
	return detailsToLines(e.fields)
}

func (e *BuildError) suggestionLines() []string {
	if e == nil {
		return nil
	}
	if len(e.fixes) == 0 {
		return nil
	}
	out := make([]string, 0, len(e.fixes))
	for i := range e.fixes {
		if e.fixes[i].Message == "" {
			continue
		}
		out = append(out, e.fixes[i].String())
	}
	return out
}

// errDetail builds one typed machine-readable BuildError detail from an explicit
// key and value. It replaces splitting a "key=value" string at construction:
// the value keeps its real type (int, string, ...) and is read back through
// BuildError.Detail(key).
func errDetail(key string, value any) buildErrorDetail {
	return buildErrorDetail{Key: key, Value: value}
}

// errNote builds one keyless detail: a machine-readable note with no key.
func errNote(message string) buildErrorDetail {
	return buildErrorDetail{Value: message}
}

// errDetails collects typed details, dropping empties. It is the typed builder
// that replaced buildErrorFields([]string{"key=value"}).
func errDetails(details ...buildErrorDetail) []buildErrorDetail {
	if len(details) == 0 {
		return nil
	}
	out := make([]buildErrorDetail, 0, len(details))
	for _, d := range details {
		if d.Key == "" && (d.Value == nil || d.Value == "") {
			continue
		}
		out = append(out, d)
	}
	return out
}

// errDetailLines converts a dynamically-built slice of "key=value" (or plain)
// lines into typed details, splitting each at the first '='. It is the runtime
// path for call sites whose detail keys are not known statically; literal detail
// lists use errDetail/errNote instead.
func errDetailLines(lines []string) []buildErrorDetail {
	if len(lines) == 0 {
		return nil
	}
	out := make([]buildErrorDetail, 0, len(lines))
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		key, value, ok := strings.Cut(lines[i], "=")
		if !ok || key == "" {
			out = append(out, buildErrorDetail{Value: lines[i]})
			continue
		}
		out = append(out, buildErrorDetail{Key: key, Value: value})
	}
	return out
}

// encodeHint is the encoder phrase refusals suggest: the bundled recipe
// encoders for the stream's media, so a video refusal never offers an audio
// codec, and every encoder when the media is unknown.
func encodeHint(media av.MediaType) string {
	switch media {
	case av.MediaAudio:
		return ".Encode(codec.Opus(...))"
	case av.MediaVideo:
		return ".Encode(codec.VP8(...)), .Encode(codec.VP9(...)), or .Encode(codec.AV1(...))"
	default:
		return ".Encode(codec.Opus(...)) for audio or .Encode(codec.VP8(...)), .Encode(codec.VP9(...)), or .Encode(codec.AV1(...)) for video"
	}
}

func buildErrorFixes(lines []string) []buildErrorFix {
	if len(lines) == 0 {
		return nil
	}
	out := make([]buildErrorFix, 0, len(lines))
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		out = append(out, buildErrorFix{Message: lines[i]})
	}
	return out
}

func detailsToLines(details []buildErrorDetail) []string {
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
