// BuildError: the one structured refusal goav raises.

package goav

import (
	"encoding/json"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

// The refusal vocabulary is internal/builderr's, shared with the compiler;
// the root package spells it by these names.
type (
	buildErrorDetail = builderr.Detail
	buildErrorFix    = builderr.Fix
)

const (
	phaseBuild = builderr.PhaseBuild
	phaseOpen  = builderr.PhaseOpen
	phaseRun   = builderr.PhaseRun
	phaseClose = builderr.PhaseClose
)

var (
	errUnsupportedBuild = builderr.ErrUnsupportedBuild
	errDetail           = builderr.KV
	errNote             = builderr.Note
	errDetails          = builderr.Details
	errDetailLines      = builderr.DetailLines
	buildErrorFixes     = builderr.Fixes
	encodeHint          = builderr.EncodeHint
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
	return builderr.Render(e.Operation, e.Node, e.Reason, e.detailLines(), e.suggestionLines())
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
	return builderr.Lines(e.fields)
}

func (e *BuildError) suggestionLines() []string {
	if e == nil {
		return nil
	}
	return builderr.FixLines(e.fixes)
}

// publicError is where a compiler Refusal becomes the public BuildError:
// every exported entry that runs the compiler returns its error through it,
// so a caller's errors.As(err, *BuildError) always holds. A BuildError whose
// cause is a Refusal has the cause converted too.
func publicError(err error) error {
	out, _ := convertRefusal(err)
	return out
}

func convertRefusal(err error) (error, bool) {
	switch e := err.(type) {
	case *builderr.Refusal:
		if e == nil {
			return err, false
		}
		cause, _ := convertRefusal(e.Cause)
		return &BuildError{
			Phase:     e.Phase,
			Family:    e.Family,
			Code:      e.Code,
			Operation: e.Operation,
			Node:      e.Node,
			Reason:    e.Reason,
			fields:    e.Details,
			fixes:     e.Fixes,
			cause:     cause,
		}, true
	case *BuildError:
		if e == nil {
			return err, false
		}
		if cause, changed := convertRefusal(e.cause); changed {
			out := *e
			out.cause = cause
			return &out, true
		}
	}
	return err, false
}
