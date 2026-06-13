// Package expect provides testing.TB-style assertions for goav pipeline tests.
//
// It is the assertion layer next to goavtest's neutral fixtures: goavtest
// sources, collectors, fake codecs, and fake containers return ordinary goav
// values; expect turns their common checks into concise test failures with
// structured diagnostics.
package expect

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
)

// TB is the small subset of testing.TB expect needs. It lets callers pass
// *testing.T, *testing.B, or a wrapper with the same failure behavior.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// NoError fails the test when err is non-nil.
func NoError(t TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error:\n%v", err)
	}
}

// Error fails the test when err is nil.
func Error(t TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error")
	}
}

// Equal fails when got != want.
func Equal[T comparable](t TB, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", label(name), got, want)
	}
}

// DeepEqual fails when got and want are not deeply equal.
func DeepEqual(t TB, name string, got, want any) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("%s mismatch (-want +got):\n%s", label(name), diff)
	}
}

// Len fails when got does not have length want. got may be a slice, array,
// map, string, or channel.
func Len(t TB, name string, got any, want int) {
	t.Helper()
	value := reflect.ValueOf(got)
	if !value.IsValid() {
		t.Fatalf("%s has no length; want %d", label(name), want)
	}
	switch value.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
	default:
		t.Fatalf("%s has type %T, which has no length; want %d", label(name), got, want)
	}
	if value.Len() != want {
		t.Fatalf("len(%s) = %d, want %d", label(name), value.Len(), want)
	}
}

// Contains fails when got does not contain fragment.
func Contains(t TB, name string, got string, fragment string) {
	t.Helper()
	if !strings.Contains(got, fragment) {
		t.Fatalf("%s = %q, want it to contain %q", label(name), got, fragment)
	}
}

// StringSliceContains fails when no element in values contains fragment.
func StringSliceContains(t TB, name string, values []string, fragment string) {
	t.Helper()
	for _, value := range values {
		if strings.Contains(value, fragment) {
			return
		}
	}
	t.Fatalf("%s = %#v, want one entry to contain %q", label(name), values, fragment)
}

// GoldenString compares got with the contents of path.
func GoldenString(t TB, path string, got string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	if string(body) != got {
		t.Fatalf("%s = %q, want %q", path, got, string(body))
	}
}

// S16 compares a collector's interleaved S16 audio frames with want.
func S16(t TB, collector *goavtest.Collector, want [][]int16) {
	t.Helper()
	if collector == nil {
		t.Fatalf("collector is nil")
	}
	DeepEqual(t, "collector S16", collector.S16(), want)
}

// BuildErrorCheck checks one property of a structured *goav.BuildError.
type BuildErrorCheck func(TB, error, *goav.BuildError)

// BuildError extracts a *goav.BuildError, checks its errcode.Code, then runs
// any extra property checks. It returns the extracted error for tests that
// need to inspect fields directly.
func BuildError(t TB, err error, code errcode.Code, checks ...BuildErrorCheck) *goav.BuildError {
	t.Helper()
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %v, want a *goav.BuildError with code %q", err, code)
	}
	if buildErr.Code != code {
		t.Fatalf("BuildError code = %q, want %q\n%s", buildErr.Code, code, formatBuildError(err, buildErr))
	}
	for _, check := range checks {
		if check != nil {
			check(t, err, buildErr)
		}
	}
	return buildErr
}

// Operation checks the BuildError operation field. Empty string is a valid
// expectation and is checked exactly.
func Operation(want string) BuildErrorCheck {
	return func(t TB, err error, buildErr *goav.BuildError) {
		t.Helper()
		if buildErr.Operation != want {
			t.Fatalf("BuildError operation = %q, want %q\n%s", buildErr.Operation, want, formatBuildError(err, buildErr))
		}
	}
}

// Node checks the BuildError node field. Empty string is a valid expectation
// and is checked exactly.
func Node(want string) BuildErrorCheck {
	return func(t TB, err error, buildErr *goav.BuildError) {
		t.Helper()
		if buildErr.Node != want {
			t.Fatalf("BuildError node = %q, want %q\n%s", buildErr.Node, want, formatBuildError(err, buildErr))
		}
	}
}

// Cause checks that err wraps want according to errors.Is.
func Cause(want error) BuildErrorCheck {
	return func(t TB, err error, buildErr *goav.BuildError) {
		t.Helper()
		if want == nil {
			return
		}
		if !errors.Is(err, want) {
			t.Fatalf("BuildError did not wrap %v\n%s", want, formatBuildError(err, buildErr))
		}
	}
}

// ReasonContains checks that BuildError.Reason contains fragment.
func ReasonContains(fragment string) BuildErrorCheck {
	return func(t TB, err error, buildErr *goav.BuildError) {
		t.Helper()
		if !strings.Contains(buildErr.Reason, fragment) {
			t.Fatalf("BuildError reason = %q, want it to contain %q\n%s", buildErr.Reason, fragment, formatBuildError(err, buildErr))
		}
	}
}

// DetailContains checks that one BuildError detail contains fragment.
func DetailContains(fragment string) BuildErrorCheck {
	return func(t TB, err error, buildErr *goav.BuildError) {
		t.Helper()
		for _, detail := range buildErr.Details {
			if strings.Contains(detail, fragment) {
				return
			}
		}
		t.Fatalf("BuildError details = %#v, want one entry to contain %q\n%s", buildErr.Details, fragment, formatBuildError(err, buildErr))
	}
}

// SuggestionContains checks that one BuildError suggestion contains fragment.
func SuggestionContains(fragment string) BuildErrorCheck {
	return func(t TB, err error, buildErr *goav.BuildError) {
		t.Helper()
		for _, suggestion := range buildErr.Suggestions {
			if strings.Contains(suggestion, fragment) {
				return
			}
		}
		t.Fatalf("BuildError suggestions = %#v, want one entry to contain %q\n%s", buildErr.Suggestions, fragment, formatBuildError(err, buildErr))
	}
}

func label(name string) string {
	if name == "" {
		return "value"
	}
	return name
}

func formatBuildError(err error, buildErr *goav.BuildError) string {
	if buildErr == nil {
		return fmt.Sprintf("err: %v", err)
	}
	return fmt.Sprintf("err: %v\nfields: code=%q operation=%q node=%q reason=%q details=%#v suggestions=%#v cause=%v",
		err,
		buildErr.Code,
		buildErr.Operation,
		buildErr.Node,
		buildErr.Reason,
		buildErr.Details,
		buildErr.Suggestions,
		buildErr.Cause,
	)
}
