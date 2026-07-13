package shape

import (
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Policy is the shape solver's conversion vocabulary: which classes of
// automatic conversion a chain allows the planner to insert when an operation
// pins format facts the current media does not satisfy. The zero Policy allows
// nothing — every needed conversion is refused with the exact allowance to
// add. Build policies with AllowResample, AllowResize, and AllowConvert and
// combine them with Union (or by passing several to .Auto(...)).
type Policy struct {
	resample bool
	resize   bool
	convert  bool
	custom   []string
}

// AllowResample permits inserting audio sample-rate and channel conversions.
func AllowResample() Policy { return Policy{resample: true} }

// AllowResize permits inserting video width and height conversions.
func AllowResize() Policy { return Policy{resize: true} }

// AllowConvert permits inserting pixel-format and sample-format conversions.
func AllowConvert() Policy { return Policy{convert: true} }

// AllowCustom permits an application-registered solver delta by name. Custom
// names are matched against per-runtime delta contributors registered with
// runconfig; empty names allow nothing.
func AllowCustom(name string) Policy {
	name = strings.TrimSpace(name)
	if name == "" {
		return Policy{}
	}
	return Policy{custom: []string{name}}
}

// Union combines two policies; the result allows everything either allows.
func (p Policy) Union(other Policy) Policy {
	return Policy{
		resample: p.resample || other.resample,
		resize:   p.resize || other.resize,
		convert:  p.convert || other.convert,
		custom:   mergeCustomPolicyNames(p.custom, other.custom),
	}
}

// AllowsResample reports whether sample-rate/channel conversions are allowed.
func (p Policy) AllowsResample() bool { return p.resample }

// AllowsResize reports whether width/height conversions are allowed.
func (p Policy) AllowsResize() bool { return p.resize }

// AllowsConvert reports whether pixel/sample format conversions are allowed.
func (p Policy) AllowsConvert() bool { return p.convert }

// Empty reports whether the policy allows nothing.
func (p Policy) Empty() bool { return !p.resample && !p.resize && !p.convert && len(p.custom) == 0 }

// Covers reports whether the policy allows every conversion class in needed.
func (p Policy) Covers(needed Policy) bool {
	if needed.resample && !p.resample {
		return false
	}
	if needed.resize && !p.resize {
		return false
	}
	if needed.convert && !p.convert {
		return false
	}
	for _, name := range needed.custom {
		if !customPolicyContains(p.custom, name) {
			return false
		}
	}
	return true
}

// Missing returns the conversion classes in needed that the policy does not
// allow.
func (p Policy) Missing(needed Policy) Policy {
	return Policy{
		resample: needed.resample && !p.resample,
		resize:   needed.resize && !p.resize,
		convert:  needed.convert && !p.convert,
		custom:   missingCustomPolicyNames(p.custom, needed.custom),
	}
}

// Constructors lists the shape package constructor calls that build this
// policy — the exact fix a refusal error should suggest.
func (p Policy) Constructors() []string {
	out := make([]string, 0, 3)
	if p.resample {
		out = append(out, "shape.AllowResample()")
	}
	if p.resize {
		out = append(out, "shape.AllowResize()")
	}
	if p.convert {
		out = append(out, "shape.AllowConvert()")
	}
	for _, name := range normalizedCustomPolicyNames(p.custom) {
		out = append(out, "shape.AllowCustom("+strconv.Quote(name)+")")
	}
	return out
}

// String renders the allowed classes, or "none" for the zero policy.
func (p Policy) String() string {
	parts := make([]string, 0, 3)
	if p.resample {
		parts = append(parts, "resample")
	}
	if p.resize {
		parts = append(parts, "resize")
	}
	if p.convert {
		parts = append(parts, "convert")
	}
	parts = append(parts, normalizedCustomPolicyNames(p.custom)...)
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "+")
}

func mergeCustomPolicyNames(a []string, b []string) []string {
	if len(a) == 0 {
		return normalizedCustomPolicyNames(b)
	}
	if len(b) == 0 {
		return normalizedCustomPolicyNames(a)
	}
	merged := make([]string, 0, len(a)+len(b))
	merged = append(merged, a...)
	merged = append(merged, b...)
	return normalizedCustomPolicyNames(merged)
}

func normalizedCustomPolicyNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func missingCustomPolicyNames(allowed []string, needed []string) []string {
	needed = normalizedCustomPolicyNames(needed)
	if len(needed) == 0 {
		return nil
	}
	out := make([]string, 0, len(needed))
	for _, name := range needed {
		if !customPolicyContains(allowed, name) {
			out = append(out, name)
		}
	}
	return out
}

func customPolicyContains(names []string, name string) bool {
	return slices.Contains(names, name)
}

// Conversions reports the conversion classes required to make actual satisfy
// the populated format facts of expected: resample for sample-rate or channel
// deltas, resize for width or height deltas, convert for pixel-format or
// sample-format deltas. Facts the expected Spec leaves empty never require a
// conversion; facts it pins follow Accepts semantics (an unknown actual fact
// does not satisfy a pinned expectation).
func Conversions(actual Spec, expected Spec) Policy {
	var needed Policy
	if expected.SampleRate != 0 && actual.SampleRate != expected.SampleRate {
		needed.resample = true
	}
	if expected.Channels != 0 && actual.Channels != expected.Channels {
		needed.resample = true
	}
	if expected.SampleFormat != "" && actual.SampleFormat != expected.SampleFormat {
		needed.convert = true
	}
	if expected.Width != 0 && actual.Width != expected.Width {
		needed.resize = true
	}
	if expected.Height != 0 && actual.Height != expected.Height {
		needed.resize = true
	}
	if expected.PixelFormat != "" && actual.PixelFormat != expected.PixelFormat {
		needed.convert = true
	}
	return needed
}
