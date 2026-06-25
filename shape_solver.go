package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// The shape solver is the upgrade of the validate-only shape walk: it
// propagates the media shape across a chain's canonical operation list and,
// where an operation pins format facts the current media does not satisfy,
// consults the chain's .Auto(...) policy. An allowed conversion is planned as a
// REAL plan.OpTransform operation — selected from the runtime's filter registry by
// capability delta — inserted into the operation list, and recorded as a plan
// diagnostic. Anything not allowed fails before any resource opens, with the
// exact policy (or missing registration) to add.

// shapeConversionPlan is one planned automatic conversion: the synthesized
// transform operation, the registry adapter that performs it, the conversion
// classes it needed, and the human delta for errors and diagnostics.
type shapeConversionPlan struct {
	operation operationSpec
	factory   string
	needed    shape.Policy
	detail    string
}

// solveOperationSpecShapes walks one stream chain's operations with the shape
// solver. It returns the solved operation list (nil when nothing was inserted)
// and one diagnostic per insertion.
func solveOperationSpecShapes(operation string, rt *runtime, stream streamIntent, initial shape.Spec) ([]operationSpec, []plan.Diagnostic, error) {
	if rt == nil {
		// No standard runtime (expert or test states): keep the validate-only walk.
		return nil, nil, validateOperationSpecShapes(operation, stream, initial)
	}
	current := normalizeTapShape(initial)
	if current.MediaKind == "" {
		current.MediaKind = stream.Select.Type
	}
	if current.Codec == "" {
		current.Codec = stream.Select.Codec
	}
	node := firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
	policy, policyActive := chainAutoPolicy(stream.Operations)
	preference, preferenceActive := chainPreference(stream.Operations)
	solved := make([]operationSpec, 0, len(stream.Operations)+1)
	var diagnostics []plan.Diagnostic
	inserted := false
	for i := range stream.Operations {
		next := stream.Operations[i]
		// Taps and shape annotations advance the lineage unchecked; a
		// .Require(...) assertion falls through to the contract check below.
		if next.Kind == plan.OpTap || (next.Kind == plan.OpShape && next.Require == nil) {
			solved = append(solved, next)
			current = operationSpecOutputShape(current, next)
			continue
		}
		expected := next.InputShapes()
		if len(expected) != 0 && !expected.Accepts(current) {
			target, fixable := shapeConversionTargetFromSet(current, expected)
			if !fixable {
				return nil, nil, operationShapeFailureError(operation, node, i, next, expected, current)
			}
			if !policyActive {
				// A conversion would fix the chain, but solving is off: fail with
				// the actual/expected shapes and the exact .Auto(...) to add.
				mismatch := operationShapeFailureError(operation, node, i, next, expected, current)
				return nil, nil, appendAutoFixSuggestions(mismatch, current, target, next)
			}
			conversion, planned, prefDiags, err := planShapeConversionPreferred(rt, current, target, rt.realtime, policy, preference, preferenceActive, node)
			if err != nil {
				return nil, nil, shapeSolverAdapterError(operation, node, i, next, current, target, err)
			}
			if !planned {
				return nil, nil, operationShapeFailureError(operation, node, i, next, expected, current)
			}
			if !policy.Covers(conversion.needed) {
				return nil, nil, shapeConversionRefusedError(operation, node, i, next, policy, conversion, current, target)
			}
			after := operationSpecOutputShape(current, conversion.operation)
			if !expected.Accepts(after) {
				return nil, nil, operationShapeFailureError(operation, node, i, next, expected, current)
			}
			conversion.operation.Shared = next.Shared
			solved = append(solved, conversion.operation)
			diagnostics = append(diagnostics, shapeConversionDiagnostic(node, conversion, next, current, target))
			diagnostics = append(diagnostics, prefDiags...)
			inserted = true
			current = after
		}
		if policyActive {
			desired := operationSoftInputShape(next)
			if !mediaShapeEmpty(desired) && !shape.Conversions(current, desired).Empty() {
				conversion, planned, prefDiags, err := planShapeConversionPreferred(rt, current, desired, rt.realtime, policy, preference, preferenceActive, node)
				if err != nil {
					return nil, nil, shapeSolverAdapterError(operation, node, i, next, current, desired, err)
				}
				if planned {
					if !policy.Covers(conversion.needed) {
						return nil, nil, shapeConversionRefusedError(operation, node, i, next, policy, conversion, current, desired)
					}
					conversion.operation.Shared = next.Shared
					solved = append(solved, conversion.operation)
					diagnostics = append(diagnostics, shapeConversionDiagnostic(node, conversion, next, current, desired))
					diagnostics = append(diagnostics, prefDiags...)
					inserted = true
					current = operationSpecOutputShape(current, conversion.operation)
				}
			}
		}
		solved = append(solved, next)
		current = operationSpecOutputShape(current, next)
	}
	if !inserted {
		return nil, nil, nil
	}
	return solved, diagnostics, nil
}

// chainPreference merges the chain's .Prefer(...) specs (later facts win) and
// reports whether any preference operation is present.
func chainPreference(operations []operationSpec) (shape.Spec, bool) {
	var preference shape.Spec
	active := false
	for i := range operations {
		if operations[i].Prefer == nil {
			continue
		}
		active = true
		preference = shape.Merge(preference, *operations[i].Prefer)
	}
	return preference, active
}

// operationSoftInputShape derives the format facts an operation pins for its
// input beyond its hard domain/media contract: an encoder's configured output
// caps are the shape its input frames must already have. Operations without
// pinned facts return the zero Spec.
func operationSoftInputShape(operation operationSpec) shape.Spec {
	if operation.Kind != plan.OpEncode || operation.Encode.Copy || operation.Encode.ID == "" {
		return shape.Spec{}
	}
	parameters := operation.Encode.Parameters
	media := firstNonEmptyMedia(operation.Encode.Type, parameters.Type, codecMedia(operation.Encode.ID))
	switch media {
	case av.MediaAudio:
		return shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			SampleRate:   parameters.SampleRate,
			Channels:     parameters.Channels,
			SampleFormat: parameters.SampleFormat,
		}
	case av.MediaVideo:
		return shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			Width:       parameters.Width,
			Height:      parameters.Height,
			PixelFormat: parameters.PixelFormat,
		}
	default:
		return shape.Spec{}
	}
}

// shapeConversionTargetFromSet picks the member of an expected Set that the
// current shape could satisfy after a format conversion: same domain, media,
// codec, and stream identity — only sample-rate/channel, width/height, or
// pixel/sample format facts differ, and the conversion target must be fully
// resolvable into explicit transform params.
func shapeConversionTargetFromSet(current shape.Spec, expected shape.Set) (shape.Spec, bool) {
	for i := range expected {
		candidate := expected[i]
		if candidate.Domain != "" && candidate.Domain != current.Domain {
			continue
		}
		if candidate.MediaKind != "" && candidate.MediaKind != current.MediaKind {
			continue
		}
		if candidate.Codec != "" && candidate.Codec != current.Codec {
			continue
		}
		if candidate.StreamID != "" && candidate.StreamID != current.StreamID {
			continue
		}
		if candidate.Format != "" && candidate.Format != current.Format {
			continue
		}
		if candidate.Realtime && !current.Realtime {
			continue
		}
		if shape.Conversions(current, candidate).Empty() {
			continue
		}
		media := firstNonEmptyMedia(candidate.MediaKind, current.MediaKind)
		if _, ok := synthesizeConversionTransform(media, current, candidate); !ok {
			continue
		}
		return candidate, true
	}
	return shape.Spec{}, false
}

// planShapeConversion synthesizes the conversion that turns actual into a
// shape satisfying expected's pinned format facts and selects the registry
// adapter that performs it. It returns planned=false when the delta is not a
// completable format conversion (the caller falls back to the plain shape
// mismatch error); adapter selection failures (none or several candidates)
// return an error.
func planShapeConversion(rt *runtime, actual shape.Spec, expected shape.Spec, realtime bool) (shapeConversionPlan, bool, error) {
	conversion, planned, _, err := planShapeConversionFor(rt, actual, expected, realtime, shape.Spec{})
	return conversion, planned, err
}

// planShapeConversionFor is planShapeConversion with an adapter preference:
// the pref spec tie-breaks otherwise-ambiguous adapter selection. resolved
// reports whether the preference decided the choice. A zero pref is exactly
// the plain plan.
func planShapeConversionFor(rt *runtime, actual shape.Spec, expected shape.Spec, realtime bool, pref shape.Spec) (shapeConversionPlan, bool, bool, error) {
	needed := shape.Conversions(actual, expected)
	if needed.Empty() {
		return shapeConversionPlan{}, false, false, nil
	}
	media := firstNonEmptyMedia(expected.MediaKind, actual.MediaKind)
	transform, ok := synthesizeConversionTransform(media, actual, expected)
	if !ok {
		return shapeConversionPlan{}, false, false, nil
	}
	descriptor, candidates, resolved, err := selectShapeConversionAdapter(rt, media, transform, realtime, pref)
	if err != nil {
		return shapeConversionPlan{}, false, false, &shapeAdapterSelectionError{media: media, needed: needed, candidates: candidates, cause: err}
	}
	operation := operationSpecForTransform(transform)
	operation.Component = descriptor.Name
	return shapeConversionPlan{
		operation: operation,
		factory:   descriptor.Name,
		needed:    needed,
		detail:    shapeConversionDetailText(descriptor.Name, actual, transform),
	}, true, resolved, nil
}

// planShapeConversionPreferred plans the conversion under the chain's
// .Prefer(...). Soft by definition: the preference fills the conversion-target
// facts the expected shape leaves open and tie-breaks otherwise-ambiguous
// adapter selection — and a preference the active policy cannot cover, no
// adapter can perform, or that has no open choice to influence is dropped (a
// diagnostic records the drop when the plain plan proceeds). Without an
// active preference the call is exactly planShapeConversion.
func planShapeConversionPreferred(rt *runtime, actual shape.Spec, expected shape.Spec, realtime bool, policy shape.Policy, pref shape.Spec, prefActive bool, node string) (shapeConversionPlan, bool, []plan.Diagnostic, error) {
	media := firstNonEmptyMedia(expected.MediaKind, actual.MediaKind)
	if !prefActive || mediaShapeEmpty(pref) || (pref.MediaKind != "" && pref.MediaKind != media) {
		conversion, planned, err := planShapeConversion(rt, actual, expected, realtime)
		return conversion, planned, nil, err
	}
	basePlan, basePlanned, baseErr := planShapeConversion(rt, actual, expected, realtime)
	effective := preferredConversionTarget(media, expected, pref)
	targetBias := effective != expected
	conversion, planned, resolved, err := planShapeConversionFor(rt, actual, effective, realtime, pref)
	reason := ""
	switch {
	case err != nil:
		reason = preferenceDropReason(err)
	case !planned:
		reason = "the preferred conversion cannot be planned"
	case !policy.Covers(conversion.needed):
		reason = fmt.Sprintf("the chain policy (%s) does not allow the preferred conversion (%s)", policy.String(), conversion.needed.String())
	case !targetBias && !resolved:
		// The preference had no open choice to influence: the plain plan is
		// already exactly this plan.
		return basePlan, basePlanned, nil, baseErr
	default:
		return conversion, true, []plan.Diagnostic{shapePreferenceAppliedDiagnostic(node, pref, conversion, targetBias, resolved)}, nil
	}
	// Drop the preference: the plain plan (or its plain failure) stands, so a
	// .Prefer(...) never fails a build that would succeed without it.
	if baseErr != nil || !basePlanned {
		return basePlan, basePlanned, nil, baseErr
	}
	return basePlan, basePlanned, []plan.Diagnostic{shapePreferenceIgnoredDiagnostic(node, pref, reason)}, nil
}

// preferredConversionTarget overlays the preference onto the conversion target
// for exactly the format facts the expected shape leaves open, scoped to the
// conversion's media. Pinned expected facts always win, and identity facts
// (domain, media, codec, stream, container, realtime) are never taken from a
// preference.
func preferredConversionTarget(media av.MediaType, expected shape.Spec, pref shape.Spec) shape.Spec {
	out := expected
	switch media {
	case av.MediaAudio:
		if out.SampleRate == 0 {
			out.SampleRate = pref.SampleRate
		}
		if out.Channels == 0 {
			out.Channels = pref.Channels
		}
		if out.SampleFormat == "" {
			out.SampleFormat = pref.SampleFormat
		}
	case av.MediaVideo:
		if out.Width == 0 {
			out.Width = pref.Width
		}
		if out.Height == 0 {
			out.Height = pref.Height
		}
		if out.PixelFormat == "" {
			out.PixelFormat = pref.PixelFormat
		}
	}
	return out
}

// preferenceDropReason renders why a preferred plan could not be honored.
func preferenceDropReason(err error) string {
	selection, ok := err.(*shapeAdapterSelectionError)
	if !ok {
		return "the preferred conversion cannot be planned"
	}
	switch selection.cause {
	case errShapeAdapterAmbiguous:
		return "several registered adapters still match under the preference: " + strings.Join(selection.candidates, ", ")
	case errShapeAdapterMissing:
		return "no registered adapter can perform the preferred conversion"
	default:
		return selection.Error()
	}
}

// synthesizeConversionTransform builds the explicit transform config from the
// expected shape, falling back to the actual shape for facts the expectation
// leaves open. Targets that cannot be fully resolved are not completable.
func synthesizeConversionTransform(media av.MediaType, actual shape.Spec, expected shape.Spec) (TransformSpec, bool) {
	switch media {
	case av.MediaAudio:
		config := filter.ResampleConfig{
			SampleRate:   firstPositiveInt(expected.SampleRate, actual.SampleRate),
			Channels:     firstPositiveInt(expected.Channels, actual.Channels),
			SampleFormat: firstNonEmpty(expected.SampleFormat, actual.SampleFormat),
		}
		if config.SampleRate <= 0 || config.Channels <= 0 {
			return TransformSpec{}, false
		}
		return TransformSpec{Resample: &config}, true
	case av.MediaVideo:
		config := filter.ResizeConfig{
			Width:       firstPositiveInt(expected.Width, actual.Width),
			Height:      firstPositiveInt(expected.Height, actual.Height),
			PixelFormat: firstNonEmpty(expected.PixelFormat, actual.PixelFormat),
			Mode:        filter.ResizeExact,
		}
		if config.Width <= 0 || config.Height <= 0 {
			return TransformSpec{}, false
		}
		return TransformSpec{Resize: &config}, true
	default:
		return TransformSpec{}, false
	}
}

var errShapeAdapterMissing = fmt.Errorf("goav: no conversion adapter")
var errShapeAdapterAmbiguous = fmt.Errorf("goav: ambiguous conversion adapters")

// shapeAdapterSelectionError carries adapter-selection context up to the walk,
// which renders the chain-aware BuildError.
type shapeAdapterSelectionError struct {
	media      av.MediaType
	needed     shape.Policy
	candidates []string
	cause      error
}

func (e *shapeAdapterSelectionError) Error() string { return e.cause.Error() }
func (e *shapeAdapterSelectionError) Unwrap() error { return e.cause }

// selectShapeConversionAdapter picks the registered filter whose declared
// capabilities cover the synthesized conversion: matching media in and out,
// the target pixel/sample format when the descriptor constrains formats, exact
// resize support, and realtime capability on realtime runtimes. When several
// candidates remain, a non-zero pref narrows them to the adapters whose
// declared capabilities cover the preference; resolved reports whether that
// tie-break decided the choice. Exactly one candidate must remain.
func selectShapeConversionAdapter(rt *runtime, media av.MediaType, transform TransformSpec, realtime bool, pref shape.Spec) (filter.Descriptor, []string, bool, error) {
	descriptors := rt.filters.Descriptors()
	matched := make([]filter.Descriptor, 0, len(descriptors))
	for i := range descriptors {
		descriptor := descriptors[i]
		if descriptor.Input != media || descriptor.Output != media {
			continue
		}
		if realtime && !descriptor.Realtime {
			continue
		}
		if transform.Resample != nil && transform.Resample.SampleFormat != "" &&
			len(descriptor.SampleFormats) != 0 && !stringAllowed(descriptor.SampleFormats, transform.Resample.SampleFormat) {
			continue
		}
		if transform.Resize != nil {
			if len(descriptor.ResizeModes) != 0 && !resizeModeAllowed(descriptor.ResizeModes, filter.ResizeExact) {
				continue
			}
			if transform.Resize.PixelFormat != "" && len(descriptor.PixelFormats) != 0 &&
				!stringAllowed(descriptor.PixelFormats, transform.Resize.PixelFormat) {
				continue
			}
		}
		matched = append(matched, descriptor)
	}
	resolved := false
	if len(matched) > 1 {
		if narrowed := narrowAdaptersByPreference(matched, media, pref); len(narrowed) == 1 {
			matched = narrowed
			resolved = true
		}
	}
	switch len(matched) {
	case 0:
		return filter.Descriptor{}, nil, false, errShapeAdapterMissing
	case 1:
		return matched[0], []string{matched[0].Name}, resolved, nil
	default:
		names := make([]string, 0, len(matched))
		for i := range matched {
			names = append(names, matched[i].Name)
		}
		return filter.Descriptor{}, names, false, errShapeAdapterAmbiguous
	}
}

// narrowAdaptersByPreference keeps the candidates whose declared capabilities
// cover the preference's adapter-relevant facts: realtime capability when the
// preference asks for it, and the preferred sample/pixel format when the
// descriptor constrains formats (an unconstrained descriptor covers every
// format, exactly like the base capability filter). A preference without such
// facts narrows nothing.
func narrowAdaptersByPreference(matched []filter.Descriptor, media av.MediaType, pref shape.Spec) []filter.Descriptor {
	if !pref.Realtime && pref.SampleFormat == "" && pref.PixelFormat == "" {
		return matched
	}
	narrowed := make([]filter.Descriptor, 0, len(matched))
	for i := range matched {
		descriptor := matched[i]
		if pref.Realtime && !descriptor.Realtime {
			continue
		}
		if media == av.MediaAudio && pref.SampleFormat != "" &&
			len(descriptor.SampleFormats) != 0 && !stringAllowed(descriptor.SampleFormats, pref.SampleFormat) {
			continue
		}
		if media == av.MediaVideo && pref.PixelFormat != "" &&
			len(descriptor.PixelFormats) != 0 && !stringAllowed(descriptor.PixelFormats, pref.PixelFormat) {
			continue
		}
		narrowed = append(narrowed, descriptor)
	}
	return narrowed
}

// --- errors and diagnostics ---

// shapeSolverAdapterError renders adapter-selection failures with the chain
// context: which operation needed the conversion, the source and expected
// shapes, and the registration (or disambiguation) that fixes it.
func shapeSolverAdapterError(operation string, node string, index int, step operationSpec, actual shape.Spec, expected shape.Spec, err error) error {
	selection, ok := err.(*shapeAdapterSelectionError)
	if !ok {
		return err
	}
	details := []string{
		fmt.Sprintf("operation_index=%d", index),
		"operation=" + string(step.Kind),
		"source=" + humanizeShape(actual),
		"actual_shape=" + actual.String(),
		"expected_shape=" + expected.String(),
		"conversion=" + selection.needed.String(),
		"media=" + string(selection.media),
	}
	if selection.cause == errShapeAdapterAmbiguous {
		return &BuildError{
			Code:      errcode.ShapeAdapterAmbiguous,
			Operation: operation,
			Node:      node,
			Reason: fmt.Sprintf("several registered filters can perform the %s conversion before %s: %s",
				selection.needed.String(), operationSpecLabel(step), strings.Join(selection.candidates, ", ")),
			Details: append(details, "candidates="+strings.Join(selection.candidates, ",")),
			Suggestions: []string{
				"keep one " + string(selection.media) + " conversion filter registered per runtime",
				"build the runtime with only the intended conversion filter via goav.MustNew(goav.WithFilter(...))",
				"bias the choice with .Prefer(shape.New(...)) toward a capability only one candidate declares",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return &BuildError{
		Code:      errcode.ShapeAdapterMissing,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf("no registered filter can perform the %s conversion before %s",
			selection.needed.String(), operationSpecLabel(step)),
		Details: details,
		Suggestions: []string{
			"register a " + string(selection.media) + " conversion filter with goav.WithFilter(filter.Descriptor{Input: ..., Output: ...}, factory)",
			"import github.com/thesyncim/goav/std and build with std.MustNewFilters(...) for the standard resample and resize adapters",
		},
		Cause: ErrUnsupportedBuild,
	}
}

// shapeConversionRefusedError is the solver's refusal: a conversion would fix
// the chain, but the active .Auto(...) policy does not allow it. The error
// carries the source shape, the expected shape, and the exact policy to add.
func shapeConversionRefusedError(operation string, node string, index int, step operationSpec, allowed shape.Policy, conversion shapeConversionPlan, actual shape.Spec, expected shape.Spec) error {
	missing := allowed.Missing(conversion.needed)
	return &BuildError{
		Code:      errcode.ShapeConversionRefused,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf("%s needs %s but the chain policy (%s) does not allow it",
			operationSpecLabel(step), conversion.detail, allowed.String()),
		Details: []string{
			fmt.Sprintf("operation_index=%d", index),
			"operation=" + string(step.Kind),
			"source=" + humanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + expected.String(),
			"needed=" + conversion.needed.String(),
			"allowed=" + allowed.String(),
		},
		Suggestions: append(
			[]string{fmt.Sprintf("add .Auto(%s) to the chain to let the planner insert the conversion", strings.Join(missing.Constructors(), ", "))},
			explicitConversionSuggestion(conversion.operation.Transform, step)...,
		),
		Cause: ErrUnsupportedBuild,
	}
}

// appendAutoFixSuggestions upgrades the plain shape mismatch error with the
// exact fix when a conversion could solve it: the .Auto(...) policy to add and
// the explicit operation to write instead.
func appendAutoFixSuggestions(err error, actual shape.Spec, expected shape.Spec, step operationSpec) error {
	buildErr, ok := err.(*BuildError)
	if !ok || buildErr == nil {
		return err
	}
	needed := shape.Conversions(actual, expected)
	buildErr.Suggestions = append(buildErr.Suggestions,
		fmt.Sprintf("add .Auto(%s) to the chain to let the planner insert the conversion", strings.Join(needed.Constructors(), ", ")))
	media := firstNonEmptyMedia(expected.MediaKind, actual.MediaKind)
	if transform, ok := synthesizeConversionTransform(media, actual, expected); ok {
		buildErr.Suggestions = append(buildErr.Suggestions, explicitConversionSuggestion(transform, step)...)
	}
	return buildErr
}

func explicitConversionSuggestion(transform TransformSpec, step operationSpec) []string {
	target := ""
	if step.Kind != "" {
		target = " before " + operationSpecLabel(step)
	}
	switch {
	case transform.Resample != nil:
		return []string{fmt.Sprintf("insert .Resample(%d, %d) explicitly%s", transform.Resample.SampleRate, transform.Resample.Channels, target)}
	case transform.Resize != nil:
		return []string{fmt.Sprintf("insert .Resize(%d, %d) explicitly%s", transform.Resize.Width, transform.Resize.Height, target)}
	default:
		return nil
	}
}

// shapePreferenceAppliedDiagnostic records a preference that influenced the
// planned conversion: it filled open target facts, resolved an otherwise
// ambiguous adapter choice, or both.
func shapePreferenceAppliedDiagnostic(node string, pref shape.Spec, conversion shapeConversionPlan, targetBias bool, resolved bool) plan.Diagnostic {
	effects := make([]string, 0, 2)
	if targetBias {
		effects = append(effects, "set the open conversion target facts")
	}
	if resolved {
		effects = append(effects, "resolved the adapter choice")
	}
	return plan.Diagnostic{
		Code:    string(errcode.ShapePreferenceApplied),
		Node:    node,
		Message: fmt.Sprintf("preference (%s) %s: %s", pref.String(), strings.Join(effects, " and "), conversion.detail),
		Details: []string{
			"preference=" + pref.String(),
			"adapter=" + conversion.factory,
		},
	}
}

// shapePreferenceIgnoredDiagnostic records a preference the solver could not
// honor — soft by definition, the plain plan proceeds untouched.
func shapePreferenceIgnoredDiagnostic(node string, pref shape.Spec, reason string) plan.Diagnostic {
	return plan.Diagnostic{
		Code:    string(errcode.ShapePreferenceIgnored),
		Node:    node,
		Message: fmt.Sprintf("preference (%s) ignored: %s", pref.String(), reason),
		Details: []string{"preference=" + pref.String()},
	}
}

// shapeConversionDiagnostic records one insertion on the plan, e.g.
// "inserted resample 44.1kHz→48kHz before encode-opus (AllowResample)".
func shapeConversionDiagnostic(node string, conversion shapeConversionPlan, step operationSpec, actual shape.Spec, expected shape.Spec) plan.Diagnostic {
	return plan.Diagnostic{
		Code:    string(errcode.ShapeConversionInserted),
		Node:    node,
		Message: fmt.Sprintf("inserted %s before %s (%s)", conversion.detail, operationSpecLabel(step), shapePolicyLabel(conversion.needed)),
		Details: []string{
			"adapter=" + conversion.factory,
			"source=" + humanizeShape(actual),
			"actual_shape=" + actual.String(),
			"expected_shape=" + expected.String(),
		},
	}
}

// shapePolicyLabel renders a policy's constructor names without the package
// qualifier: "AllowResample", "AllowResample+AllowConvert".
func shapePolicyLabel(policy shape.Policy) string {
	constructors := policy.Constructors()
	for i := range constructors {
		constructors[i] = strings.TrimSuffix(strings.TrimPrefix(constructors[i], "shape."), "()")
	}
	if len(constructors) == 0 {
		return "none"
	}
	return strings.Join(constructors, "+")
}

// operationSpecLabel names an operation for solver errors and diagnostics:
// "encode-opus", "decode-vp8", "stage-visualizer", "resample".
func operationSpecLabel(operation operationSpec) string {
	switch operation.Kind {
	case plan.OpEncode:
		return "encode-" + firstNonEmpty(string(operation.Encode.ID), operation.Component, "encoder")
	case plan.OpDecode:
		return "decode-" + firstNonEmpty(string(operation.Decode.ID), operation.Component, "decoder")
	case plan.OpStage:
		return "stage-" + firstNonEmpty(operation.Component, "custom")
	case plan.OpTransform:
		return firstNonEmpty(operationSpecComponent(operation), "transform")
	default:
		return firstNonEmpty(operation.Component, string(operation.Kind), "operation")
	}
}

// shapeConversionDetailText renders the conversion delta for humans:
// "resample 44.1kHz→48kHz", "resize 1280x720→640x360", "resample s16→f32".
func shapeConversionDetailText(factory string, actual shape.Spec, transform TransformSpec) string {
	parts := []string{factory}
	switch {
	case transform.Resample != nil:
		config := transform.Resample
		if config.SampleRate != 0 && config.SampleRate != actual.SampleRate {
			parts = append(parts, formatSampleRate(actual.SampleRate)+"→"+formatSampleRate(config.SampleRate))
		}
		if config.Channels != 0 && config.Channels != actual.Channels {
			parts = append(parts, formatChannels(actual.Channels)+"→"+formatChannels(config.Channels))
		}
		if config.SampleFormat != "" && config.SampleFormat != actual.SampleFormat {
			parts = append(parts, formatMediaFormat(actual.SampleFormat)+"→"+config.SampleFormat)
		}
	case transform.Resize != nil:
		config := transform.Resize
		if (config.Width != 0 && config.Width != actual.Width) || (config.Height != 0 && config.Height != actual.Height) {
			parts = append(parts, formatFrameSize(actual.Width, actual.Height)+"→"+formatFrameSize(config.Width, config.Height))
		}
		if config.PixelFormat != "" && config.PixelFormat != actual.PixelFormat {
			parts = append(parts, formatMediaFormat(actual.PixelFormat)+"→"+config.PixelFormat)
		}
	}
	return strings.Join(parts, " ")
}

// humanizeShape renders a Spec for error prose: "frame audio 44.1kHz 2ch s16".
func humanizeShape(spec shape.Spec) string {
	parts := make([]string, 0, 6)
	if spec.Domain != "" {
		parts = append(parts, string(spec.Domain))
	}
	if spec.MediaKind != "" {
		parts = append(parts, string(spec.MediaKind))
	}
	if spec.SampleRate != 0 {
		parts = append(parts, formatSampleRate(spec.SampleRate))
	}
	if spec.Channels != 0 {
		parts = append(parts, formatChannels(spec.Channels))
	}
	if spec.SampleFormat != "" {
		parts = append(parts, spec.SampleFormat)
	}
	if spec.Width != 0 || spec.Height != 0 {
		parts = append(parts, formatFrameSize(spec.Width, spec.Height))
	}
	if spec.PixelFormat != "" {
		parts = append(parts, spec.PixelFormat)
	}
	if len(parts) == 0 {
		return "any"
	}
	return strings.Join(parts, " ")
}

func formatSampleRate(rate int) string {
	if rate <= 0 {
		return "unknown rate"
	}
	if rate%1000 == 0 {
		return fmt.Sprintf("%dkHz", rate/1000)
	}
	text := strings.TrimRight(fmt.Sprintf("%.3f", float64(rate)/1000), "0")
	return strings.TrimRight(text, ".") + "kHz"
}

func formatChannels(channels int) string {
	if channels <= 0 {
		return "unknown channels"
	}
	return fmt.Sprintf("%dch", channels)
}

func formatFrameSize(width int, height int) string {
	if width <= 0 && height <= 0 {
		return "unknown size"
	}
	return fmt.Sprintf("%dx%d", width, height)
}

func formatMediaFormat(format string) string {
	if format == "" {
		return "unknown format"
	}
	return format
}

func firstPositiveInt(values ...int) int {
	for i := range values {
		if values[i] > 0 {
			return values[i]
		}
	}
	return 0
}
