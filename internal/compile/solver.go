package compile

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

// shapeConversionPlan is one planned automatic conversion: the synthesized
// transform operation, the registry adapter that performs it, the conversion
// classes it needed, and the human delta for errors and diagnostics.
type shapeConversionPlan struct {
	operation recipeir.Operation
	Transform recipeir.Transform
	Factory   string
	needed    shape.Policy
	detail    string
}

// solveRecipeIROperationShapes walks one stream chain's operations with the shape
// solver. It returns the solved operation list (nil when nothing was inserted)
// and one diagnostic per insertion.
func SolveRecipeIROperationShapes(operation string, env Env, stream recipeir.Stream, initial shape.Spec) ([]recipeir.Operation, []plan.Diagnostic, error) {
	if !env.Present {
		// No bundled runtime (expert or test states): keep the validate-only walk.
		return nil, nil, ValidateRecipeIROperationShapes(operation, stream, stream.Operations, initial)
	}
	current := NormalizeTapShape(initial)
	if current.MediaKind == "" {
		current.MediaKind = stream.Selector.Type
	}
	if current.Codec == "" {
		current.Codec = stream.Selector.Codec
	}
	node := cmp.Or(stream.Name, string(stream.Selector.ID), string(stream.Selector.Type), "stream")
	policy, policyActive := recipeIRChainAutoPolicy(stream.Operations)
	preference, preferenceActive := recipeIRChainPreference(stream.Operations)
	solved := make([]recipeir.Operation, 0, len(stream.Operations)+1)
	var diagnostics []plan.Diagnostic
	inserted := false
	for i := range stream.Operations {
		next := stream.Operations[i]
		// Taps and same-domain shape annotations advance the lineage; a
		// .Require(...) assertion falls through to the contract check below.
		if next.Kind == plan.OpTap {
			solved = append(solved, next)
			current = RecipeIROperationOutputShape(current, next)
			continue
		}
		if next.Kind == plan.OpShape && next.Require == nil {
			if err := ValidateShapeAnnotationDomain(operation, node, i, current, next.Shape); err != nil {
				return nil, nil, err
			}
			solved = append(solved, next)
			current = RecipeIROperationOutputShape(current, next)
			continue
		}
		expected := RecipeIROperationInputShapes(next)
		if len(expected) != 0 && !expected.Accepts(current) {
			target, fixable := ShapeConversionTargetFromSet(current, expected)
			if !fixable {
				return nil, nil, recipeIROperationShapeFailureError(operation, node, i, next, expected, current)
			}
			if !policyActive {
				// A conversion would fix the chain, but solving is off: fail with
				// the actual/expected shapes and the exact .Auto(...) to add.
				mismatch := recipeIROperationShapeFailureError(operation, node, i, next, expected, current)
				return nil, nil, appendRecipeIRAutoFixSuggestions(mismatch, current, target, next)
			}
			conversion, planned, prefDiags, err := planShapeConversionPreferred(env, current, target, env.Realtime, policy, preference, preferenceActive, node)
			if err != nil {
				return nil, nil, recipeIRShapeSolverAdapterError(operation, node, i, next, current, target, err)
			}
			if !planned {
				return nil, nil, recipeIROperationShapeFailureError(operation, node, i, next, expected, current)
			}
			if !policy.Covers(conversion.needed) {
				return nil, nil, recipeIRShapeConversionRefusedError(operation, node, i, next, policy, conversion, current, target)
			}
			after := RecipeIROperationOutputShape(current, conversion.operation)
			if !expected.Accepts(after) {
				return nil, nil, recipeIROperationShapeFailureError(operation, node, i, next, expected, current)
			}
			conversion.operation.Shared = next.Shared
			// An inserted conversion is no Stream's node: callers give it one.
			conversion.operation.Node = -1
			solved = append(solved, conversion.operation)
			diagnostics = append(diagnostics, shapeConversionDiagnostic(node, conversion, next, current, target))
			diagnostics = append(diagnostics, prefDiags...)
			inserted = true
			current = after
		}
		if policyActive {
			desired := recipeIROperationSoftInputShape(next)
			if !MediaShapeEmpty(desired) && !shape.Conversions(current, desired).Empty() {
				conversion, planned, prefDiags, err := planShapeConversionPreferred(env, current, desired, env.Realtime, policy, preference, preferenceActive, node)
				if err != nil {
					return nil, nil, recipeIRShapeSolverAdapterError(operation, node, i, next, current, desired, err)
				}
				if planned {
					if !policy.Covers(conversion.needed) {
						return nil, nil, recipeIRShapeConversionRefusedError(operation, node, i, next, policy, conversion, current, desired)
					}
					conversion.operation.Shared = next.Shared
					conversion.operation.Node = -1
					solved = append(solved, conversion.operation)
					diagnostics = append(diagnostics, shapeConversionDiagnostic(node, conversion, next, current, desired))
					diagnostics = append(diagnostics, prefDiags...)
					inserted = true
					current = RecipeIROperationOutputShape(current, conversion.operation)
				}
			}
		}
		solved = append(solved, next)
		current = RecipeIROperationOutputShape(current, next)
	}
	if !inserted {
		return nil, nil, nil
	}
	return solved, diagnostics, nil
}

func recipeIRChainAutoPolicy(operations []recipeir.Operation) (shape.Policy, bool) {
	var policy shape.Policy
	active := false
	for i := range operations {
		if operations[i].Auto == nil {
			continue
		}
		active = true
		policy = policy.Union(*operations[i].Auto)
	}
	return policy, active
}

// recipeIRChainPreference merges the chain's .Prefer(...) specs (later facts win) and
// reports whether any preference operation is present.
func recipeIRChainPreference(operations []recipeir.Operation) (shape.Spec, bool) {
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

// recipeIROperationSoftInputShape derives the format facts an operation pins for its
// input beyond its hard domain/media contract: an encoder's configured output
// caps are the shape its input frames must already have. Operations without
// pinned facts return the zero Spec.
func recipeIROperationSoftInputShape(operation recipeir.Operation) shape.Spec {
	if operation.Kind != plan.OpEncode || operation.Encode.Copy || operation.Encode.ID == "" {
		return shape.Spec{}
	}
	parameters := operation.Encode.Parameters
	media := FirstNonEmptyMedia(operation.Encode.Type, parameters.Type, CodecMedia(operation.Encode.ID))
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
func ShapeConversionTargetFromSet(current shape.Spec, expected shape.Set) (shape.Spec, bool) {
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
		media := FirstNonEmptyMedia(candidate.MediaKind, current.MediaKind)
		if _, ok := SynthesizeConversionTransform(media, current, candidate); !ok {
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
func PlanShapeConversion(env Env, actual shape.Spec, expected shape.Spec, realtime bool) (shapeConversionPlan, bool, error) {
	conversion, planned, _, err := planShapeConversionFor(env, actual, expected, realtime, shape.Spec{})
	return conversion, planned, err
}

// planShapeConversionFor is planShapeConversion with an adapter preference:
// the pref spec tie-breaks otherwise-ambiguous adapter selection. resolved
// reports whether the preference decided the choice. A zero pref is exactly
// the plain plan.
func planShapeConversionFor(env Env, actual shape.Spec, expected shape.Spec, realtime bool, pref shape.Spec) (shapeConversionPlan, bool, bool, error) {
	needed := shape.Conversions(actual, expected)
	if needed.Empty() {
		return shapeConversionPlan{}, false, false, nil
	}
	media := FirstNonEmptyMedia(expected.MediaKind, actual.MediaKind)
	transform, ok := SynthesizeConversionTransform(media, actual, expected)
	if !ok {
		return shapeConversionPlan{}, false, false, nil
	}
	descriptor, candidates, resolved, err := selectShapeConversionAdapter(env, media, transform, realtime, pref)
	if err != nil {
		return shapeConversionPlan{}, false, false, &ShapeAdapterSelectionError{Media: media, Needed: needed, Candidates: candidates, Cause: err}
	}
	operation := recipeir.Operation{
		Kind:      plan.OpTransform,
		Component: descriptor.Name,
		Transform: transform,
	}
	return shapeConversionPlan{
		operation: operation,
		Transform: transform,
		Factory:   descriptor.Name,
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
func planShapeConversionPreferred(env Env, actual shape.Spec, expected shape.Spec, realtime bool, policy shape.Policy, pref shape.Spec, prefActive bool, node string) (shapeConversionPlan, bool, []plan.Diagnostic, error) {
	media := FirstNonEmptyMedia(expected.MediaKind, actual.MediaKind)
	custom, customPlanned, err := planCustomShapeConversion(env, actual, expected, realtime, pref)
	if err != nil {
		return shapeConversionPlan{}, false, nil, err
	}
	baseNeeded := shape.Conversions(actual, expected)
	if customPlanned && (policy.Covers(custom.needed) || !policy.Covers(baseNeeded)) {
		return custom, true, nil, nil
	}
	if !prefActive || MediaShapeEmpty(pref) || (pref.MediaKind != "" && pref.MediaKind != media) {
		conversion, planned, err := PlanShapeConversion(env, actual, expected, realtime)
		return conversion, planned, nil, err
	}
	basePlan, basePlanned, baseErr := PlanShapeConversion(env, actual, expected, realtime)
	effective := PreferredConversionTarget(media, expected, pref)
	targetBias := effective != expected
	conversion, planned, resolved, err := planShapeConversionFor(env, actual, effective, realtime, pref)
	reason := ""
	switch {
	case err != nil:
		reason = PreferenceDropReason(err)
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

func planCustomShapeConversion(env Env, actual shape.Spec, expected shape.Spec, realtime bool, pref shape.Spec) (shapeConversionPlan, bool, error) {
	if !env.Present || len(env.ShapeDeltas) == 0 {
		return shapeConversionPlan{}, false, nil
	}
	request := runconfig.ShapeDeltaRequest{
		Actual:     actual,
		Expected:   expected,
		Preference: pref,
		Realtime:   realtime,
	}
	for i := range env.ShapeDeltas {
		planned, ok, err := env.ShapeDeltas[i].ShapeDelta(request)
		if err != nil {
			return shapeConversionPlan{}, false, fmt.Errorf("goav: shape delta contributor %d rejected conversion: %w", i, err)
		}
		if !ok {
			continue
		}
		if planned.Stage == nil {
			return shapeConversionPlan{}, false, fmt.Errorf("goav: shape delta contributor %d returned a nil stage", i)
		}
		if planned.Needed.Empty() {
			return shapeConversionPlan{}, false, fmt.Errorf("goav: shape delta contributor %d returned an empty policy", i)
		}
		component := cmp.Or(planned.Component, planned.Stage.Name())
		if component == "" {
			return shapeConversionPlan{}, false, fmt.Errorf("goav: shape delta contributor %d returned an unnamed stage", i)
		}
		detail := cmp.Or(planned.Detail, component)
		return shapeConversionPlan{
			operation: recipeir.Operation{
				Kind:      plan.OpStage,
				Component: component,
				Detail:    detail,
				Stage:     planned.Stage,
			},
			Factory: component,
			needed:  planned.Needed,
			detail:  detail,
		}, true, nil
	}
	return shapeConversionPlan{}, false, nil
}

// preferredConversionTarget overlays the preference onto the conversion target
// for exactly the format facts the expected shape leaves open, scoped to the
// conversion's media. Pinned expected facts always win, and identity facts
// (domain, media, codec, stream, container, realtime) are never taken from a
// preference.
func PreferredConversionTarget(media av.MediaType, expected shape.Spec, pref shape.Spec) shape.Spec {
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
func PreferenceDropReason(err error) string {
	selection, ok := err.(*ShapeAdapterSelectionError)
	if !ok {
		return "the preferred conversion cannot be planned"
	}
	switch selection.Cause {
	case ErrShapeAdapterAmbiguous:
		return "several registered adapters still match under the preference: " + strings.Join(selection.Candidates, ", ")
	case ErrShapeAdapterMissing:
		return "no registered adapter can perform the preferred conversion"
	default:
		return selection.Error()
	}
}

// synthesizeConversionTransform builds the explicit transform config from the
// expected shape, falling back to the actual shape for facts the expectation
// leaves open. Targets that cannot be fully resolved are not completable.
func SynthesizeConversionTransform(media av.MediaType, actual shape.Spec, expected shape.Spec) (recipeir.Transform, bool) {
	switch media {
	case av.MediaAudio:
		config := filter.ResampleConfig{
			SampleRate:   FirstPositiveInt(expected.SampleRate, actual.SampleRate),
			Channels:     FirstPositiveInt(expected.Channels, actual.Channels),
			SampleFormat: cmp.Or(expected.SampleFormat, actual.SampleFormat),
		}
		if config.SampleRate <= 0 || config.Channels <= 0 {
			return recipeir.Transform{}, false
		}
		return recipeir.Transform{Kind: recipeir.TransformResample, Resample: config}, true
	case av.MediaVideo:
		config := filter.ResizeConfig{
			Width:       FirstPositiveInt(expected.Width, actual.Width),
			Height:      FirstPositiveInt(expected.Height, actual.Height),
			PixelFormat: cmp.Or(expected.PixelFormat, actual.PixelFormat),
			Mode:        filter.ResizeExact,
		}
		if config.Width <= 0 || config.Height <= 0 {
			return recipeir.Transform{}, false
		}
		return recipeir.Transform{Kind: recipeir.TransformResize, Resize: config}, true
	default:
		return recipeir.Transform{}, false
	}
}

var ErrShapeAdapterMissing = fmt.Errorf("goav: no conversion adapter")

var ErrShapeAdapterAmbiguous = fmt.Errorf("goav: ambiguous conversion adapters")

// shapeAdapterSelectionError carries adapter-selection context up to the walk,
// which renders the chain-aware BuildError.
type ShapeAdapterSelectionError struct {
	Media      av.MediaType
	Needed     shape.Policy
	Candidates []string
	Cause      error
}

func (e *ShapeAdapterSelectionError) Error() string { return e.Cause.Error() }

func (e *ShapeAdapterSelectionError) Unwrap() error { return e.Cause }

// selectShapeConversionAdapter picks the registered filter whose declared
// capabilities cover the synthesized conversion: matching media in and out,
// the target pixel/sample format when the descriptor constrains formats, exact
// resize support, and realtime capability on realtime runtimes. When several
// candidates remain, a non-zero pref narrows them to the adapters whose
// declared capabilities cover the preference; resolved reports whether that
// tie-break decided the choice. Exactly one candidate must remain.
func selectShapeConversionAdapter(env Env, media av.MediaType, transform recipeir.Transform, realtime bool, pref shape.Spec) (filter.Descriptor, []string, bool, error) {
	descriptors := env.Filters.Descriptors()
	matched := make([]filter.Descriptor, 0, len(descriptors))
	for i := range descriptors {
		descriptor := descriptors[i]
		if descriptor.Input != media || descriptor.Output != media {
			continue
		}
		if realtime && !descriptor.Realtime {
			continue
		}
		if transform.Kind == recipeir.TransformResample && transform.Resample.SampleFormat != "" &&
			len(descriptor.SampleFormats) != 0 && !stringAllowed(descriptor.SampleFormats, transform.Resample.SampleFormat) {
			continue
		}
		if transform.Kind == recipeir.TransformResize {
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
		return filter.Descriptor{}, nil, false, ErrShapeAdapterMissing
	case 1:
		return matched[0], []string{matched[0].Name}, resolved, nil
	default:
		names := make([]string, 0, len(matched))
		for i := range matched {
			names = append(names, matched[i].Name)
		}
		return filter.Descriptor{}, names, false, ErrShapeAdapterAmbiguous
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

// recipeIRShapeSolverAdapterError renders adapter-selection failures with the chain
// context: which operation needed the conversion, the source and expected
// shapes, and the registration (or disambiguation) that fixes it.
func recipeIRShapeSolverAdapterError(operation string, node string, index int, step recipeir.Operation, actual shape.Spec, expected shape.Spec, err error) error {
	selection, ok := err.(*ShapeAdapterSelectionError)
	if !ok {
		return err
	}
	details := []string{
		fmt.Sprintf("operation_index=%d", index),
		"operation=" + string(step.Kind),
		"source=" + HumanizeShape(actual),
		"actual_shape=" + actual.String(),
		"expected_shape=" + expected.String(),
		"conversion=" + selection.Needed.String(),
		"media=" + string(selection.Media),
	}
	if selection.Cause == ErrShapeAdapterAmbiguous {
		return &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.ShapeAdapterAmbiguousCode),
			Code:      builderr.ShapeAdapterAmbiguousCode,
			Operation: operation,
			Node:      node,
			Reason: fmt.Sprintf("several registered filters can perform the %s conversion before %s: %s",
				selection.Needed.String(), RecipeIROperationLabel(step), strings.Join(selection.Candidates, ", ")),
			Details: builderr.DetailLines(append(details, "candidates="+strings.Join(selection.Candidates, ","))),
			Fixes: builderr.Fixes([]string{
				"keep one " + string(selection.Media) + " conversion filter registered per runtime",
				"build the runtime with only the intended conversion filter via goav.New(runconfig.WithFilter(...))",
				"bias the choice with .Prefer(shape.New(...)) toward a capability only one candidate declares",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.ShapeAdapterMissingCode),
		Code:      builderr.ShapeAdapterMissingCode,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf("no registered filter can perform the %s conversion before %s",
			selection.Needed.String(), RecipeIROperationLabel(step)),
		Details: builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			"register a " + string(selection.Media) + " conversion filter with runconfig.WithFilter(filter.Descriptor{Input: ..., Output: ...}, factory)",
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), or goav.New(bundle.Filters(), ...) for the bundled resample and resize adapters",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// recipeIRShapeConversionRefusedError is the solver's refusal: a conversion would fix
// the chain, but the active .Auto(...) policy does not allow it. The error
// carries the source shape, the expected shape, and the exact policy to add.
func recipeIRShapeConversionRefusedError(operation string, node string, index int, step recipeir.Operation, allowed shape.Policy, conversion shapeConversionPlan, actual shape.Spec, expected shape.Spec) error {
	missing := allowed.Missing(conversion.needed)
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.ShapeConversionRefusedCode),
		Code:      builderr.ShapeConversionRefusedCode,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf("%s needs %s but the chain policy (%s) does not allow it",
			RecipeIROperationLabel(step), conversion.detail, allowed.String()),
		Details: builderr.Details(builderr.KV("operation_index", fmt.Sprintf("%d", index)), builderr.KV("operation", string(step.Kind)), builderr.KV("source", HumanizeShape(actual)), builderr.KV("actual_shape", actual.String()), builderr.KV("expected_shape", expected.String()), builderr.KV("needed", conversion.needed.String()), builderr.KV("allowed", allowed.String())),
		Fixes: builderr.Fixes(append(
			[]string{fmt.Sprintf("add .Auto(%s) to the chain to let the planner insert the conversion", strings.Join(missing.Constructors(), ", "))},
			RecipeIRExplicitConversionSuggestion(conversion.Transform, step)...,
		)),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// appendRecipeIRAutoFixSuggestions upgrades the plain shape mismatch error with the
// exact fix when a conversion could solve it: the .Auto(...) policy to add and
// the explicit operation to write instead.
func appendRecipeIRAutoFixSuggestions(err error, actual shape.Spec, expected shape.Spec, step recipeir.Operation) error {
	buildErr, ok := err.(*builderr.Refusal)
	if !ok || buildErr == nil {
		return err
	}
	needed := shape.Conversions(actual, expected)
	buildErr.Fixes = append(buildErr.Fixes,
		builderr.Fix{Message: fmt.Sprintf("add .Auto(%s) to the chain to let the planner insert the conversion", strings.Join(needed.Constructors(), ", "))})
	media := FirstNonEmptyMedia(expected.MediaKind, actual.MediaKind)
	if transform, ok := SynthesizeConversionTransform(media, actual, expected); ok {
		buildErr.Fixes = append(buildErr.Fixes, builderr.Fixes(RecipeIRExplicitConversionSuggestion(transform, step))...)
	}
	return buildErr
}

func RecipeIRExplicitConversionSuggestion(transform recipeir.Transform, step recipeir.Operation) []string {
	target := ""
	if step.Kind != "" {
		target = " before " + RecipeIROperationLabel(step)
	}
	switch {
	case transform.Kind == recipeir.TransformResample:
		return []string{fmt.Sprintf("insert .Resample(%d, %d) explicitly%s", transform.Resample.SampleRate, transform.Resample.Channels, target)}
	case transform.Kind == recipeir.TransformResize:
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
		Code:    diagnosticShapePreferenceApplied,
		Node:    node,
		Message: fmt.Sprintf("preference (%s) %s: %s", pref.String(), strings.Join(effects, " and "), conversion.detail),
		Details: []string{
			"preference=" + pref.String(),
			"adapter=" + conversion.Factory,
		},
	}
}

// shapePreferenceIgnoredDiagnostic records a preference the solver could not
// honor — soft by definition, the plain plan proceeds untouched.
func shapePreferenceIgnoredDiagnostic(node string, pref shape.Spec, reason string) plan.Diagnostic {
	return plan.Diagnostic{
		Code:    diagnosticShapePreferenceIgnored,
		Node:    node,
		Message: fmt.Sprintf("preference (%s) ignored: %s", pref.String(), reason),
		Details: []string{"preference=" + pref.String()},
	}
}

// shapeConversionDiagnostic records one insertion on the plan, e.g.
// "inserted resample 44.1kHz→48kHz before encode-opus (AllowResample)".
func shapeConversionDiagnostic(node string, conversion shapeConversionPlan, step recipeir.Operation, actual shape.Spec, expected shape.Spec) plan.Diagnostic {
	return plan.Diagnostic{
		Code:    DiagnosticShapeConversionInserted,
		Node:    node,
		Message: fmt.Sprintf("inserted %s before %s (%s)", conversion.detail, RecipeIROperationLabel(step), shapePolicyLabel(conversion.needed)),
		Details: []string{
			"adapter=" + conversion.Factory,
			"source=" + HumanizeShape(actual),
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

// recipeIROperationLabel names an operation for solver errors and diagnostics:
// "encode-opus", "decode-vp8", "stage-visualizer", "resample".
func RecipeIROperationLabel(operation recipeir.Operation) string {
	switch operation.Kind {
	case plan.OpEncode:
		return "encode-" + cmp.Or(string(operation.Encode.ID), operation.Component, "encoder")
	case plan.OpDecode:
		return "decode-" + cmp.Or(string(operation.Decode.ID), operation.Component, "decoder")
	case plan.OpStage:
		return "stage-" + cmp.Or(operation.Component, "custom")
	case plan.OpTransform:
		return cmp.Or(recipeIROperationComponent(operation), "transform")
	default:
		return cmp.Or(operation.Component, string(operation.Kind), "operation")
	}
}

// shapeConversionDetailText renders the conversion delta for humans:
// "resample 44.1kHz→48kHz", "resize 1280x720→640x360", "resample s16→f32".
func shapeConversionDetailText(factory string, actual shape.Spec, transform recipeir.Transform) string {
	parts := []string{factory}
	switch {
	case transform.Kind == recipeir.TransformResample:
		config := transform.Resample
		if config.SampleRate != 0 && config.SampleRate != actual.SampleRate {
			parts = append(parts, formatSampleRate(actual.SampleRate)+"→"+formatSampleRate(config.SampleRate))
		}
		if config.Channels != 0 && config.Channels != actual.Channels {
			parts = append(parts, FormatChannels(actual.Channels)+"→"+FormatChannels(config.Channels))
		}
		if config.SampleFormat != "" && config.SampleFormat != actual.SampleFormat {
			parts = append(parts, FormatMediaFormat(actual.SampleFormat)+"→"+config.SampleFormat)
		}
	case transform.Kind == recipeir.TransformResize:
		config := transform.Resize
		if (config.Width != 0 && config.Width != actual.Width) || (config.Height != 0 && config.Height != actual.Height) {
			parts = append(parts, FormatFrameSize(actual.Width, actual.Height)+"→"+FormatFrameSize(config.Width, config.Height))
		}
		if config.PixelFormat != "" && config.PixelFormat != actual.PixelFormat {
			parts = append(parts, FormatMediaFormat(actual.PixelFormat)+"→"+config.PixelFormat)
		}
	}
	return strings.Join(parts, " ")
}

// humanizeShape renders a Spec for error prose: "frame audio 44.1kHz 2ch s16".
func HumanizeShape(spec shape.Spec) string {
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
		parts = append(parts, FormatChannels(spec.Channels))
	}
	if spec.SampleFormat != "" {
		parts = append(parts, spec.SampleFormat)
	}
	if spec.Width != 0 || spec.Height != 0 {
		parts = append(parts, FormatFrameSize(spec.Width, spec.Height))
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

func FormatChannels(channels int) string {
	if channels <= 0 {
		return "unknown channels"
	}
	return fmt.Sprintf("%dch", channels)
}

func FormatFrameSize(width int, height int) string {
	if width <= 0 && height <= 0 {
		return "unknown size"
	}
	return fmt.Sprintf("%dx%d", width, height)
}

func FormatMediaFormat(format string) string {
	if format == "" {
		return "unknown format"
	}
	return format
}

func FirstPositiveInt(values ...int) int {
	for i := range values {
		if values[i] > 0 {
			return values[i]
		}
	}
	return 0
}
