package goav

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

type operationFacts struct {
	operations        []operationSpec
	sharedOperations  []operationSpec
	privateOperations []operationSpec
	encode            codec.Spec
	decode            codec.Spec
}

func recipeIROperationFacts(operations []recipeir.Operation) operationFacts {
	specs := operationSpecsFromRecipeIROperations(operations)
	return operationFactsFromSpecs(specs)
}

func solveOperationSpecShapes(operation string, rt *runtime, stream streamIntent, initial shape.Spec) ([]operationSpec, []plan.Diagnostic, error) {
	solved, diagnostics, err := solveRecipeIROperationShapes(operation, rt, recipeIRStreamFromIntent(stream), initial)
	if err != nil || solved == nil {
		return nil, diagnostics, err
	}
	return operationSpecsFromRecipeIROperations(solved), diagnostics, nil
}

func shapeSolverAdapterError(operation string, node string, index int, step operationSpec, actual shape.Spec, expected shape.Spec, err error) error {
	return recipeIRShapeSolverAdapterError(operation, node, index, recipeIROperationFromSpec(step), actual, expected, err)
}

func appendAutoFixSuggestions(err error, actual shape.Spec, expected shape.Spec, step operationSpec) error {
	return appendRecipeIRAutoFixSuggestions(err, actual, expected, recipeIROperationFromSpec(step))
}

func explicitConversionSuggestion(transform transformSpec, step operationSpec) []string {
	return recipeIRExplicitConversionSuggestion(transform, recipeIROperationFromSpec(step))
}

func operationSpecLabel(operation operationSpec) string {
	return recipeIROperationLabel(recipeIROperationFromSpec(operation))
}

func operationSpecsRequireRuntime(operations []operationSpec) bool {
	for i := range operations {
		switch operations[i].Kind {
		case plan.OpDecode, plan.OpEncode, plan.OpTransform:
			return true
		}
	}
	return false
}

// operationShapeFailureError dispatches a failed shape contract to its
// surface: a .Require(...) assertion gets the requirement-specific refusal,
// every other operation keeps the established mismatch error.
func operationShapeFailureError(operation string, node string, index int, step operationSpec, expected shape.Set, actual shape.Spec) error {
	if step.Kind == plan.OpShape && step.Require != nil {
		return shapeRequirementUnmetError(operation, node, index, step, expected, actual)
	}
	return operationShapeMismatchError(operation, node, index, step, expected, actual)
}

// shapeRequirementUnmetError is the hard .Require(...) refusal: the stream at
// this chain position does not satisfy the asserted shape. It carries the
// actual and required shapes in the established refusal format; the solver
// appends the exact .Auto(...) fix when a conversion could satisfy it.
func shapeRequirementUnmetError(operation string, node string, index int, step operationSpec, expected shape.Set, actual shape.Spec) error {
	required := shape.Spec{}
	if step.Require != nil {
		required = *step.Require
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(shapeRequirementUnmetCode),
		Code:      shapeRequirementUnmetCode,
		Operation: operation,
		Node:      node,
		Reason: fmt.Sprintf(".Require(...) is not satisfied: the stream is %s, required %s",
			humanizeShape(actual), humanizeShape(required)),
		fields: errDetails(errDetail("operation_index", fmt.Sprintf("%d", index)), errDetail("operation", "require"), errDetail("source", humanizeShape(actual)), errDetail("actual_shape", actual.String()), errDetail("expected_shape", shapeSetString(expected))),
		fixes: buildErrorFixes([]string{
			"adjust the chain so the stream satisfies the required shape before .Require(...)",
			"relax or remove the .Require(...) assertion",
		}),
		cause: errUnsupportedBuild,
	}
}

func operationSpecOutputShape(input shape.Spec, operation operationSpec) shape.Spec {
	out := operation.OutputShapes(input)
	if len(out) == 0 {
		return input
	}
	return out[0]
}

func operationShapeMismatchError(operation string, node string, index int, step operationSpec, expected shape.Set, actual shape.Spec) error {
	component := firstNonEmpty(step.Component, operationSpecComponent(step), string(step.Kind), "operation")
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      node,
		Reason:    component + " cannot consume the current media shape",
		fields:    errDetails(errDetail("operation_index", fmt.Sprintf("%d", index)), errDetail("operation", string(step.Kind)), errDetail("expected_shape", shapeSetString(expected)), errDetail("actual_shape", actual.String())),
		fixes:     buildErrorFixes(operationShapeMismatchSuggestions(step)),
		cause:     errUnsupportedBuild,
	}
}

func operationSpecComponent(operation operationSpec) string {
	switch operation.Kind {
	case plan.OpDecode:
		return firstNonEmpty(string(operation.Decode.ID), operation.Component, "decode")
	case plan.OpTransform:
		return firstNonEmpty(transformFactoryName(operation.Transform), "transform")
	case plan.OpEncode:
		return firstNonEmpty(string(operation.Encode.ID), operation.Component, "encode")
	case plan.OpCopy:
		return "packet-copy"
	default:
		return operation.Component
	}
}

func operationShapeMismatchSuggestions(operation operationSpec) []string {
	switch operation.Kind {
	case plan.OpDecode:
		return []string{
			"decode only consumes packet-domain media",
			"remove duplicate .Decode() calls after a frame tap",
			"start from goav.PacketTap(name) when a runtime branch should decode",
		}
	case plan.OpTransform:
		return []string{
			"call .Decode() before frame transforms when starting from packets",
			"use .Video().Resize(...) for video frames",
			"use .Audio().Resample(...) for audio frames",
		}
	case plan.OpEncode:
		return []string{
			"call .Decode() before encoding when starting from packets",
			"keep .Shape(...) annotations in the frame domain before encoders",
			"use .Copy() instead of an encoder for packet-preserving fanout",
		}
	case plan.OpCopy:
		return []string{
			"copy only consumes packet-domain media",
			"move .Copy() before decode or start from goav.PacketTap(name)",
			"use .Encode(codec...) instead of .Copy() to turn decoded frames back into packets",
			"use a sink destination when the branch should remain decoded",
		}
	default:
		return []string{
			"inspect Explain(ctx) to see operation shapes",
			"keep structural facts in goav.Shape(...) and codec behavior in codec.Spec options",
		}
	}
}

func operationFactsFromSpecs(specs []operationSpec) operationFacts {
	shared, private := splitOperationSpecsByShared(specs)
	return operationFacts{
		operations:        cloneOperationSpecs(specs),
		sharedOperations:  shared,
		privateOperations: private,
		encode:            cloneCodecSpec(chainEncodeSpec(specs)),
		decode:            cloneCodecSpec(chainDecodeCodec(specs)),
	}
}

func operationSpecsFromRecipeIROperations(operations []recipeir.Operation) []operationSpec {
	if len(operations) == 0 {
		return nil
	}
	out := make([]operationSpec, 0, len(operations))
	for i := range operations {
		out = append(out, operationSpecFromRecipeIR(operations[i]))
	}
	return out
}

func (f operationFacts) Clone() operationFacts {
	return operationFacts{
		operations:        cloneOperationSpecs(f.operations),
		sharedOperations:  cloneOperationSpecs(f.sharedOperations),
		privateOperations: cloneOperationSpecs(f.privateOperations),
		encode:            cloneCodecSpec(f.encode),
		decode:            cloneCodecSpec(f.decode),
	}
}

func (f operationFacts) Operations() []operationSpec {
	return cloneOperationSpecs(f.operations)
}

func (f operationFacts) SharedOperations() []operationSpec {
	return cloneOperationSpecs(f.sharedOperations)
}

func (f operationFacts) PrivateOperations() []operationSpec {
	return cloneOperationSpecs(f.privateOperations)
}

func (f operationFacts) SharedFacts() operationFacts {
	return operationFactsFromSpecs(f.sharedOperations)
}

func (f operationFacts) PrivateFacts() operationFacts {
	return operationFactsFromSpecs(f.privateOperations)
}

func (f operationFacts) Encode() codec.Spec {
	return cloneCodecSpec(f.encode)
}

func (f operationFacts) Decode() codec.Spec {
	return cloneCodecSpec(f.decode)
}

func (f operationFacts) HasKind(kind plan.OperationKind) bool {
	for i := range f.operations {
		if f.operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func splitOperationSpecsByShared(operations []operationSpec) ([]operationSpec, []operationSpec) {
	if len(operations) == 0 {
		return nil, nil
	}
	shared := make([]operationSpec, 0)
	private := make([]operationSpec, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		if operation.Shared {
			shared = append(shared, operation)
			continue
		}
		private = append(private, operation)
	}
	return cloneOperationSpecs(shared), cloneOperationSpecs(private)
}

func chainStepsFromChainOperations(operations []operationSpec) []chainStep {
	if len(operations) == 0 {
		return nil
	}
	steps := make([]chainStep, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case plan.OpStage:
			if operation.Stage != nil {
				steps = append(steps, chainStep{stage: operation.Stage})
			}
		case plan.OpShape:
			if !mediaShapeEmpty(operation.Shape) {
				steps = append(steps, chainStep{shape: operation.Shape})
			}
		case plan.OpTransform:
			if operation.Transform.resize != nil || operation.Transform.resample != nil {
				steps = append(steps, chainStep{transform: cloneTransformSpec(operation.Transform)})
			}
		case plan.OpTap:
			if operation.Tap.Name != "" && operation.Tap.Domain != shape.DomainPacket {
				steps = append(steps, chainStep{tap: operation.Tap.Name, tapDomain: operation.Tap.Domain})
			}
		}
	}
	return steps
}

func streamIntentTransformSpecs(stream streamIntent) []transformSpec {
	return transformSpecsFromOperationSpecs(stream.Operations)
}

func transformSpecsFromOperationSpecs(operations []operationSpec) []transformSpec {
	if len(operations) == 0 {
		return nil
	}
	transforms := make([]transformSpec, 0)
	for i := range operations {
		if operations[i].Kind != plan.OpTransform {
			continue
		}
		transform := cloneTransformSpec(operations[i].Transform)
		transforms = append(transforms, transform)
	}
	return transforms
}

func mediaPlanFilterRouteOperations(filters []filterRequest) (operationFacts, error) {
	operations := make([]operationSpec, 0, len(filters))
	for i := range filters {
		filter := filters[i]
		switch {
		case filter.stage != nil:
			if err := validateStageComponent(filter.stage); err != nil {
				return operationFacts{}, err
			}
			operations = append(operations, operationSpecForStage(filter.stage))
		case filter.transform != nil:
			operation := operationSpecForTransform(transformSpecFromMediaTransform(*filter.transform))
			// Keep the (possibly solver-selected) adapter as the component, so
			// the route operations name and instantiate the same filter.
			operation.Component = firstNonEmpty(filter.transform.factory, operation.Component)
			operations = append(operations, operation)
		default:
			return operationFacts{}, errNilStage
		}
	}
	return operationFactsFromSpecs(operations), nil
}

// applyTransformComponentOverride re-points a lowered transform at the
// solver-selected adapter when the operation's component differs from the
// standard factory: the component is both the node-name prefix and the filter
// registry key, so the planned spec and the built graph stay identical.
func applyTransformComponentOverride(transform mediaTransform, operation operationSpec) mediaTransform {
	factory := operation.Component
	if operation.Kind != plan.OpTransform || factory == "" || factory == transform.factory {
		return transform
	}
	transform.name = factory + strings.TrimPrefix(transform.name, transform.factory)
	transform.factory = factory
	return transform
}

func branchComposeRouteOperationTransformsForName(name string, operations operationFacts) ([]mediaTransform, error) {
	specs := operations.Operations()
	out := make([]mediaTransform, 0, len(specs))
	transformIndex := 0
	for i := range specs {
		operation := specs[i]
		step, err := branchComposeRouteOperationTransform(name, transformIndex, operations, i)
		if err != nil {
			return nil, err
		}
		if operation.Kind == plan.OpTransform && (operation.Transform.resize != nil || operation.Transform.resample != nil) {
			transformIndex++
		}
		if !mediaTransformEmpty(step) {
			out = append(out, step)
		}
	}
	return out, nil
}

func branchComposeRouteOperationTransform(branchName string, transformIndex int, operations operationFacts, index int) (mediaTransform, error) {
	specs := operations.Operations()
	if index < 0 || index >= len(specs) {
		return mediaTransform{}, nil
	}
	operation := specs[index]
	suffix := ""
	if transformIndex > 0 {
		suffix = "-" + strconv.Itoa(transformIndex+1)
	}
	switch operation.Kind {
	case plan.OpStage:
		if operation.Stage == nil {
			return mediaTransform{}, branchChainStepError(branchName, "branch stage operation has no stage")
		}
		if _, ok := operation.Stage.(*syncGate); ok {
			return mediaTransform{
				name:  syncStageOperationName(operation, branchName),
				stage: operation.Stage,
			}, nil
		}
		return mediaTransform{
			name:  operation.Stage.Name(),
			stage: operation.Stage,
		}, nil
	case plan.OpTransform:
		if operation.Transform.resize != nil && operation.Transform.resample != nil {
			return mediaTransform{}, branchChainStepError(branchName, "branch transform operation cannot combine resize and resample")
		}
		if operation.Transform.resize != nil {
			resize := *operation.Transform.resize
			factory := firstNonEmpty(operation.Component, filter.FactoryResize)
			return mediaTransform{
				name:    factory + "-" + branchName + suffix,
				factory: factory,
				video:   &resize,
			}, nil
		}
		if operation.Transform.resample != nil {
			resample := *operation.Transform.resample
			factory := firstNonEmpty(operation.Component, filter.FactoryResample)
			return mediaTransform{
				name:    factory + "-" + branchName + suffix,
				factory: factory,
				audio:   &resample,
			}, nil
		}
		return mediaTransform{}, branchChainStepError(branchName, "branch transform operation is empty")
	default:
		return mediaTransform{}, nil
	}
}

func branchComposeRouteStageOperationCount(operations operationFacts) int {
	specs := operations.Operations()
	count := 0
	for i := range specs {
		switch specs[i].Kind {
		case plan.OpStage:
			if specs[i].Stage != nil {
				count++
			}
		case plan.OpTransform:
			if specs[i].Transform.resize != nil || specs[i].Transform.resample != nil {
				count++
			}
		}
	}
	return count
}

func branchComposeRouteStageOperations(operations operationFacts) operationFacts {
	specs := operations.Operations()
	out := make([]operationSpec, 0, len(specs))
	for i := range specs {
		operation := specs[i]
		switch operation.Kind {
		case plan.OpStage:
			if operation.Stage != nil {
				out = append(out, operation)
			}
		case plan.OpTransform:
			if operation.Transform.resize != nil || operation.Transform.resample != nil {
				out = append(out, operation)
			}
		}
	}
	return operationFactsFromSpecs(out)
}

func branchComposeRouteOperationsKey(operations operationFacts) string {
	return mediaTransformsKeyFromOperations(branchComposeRouteStageOperations(operations))
}

func mediaTransformsKeyFromOperations(operations operationFacts) string {
	if len(operations.Operations()) == 0 {
		return ""
	}
	transforms, err := branchComposeRouteOperationTransformsForName("", operations)
	if err != nil {
		return ""
	}
	return mediaTransformsKey(transforms)
}
