package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// solveOperationShapes is the shape solver pass: it propagates each branch's
// media shape across its operations, validates every operation contract,
// and — under an active .Auto(...) policy — inserts the allowed conversions
// as real planned operations. An insertion before a node several branches
// share is one node, the same for every branch.
func (s *recipeCompileState) solveOperationShapes() error {
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		initial := s.initialStreamShape(i)
		if _, join := s.joins[stream.Root]; join && stream.Input < 0 {
			if err := validateExplicitEncodeShapes(s.operation, stream, initial); err != nil {
				return err
			}
		}
		solved, diagnostics, err := solveRecipeIROperationShapes(s.operation, s.runtime, stream, initial)
		if err != nil {
			return err
		}
		if solved == nil {
			continue
		}
		for j := range solved {
			if solved[j].Node >= 0 && solved[j].Node < recipeir.NodeID(len(s.graph.graph.Nodes)) && solved[j].Kind == s.graph.graph.Nodes[solved[j].Node].Op.Kind {
				continue
			}
			// A conversion: its node is keyed by the operation it feeds.
			next := recipeir.NodeID(-1)
			if j+1 < len(solved) {
				next = solved[j+1].Node
			}
			key := fmt.Sprintf("convert@%d/%s/%s", next, solved[j].Component, transformKey(solved[j].Transform))
			solved[j] = s.insertedOperation(key, next, solved[j])
		}
		s.recipe.Streams[i].Operations = solved
		s.shapeDiagnostics = append(s.shapeDiagnostics, diagnostics...)
	}
	s.markShared()
	return nil
}

func transformKey(transform recipeir.Transform) string {
	return fmt.Sprintf("%s/%+v/%+v", transform.Kind, transform.Resize, transform.Resample)
}

// initialStreamShape is the shape a branch's operations start from: the
// selected stream of its input, or its join's output.
func (s *recipeCompileState) initialStreamShape(index int) shape.Spec {
	stream := s.recipe.Streams[index]
	if stream.Input < 0 {
		if p, ok := s.joins[stream.Root]; ok {
			return p.joinedShape()
		}
		return shape.Spec{Domain: shape.DomainFrame}
	}
	input := s.recipe.Inputs[stream.Input]
	var spec shape.Spec
	sourceShape, sourceShapeOK := recipeIRInputSourceShape(input)
	if selected, ok := s.selectedStream(stream); ok {
		domain := shape.DomainPacket
		if sourceShapeOK && sourceShape.Domain != "" {
			domain = sourceShape.Domain
		}
		spec = shape.FromStream(selected, domain)
		if sourceShapeOK {
			spec = shape.Merge(spec, sourceShape)
		}
	}
	return normalizeRecipeIRPlanBranchShape(spec, stream, input)
}

func normalizeRecipeIRPlanBranchShape(spec shape.Spec, stream recipeir.Stream, input recipeir.Input) shape.Spec {
	inputShape := mediaShapeFromCodecSpec(input.Codec, firstNonEmptyDomain(spec.Domain, shape.DomainPacket))
	inputShape.Realtime = input.Realtime
	spec = shape.Merge(inputShape, spec)
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	if spec.MediaKind == "" {
		spec.MediaKind = firstNonEmptyMedia(stream.Selector.Type, recipeIRStreamEncodeSpec(stream).Type, input.Codec.Type)
	}
	if spec.StreamID == "" {
		spec.StreamID = stream.Selector.ID
	}
	if spec.Codec == "" {
		spec.Codec = firstNonEmptyCodec(stream.Selector.Codec, input.Codec.ID)
	}
	if input.Realtime {
		spec.Realtime = true
	}
	return spec
}

// finalStreamShape is the shape a branch delivers to its destinations.
func (s *recipeCompileState) finalStreamShape(index int) shape.Spec {
	stream := s.recipe.Streams[index]
	current := normalizeTapShape(s.initialStreamShape(index))
	if current.MediaKind == "" {
		current.MediaKind = stream.Selector.Type
	}
	if current.Codec == "" {
		current.Codec = stream.Selector.Codec
	}
	for i := range stream.Operations {
		current = recipeIROperationOutputShape(current, stream.Operations[i])
	}
	return current
}

// validateDestinationShapes refuses frames routed to a byte or mux
// destination.
func (s *recipeCompileState) validateDestinationShapes() error {
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		final := s.finalStreamShape(i)
		node := recipeIRStreamName(stream)
		for _, dest := range s.facts[i].dests {
			destination := s.outputAttachments[dest]
			kind := s.recipe.Destinations[dest].Kind
			if err := validateRecipeDestinationShape(s.operation, node, s.outputDestinationNames[dest], kind, destination, final); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRecipeDestinationShape(operation string, node string, destinationName string, kind recipeir.DestinationKind, destination destinationSpec, spec shape.Spec) error {
	if kind == recipeir.DestinationKindSink {
		return nil
	}
	if kind == recipeir.DestinationKindUnknown && !destinationSpecHasOutput(destination) {
		return nil
	}
	if spec.Domain == shape.DomainPacket {
		return nil
	}
	return destinationShapeMismatchError(operation, node, destinationName, destination, spec)
}

func destinationShapeMismatchError(operation string, node string, destinationName string, destination destinationSpec, spec shape.Spec) error {
	label := firstNonEmpty(destinationName, destination.label("destination"))
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationShapeMismatchCode),
		Code:      destinationShapeMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, label, "destination"),
		Reason:    "byte or mux destination requires packet-domain media",
		fields:    errDetails(errDetail("destination", label), errDetail("expected_shape", shape.New(shape.Domain(shape.DomainPacket), shape.Media(spec.MediaKind)).String()), errDetail("actual_shape", spec.String())),
		fixes: buildErrorFixes([]string{
			"call " + encodeHint(spec.MediaKind) + " before writing to file, URI, or writer destinations",
			"leave out .Decode() for packet-preserving output: input.Video().To(output)",
			"send frame-domain media to goav.Sink(...) instead of a byte destination",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateShapeAnnotationDomain(operation string, node string, index int, current shape.Spec, annotation shape.Spec) error {
	if annotation.Domain == "" || current.Domain == "" || annotation.Domain == current.Domain {
		return nil
	}
	if !packetOrFrameDomain(annotation.Domain) || !packetOrFrameDomain(current.Domain) {
		return nil
	}
	return shapeAnnotationDomainMismatchError(operation, node, index, current, annotation)
}

func packetOrFrameDomain(domain shape.MediaDomain) bool {
	return domain == shape.DomainPacket || domain == shape.DomainFrame
}

func shapeAnnotationDomainMismatchError(operation string, node string, index int, current shape.Spec, annotation shape.Spec) error {
	after := shape.Merge(current, annotation)
	media := firstNonEmptyMedia(after.MediaKind, current.MediaKind)
	expected := shape.New(shape.Domain(current.Domain), shape.Media(media))
	actual := shape.New(shape.Domain(after.Domain), shape.Media(media))
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(operationShapeMismatchCode),
		Code:      operationShapeMismatchCode,
		Operation: operation,
		Node:      firstNonEmpty(node, "stream"),
		Reason:    "shape annotation cannot change packet/frame domain",
		fields:    errDetails(errDetail("operation_index", fmt.Sprintf("%d", index)), errDetail("operation", "shape"), errDetail("expected_shape", expected.String()), errDetail("actual_shape", actual.String())),
		fixes: buildErrorFixes([]string{
			"keep .Shape(...) annotations in the current packet/frame domain",
			"use .Decode() to move packets to decoded frames",
			"use .Encode(codec...) to move frames back to packets",
		}),
		cause: errUnsupportedBuild,
	}
}

func shapeSetString(shapes shape.Set) string {
	if len(shapes) == 0 {
		return "any"
	}
	parts := make([]string, 0, len(shapes))
	for i := range shapes {
		parts = append(parts, shapes[i].String())
	}
	return strings.Join(parts, " | ")
}

var _ = plan.OpDecode
var _ av.MediaType

// validateExplicitEncodeShapes refuses an encoder on a joined stream whose
// settings name a sample rate, channel count, or format the join does not
// produce, unless the branch lets the solver convert with .Auto(...).
func validateExplicitEncodeShapes(operation string, stream recipeir.Stream, initial shape.Spec) error {
	if _, active := operationsAutoPolicy(stream.Operations); active {
		return nil
	}
	current := normalizeTapShape(initial)
	for i, step := range stream.Operations {
		if step.Kind == plan.OpEncode {
			if expected := explicitEncodeSoftInputShape(step.Encode); !mediaShapeEmpty(expected) && current.Domain == shape.DomainFrame {
				if set := (shape.Set{expected}); !set.Accepts(current) {
					err := recipeIROperationShapeFailureError(operation, recipeIRStreamName(stream), i, step, set, current)
					if target, fixable := shapeConversionTargetFromSet(current, set); fixable {
						err = appendRecipeIRAutoFixSuggestions(err, current, target, step)
					}
					return err
				}
			}
		}
		current = recipeIROperationOutputShape(current, step)
	}
	return nil
}
