package compile

import (
	"cmp"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// solveOperationShapes is the shape solver pass: it propagates each branch's
// media shape across its operations, validates every operation contract,
// and — under an active .Auto(...) policy — inserts the allowed conversions
// as real planned operations. An insertion before a node several branches
// share is one node, the same for every branch.
func (s *recipeState) solveOperationShapes() error {
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		initial := s.initialStreamShape(i)
		if _, join := s.joins[stream.Root]; join && stream.Input < 0 {
			if err := validateExplicitEncodeShapes(s.operation, stream, initial); err != nil {
				return err
			}
		}
		solved, diagnostics, err := SolveRecipeIROperationShapes(s.operation, s.env, stream, initial)
		if err != nil {
			return err
		}
		if solved == nil {
			continue
		}
		for j := range solved {
			if solved[j].Node >= 0 && solved[j].Node < recipeir.NodeID(len(s.req.Graph.Nodes)) && solved[j].Kind == s.req.Graph.Nodes[solved[j].Node].Op.Kind {
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
func (s *recipeState) initialStreamShape(index int) shape.Spec {
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
	inputShape := MediaShapeFromCodecSpec(input.Codec, firstNonEmptyDomain(spec.Domain, shape.DomainPacket))
	inputShape.Realtime = input.Realtime
	spec = shape.Merge(inputShape, spec)
	if spec.Domain == "" {
		spec.Domain = shape.DomainPacket
	}
	if spec.MediaKind == "" {
		spec.MediaKind = FirstNonEmptyMedia(stream.Selector.Type, recipeIRStreamEncodeSpec(stream).Type, input.Codec.Type)
	}
	if spec.StreamID == "" {
		spec.StreamID = stream.Selector.ID
	}
	if spec.Codec == "" {
		spec.Codec = FirstNonEmptyCodec(stream.Selector.Codec, input.Codec.ID)
	}
	if input.Realtime {
		spec.Realtime = true
	}
	return spec
}

// finalStreamShape is the shape a branch delivers to its destinations.
func (s *recipeState) finalStreamShape(index int) shape.Spec {
	stream := s.recipe.Streams[index]
	current := NormalizeTapShape(s.initialStreamShape(index))
	if current.MediaKind == "" {
		current.MediaKind = stream.Selector.Type
	}
	if current.Codec == "" {
		current.Codec = stream.Selector.Codec
	}
	for i := range stream.Operations {
		current = RecipeIROperationOutputShape(current, stream.Operations[i])
	}
	return current
}

// validateDestinationShapes refuses frames routed to a byte or mux
// destination, and media a media sink does not take.
func (s *recipeState) validateDestinationShapes() error {
	if err := s.validateRemuxSinkShapes(); err != nil {
		return err
	}
	for i := range s.recipe.Streams {
		stream := s.recipe.Streams[i]
		final := s.finalStreamShape(i)
		node := recipeIRStreamName(stream)
		for _, dest := range s.facts[i].dests {
			destination := s.outputs[dest]
			kind := s.recipe.Destinations[dest].Kind
			if err := validateRecipeDestinationShape(s.operation, node, s.outputDestinationNames[dest], kind, destination, final); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRecipeDestinationShape(operation string, node string, destinationName string, kind recipeir.DestinationKind, destination Destination, spec shape.Spec) error {
	if kind == recipeir.DestinationKindSink {
		return ValidateSinkShape(operation, node, destinationName, destination, spec)
	}
	if kind == recipeir.DestinationKindUnknown && !destination.HasOutput {
		return nil
	}
	if spec.Domain == shape.DomainPacket {
		return nil
	}
	return DestinationShapeMismatchError(operation, node, destinationName, destination, spec)
}

func DestinationShapeMismatchError(operation string, node string, destinationName string, destination Destination, spec shape.Spec) error {
	label := cmp.Or(destinationName, destination.Label, "destination")
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DestinationShapeMismatchCode),
		Code:      builderr.DestinationShapeMismatchCode,
		Operation: operation,
		Node:      cmp.Or(node, label, "destination"),
		Reason:    "byte or mux destination requires packet-domain media",
		Details:   builderr.Details(builderr.KV("destination", label), builderr.KV("expected_shape", shape.New(shape.Domain(shape.DomainPacket), shape.Media(spec.MediaKind)).String()), builderr.KV("actual_shape", spec.String())),
		Fixes: builderr.Fixes([]string{
			"call " + builderr.EncodeHint(spec.MediaKind) + " before writing to file, URI, or writer destinations",
			"leave out .Decode() for packet-preserving output: input.Video().To(output)",
			"send frame-domain media to goav.Sink(...) instead of a byte destination",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func ValidateShapeAnnotationDomain(operation string, node string, index int, current shape.Spec, annotation shape.Spec) error {
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
	media := FirstNonEmptyMedia(after.MediaKind, current.MediaKind)
	expected := shape.New(shape.Domain(current.Domain), shape.Media(media))
	actual := shape.New(shape.Domain(after.Domain), shape.Media(media))
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.OperationShapeMismatchCode),
		Code:      builderr.OperationShapeMismatchCode,
		Operation: operation,
		Node:      cmp.Or(node, "stream"),
		Reason:    "shape annotation cannot change packet/frame domain",
		Details:   builderr.Details(builderr.KV("operation_index", fmt.Sprintf("%d", index)), builderr.KV("operation", "shape"), builderr.KV("expected_shape", expected.String()), builderr.KV("actual_shape", actual.String())),
		Fixes: builderr.Fixes([]string{
			"keep .Shape(...) annotations in the current packet/frame domain",
			"use .Decode() to move packets to decoded frames",
			"use .Encode(codec...) to move frames back to packets",
		}),
		Cause: builderr.ErrUnsupportedBuild,
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

// validateExplicitEncodeShapes refuses an encoder on a joined stream whose
// settings name a sample rate, channel count, or format the join does not
// produce, unless the branch lets the solver convert with .Auto(...).
func validateExplicitEncodeShapes(operation string, stream recipeir.Stream, initial shape.Spec) error {
	if _, active := operationsAutoPolicy(stream.Operations); active {
		return nil
	}
	current := NormalizeTapShape(initial)
	for i, step := range stream.Operations {
		if step.Kind == plan.OpEncode {
			if expected := explicitEncodeSoftInputShape(step.Encode); !MediaShapeEmpty(expected) && current.Domain == shape.DomainFrame {
				if set := (shape.Set{expected}); !set.Accepts(current) {
					err := recipeIROperationShapeFailureError(operation, recipeIRStreamName(stream), i, step, set, current)
					if target, fixable := shapeConversionTargetFromSet(current, set); fixable {
						err = appendRecipeIRAutoFixSuggestions(err, current, target, step)
					}
					return err
				}
			}
		}
		current = RecipeIROperationOutputShape(current, step)
	}
	return nil
}

// recipeIRInputSourceShape is the stream shape a provider or custom source
// declares, normalized.
func recipeIRInputSourceShape(input recipeir.Input) (shape.Spec, bool) {
	switch input.Kind {
	case recipeir.InputKindCustomSource, recipeir.InputKindProvider:
	default:
		return shape.Spec{}, false
	}
	if input.SourceShape.Domain == "" {
		return shape.Spec{}, false
	}
	return recipeir.NormalizeSourceShape(input.Name, input.SourceShape), true
}
