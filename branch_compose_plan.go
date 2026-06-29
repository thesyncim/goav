// Branch composition planning: recipe IR validation and graph handoff.

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

type namedDestinationSpec struct {
	name   string
	output destinationSpec
}

func destinationIdentity(destination namedDestinationSpec) string {
	output := destination.output
	shareKey := destinationShareKey(output, output.id)
	if shareKey == "" {
		return ""
	}
	return strings.Join([]string{
		destination.name,
		shareKey,
		output.label(""),
		destinationSinkName(output),
		output.output.Name,
		output.output.URI,
		string(output.output.Protocol),
		output.output.MIMEType,
		string(output.format),
		string(output.resolvedFormat),
	}, "\x00")
}

func destinationSinkName(output destinationSpec) string {
	if output.sink == nil {
		return ""
	}
	return output.sink.Name()
}

func destinationsShareExplicitGroup(first namedDestinationSpec, second namedDestinationSpec) bool {
	return destinationIdentity(first) != "" && destinationIdentity(first) == destinationIdentity(second)
}

const branchCompositionOperation = "build branch composition"

func planBranchCompositionRecipe(recipe recipeir.Recipe, input InputSpec, namedOutputs []namedDestinationSpec) (branchComposePlan, error) {
	streams := recipe.Streams
	outputs, outputOrder := branchDestinationAttachmentSet(namedOutputs)

	branches := make([]branchComposeBranch, 0, len(streams))
	outputBranches := make(map[string][]string, len(outputs))
	if len(streams) == 0 {
		return branchComposePlan{}, branchStreamMissingError()
	}
	for i := range streams {
		stream := streams[i]
		branchName := stream.Name
		selector := recipeIRStreamSelector(stream)
		operations := operationSpecsFromRecipeIROperations(stream.Operations)
		sharedOperations, privateOperations := splitOperationSpecsByShared(operations)
		encode := chainEncodeSpec(operations)
		labels := recipeIROutputRefsToStrings(stream.Outputs)
		branch := branchComposeBranch{
			Name:              branchName,
			Selector:          selector,
			Input:             stream.Selector.Input,
			Copy:              encode.Copy,
			Operations:        cloneOperationSpecs(operations),
			SharedOperations:  sharedOperations,
			PrivateOperations: privateOperations,
			DecodeConfig:      cloneCodecSpec(chainDecodeCodec(operations)),
			CodecChange:       rootCodecChangeFromRecipeIR(stream.CodecChange),
			Encode:            encodeConfigFromSpec(encode),
			Labels:            labels,
		}
		for _, label := range labels {
			outputBranches[label] = append(outputBranches[label], branchName)
		}
		if err := validateBranchRecipeTransforms(stream); err != nil {
			return branchComposePlan{}, err
		}
		branches = append(branches, branch)
	}

	planDestinations := make([]branchComposeTarget, 0, len(outputOrder))
	for i := range outputOrder {
		name := outputOrder[i]
		output := outputs[name]
		planTarget := branchComposeTarget{
			Name:        name,
			Destination: cloneDestinationSpec(output),
			Target:      output.output,
			Sink:        output.sink,
			Format:      output.format,
			Branches:    append([]string(nil), outputBranches[name]...),
		}
		if output.resolvedFormat != "" {
			planTarget = resolveBranchComposeTargetFormat(planTarget, output.resolvedFormat)
		}
		planDestinations = append(planDestinations, planTarget)
	}
	return branchComposePlan{
		Name:         "branch-composition",
		Input:        input.input,
		Branches:     branches,
		Destinations: planDestinations,
	}, nil
}

func branchComposePlanReady(composePlan branchComposePlan) bool {
	return len(composePlan.Branches) != 0 || len(composePlan.Destinations) != 0
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

func validateBranchCompositionRecipeShape(operation string, recipe recipeir.Recipe) error {
	if len(recipe.Inputs) == 0 {
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(inputMissingCode),
			Code:      inputMissingCode,
			Operation: operation,
			Reason:    "no input is configured",
			fixes: buildErrorFixes([]string{
				"start the recipe from an input: goav.From(goav.FileInput(\"in.webm\", reader))",
			}),
			cause: errUnsupportedBuild,
		}
	}
	if len(recipe.Inputs) > 1 {
		return &BuildError{
			Phase:     phaseBuild,
			Family:    errcode.FamilyForCode(inputCountUnsupportedCode),
			Code:      inputCountUnsupportedCode,
			Operation: operation,
			Reason:    "transcode recipes currently take one input",
			fields:    errDetails(errNote(fmt.Sprintf("inputs=%d", len(recipe.Inputs)))),
			fixes: buildErrorFixes([]string{
				"use one goav.From(input) source per composed job",
				"use the expert graph API when multiple sources must be composed manually",
			}),
			cause: errUnsupportedBuild,
		}
	}
	streams := recipe.Streams
	if len(streams) == 0 {
		return branchStreamMissingError()
	}
	branchNames := make(map[string]int, len(streams))
	for i := range streams {
		stream := streams[i]
		if err := validateBranchRecipeStreamShape(stream, i); err != nil {
			return err
		}
		if err := validateBranchRecipeTransforms(stream); err != nil {
			return err
		}
		branchName := stream.Name
		if firstIndex, ok := branchNames[branchName]; ok {
			return branchIntentDuplicateError(branchName, firstIndex, i)
		}
		branchNames[branchName] = i
	}
	return nil
}

func validateBranchRecipeStreamShape(stream recipeir.Stream, index int) error {
	streamIntent := streamIntentFromRecipeIR(stream)
	selector := recipeIRStreamSelector(stream)
	if stream.Name == "" {
		return branchIntentNameMissingError(index, streamIntent)
	}
	if err := validateRecipeStreamSelector(branchCompositionOperation, branchRecipeStreamName(stream), selector); err != nil {
		return err
	}
	encode := recipeIRStreamEncodeSpec(stream)
	if codecIntentSet(encode) {
		if encode.Copy && recipeIRStreamHasDecode(stream) {
			return branchCopyUnsupportedError(streamIntent)
		}
		if encode.Copy && len(recipeIRStreamTransforms(stream)) != 0 {
			return branchPacketTransformUnsupportedError(streamIntent)
		}
		if err := validateRecipeEncode(encode, branchCompositionOperation, stream.Name); err != nil {
			return err
		}
	}
	if len(stream.Outputs) == 0 {
		return branchIntentDestinationMissingError(streamIntent)
	}
	return validateBranchRecipeDestinations(stream)
}

func branchRecipeStreamName(stream recipeir.Stream) string {
	return firstNonEmpty(stream.Name, string(stream.Selector.Type), "stream")
}

func recipeIRStreamHasDecode(stream recipeir.Stream) bool {
	for i := range stream.Operations {
		if stream.Operations[i].Kind == plan.OpDecode {
			return true
		}
	}
	return false
}

func validateBranchCompositionAttachments(input InputSpec, namedOutputs []namedDestinationSpec, fromBranchSplit bool) error {
	if err := input.validate(); err != nil {
		return err
	}
	if input.provider != nil {
		if !fromBranchSplit {
			return transcodeUnsupportedLiveInputError()
		}
	}
	seen := make(map[string]struct{}, len(namedOutputs))
	for i := range namedOutputs {
		if err := namedOutputs[i].output.validate(branchCompositionOperation, fmt.Sprintf("output-%d", i)); err != nil {
			return err
		}
		name := namedOutputs[i].name
		if _, ok := seen[name]; ok {
			return branchDestinationDuplicateError(name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func branchDestinationAttachmentSet(namedOutputs []namedDestinationSpec) (map[string]destinationSpec, []string) {
	outputs := make(map[string]destinationSpec, len(namedOutputs))
	outputOrder := make([]string, 0, len(namedOutputs))
	for i := range namedOutputs {
		name := namedOutputs[i].name
		outputOrder = append(outputOrder, name)
		outputs[name] = namedOutputs[i].output.withName(firstNonEmpty(namedOutputs[i].output.name, name))
	}
	return outputs, outputOrder
}

func branchStreamMissingError() error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamMissingCode),
		Code:      streamMissingCode,
		Operation: branchCompositionOperation,
		Reason:    "no audio or video branches are configured",
		fixes: buildErrorFixes([]string{
			"add a video branch such as .Video(\"720p\").Resize(...).Encode(codec.VP9(...)).To(...)",
			"add an audio branch such as .Audio(\"main\").Resample(...).Encode(codec.Opus(...)).To(...)",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchEncodeMissingError(stream streamIntent) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeMissingCode),
		Code:      encodeMissingCode,
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "branch needs an encoder before writing to a muxed destination",
		fields:    errDetails(errDetail("expected_shape", shape.New(shape.Domain(shape.DomainPacket), shape.Media(stream.Select.Type)).String()), errDetail("actual_shape", shape.Frame(stream.Select.Type).String())),
		fixes: buildErrorFixes([]string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(...)",
			"route raw frames to goav.Sink(...) when the branch should stay decoded",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchCopyUnsupportedError(stream streamIntent) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(copyUnsupportedCode),
		Code:      copyUnsupportedCode,
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    "copy branches require a packet-domain stream point",
		fixes: buildErrorFixes([]string{
			"use goav.From(input).Copy().Branches(...) for packet-preserving planned branches",
			"attach a runtime branch from a packet tap and call .Copy() when packet-domain fanout is needed",
			"omit .Copy() when the branch should deliver decoded frames to goav.Sink(...)",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchIntentDestinationMissingError(stream streamIntent) error {
	selector := streamIntentSelector(stream)
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationMissingCode),
		Code:      destinationMissingCode,
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.Name, string(selector.Type), "stream"),
		Reason:    "branch has no destination",
		fixes: buildErrorFixes([]string{
			"finish the branch with .To(goav.Write(\"web.ivf\", writer)) or .To(goav.Sink(sink))",
			"pass goav.Mux(name, destination) when branches should share one mux group",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchDestinationReferenceMissingError(stream streamIntent, label string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationMissingCode),
		Code:      destinationMissingCode,
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "destination " + label + " is referenced but not defined",
		fixes: buildErrorFixes([]string{
			"pass a named goav.Write(...), goav.URI(...), or goav.Sink(...) destination to the branch .To(...) call",
			"pass goav.Mux(name, destination) when helpers construct matching grouped destinations",
		}),
		cause: errUnsupportedBuild,
	}
}

func transcodeUnsupportedLiveInputError() error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(unsupportedInputCode),
		Code:      unsupportedInputCode,
		Operation: branchCompositionOperation,
		Reason:    "live provider transcode recipes are not supported by the transcode recipe compiler yet",
		fixes: buildErrorFixes([]string{
			"use From(...).Copy().To(...) for packet recording",
			"use From(...).Audio().Decode() or From(...).Video().Decode() for one selected receive path",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchDestinationNameEmptyError(stream streamIntent, index int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationInvalidCode),
		Code:      destinationInvalidCode,
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.Name, string(stream.Select.Type), "stream"),
		Reason:    "branch destinations must be non-empty",
		fields:    errDetails(errNote(fmt.Sprintf("destination index: %d", index))),
		fixes: buildErrorFixes([]string{
			"call .To(goav.Write(\"web.ivf\", writer)) with a non-empty destination name",
			"pass goav.Sink(component.SinkFunc(name, fn)) for sink destinations",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchDestinationDuplicateError(name string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationDuplicateCode),
		Code:      destinationDuplicateCode,
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is defined more than once with different destination handles", name),
		fixes: buildErrorFixes([]string{
			"pass goav.Mux(name, destination) when multiple branches should share one mux group",
			"use distinct destination names when branches should write to different destinations",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchIntentDuplicateError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamDuplicateCode),
		Code:      streamDuplicateCode,
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("branch name %q is defined more than once", name),
		fields:    errDetails(errNote(fmt.Sprintf("first branch index: %d", firstIndex)), errNote(fmt.Sprintf("second branch index: %d", secondIndex))),
		fixes: buildErrorFixes([]string{
			"use unique names such as .Video(\"720p\") and .Video(\"360p\")",
			"route one branch to multiple destinations by calling .To(destination, otherDestination)",
			"route different branches to the same destination with goav.Mux(name, destination)",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchIntentNameMissingError(index int, stream streamIntent) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamNameMissingCode),
		Code:      streamNameMissingCode,
		Operation: branchCompositionOperation,
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "branches need stable names",
		fields:    errDetails(errNote("media type: " + firstNonEmpty(string(stream.Select.Type), "unknown"))),
		fixes: buildErrorFixes([]string{
			"call .Video(\"720p\") for video branches",
			"call .Audio(\"main\") for audio branches",
			"use branch names as handles for graph inspection and destination planning",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateBranchRecipeDestinations(stream recipeir.Stream) error {
	streamIntent := streamIntentFromRecipeIR(stream)
	seen := make(map[string]int, len(stream.Outputs))
	for i, output := range stream.Outputs {
		target := string(output)
		if firstIndex, ok := seen[target]; ok {
			return duplicateBranchDestinationError(streamIntent, target, firstIndex, i)
		}
		seen[target] = i
	}
	return nil
}

func duplicateBranchDestinationError(stream streamIntent, target string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(destinationDuplicateCode),
		Code:      destinationDuplicateCode,
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", target),
		fields:    errDetails(errNote(fmt.Sprintf("first destination index: %d", firstIndex)), errNote(fmt.Sprintf("second destination index: %d", secondIndex))),
		fixes: buildErrorFixes([]string{
			"list each destination once in .To(...)",
			"route one branch to multiple destinations with distinct values such as .To(archive, preview)",
			"pass goav.Mux(name, destination) when repeated destination names should form one group",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateBranchRecipeTransforms(stream recipeir.Stream) error {
	transforms := recipeIRStreamTransforms(stream)
	for i := range transforms {
		if err := validateRecipeIRTransform(branchCompositionOperation, branchRecipeStreamName(stream), transforms[i]); err != nil {
			return err
		}
	}
	return nil
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

func streamIntentSelector(stream streamIntent) av.StreamSelector {
	return av.StreamSelector{
		ID:       stream.Select.ID,
		Index:    stream.Select.Index,
		UseIndex: stream.Select.UseIndex,
		Type:     stream.Select.Type,
		Codec:    stream.Select.Codec,
		Name:     stream.Select.Name,
	}
}

func branchIntentName(stream streamIntent) string {
	return firstNonEmpty(stream.Name, string(stream.Select.Type), "stream")
}
