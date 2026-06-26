// Branch composition planning: branchCompositionJob and its intent validation.

package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

type branchCompositionJob struct {
	runtime         *Runtime
	runtimeExplicit bool
	name            string
	input           InputSpec
	streams         []streamBuild
	outputs         []namedDestinationSpec
	streamRules     []streamRule
	err             error

	fromBranchSplit bool
}

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

func (j *branchCompositionJob) plan() intent {
	intent := intent{
		Name:   firstNonEmpty(j.name, "branch-composition"),
		Inputs: []inputIntent{j.input.intent()},
	}
	if j.runtime != nil {
		intent.Policies.Realtime = j.runtime.realtime
	}
	for i := range j.streams {
		intent.Streams = append(intent.Streams, branchStreamIntent(j.streams[i]))
	}
	for i := range j.outputs {
		intent.Destinations = append(intent.Destinations, j.outputs[i].output.intentWithName(j.outputs[i].name))
	}
	return intent
}

func (j *branchCompositionJob) composePlan() (branchComposePlan, error) {
	if j == nil {
		return branchComposePlan{}, nil
	}
	return planBranchCompositionRecipe(j.plan(), j.input, j.outputs)
}

func planBranchCompositionRecipe(intent intent, input InputSpec, namedOutputs []namedDestinationSpec) (branchComposePlan, error) {
	streams := intent.Streams
	outputs, outputOrder := branchDestinationAttachmentSet(namedOutputs)

	branches := make([]branchComposeBranch, 0, len(streams))
	outputBranches := make(map[string][]string, len(outputs))
	if len(streams) == 0 {
		return branchComposePlan{}, branchStreamMissingError()
	}
	for i := range streams {
		stream := streams[i]
		branchName := stream.Name
		selector := streamIntentSelector(stream)
		operations := cloneOperationSpecs(stream.Operations)
		sharedOperations, privateOperations := splitOperationSpecsByShared(operations)
		encode := chainEncodeSpec(operations)
		branch := branchComposeBranch{
			Name:              branchName,
			Selector:          selector,
			Input:             stream.Select.Input,
			Copy:              encode.Copy,
			Operations:        cloneOperationSpecs(operations),
			SharedOperations:  sharedOperations,
			PrivateOperations: privateOperations,
			DecodeConfig:      cloneCodecSpec(chainDecodeCodec(operations)),
			CodecChange:       stream.CodecChange,
			Encode:            encodeConfigFromSpec(encode),
			Labels:            append([]string(nil), stream.Destinations...),
		}
		for _, label := range stream.Destinations {
			outputBranches[label] = append(outputBranches[label], branchName)
		}
		if err := validateBranchTransforms(stream); err != nil {
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

func validateBranchCompositionIntentShape(operation string, intent intent) error {
	if len(intent.Inputs) == 0 {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.InputMissing),
			Code:      errcode.InputMissing,
			Operation: operation,
			Reason:    "no input is configured",
			fixes: buildErrorFixes([]string{
				"start the recipe from an input: goav.From(goav.FileInput(\"in.webm\", reader))",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	if len(intent.Inputs) > 1 {
		return &BuildError{
			Family:    errcode.FamilyForCode(errcode.InputCountUnsupported),
			Code:      errcode.InputCountUnsupported,
			Operation: operation,
			Reason:    "transcode recipes currently take one input",
			fields: buildErrorFields([]string{
				fmt.Sprintf("inputs=%d", len(intent.Inputs)),
			}),
			fixes: buildErrorFixes([]string{
				"use one goav.From(input) source per composed job",
				"use the expert graph API when multiple sources must be composed manually",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	streams := intent.Streams
	if len(streams) == 0 {
		return branchStreamMissingError()
	}
	branchNames := make(map[string]int, len(streams))
	for i := range streams {
		stream := streams[i]
		if err := validateBranchIntentShape(stream, i); err != nil {
			return err
		}
		if err := validateBranchTransforms(stream); err != nil {
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

func validateBranchIntentShape(stream streamIntent, index int) error {
	selector := streamIntentSelector(stream)
	if stream.Name == "" {
		return branchIntentNameMissingError(index, stream)
	}
	if err := validateRecipeStreamSelector(branchCompositionOperation, branchIntentName(stream), selector); err != nil {
		return err
	}
	encode := chainEncodeSpec(stream.Operations)
	if codecIntentSet(encode) {
		if encode.Copy && chainHasDecode(stream.Operations) {
			return branchCopyUnsupportedError(stream)
		}
		if encode.Copy && len(streamIntentTransformSpecs(stream)) != 0 {
			return branchPacketTransformUnsupportedError(stream)
		}
		if err := validateRecipeEncode(encode, branchCompositionOperation, stream.Name); err != nil {
			return err
		}
	}
	if len(stream.Destinations) == 0 {
		return branchIntentDestinationMissingError(stream)
	}
	return validateBranchDestinations(stream)
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

func validateBranchDestinationKinds(intent intent, namedOutputs []namedDestinationSpec) error {
	outputs := branchDestinationSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		hasMuxDestination := false
		for _, label := range stream.Destinations {
			output, ok := outputs[label]
			if !ok {
				continue
			}
			if output.sink == nil {
				hasMuxDestination = true
				break
			}
		}
		if hasMuxDestination && !codecIntentSet(chainEncodeSpec(stream.Operations)) {
			return branchEncodeMissingError(stream)
		}
	}
	return nil
}

func validateBranchDestinationBindings(intent intent, namedOutputs []namedDestinationSpec) error {
	outputs := branchDestinationLabelSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		for _, label := range stream.Destinations {
			if _, ok := outputs[label]; ok {
				continue
			}
			return branchDestinationReferenceMissingError(stream, label)
		}
	}
	return nil
}

func branchDestinationSet(namedOutputs []namedDestinationSpec) map[string]destinationSpec {
	outputs := make(map[string]destinationSpec, len(namedOutputs))
	for i := range namedOutputs {
		outputs[namedOutputs[i].name] = namedOutputs[i].output
	}
	return outputs
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

func branchDestinationLabelSet(namedOutputs []namedDestinationSpec) map[string]struct{} {
	outputs := make(map[string]struct{}, len(namedOutputs))
	for i := range namedOutputs {
		outputs[namedOutputs[i].name] = struct{}{}
	}
	return outputs
}

func branchStreamMissingError() error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.StreamMissing),
		Code:      errcode.StreamMissing,
		Operation: branchCompositionOperation,
		Reason:    "no audio or video branches are configured",
		fixes: buildErrorFixes([]string{
			"add a video branch such as .Video(\"720p\").Resize(...).Encode(codec.VP9(...)).To(...)",
			"add an audio branch such as .Audio(\"main\").Resample(...).Encode(codec.Opus(...)).To(...)",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchEncodeMissingError(stream streamIntent) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.EncodeMissing),
		Code:      errcode.EncodeMissing,
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "branch needs an encoder before writing to a muxed destination",
		fields: buildErrorFields([]string{
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(stream.Select.Type)).String(),
			"actual_shape=" + shape.Frame(stream.Select.Type).String(),
		}),
		fixes: buildErrorFixes([]string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(...)",
			"route raw frames to goav.Sink(...) when the branch should stay decoded",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchCopyUnsupportedError(stream streamIntent) error {
	return &BuildError{
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
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentDestinationMissingError(stream streamIntent) error {
	selector := streamIntentSelector(stream)
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationMissingCode),
		Code:      destinationMissingCode,
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.Name, string(selector.Type), "stream"),
		Reason:    "branch has no destination",
		fixes: buildErrorFixes([]string{
			"finish the branch with .To(goav.Write(\"web.ivf\", writer)) or .To(goav.Sink(sink))",
			"pass goav.Mux(name, destination) when branches should share one mux group",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationReferenceMissingError(stream streamIntent, label string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationMissingCode),
		Code:      destinationMissingCode,
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "destination " + label + " is referenced but not defined",
		fixes: buildErrorFixes([]string{
			"pass a named goav.Write(...), goav.URI(...), or goav.Sink(...) destination to the branch .To(...) call",
			"pass goav.Mux(name, destination) when helpers construct matching grouped destinations",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeUnsupportedLiveInputError() error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.UnsupportedInput),
		Code:      errcode.UnsupportedInput,
		Operation: branchCompositionOperation,
		Reason:    "live provider transcode recipes are not supported by the transcode recipe compiler yet",
		fixes: buildErrorFixes([]string{
			"use From(...).Copy().To(...) for packet recording",
			"use From(...).Audio().Decode() or From(...).Video().Decode() for one selected receive path",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationNameEmptyError(stream streamBuild, index int) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationInvalidCode),
		Code:      destinationInvalidCode,
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.name, string(stream.selector.Type), "stream"),
		Reason:    "branch destinations must be non-empty",
		fields: buildErrorFields([]string{
			fmt.Sprintf("destination index: %d", index),
		}),
		fixes: buildErrorFixes([]string{
			"call .To(goav.Write(\"web.ivf\", writer)) with a non-empty destination name",
			"pass goav.Sink(component.SinkFunc(name, fn)) for sink destinations",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationDuplicateError(name string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationDuplicateCode),
		Code:      destinationDuplicateCode,
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is defined more than once with different destination handles", name),
		fixes: buildErrorFixes([]string{
			"pass goav.Mux(name, destination) when multiple branches should share one mux group",
			"use distinct destination names when branches should write to different destinations",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentDuplicateError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.StreamDuplicate),
		Code:      errcode.StreamDuplicate,
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("branch name %q is defined more than once", name),
		fields: buildErrorFields([]string{
			fmt.Sprintf("first branch index: %d", firstIndex),
			fmt.Sprintf("second branch index: %d", secondIndex),
		}),
		fixes: buildErrorFixes([]string{
			"use unique names such as .Video(\"720p\") and .Video(\"360p\")",
			"route one branch to multiple destinations by calling .To(destination, otherDestination)",
			"route different branches to the same destination with goav.Mux(name, destination)",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentNameMissingError(index int, stream streamIntent) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.StreamNameMissing),
		Code:      errcode.StreamNameMissing,
		Operation: branchCompositionOperation,
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "branches need stable names",
		fields: buildErrorFields([]string{
			"media type: " + firstNonEmpty(string(stream.Select.Type), "unknown"),
		}),
		fixes: buildErrorFixes([]string{
			"call .Video(\"720p\") for video branches",
			"call .Audio(\"main\") for audio branches",
			"use branch names as handles for graph inspection and destination planning",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func validateBranchDestinations(stream streamIntent) error {
	seen := make(map[string]int, len(stream.Destinations))
	for i, target := range stream.Destinations {
		if firstIndex, ok := seen[target]; ok {
			return duplicateBranchDestinationError(stream, target, firstIndex, i)
		}
		seen[target] = i
	}
	return nil
}

func duplicateBranchDestinationError(stream streamIntent, target string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationDuplicateCode),
		Code:      destinationDuplicateCode,
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", target),
		fields: buildErrorFields([]string{
			fmt.Sprintf("first destination index: %d", firstIndex),
			fmt.Sprintf("second destination index: %d", secondIndex),
		}),
		fixes: buildErrorFixes([]string{
			"list each destination once in .To(...)",
			"route one branch to multiple destinations with distinct values such as .To(archive, preview)",
			"pass goav.Mux(name, destination) when repeated destination names should form one group",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func validateBranchTransforms(stream streamIntent) error {
	transforms := streamIntentTransformSpecs(stream)
	for i := range transforms {
		transform := transforms[i]
		if err := validateTransformSpec(branchCompositionOperation, branchIntentName(stream), transform); err != nil {
			return err
		}
		switch {
		case transform.resize != nil && transform.resample != nil:
			return &BuildError{
				Family:    errcode.FamilyForCode(errcode.TransformInvalid),
				Code:      errcode.TransformInvalid,
				Operation: branchCompositionOperation,
				Node:      branchIntentName(stream),
				Reason:    "one transform cannot be both resize and resample",
				fixes:     buildErrorFixes([]string{"declare two separate steps instead: .Resize(width, height).Resample(rate, channels)"}),
				Cause:     ErrUnsupportedBuild,
			}
		case transform.resize != nil, transform.resample != nil:
			continue
		default:
			return &BuildError{
				Family:    errcode.FamilyForCode(errcode.TransformInvalid),
				Code:      errcode.TransformInvalid,
				Operation: branchCompositionOperation,
				Node:      branchIntentName(stream),
				Reason:    "empty stream transform",
				fixes: buildErrorFixes([]string{
					"call .Resize(width, height) on video branches",
					"call .Resample(sampleRate, channels) on audio branches",
				}),
				Cause: ErrUnsupportedBuild,
			}
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
