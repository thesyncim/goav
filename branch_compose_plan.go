// Branch composition planning: branchCompositionJob and its intent validation.

package goav

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/info"
	"github.com/thesyncim/goav/shape"
)

type branchCompositionJob struct {
	runtime     Runtime
	name        string
	input       InputSpec
	streams     []streamBuild
	outputs     []namedDestinationSpec
	streamRules []streamRule
	err         error

	fromBranchSplit bool
}

type namedDestinationSpec struct {
	name   string
	output destinationSpec
}

func destinationIdentity(destination namedDestinationSpec) string {
	output := destination.output
	sinkName := ""
	sinkAddr := ""
	if output.sink != nil {
		sinkName = output.sink.Name()
		sinkAddr = fmt.Sprintf("%p", output.sink)
	}
	return strings.Join([]string{
		destination.name,
		strconv.FormatUint(destination.output.id, 10),
		output.label(""),
		sinkName,
		sinkAddr,
		output.output.Name,
		output.output.URI,
		string(output.output.Protocol),
		output.output.MIMEType,
		string(output.format),
		string(output.resolvedFormat),
	}, "\x00")
}

const branchCompositionOperation = "build branch composition"

func (j *branchCompositionJob) plan() Intent {
	intent := Intent{
		Name:   firstNonEmpty(j.name, "branch-composition"),
		Inputs: []inputIntent{j.input.intent()},
	}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
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
	return planBranchCompositionRecipe(j.plan(), j.input, j.outputs, j.streams)
}

func planBranchCompositionRecipe(intent Intent, input InputSpec, namedOutputs []namedDestinationSpec, branchBuilds []streamBuild) (branchComposePlan, error) {
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
		var sharedOperations []OperationSpec
		var privateOperations []OperationSpec
		if i < len(branchBuilds) {
			branchBuild := branchBuilds[i]
			operations = streamBuildOperationSpecs(branchBuild)
			sharedOperations = cloneOperationSpecs(branchBuild.sharedOps)
			privateOperations = cloneOperationSpecs(branchBuild.privateOps)
		} else {
			sharedOperations, privateOperations = splitOperationSpecsByShared(operations)
		}
		branch := branchComposeBranch{
			Name:              branchName,
			Selector:          selector,
			Input:             stream.Select.Input,
			Copy:              stream.Encode.Copy,
			Operations:        cloneOperationSpecs(operations),
			SharedOperations:  sharedOperations,
			PrivateOperations: privateOperations,
			DecodeConfig:      cloneCodecSpec(stream.DecodeCodec),
			CodecChange:       stream.CodecChange,
			Encode:            encodeConfigFromSpec(stream.Encode),
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

func branchComposePlanReady(plan branchComposePlan) bool {
	return len(plan.Branches) != 0 || len(plan.Destinations) != 0
}

func splitOperationSpecsByShared(operations []OperationSpec) ([]OperationSpec, []OperationSpec) {
	if len(operations) == 0 {
		return nil, nil
	}
	shared := make([]OperationSpec, 0)
	private := make([]OperationSpec, 0, len(operations))
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

func chainStepsFromChainOperations(operations []OperationSpec) []chainStep {
	if len(operations) == 0 {
		return nil
	}
	steps := make([]chainStep, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case info.OpStage:
			if operation.Stage != nil {
				steps = append(steps, chainStep{stage: operation.Stage})
			}
		case info.OpShape:
			if !mediaShapeEmpty(operation.Shape) {
				steps = append(steps, chainStep{shape: operation.Shape})
			}
		case info.OpTransform:
			if operation.Transform.Resize != nil || operation.Transform.Resample != nil {
				steps = append(steps, chainStep{transform: cloneTransformSpec(operation.Transform)})
			}
		case info.OpTap:
			if operation.Tap.Name != "" && operation.Tap.Domain != shape.DomainPacket {
				steps = append(steps, chainStep{tap: operation.Tap.Name, tapDomain: operation.Tap.Domain})
			}
		}
	}
	return steps
}

func validateBranchCompositionIntentShape(operation string, intent Intent) error {
	if len(intent.Inputs) == 0 {
		return &BuildError{Code: "input_missing", Operation: operation, Reason: "no input is configured", Cause: ErrUnsupportedBuild}
	}
	if len(intent.Inputs) > 1 {
		return &BuildError{
			Code:      "input_count_unsupported",
			Operation: operation,
			Reason:    "transcode recipes currently take one input",
			Details: []string{
				fmt.Sprintf("inputs=%d", len(intent.Inputs)),
			},
			Suggestions: []string{
				"use one goav.From(input) source per composed job",
				"use the expert graph API when multiple sources must be composed manually",
			},
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
	if codecIntentSet(stream.Encode) {
		if stream.Encode.Copy && stream.Decode {
			return branchCopyUnsupportedError(stream)
		}
		if stream.Encode.Copy && len(streamIntentTransformSpecs(stream)) != 0 {
			return branchPacketTransformUnsupportedError(stream)
		}
		if err := validateRecipeEncode(stream.Encode, branchCompositionOperation, stream.Name); err != nil {
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

func validateBranchDestinationKinds(intent Intent, namedOutputs []namedDestinationSpec) error {
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
		if hasMuxDestination && !codecIntentSet(stream.Encode) {
			return branchEncodeMissingError(stream)
		}
	}
	return nil
}

func validateBranchDestinationBindings(intent Intent, namedOutputs []namedDestinationSpec) error {
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
		Code:      "stream_missing",
		Operation: branchCompositionOperation,
		Reason:    "no audio or video branches are configured",
		Suggestions: []string{
			"add a video branch such as .Video(\"720p\").Resize(...).Encode(codec.VP9(...)).To(...)",
			"add an audio branch such as .Audio(\"main\").Resample(...).Encode(codec.Opus(...)).To(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchEncodeMissingError(stream streamIntent) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "branch needs an encoder before writing to a muxed destination",
		Details: []string{
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(stream.Select.Type)).String(),
			"actual_shape=" + shape.Frame(stream.Select.Type).String(),
		},
		Suggestions: []string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(...)",
			"route raw frames to goav.Sink(...) when the branch should stay decoded",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchCopyUnsupportedError(stream streamIntent) error {
	return &BuildError{
		Code:      "copy_unsupported",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    "copy branches require a packet-domain stream point",
		Suggestions: []string{
			"use goav.From(input).Copy().Branches(...) for packet-preserving planned branches",
			"attach a runtime branch from a packet tap and call .Copy() when packet-domain fanout is needed",
			"omit .Copy() when the branch should deliver decoded frames to goav.Sink(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentDestinationMissingError(stream streamIntent) error {
	selector := streamIntentSelector(stream)
	return &BuildError{
		Code:      "destination_missing",
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.Name, string(selector.Type), "stream"),
		Reason:    "branch has no destination",
		Suggestions: []string{
			"finish the branch with .To(goav.File(\"web.ivf\", writer)) or .To(goav.Sink(sink))",
			"reuse the same destination value from multiple branches when they should share one mux group",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationReferenceMissingError(stream streamIntent, label string) error {
	return &BuildError{
		Code:      "destination_missing",
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "destination " + label + " is referenced but not defined",
		Suggestions: []string{
			"pass a named goav.File(...), goav.URIOut(...), or goav.Sink(...) destination to the branch .To(...) call",
			"reuse destination values instead of repeating string destination names",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeUnsupportedLiveInputError() error {
	return &BuildError{
		Code:      "unsupported_input",
		Operation: branchCompositionOperation,
		Reason:    "live provider transcode recipes are not supported by the transcode recipe compiler yet",
		Suggestions: []string{
			"use From(...).Copy().To(...) for packet recording",
			"use From(...).Audio().Decode() or From(...).Video().Decode() for one selected receive path",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationNameEmptyError(stream streamBuild, index int) error {
	return &BuildError{
		Code:      "destination_invalid",
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.name, string(stream.selector.Type), "stream"),
		Reason:    "branch destinations must be non-empty",
		Details: []string{
			fmt.Sprintf("destination index: %d", index),
		},
		Suggestions: []string{
			"call .To(goav.File(\"web.ivf\", writer)) with a non-empty destination name",
			"pass goav.Sink(goav.SinkFunc(name, fn)) for sink destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationDuplicateError(name string) error {
	return &BuildError{
		Code:      "destination_duplicate",
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is defined more than once with different destination handles", name),
		Suggestions: []string{
			"reuse the same destination value when multiple branches should share one mux group",
			"use distinct destination names when branches should write to different destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentDuplicateError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "stream_duplicate",
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("branch name %q is defined more than once", name),
		Details: []string{
			fmt.Sprintf("first branch index: %d", firstIndex),
			fmt.Sprintf("second branch index: %d", secondIndex),
		},
		Suggestions: []string{
			"use unique names such as .Video(\"720p\") and .Video(\"360p\")",
			"route one branch to multiple destinations by calling .To(destination, otherDestination)",
			"route different branches to the same destination by reusing the destination value",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentNameMissingError(index int, stream streamIntent) error {
	return &BuildError{
		Code:      "stream_name_missing",
		Operation: branchCompositionOperation,
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "branches need stable names",
		Details: []string{
			"media type: " + firstNonEmpty(string(stream.Select.Type), "unknown"),
		},
		Suggestions: []string{
			"call .Video(\"720p\") for video branches",
			"call .Audio(\"main\") for audio branches",
			"use branch names as handles for graph inspection and destination planning",
		},
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
		Code:      "destination_duplicate",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", target),
		Details: []string{
			fmt.Sprintf("first destination index: %d", firstIndex),
			fmt.Sprintf("second destination index: %d", secondIndex),
		},
		Suggestions: []string{
			"list each destination once in .To(...)",
			"route one branch to multiple destinations with distinct values such as .To(archive, preview)",
			"reuse destination values instead of repeating destination names",
		},
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
		case transform.Resize != nil && transform.Resample != nil:
			return &BuildError{
				Code:      "transform_invalid",
				Operation: branchCompositionOperation,
				Node:      branchIntentName(stream),
				Reason:    "one transform cannot be both resize and resample",
				Cause:     ErrUnsupportedBuild,
			}
		case transform.Resize != nil:
			if stream.Select.Type == av.MediaAudio {
				return branchTransformMediaError(stream, "resize", av.MediaVideo, stream.Select.Type)
			}
		case transform.Resample != nil:
			if stream.Select.Type == av.MediaVideo {
				return branchTransformMediaError(stream, "resample", av.MediaAudio, stream.Select.Type)
			}
		default:
			return &BuildError{
				Code:      "transform_invalid",
				Operation: branchCompositionOperation,
				Node:      branchIntentName(stream),
				Reason:    "empty stream transform",
				Suggestions: []string{
					"call .Resize(width, height) on video branches",
					"call .Resample(sampleRate, channels) on audio branches",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
	}
	return nil
}

func streamIntentTransformSpecs(stream streamIntent) []TransformSpec {
	return transformSpecsFromOperationSpecs(stream.Operations)
}

func transformSpecsFromOperationSpecs(operations []OperationSpec) []TransformSpec {
	if len(operations) == 0 {
		return nil
	}
	transforms := make([]TransformSpec, 0)
	for i := range operations {
		if operations[i].Kind != info.OpTransform {
			continue
		}
		transform := cloneTransformSpec(operations[i].Transform)
		transforms = append(transforms, transform)
	}
	return transforms
}

func branchTransformMediaError(stream streamIntent, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    transform + " applies to " + string(expected) + " branches",
		Details: []string{
			"expected_shape=" + shape.Frame(expected).String(),
			"actual_shape=" + shape.Frame(actual).String(),
		},
		Suggestions: []string{
			"use .Video(...).Resize(...) for video ladder branches",
			"use .Audio(...).Resample(...) for audio branches",
		},
		Cause: ErrUnsupportedBuild,
	}
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
