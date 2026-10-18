// Branch composition planning: recipe IR validation and graph handoff.

package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

const branchCompositionOperation = "build branch composition"

func planBranchCompositionSnapshot(recipe recipeir.Recipe, attachments []branchDestinationAttachment) (branchComposePlan, error) {
	streams := recipe.Streams
	outputs, outputOrder := branchDestinationAttachmentSet(attachments)

	branches := make([]branchComposeBranch, 0, len(streams))
	outputBranches := make(map[string][]string, len(outputs))
	if len(streams) == 0 {
		return branchComposePlan{}, branchStreamMissingError()
	}
	for i := range streams {
		stream := streams[i]
		branchName := stream.Name
		selector := recipeIRStreamSelector(stream)
		operations := recipeIROperationFacts(stream.Operations)
		encode := operations.Encode()
		labels := recipeIROutputRefsToStrings(stream.Outputs)
		branch := branchComposeBranch{
			Name:              branchName,
			Selector:          selector,
			Input:             stream.Selector.Input,
			Copy:              encode.Copy,
			SharedOperations:  operations.SharedFacts(),
			PrivateOperations: operations.PrivateFacts(),
			DecodeConfig:      operations.Decode(),
			Encode:            encodeConfigFromSpec(encode),
			Labels:            labels,
			Buffer:            stream.Buffer,
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
		attachment := outputs[name]
		facts := attachment.details
		planTarget := branchComposeTarget{
			Name:        facts.routingName,
			Destination: branchDestinationAttachmentDestination(attachment),
			Target:      branchDestinationAttachmentOutput(attachment),
			Sink:        branchDestinationAttachmentSink(attachment),
			Format:      facts.requestedContainer,
			Branches:    append([]string(nil), outputBranches[name]...),
		}
		if facts.resolvedContainer != "" {
			planTarget = resolveBranchComposeTargetFormat(planTarget, facts.resolvedContainer)
		}
		planDestinations = append(planDestinations, planTarget)
	}
	return branchComposePlan{
		Name:         "branch-composition",
		Branches:     branches,
		Destinations: planDestinations,
	}, nil
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
			"call " + encodeHint(stream.Select.Type) + " before .To(...)",
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
			"fork in.Video() before .Decode() for packet-preserving planned forks",
			"attach a runtime copy from a packet tap when packet-domain fanout is needed: task.Attach(ctx, task.Tap(name).To(dst))",
			"fork after .Decode() when the fork should deliver decoded frames to goav.Sink(...)",
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
			"pass the same destination value to every branch that should share one mux",
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
			"pass the same destination value to every branch that should share one mux",
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
			"route different branches to one output by passing them the same destination value",
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
			"reuse one destination value instead of constructing the same name twice",
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
