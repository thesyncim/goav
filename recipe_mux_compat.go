package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

type muxCompatibilityIssue struct {
	Code        string
	Output      string
	Format      av.FormatID
	Reason      string
	Details     []string
	Suggestions []string
}

type plannedMuxStream struct {
	Branch string
	Codec  av.CodecID
	Media  av.MediaType
}

func validateMuxCompatibilityPass() recipeCompilePass {
	return recipeCompilePassFunc{name: "validate mux compatibility", fn: func(state *recipeCompileState) error {
		if !state.options.preflightMuxCompatibility {
			return nil
		}
		issues := stateMuxCompatibilityIssues(state)
		if len(issues) == 0 {
			return nil
		}
		return muxCompatibilityBuildError(state.operation, issues[0])
	}}
}

func stateMuxCompatibilityIssues(state *recipeCompileState) []muxCompatibilityIssue {
	if state == nil || !state.specReady || state.specOrigin != graphSpecOriginMediaPlan {
		return nil
	}
	plan := buildMediaPlan(state)
	return muxCompatibilityIssues(plan, state.intent, state.inputProbes, state.branchInputProbe, state.branchInputProbeReady)
}

func resolvedMuxCompatibilityIssues(resolved recipeResolved) []muxCompatibilityIssue {
	return muxCompatibilityIssues(resolved.mediaPlan, resolved.intent, resolved.inputProbes, resolved.branchInputProbe, resolved.branchInputProbeReady)
}

func muxCompatibilityIssues(
	plan mediaPlan,
	intent Intent,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) []muxCompatibilityIssue {
	var issues []muxCompatibilityIssue
	branches := planBranchesByName(plan.Branches)
	for i := range plan.Outputs {
		output := plan.Outputs[i]
		if output.Operation != OpMux || output.Format == "" {
			continue
		}
		streams := muxOutputStreams(output, branches, plan.Branches, intent, inputProbes, transcodeProbe, transcodeProbeReady)
		if issue, ok := checkKnownMuxCompatibility(output, streams); ok {
			issues = append(issues, issue)
		}
	}
	return issues
}

func planBranchesByName(branches []planBranch) map[string]planBranch {
	out := make(map[string]planBranch, len(branches))
	for i := range branches {
		if branches[i].Name == "" {
			continue
		}
		out[branches[i].Name] = branches[i]
	}
	return out
}

func muxOutputStreams(
	output planOutput,
	branchesByName map[string]planBranch,
	branches []planBranch,
	intent Intent,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) []plannedMuxStream {
	streams := make([]plannedMuxStream, 0, len(output.BranchRefs))
	for i := range output.BranchRefs {
		branchName := output.BranchRefs[i]
		branch, ok := branchesByName[branchName]
		if !ok {
			continue
		}
		branchIndex := planBranchIndex(branches, branchName)
		stream, streamOK := planStreamForBranch(intent.Streams, branch, branchIndex)
		streams = append(streams, muxStreamForBranch(branch, branchIndex, stream, streamOK, intent, inputProbes, transcodeProbe, transcodeProbeReady))
	}
	return streams
}

func planBranchIndex(branches []planBranch, name string) int {
	for i := range branches {
		if branches[i].Name == name {
			return i
		}
	}
	return -1
}

func planStreamForBranch(streams []StreamIntent, branch planBranch, index int) (StreamIntent, bool) {
	for i := range streams {
		if reportBranchNameForStream(streams[i], i) == branch.Name {
			return streams[i], true
		}
	}
	if index >= 0 && index < len(streams) {
		return streams[index], true
	}
	return StreamIntent{}, false
}

func muxStreamForBranch(
	branch planBranch,
	branchIndex int,
	stream StreamIntent,
	streamOK bool,
	intent Intent,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) plannedMuxStream {
	out := plannedMuxStream{Branch: firstNonEmpty(branch.Name, fmt.Sprintf("branch-%d", branchIndex))}
	for i := range branch.Operations {
		operation := branch.Operations[i]
		if operation.Kind != OpEncode {
			continue
		}
		if streamOK && stream.Encode.ID != "" {
			out.Codec = stream.Encode.ID
			out.Media = firstNonEmptyMedia(stream.Encode.Type, stream.Encode.Parameters.Type, codecMedia(stream.Encode.ID), stream.Select.Type)
			return out
		}
		out.Codec = av.CodecID(operation.Component)
		out.Media = codecMedia(out.Codec)
		return out
	}
	if codecID, media, ok := copyMuxStreamCodec(branch, stream, streamOK, intent, inputProbes, transcodeProbe, transcodeProbeReady); ok {
		out.Codec = codecID
		out.Media = media
	}
	return out
}

func copyMuxStreamCodec(
	branch planBranch,
	stream StreamIntent,
	streamOK bool,
	intent Intent,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) (av.CodecID, av.MediaType, bool) {
	if streamOK {
		if stream.Select.Codec != "" {
			return stream.Select.Codec, firstNonEmptyMedia(stream.Select.Type, codecMedia(stream.Select.Codec)), true
		}
	}
	if codecID, media, ok := liveMuxInputCodec(intent.Inputs, branch); ok {
		return codecID, media, true
	}
	if transcodeProbeReady {
		if codecID, media, ok := probedMuxInputCodec([]format.ProbeResult{transcodeProbe}, branch); ok {
			return codecID, media, true
		}
	}
	return probedMuxInputCodec(inputProbes, branch)
}

func liveMuxInputCodec(inputs []InputIntent, branch planBranch) (av.CodecID, av.MediaType, bool) {
	for i := range inputs {
		input := inputs[i]
		if !input.Realtime || input.Codec.ID == "" {
			continue
		}
		name := firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i))
		if branch.Input != "" && branch.Input != name {
			continue
		}
		media := firstNonEmptyMedia(input.Codec.Type, input.Codec.Parameters.Type, codecMedia(input.Codec.ID))
		return input.Codec.ID, media, true
	}
	return "", "", false
}

func probedMuxInputCodec(probes []format.ProbeResult, branch planBranch) (av.CodecID, av.MediaType, bool) {
	selector := av.StreamSelector{
		ID:       branch.Stream.ID,
		Index:    branch.Stream.Index,
		UseIndex: branch.Stream.UseIndex,
		Type:     branch.Stream.Type,
		Codec:    branch.Stream.Codec,
		Name:     branch.Stream.Name,
	}
	candidates := make([]av.Stream, 0, len(probes))
	for i := range probes {
		if len(probes[i].Streams) == 0 {
			continue
		}
		selected, err := selectDecodeStream(probes[i].Streams, selector)
		if err != nil || selected.Codec.ID == "" {
			continue
		}
		candidates = append(candidates, selected)
	}
	if len(candidates) != 1 {
		return "", "", false
	}
	stream := candidates[0]
	media := firstNonEmptyMedia(stream.Type, stream.Codec.Type, codecMedia(stream.Codec.ID))
	return stream.Codec.ID, media, true
}

func checkKnownMuxCompatibility(output planOutput, streams []plannedMuxStream) (muxCompatibilityIssue, bool) {
	switch output.Format {
	case av.FormatIVF:
		return checkSingleVideoMuxCompatibility(output, streams, map[av.CodecID]bool{
			av.CodecVP8: true,
			av.CodecVP9: true,
			av.CodecAV1: true,
		}, "IVF outputs support one VP8, VP9, or AV1 video stream")
	case av.FormatAnnexB:
		return checkSingleVideoMuxCompatibility(output, streams, map[av.CodecID]bool{
			av.CodecH264: true,
		}, "Annex B outputs support one H264 video stream")
	default:
		return muxCompatibilityIssue{}, false
	}
}

func checkSingleVideoMuxCompatibility(output planOutput, streams []plannedMuxStream, codecs map[av.CodecID]bool, reason string) (muxCompatibilityIssue, bool) {
	if len(streams) != 1 {
		return newMuxCompatibilityIssue(output, streams, reason), true
	}
	stream := streams[0]
	if stream.Codec == "" && stream.Media == "" {
		return muxCompatibilityIssue{}, false
	}
	if stream.Media != "" && stream.Media != av.MediaVideo {
		return newMuxCompatibilityIssue(output, streams, reason), true
	}
	if stream.Codec != "" && !codecs[stream.Codec] {
		return newMuxCompatibilityIssue(output, streams, reason), true
	}
	return muxCompatibilityIssue{}, false
}

func newMuxCompatibilityIssue(output planOutput, streams []plannedMuxStream, reason string) muxCompatibilityIssue {
	return muxCompatibilityIssue{
		Code:        "output_mux_incompatible",
		Output:      output.Name,
		Format:      output.Format,
		Reason:      reason,
		Details:     muxCompatibilityDetails(output, streams),
		Suggestions: muxCompatibilitySuggestions(output.Format),
	}
}

func muxCompatibilityDetails(output planOutput, streams []plannedMuxStream) []string {
	details := []string{
		"output=" + output.Name,
		"format=" + string(output.Format),
	}
	if len(streams) == 0 {
		return append(details, "branches=0")
	}
	branchNames := make([]string, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		branchNames = append(branchNames, stream.Branch)
		detail := "branch=" + stream.Branch
		if stream.Codec != "" {
			detail += " codec=" + string(stream.Codec)
		}
		if stream.Media != "" {
			detail += " media=" + string(stream.Media)
		}
		details = append(details, detail)
	}
	return append(details, "branches="+strings.Join(branchNames, ","))
}

func muxCompatibilitySuggestions(formatID av.FormatID) []string {
	switch formatID {
	case av.FormatIVF:
		return []string{
			"route exactly one VP8, VP9, or AV1 video branch to each .ivf output",
			"send audio or additional video branches to a separate target",
			"choose a container adapter that supports the planned mux group",
		}
	case av.FormatAnnexB:
		return []string{
			"route exactly one H264 video branch to each Annex B output",
			"send audio or additional video branches to a separate target",
			"choose a container adapter that supports the planned mux group",
		}
	default:
		return []string{"choose a container adapter that supports the planned mux group"}
	}
}

func muxCompatibilityBuildError(operation string, issue muxCompatibilityIssue) error {
	return &BuildError{
		Code:        issue.Code,
		Operation:   operation,
		Node:        issue.Output,
		Reason:      issue.Reason,
		Details:     append([]string(nil), issue.Details...),
		Suggestions: append([]string(nil), issue.Suggestions...),
		Cause:       ErrUnsupportedBuild,
	}
}

func muxCompatibilityDiagnostics(issues []muxCompatibilityIssue) []PlanDiagnostic {
	diagnostics := make([]PlanDiagnostic, 0, len(issues))
	for i := range issues {
		diagnostics = append(diagnostics, PlanDiagnostic{
			Code:        issues[i].Code,
			Node:        issues[i].Output,
			Message:     issues[i].Reason,
			Details:     append([]string(nil), issues[i].Details...),
			Suggestions: append([]string(nil), issues[i].Suggestions...),
		})
	}
	return diagnostics
}

func codecMedia(codecID av.CodecID) av.MediaType {
	switch codecID {
	case av.CodecOpus, av.CodecPCM:
		return av.MediaAudio
	case av.CodecVP8, av.CodecVP9, av.CodecH264, av.CodecAV1:
		return av.MediaVideo
	default:
		return ""
	}
}

func firstNonEmptyMedia(values ...av.MediaType) av.MediaType {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}
