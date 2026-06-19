package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/plan"
)

type muxCompatibilityIssue struct {
	Code        errcode.Code
	Destination string
	Format      av.FormatID
	Reason      string
	Details     []string
	Suggestions []string
}

type plannedMuxStream struct {
	Branch   string
	Codec    av.CodecID
	Media    av.MediaType
	TimeBase av.TimeBase
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
	if state == nil || !state.specReady || state.specOrigin != graphSpecOriginGraphPlan {
		return nil
	}
	return muxCompatibilityIssues(state.graphPlan.work, state.inputProbes, state.branchInputProbe, state.branchInputProbeReady, state.runtime)
}

func resolvedMuxCompatibilityIssues(resolved recipeResolved) []muxCompatibilityIssue {
	return muxCompatibilityIssues(resolved.workIR(), resolved.inputProbes, resolved.branchInputProbe, resolved.branchInputProbeReady, resolved.runtime)
}

func muxCompatibilityIssues(
	work workPlan,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
	rt Runtime,
) []muxCompatibilityIssue {
	var issues []muxCompatibilityIssue
	branches := workBranchesByName(work.Branches)
	operations := workOperationsByID(work.Operations)
	for i := range work.Destinations {
		output := work.Destinations[i]
		if output.Operation != plan.OpMux || output.Format == "" {
			continue
		}
		streams := muxOutputStreams(output, branches, operations, work.Inputs, inputProbes, transcodeProbe, transcodeProbeReady)
		if issue, ok := checkKnownMuxCompatibility(output, streams, rt); ok {
			issues = append(issues, issue)
		}
	}
	return issues
}

func muxOutputStreams(
	output workDestination,
	branchesByName map[string]workBranch,
	operations map[string]workOperation,
	inputs []workInput,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) []plannedMuxStream {
	streams := make([]plannedMuxStream, 0, len(output.Branches))
	for i := range output.Branches {
		branchName := output.Branches[i]
		branch, ok := branchesByName[branchName]
		if !ok {
			continue
		}
		streams = append(streams, muxStreamForBranch(branch, i, operations, inputs, inputProbes, transcodeProbe, transcodeProbeReady))
	}
	return streams
}

func muxStreamForBranch(
	branch workBranch,
	branchIndex int,
	operations map[string]workOperation,
	inputs []workInput,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) plannedMuxStream {
	out := plannedMuxStream{Branch: firstNonEmpty(branch.Name, fmt.Sprintf("branch-%d", branchIndex))}
	for _, id := range branch.Operations {
		operation, ok := operations[id]
		if !ok || operation.Kind != plan.OpEncode {
			continue
		}
		if encode := operation.Codec; encode.ID != "" {
			out.Codec = encode.ID
			out.Media = firstNonEmptyMedia(encode.Type, encode.Parameters.Type, codecMedia(encode.ID), branch.Stream.Type)
			out.TimeBase = codecSpecMuxTimeBase(encode)
			return out
		}
		out.Codec = av.CodecID(operation.Component)
		out.Media = codecMedia(out.Codec)
		return out
	}
	if codecID, media, base, ok := copyMuxStreamFacts(branch, inputs, inputProbes, transcodeProbe, transcodeProbeReady); ok {
		out.Codec = codecID
		out.Media = media
		out.TimeBase = base
	}
	return out
}

func copyMuxStreamFacts(
	branch workBranch,
	inputs []workInput,
	inputProbes []format.ProbeResult,
	transcodeProbe format.ProbeResult,
	transcodeProbeReady bool,
) (av.CodecID, av.MediaType, av.TimeBase, bool) {
	if branch.Stream.Codec != "" {
		return branch.Stream.Codec, firstNonEmptyMedia(branch.Stream.Type, codecMedia(branch.Stream.Codec)), av.TimeBase{}, true
	}
	if codecID, media, base, ok := liveMuxInputFacts(inputs, branch); ok {
		return codecID, media, base, true
	}
	if transcodeProbeReady {
		if codecID, media, base, ok := probedMuxInputFacts([]format.ProbeResult{transcodeProbe}, branch); ok {
			return codecID, media, base, true
		}
	}
	return probedMuxInputFacts(inputProbes, branch)
}

func liveMuxInputFacts(inputs []workInput, branch workBranch) (av.CodecID, av.MediaType, av.TimeBase, bool) {
	for i := range inputs {
		input := inputs[i]
		if !input.Realtime || input.Codec == "" {
			continue
		}
		name := firstNonEmpty(input.Name, fmt.Sprintf("input-%d", i))
		if branch.Input != "" && branch.Input != name {
			continue
		}
		spec := input.CodecSpec
		media := firstNonEmptyMedia(spec.Type, spec.Parameters.Type, codecMedia(input.Codec))
		return input.Codec, media, codecSpecMuxTimeBase(spec), true
	}
	return "", "", av.TimeBase{}, false
}

func probedMuxInputFacts(probes []format.ProbeResult, branch workBranch) (av.CodecID, av.MediaType, av.TimeBase, bool) {
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
		return "", "", av.TimeBase{}, false
	}
	stream := candidates[0]
	media := firstNonEmptyMedia(stream.Type, stream.Codec.Type, codecMedia(stream.Codec.ID))
	return stream.Codec.ID, media, muxStreamTimeBase(stream), true
}

func codecSpecMuxTimeBase(spec codec.CodecSpec) av.TimeBase {
	if spec.Parameters.ClockRate != 0 {
		return av.RTPTimeBase(spec.Parameters.ClockRate)
	}
	if spec.Parameters.SampleRate > 0 {
		return av.TimeBase{Num: 1, Den: int64(spec.Parameters.SampleRate)}
	}
	return av.TimeBase{}
}

func muxStreamTimeBase(stream av.Stream) av.TimeBase {
	if stream.TimeBase != (av.TimeBase{}) {
		return stream.TimeBase
	}
	if stream.Codec.ClockRate != 0 {
		return av.RTPTimeBase(stream.Codec.ClockRate)
	}
	if stream.Codec.SampleRate > 0 {
		return av.TimeBase{Num: 1, Den: int64(stream.Codec.SampleRate)}
	}
	return av.TimeBase{}
}

func checkKnownMuxCompatibility(output workDestination, streams []plannedMuxStream, rt Runtime) (muxCompatibilityIssue, bool) {
	if issue, ok := checkMuxTimebaseCompatibility(output, streams); ok {
		return issue, true
	}
	if desc, ok := muxerDescriptorForRuntime(rt, output.Format); ok {
		if issue, checked := checkDescriptorMuxCompatibility(output, streams, desc); checked {
			return issue, true
		}
	}
	switch output.Format {
	case av.FormatIVF:
		return checkSingleVideoMuxCompatibility(output, streams, map[av.CodecID]bool{
			av.CodecVP8: true,
			av.CodecVP9: true,
			av.CodecAV1: true,
		}, "IVF destinations support one VP8, VP9, or AV1 video stream")
	case av.FormatAnnexB:
		return checkSingleVideoMuxCompatibility(output, streams, map[av.CodecID]bool{
			av.CodecH264: true,
		}, "Annex B destinations support one H264 video stream")
	default:
		return muxCompatibilityIssue{}, false
	}
}

func muxerDescriptorForRuntime(rt Runtime, formatID av.FormatID) (format.Descriptor, bool) {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return format.Descriptor{}, false
	}
	desc, err := standard.formats.MuxerDescriptor(formatID)
	if err != nil {
		return format.Descriptor{}, false
	}
	if desc.Format == "" {
		desc.Format = formatID
	}
	return desc, true
}

func checkDescriptorMuxCompatibility(output workDestination, streams []plannedMuxStream, desc format.Descriptor) (muxCompatibilityIssue, bool) {
	if desc.MinStreams > 0 && len(streams) < desc.MinStreams {
		return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
	}
	if desc.MaxStreams > 0 && len(streams) > desc.MaxStreams {
		return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
	}
	if len(desc.Media) == 0 && len(desc.Codecs) == 0 {
		return muxCompatibilityIssue{}, false
	}
	for i := range streams {
		stream := streams[i]
		if stream.Media != "" && len(desc.Media) != 0 && !mediaAllowed(desc.Media, stream.Media) {
			return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
		}
		if stream.Codec != "" && len(desc.Codecs) != 0 && !codecAllowed(desc.Codecs, stream.Codec) {
			return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
		}
	}
	return muxCompatibilityIssue{}, false
}

func checkMuxTimebaseCompatibility(output workDestination, streams []plannedMuxStream) (muxCompatibilityIssue, bool) {
	for i := range streams {
		base := streams[i].TimeBase
		if base == (av.TimeBase{}) {
			continue
		}
		if !base.Valid() {
			return newMuxCompatibilityIssue(output, streams, "mux destination streams must declare a valid timebase when timebase facts are present"), true
		}
	}
	return muxCompatibilityIssue{}, false
}

func descriptorMuxReason(formatID av.FormatID, desc format.Descriptor) string {
	if desc.Metadata != nil && desc.Metadata["summary"] != "" {
		return desc.Metadata["summary"]
	}
	var parts []string
	if desc.MaxStreams > 0 {
		if desc.MinStreams == desc.MaxStreams {
			parts = append(parts, fmt.Sprintf("%d stream(s)", desc.MaxStreams))
		} else {
			parts = append(parts, fmt.Sprintf("up to %d stream(s)", desc.MaxStreams))
		}
	}
	if len(desc.Media) != 0 {
		parts = append(parts, "media="+joinMediaTypes(desc.Media))
	}
	if len(desc.Codecs) != 0 {
		parts = append(parts, "codecs="+joinCodecIDs(desc.Codecs))
	}
	if len(parts) == 0 {
		return string(formatID) + " destination rejected the planned mux group"
	}
	return string(formatID) + " destinations support " + strings.Join(parts, ", ")
}

func mediaAllowed(allowed []av.MediaType, media av.MediaType) bool {
	for i := range allowed {
		if allowed[i] == media {
			return true
		}
	}
	return false
}

func codecAllowed(allowed []av.CodecID, codecID av.CodecID) bool {
	for i := range allowed {
		if allowed[i] == codecID {
			return true
		}
	}
	return false
}

func joinMediaTypes(values []av.MediaType) string {
	if len(values) == 0 {
		return ""
	}
	out := make([]string, 0, len(values))
	for i := range values {
		if values[i] != "" {
			out = append(out, string(values[i]))
		}
	}
	return strings.Join(out, ",")
}

func joinCodecIDs(values []av.CodecID) string {
	if len(values) == 0 {
		return ""
	}
	out := make([]string, 0, len(values))
	for i := range values {
		if values[i] != "" {
			out = append(out, string(values[i]))
		}
	}
	return strings.Join(out, ",")
}

func checkSingleVideoMuxCompatibility(output workDestination, streams []plannedMuxStream, codecs map[av.CodecID]bool, reason string) (muxCompatibilityIssue, bool) {
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

func newMuxCompatibilityIssue(output workDestination, streams []plannedMuxStream, reason string) muxCompatibilityIssue {
	return muxCompatibilityIssue{
		Code:        errcode.DestinationMuxIncompatible,
		Destination: output.Name,
		Format:      output.Format,
		Reason:      reason,
		Details:     muxCompatibilityDetails(output, streams),
		Suggestions: muxCompatibilitySuggestions(output.Format),
	}
}

func muxCompatibilityDetails(output workDestination, streams []plannedMuxStream) []string {
	details := []string{
		"destination=" + output.Name,
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
		if stream.TimeBase != (av.TimeBase{}) {
			detail += " timebase=" + muxTimebaseDetail(stream.TimeBase)
		}
		details = append(details, detail)
	}
	return append(details, "branches="+strings.Join(branchNames, ","))
}

func muxTimebaseDetail(base av.TimeBase) string {
	return fmt.Sprintf("%d/%d", base.Num, base.Den)
}

func muxCompatibilitySuggestions(formatID av.FormatID) []string {
	switch formatID {
	case av.FormatIVF:
		return []string{
			"route exactly one VP8, VP9, or AV1 video branch to each .ivf destination",
			"send audio or additional video branches to a separate destination",
			"choose a container adapter that supports the planned mux group",
		}
	case av.FormatAnnexB:
		return []string{
			"route exactly one H264 video branch to each Annex B destination",
			"send audio or additional video branches to a separate destination",
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
		Node:        issue.Destination,
		Reason:      issue.Reason,
		Details:     append([]string(nil), issue.Details...),
		Suggestions: append([]string(nil), issue.Suggestions...),
		Cause:       ErrUnsupportedBuild,
	}
}

func muxCompatibilityDiagnostics(issues []muxCompatibilityIssue) []plan.Diagnostic {
	diagnostics := make([]plan.Diagnostic, 0, len(issues))
	for i := range issues {
		diagnostics = append(diagnostics, plan.Diagnostic{
			Code:        string(issues[i].Code),
			Node:        issues[i].Destination,
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
