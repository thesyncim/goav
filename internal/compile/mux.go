package compile

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/plan"
)

type MuxCompatibilityIssue struct {
	Code        errcode.Code
	Destination string
	Format      av.FormatID
	Reason      string
	Details     []string
	Suggestions []string
}

type PlannedMuxStream struct {
	Branch   string
	Codec    av.CodecID
	Media    av.MediaType
	TimeBase av.TimeBase
}

// validateMuxCompatibility refuses, before a build opens anything, a
// destination whose format cannot carry the streams planned into it.
func validateMuxCompatibility(state *recipeState) error {
	if !state.req.Preflight {
		return nil
	}
	issues := MuxCompatibilityIssues(state.work, state.inputProbes, state.env)
	if len(issues) == 0 {
		return nil
	}
	return MuxCompatibilityBuildError(state.operation, issues[0])
}

func MuxCompatibilityIssues(
	work WorkPlan,
	inputProbes []format.ProbeResult,
	env Env,
) []MuxCompatibilityIssue {
	var issues []MuxCompatibilityIssue
	branches := workBranchesByName(work.Branches)
	operations := WorkOperationsByID(work.Operations)
	for i := range work.Destinations {
		output := work.Destinations[i]
		if output.Operation != plan.OpMux || output.Format == "" {
			continue
		}
		streams := muxOutputStreams(output, branches, operations, work.Inputs, inputProbes)
		if issue, ok := CheckKnownMuxCompatibility(output, streams, env); ok {
			issues = append(issues, issue)
		}
	}
	return issues
}

func muxOutputStreams(
	output WorkDestination,
	branchesByName map[string]WorkBranch,
	operations map[string]WorkOperation,
	inputs []WorkInput,
	inputProbes []format.ProbeResult,
) []PlannedMuxStream {
	streams := make([]PlannedMuxStream, 0, len(output.Branches))
	for i := range output.Branches {
		branchName := output.Branches[i]
		branch, ok := branchesByName[branchName]
		if !ok {
			continue
		}
		streams = append(streams, muxStreamForBranch(branch, i, operations, inputs, inputProbes))
	}
	return streams
}

func muxStreamForBranch(
	branch WorkBranch,
	branchIndex int,
	operations map[string]WorkOperation,
	inputs []WorkInput,
	inputProbes []format.ProbeResult,
) PlannedMuxStream {
	out := PlannedMuxStream{Branch: cmp.Or(branch.Name, fmt.Sprintf("branch-%d", branchIndex))}
	for _, id := range branch.Operations {
		operation, ok := operations[id]
		if !ok || operation.Kind != plan.OpEncode {
			continue
		}
		if encode := operation.Codec; encode.ID != "" {
			out.Codec = encode.ID
			out.Media = FirstNonEmptyMedia(encode.Type, encode.Parameters.Type, CodecMedia(encode.ID), branch.Stream.Type)
			out.TimeBase = codecSpecMuxTimeBase(encode)
			return out
		}
		out.Codec = av.CodecID(operation.Component)
		out.Media = CodecMedia(out.Codec)
		return out
	}
	if codecID, media, base, ok := CopyMuxStreamFacts(branch, inputs, inputProbes); ok {
		out.Codec = codecID
		out.Media = media
		out.TimeBase = base
	}
	return out
}

func CopyMuxStreamFacts(
	branch WorkBranch,
	inputs []WorkInput,
	inputProbes []format.ProbeResult,
) (av.CodecID, av.MediaType, av.TimeBase, bool) {
	if branch.Stream.Codec != "" {
		return branch.Stream.Codec, FirstNonEmptyMedia(branch.Stream.Type, CodecMedia(branch.Stream.Codec)), av.TimeBase{}, true
	}
	if codecID, media, base, ok := liveMuxInputFacts(inputs, branch); ok {
		return codecID, media, base, true
	}
	return probedMuxInputFacts(inputProbes, branch)
}

func liveMuxInputFacts(inputs []WorkInput, branch WorkBranch) (av.CodecID, av.MediaType, av.TimeBase, bool) {
	for i := range inputs {
		input := inputs[i]
		if !input.Realtime || input.Codec == "" {
			continue
		}
		name := cmp.Or(input.Name, fmt.Sprintf("input-%d", i))
		if branch.Input != "" && branch.Input != name {
			continue
		}
		spec := input.CodecSpec
		media := FirstNonEmptyMedia(spec.Type, spec.Parameters.Type, CodecMedia(input.Codec))
		return input.Codec, media, codecSpecMuxTimeBase(spec), true
	}
	return "", "", av.TimeBase{}, false
}

func probedMuxInputFacts(probes []format.ProbeResult, branch WorkBranch) (av.CodecID, av.MediaType, av.TimeBase, bool) {
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
		selected, err := SelectDecodeStream(probes[i].Streams, selector)
		if err != nil || selected.Codec.ID == "" {
			continue
		}
		candidates = append(candidates, selected)
	}
	if len(candidates) != 1 {
		return "", "", av.TimeBase{}, false
	}
	stream := candidates[0]
	media := FirstNonEmptyMedia(stream.Type, stream.Codec.Type, CodecMedia(stream.Codec.ID))
	return stream.Codec.ID, media, MuxStreamTimeBase(stream), true
}

func codecSpecMuxTimeBase(spec codec.Spec) av.TimeBase {
	if spec.Parameters.ClockRate != 0 {
		return av.RTPTimeBase(spec.Parameters.ClockRate)
	}
	if spec.Parameters.SampleRate > 0 {
		return av.TimeBase{Num: 1, Den: int64(spec.Parameters.SampleRate)}
	}
	return av.TimeBase{}
}

func MuxStreamTimeBase(stream av.Stream) av.TimeBase {
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

func CheckKnownMuxCompatibility(output WorkDestination, streams []PlannedMuxStream, env Env) (MuxCompatibilityIssue, bool) {
	if issue, ok := checkMuxTimebaseCompatibility(output, streams); ok {
		return issue, true
	}
	if desc, ok := muxerDescriptorForRuntime(env, output.Format); ok {
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
		return MuxCompatibilityIssue{}, false
	}
}

func muxerDescriptorForRuntime(env Env, formatID av.FormatID) (format.Descriptor, bool) {
	if !env.Present {
		return format.Descriptor{}, false
	}
	desc, err := env.Formats.MuxerDescriptor(formatID)
	if err != nil {
		return format.Descriptor{}, false
	}
	if desc.Format == "" {
		desc.Format = formatID
	}
	return desc, true
}

func checkDescriptorMuxCompatibility(output WorkDestination, streams []PlannedMuxStream, desc format.Descriptor) (MuxCompatibilityIssue, bool) {
	if desc.MinStreams > 0 && len(streams) < desc.MinStreams {
		return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
	}
	if desc.MaxStreams > 0 && len(streams) > desc.MaxStreams {
		return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
	}
	if len(desc.Media) == 0 && len(desc.Codecs) == 0 {
		return MuxCompatibilityIssue{}, false
	}
	for i := range streams {
		stream := streams[i]
		if stream.Media != "" && len(desc.Media) != 0 && !MediaAllowed(desc.Media, stream.Media) {
			return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
		}
		if stream.Codec != "" && len(desc.Codecs) != 0 && !codecAllowed(desc.Codecs, stream.Codec) {
			return newMuxCompatibilityIssue(output, streams, descriptorMuxReason(output.Format, desc)), true
		}
	}
	return MuxCompatibilityIssue{}, false
}

func checkMuxTimebaseCompatibility(output WorkDestination, streams []PlannedMuxStream) (MuxCompatibilityIssue, bool) {
	for i := range streams {
		base := streams[i].TimeBase
		if base == (av.TimeBase{}) {
			continue
		}
		if !base.Valid() {
			return newMuxCompatibilityIssue(output, streams, "mux destination streams must declare a valid timebase when timebase facts are present"), true
		}
	}
	return MuxCompatibilityIssue{}, false
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

func MediaAllowed(allowed []av.MediaType, media av.MediaType) bool {
	return slices.Contains(allowed, media)
}

func codecAllowed(allowed []av.CodecID, codecID av.CodecID) bool {
	return slices.Contains(allowed, codecID)
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

func checkSingleVideoMuxCompatibility(output WorkDestination, streams []PlannedMuxStream, codecs map[av.CodecID]bool, reason string) (MuxCompatibilityIssue, bool) {
	if len(streams) != 1 {
		return newMuxCompatibilityIssue(output, streams, reason), true
	}
	stream := streams[0]
	if stream.Codec == "" && stream.Media == "" {
		return MuxCompatibilityIssue{}, false
	}
	if stream.Media != "" && stream.Media != av.MediaVideo {
		return newMuxCompatibilityIssue(output, streams, reason), true
	}
	if stream.Codec != "" && !codecs[stream.Codec] {
		return newMuxCompatibilityIssue(output, streams, reason), true
	}
	return MuxCompatibilityIssue{}, false
}

func newMuxCompatibilityIssue(output WorkDestination, streams []PlannedMuxStream, reason string) MuxCompatibilityIssue {
	return MuxCompatibilityIssue{
		Code:        builderr.DestinationMuxIncompatibleCode,
		Destination: output.Name,
		Format:      output.Format,
		Reason:      reason,
		Details:     muxCompatibilityDetails(output, streams),
		Suggestions: muxCompatibilitySuggestions(output.Format),
	}
}

func muxCompatibilityDetails(output WorkDestination, streams []PlannedMuxStream) []string {
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

func MuxCompatibilityBuildError(operation string, issue MuxCompatibilityIssue) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(issue.Code),
		Code:      issue.Code,
		Operation: operation,
		Node:      issue.Destination,
		Reason:    issue.Reason,
		Details:   builderr.DetailLines(append([]string(nil), issue.Details...)),
		Fixes:     builderr.Fixes(append([]string(nil), issue.Suggestions...)),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

func MuxCompatibilityDiagnostics(issues []MuxCompatibilityIssue) []plan.Diagnostic {
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

func CodecMedia(codecID av.CodecID) av.MediaType {
	switch codecID {
	case av.CodecOpus, av.CodecPCM:
		return av.MediaAudio
	case av.CodecVP8, av.CodecVP9, av.CodecH264, av.CodecAV1:
		return av.MediaVideo
	default:
		return ""
	}
}

func FirstNonEmptyMedia(values ...av.MediaType) av.MediaType {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}

// OpenedMuxRefusal refuses, when a destination opens, a container that
// cannot carry every stream routed to it: the plan-time check, run again on
// the streams an input turned out to have (a whole file into IVF), so a
// muxer is never handed a stream it would drop.
func OpenedMuxRefusal(name string, formatID av.FormatID, streams []av.Stream, env Env) error {
	planned := make([]PlannedMuxStream, len(streams))
	for i := range streams {
		planned[i] = PlannedMuxStream{
			Branch: string(streams[i].ID),
			Codec:  streams[i].Codec.ID,
			Media:  FirstNonEmptyMedia(streams[i].Type, streams[i].Codec.Type, CodecMedia(streams[i].Codec.ID)),
		}
	}
	output := WorkDestination{Name: name, Operation: plan.OpMux, Format: formatID}
	issue, ok := CheckKnownMuxCompatibility(output, planned, env)
	if !ok {
		return nil
	}
	if len(streams) > 1 {
		issue.Suggestions = append([]string{"route only the stream it carries: input.Video().To(destination), and the others to another destination"}, issue.Suggestions...)
	}
	return MuxCompatibilityBuildError("open destination", issue)
}
