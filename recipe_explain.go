package goav

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func newPlanReport(operation string, resolved recipeResolved) (plan.Report, error) {
	graph, err := resolved.Describe()
	if err != nil {
		return plan.Report{}, err
	}
	report := plan.Report{
		Operation: operation,
		Graph:     graph,
	}
	work := resolved.workIR()
	report.Inputs = explainInputs(resolved)
	report.Streams = explainStreams(work.Streams)
	report.Taps = explainTaps(work.Taps)
	report.Branches = explainBranches(work)
	report.Destinations = explainDestinations(resolved.recipe.Destinations, resolved.outputFormats, work.Destinations)
	report.Decisions = explainDecisions(work.Decisions)
	report.Decisions = append(report.Decisions, explainStreamRuleFacts(resolved.recipe.StreamRules)...)
	if decision, ok := explainExecutionDecision(resolved); ok {
		report.Decisions = append(report.Decisions, decision)
	}
	report.RequiredAdapters, report.Warnings = explainRequirements(resolved, work, report)
	report.Warnings = appendPlanDiagnostics(report.Warnings, work.Diagnostics...)
	report.Warnings = appendPlanDiagnostics(report.Warnings, compile.MuxCompatibilityDiagnostics(resolvedMuxCompatibilityIssues(resolved))...)
	report.Summary = explainSummary(resolved.name, report)
	return report, nil
}

func explainTaps(taps []compile.WorkTap) []plan.Tap {
	reports := make([]plan.Tap, 0, len(taps))
	for i := range taps {
		reports = append(reports, plan.Tap{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			Shape:     taps[i].Shape,
			Node:      taps[i].Node,
		})
	}
	return reports
}

func explainInputs(resolved recipeResolved) []plan.Input {
	inputs := resolved.recipe.Inputs
	reports := make([]plan.Input, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		report := plan.Input{
			Name:     input.Name,
			URI:      input.URI,
			Protocol: input.Protocol,
			MIMEType: input.MIMEType,
			Realtime: input.Realtime,
			Codec:    input.Codec.ID,
		}
		probe, ok := resolved.inputProbe(i)
		if ok {
			report.Format = probe.Format
			report.Streams = append([]av.Stream(nil), probe.Streams...)
		}
		if len(report.Streams) == 0 && input.Realtime && input.Codec.ID != "" {
			report.Streams = compile.LiveInputStreams([]recipeir.Input{input})
		}
		reports = append(reports, report)
	}
	return reports
}

func (r recipeResolved) inputProbe(index int) (format.ProbeResult, bool) {
	if index < 0 || index >= len(r.inputProbes) {
		return format.ProbeResult{}, false
	}
	probe := r.inputProbes[index]
	if probe.Format == "" && len(probe.Streams) == 0 && probe.Score == 0 && probe.Reason == "" {
		return format.ProbeResult{}, false
	}
	return probe, true
}

func explainStreams(streams []compile.WorkStream) []plan.Stream {
	reports := make([]plan.Stream, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		reports = append(reports, plan.Stream{
			Name:         stream.Name,
			Select:       stream.Select,
			Decode:       compile.OperationsHaveDecode(stream.Operations),
			Operations:   explainOperations(stream.Operations),
			Encode:       compile.CloneCodecSpec(compile.OperationsEncodeSpec(stream.Operations)),
			Destinations: append([]string(nil), stream.Destinations...),
		})
	}
	return reports
}

func explainOperations(operations []recipeir.Operation) []plan.Operation {
	reports := make([]plan.Operation, 0, len(operations))
	for i := range operations {
		reports = append(reports, plan.Operation{
			Kind:      operations[i].Kind,
			Component: operations[i].Component,
			Detail:    operationDetail(operations[i]),
			Shape:     operationShape(operations[i]),
			Adapter:   operationAdapter(operations[i]),
			Config:    operationConfig(operations[i]),
			Shared:    operations[i].Shared,
		})
	}
	return reports
}

// operationAdapter names the implementation an operation lowers to: the
// codec id for decode/encode, the filter factory for a transform, or the
// component name for a custom stage. Empty when the operation lowers to no
// adapter (shape annotations, taps).
func operationAdapter(operation recipeir.Operation) string {
	switch operation.Kind {
	case plan.OpDecode:
		return string(operation.Decode.ID)
	case plan.OpEncode:
		return string(operation.Encode.ID)
	case plan.OpTransform:
		return firstNonEmpty(operation.Component, compile.RecipeIRTransformFactoryName(operation.Transform))
	case plan.OpStage:
		return operation.Component
	default:
		return ""
	}
}

// operationConfig reports an operation's small typed settings as stable
// key/value strings: transform geometry/rate and encoder settings. Nil when the
// operation carries no settings worth reporting.
func operationConfig(operation recipeir.Operation) map[string]string {
	switch operation.Kind {
	case plan.OpTransform:
		return transformConfig(operation.Transform)
	case plan.OpEncode:
		return codecSpecConfig(operation.Encode)
	default:
		return nil
	}
}

func transformConfig(transform recipeir.Transform) map[string]string {
	config := map[string]string{}
	switch transform.Kind {
	case recipeir.TransformResize:
		putConfigInt(config, "width", transform.Resize.Width)
		putConfigInt(config, "height", transform.Resize.Height)
		putConfigString(config, "pixelFormat", transform.Resize.PixelFormat)
	case recipeir.TransformResample:
		putConfigInt(config, "sampleRate", transform.Resample.SampleRate)
		putConfigInt(config, "channels", transform.Resample.Channels)
		putConfigString(config, "sampleFormat", transform.Resample.SampleFormat)
	}
	if len(config) == 0 {
		return nil
	}
	return config
}

func codecSpecConfig(spec codec.Spec) map[string]string {
	config := map[string]string{}
	putConfigInt(config, "bitrate", spec.Settings.Bitrate)
	putConfigInt(config, "keyframeInterval", spec.Settings.KeyframeInterval)
	putConfigInt(config, "sampleRate", spec.Settings.SampleRate)
	putConfigInt(config, "channels", spec.Settings.Channels)
	putConfigString(config, "profile", spec.Settings.Profile)
	putConfigString(config, "level", spec.Settings.Level)
	if len(config) == 0 {
		return nil
	}
	return config
}

func putConfigInt(config map[string]string, key string, value int) {
	if value != 0 {
		config[key] = strconv.Itoa(value)
	}
}

func putConfigString(config map[string]string, key, value string) {
	if value != "" {
		config[key] = value
	}
}

func operationShape(operation recipeir.Operation) shape.Spec {
	switch operation.Kind {
	case plan.OpTransform:
		return compile.MediaShapeFromRecipeIRTransform(operation.Transform)
	case plan.OpShape:
		if operation.Require != nil {
			return *operation.Require
		}
		return operation.Shape
	case plan.OpEncode:
		return compile.MediaShapeFromCodecSpec(operation.Encode, shape.DomainPacket)
	default:
		return shape.Spec{}
	}
}

func operationDetail(operation recipeir.Operation) string {
	switch operation.Kind {
	case plan.OpTransform:
		return firstNonEmpty(compile.RecipeIRTransformFactoryName(operation.Transform), "transform frames")
	case plan.OpShape:
		switch {
		case operation.Auto != nil:
			return "shape solver policy"
		case operation.Require != nil:
			return "shape requirement"
		case operation.Prefer != nil:
			return "shape preference"
		}
		return "media shape annotation"
	case plan.OpStage:
		return "custom stage"
	case plan.OpTap:
		return "named media outlet"
	case plan.OpEncode:
		return "frames to packets"
	case plan.OpDecode:
		return "packets to frames"
	default:
		return ""
	}
}

func explainBranches(work compile.WorkPlan) []plan.Branch {
	operations := compile.WorkOperationsByID(work.Operations)
	reports := make([]plan.Branch, 0, len(work.Branches))
	for i := range work.Branches {
		branch := work.Branches[i]
		destinations := make([]string, 0, len(branch.Destinations))
		for _, id := range branch.Destinations {
			destinations = append(destinations, compile.WorkDestinationNameByID(work.Destinations, id))
		}
		reports = append(reports, plan.Branch{
			Name:         branch.Name,
			Input:        branch.Input,
			Stream:       branch.Stream,
			Shape:        branch.SourceShape,
			Operations:   explainBranchOperations(branch, operations),
			Destinations: destinations,
		})
	}
	return reports
}

// explainBranchOperations renders a branch's operation rows from the work
// plan; the terminal destination operations are reported as destinations, not
// branch operations.
func explainBranchOperations(branch compile.WorkBranch, operations map[string]compile.WorkOperation) []plan.Operation {
	reports := make([]plan.Operation, 0, len(branch.Operations))
	for _, id := range branch.Operations {
		operation, ok := operations[id]
		if !ok || compile.WorkOperationTerminal(operation.Kind) {
			continue
		}
		reports = append(reports, plan.Operation{
			Kind:      operation.Kind,
			Component: operation.Component,
			Detail:    operation.Detail,
			Shape:     operation.ShapeOut,
			Adapter:   workOperationAdapter(operation),
			Config:    workOperationConfig(operation),
			Shared:    operation.Shared,
		})
	}
	return reports
}

// workOperationAdapter names the implementation a planned branch operation
// lowers to: the codec id for decode/encode or the component name otherwise.
func workOperationAdapter(operation compile.WorkOperation) string {
	switch operation.Kind {
	case plan.OpDecode, plan.OpEncode:
		if id := string(operation.Codec.ID); id != "" {
			return id
		}
		return operation.Component
	case plan.OpStage, plan.OpTransform:
		return operation.Component
	default:
		return ""
	}
}

// workOperationConfig reports a planned branch operation's typed settings as
// stable key/value strings — encoder settings for encode, and geometry/rate for
// a transform (from its output shape, the only transform fact a work operation
// carries). It mirrors the stream path's operationConfig so transform and
// encode operations report config consistently whether they sit on a stream or
// a branch. Nil when the operation carries no settings.
func workOperationConfig(operation compile.WorkOperation) map[string]string {
	switch operation.Kind {
	case plan.OpTransform:
		return shapeTransformConfig(operation.ShapeOut)
	case plan.OpEncode:
		return codecSpecConfig(operation.Codec)
	default:
		return nil
	}
}

// shapeTransformConfig reports a frame transform's geometry (video) or rate
// (audio) from its output shape, as stable key/value strings.
func shapeTransformConfig(spec shape.Spec) map[string]string {
	config := map[string]string{}
	switch spec.MediaKind {
	case av.MediaVideo:
		putConfigInt(config, "width", spec.Width)
		putConfigInt(config, "height", spec.Height)
		putConfigString(config, "pixelFormat", spec.PixelFormat)
	case av.MediaAudio:
		putConfigInt(config, "sampleRate", spec.SampleRate)
		putConfigInt(config, "channels", spec.Channels)
		putConfigString(config, "sampleFormat", spec.SampleFormat)
	}
	if len(config) == 0 {
		return nil
	}
	return config
}

func explainDecisions(decisions []compile.PlanDecision) []plan.Decision {
	reports := make([]plan.Decision, 0, len(decisions))
	for i := range decisions {
		reports = append(reports, plan.Decision(decisions[i]))
	}
	return reports
}

func explainDestinations(destinations []recipeir.Destination, outputFormats map[string]av.FormatID, workDestinations []compile.WorkDestination) []plan.Destination {
	reports := make([]plan.Destination, 0, len(destinations))
	branchesByDestination := workDestinationBranches(workDestinations)
	for i := range destinations {
		destination := destinations[i]
		name := firstNonEmpty(destination.Name, destination.URI, fmt.Sprintf("destination-%d", i))
		formatID := destination.Format
		if resolved := outputFormats[name]; resolved != "" {
			formatID = resolved
		}
		kind := "sink"
		if destination.URI != "" || destination.Protocol != "" || destination.MIMEType != "" || formatID != "" {
			kind = "mux"
		}
		reports = append(reports, plan.Destination{
			Name:     name,
			URI:      destination.URI,
			Protocol: destination.Protocol,
			MIMEType: destination.MIMEType,
			Format:   formatID,
			Kind:     kind,
			Branches: append([]string(nil), branchesByDestination[name]...),
		})
	}
	return reports
}

func workDestinationBranches(destinations []compile.WorkDestination) map[string][]string {
	branches := make(map[string][]string, len(destinations))
	for i := range destinations {
		if len(destinations[i].Branches) == 0 {
			continue
		}
		branches[destinations[i].Name] = append([]string(nil), destinations[i].Branches...)
	}
	return branches
}

func explainRequirements(resolved recipeResolved, work compile.WorkPlan, report plan.Report) ([]plan.AdapterRequirement, []plan.Diagnostic) {
	var requirements []plan.AdapterRequirement
	var warnings []plan.Diagnostic
	for i := range report.Inputs {
		input := report.Inputs[i]
		switch {
		case input.Format != "":
			requirements = appendAdapterRequirement(requirements, formatAdapterRequirement(
				resolved.runtime,
				"demuxer",
				input.Format,
				firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i)),
			))
		case input.Realtime && input.Codec != "":
			requirements = appendAdapterRequirement(requirements, plan.AdapterRequirement{
				Kind:       "depacketizer",
				Name:       string(input.Codec),
				Codec:      input.Codec,
				RequiredBy: firstNonEmpty(input.Name, fmt.Sprintf("input-%d", i)),
				Status:     "built-in",
			})
		}
	}
	for i := range report.Destinations {
		output := report.Destinations[i]
		if output.Kind != "mux" || output.Format == "" {
			continue
		}
		requirements = appendAdapterRequirement(requirements, formatAdapterRequirement(
			resolved.runtime,
			"muxer",
			output.Format,
			firstNonEmpty(output.Name, output.URI, fmt.Sprintf("output-%d", i)),
		))
	}
	operations := compile.WorkOperationsByID(work.Operations)
	for i := range work.Branches {
		branch := work.Branches[i]
		var branchWarnings []plan.Diagnostic
		requirements, branchWarnings = appendWorkBranchOperationRequirements(requirements, resolved, branch, operations, work.Inputs)
		warnings = append(warnings, branchWarnings...)
	}
	return requirements, warnings
}

func appendWorkBranchOperationRequirements(requirements []plan.AdapterRequirement, resolved recipeResolved, branch compile.WorkBranch, operations map[string]compile.WorkOperation, inputs []compile.WorkInput) ([]plan.AdapterRequirement, []plan.Diagnostic) {
	var warnings []plan.Diagnostic
	requiredBy := firstNonEmpty(branch.Name, "branch")
	for _, id := range branch.Operations {
		operation, ok := operations[id]
		if !ok {
			continue
		}
		switch operation.Kind {
		case plan.OpDecode:
			codecID, ok := workOperationDecodeCodec(resolved, branch, operation, inputs)
			if !ok || codecID == "" {
				warnings = append(warnings, plan.Diagnostic{
					Code:    compile.DiagnosticDecodeCodecDeferred,
					Node:    requiredBy,
					Message: "decode codec will be resolved when the input opens",
					Suggestions: []string{
						"declare the provider codec intent when the receive codec is already known",
						"use input metadata or a format adapter that reports streams during probing",
					},
				})
				continue
			}
			requirements = appendAdapterRequirement(requirements, codecAdapterRequirement(resolved.runtime, "decoder", codecID, requiredBy, operation.Codec))
		case plan.OpTransform:
			name := operation.Component
			if name == "" || name == "transform" {
				continue
			}
			requirements = appendAdapterRequirement(requirements, filterAdapterRequirement(resolved.runtime, name, requiredBy))
		case plan.OpEncode:
			codecID := workOperationEncodeCodec(operation)
			if codecID == "" {
				continue
			}
			requirements = appendAdapterRequirement(requirements, codecAdapterRequirement(resolved.runtime, "encoder", codecID, requiredBy, operation.Codec))
		}
	}
	return requirements, warnings
}

func workOperationDecodeCodec(resolved recipeResolved, branch compile.WorkBranch, operation compile.WorkOperation, inputs []compile.WorkInput) (av.CodecID, bool) {
	if operation.Codec.ID != "" {
		return operation.Codec.ID, true
	}
	if codecID, _, _, ok := compile.CopyMuxStreamFacts(branch, inputs, resolved.inputProbes); ok {
		return codecID, true
	}
	codecID := av.CodecID(operation.Component)
	if codecID == "" || codecID == "decoder" {
		return "", false
	}
	return codecID, true
}

func workOperationEncodeCodec(operation compile.WorkOperation) av.CodecID {
	if operation.Codec.ID != "" {
		return operation.Codec.ID
	}
	codecID := av.CodecID(operation.Component)
	if codecID == "" || codecID == "encoder" {
		return ""
	}
	return codecID
}

func adapterRequirementRuntimeStatus(rt *Runtime, kind string, formatID av.FormatID, codecID av.CodecID, transform string) string {
	if rt == nil {
		return "required"
	}
	switch kind {
	case "demuxer":
		if formatID == "" {
			return "unknown"
		}
		if _, err := rt.formats.DemuxerFactory(formatID); err != nil {
			return "missing"
		}
		return "available"
	case "muxer":
		if formatID == "" {
			return "unknown"
		}
		if _, err := rt.formats.MuxerFactory(formatID); err != nil {
			return "missing"
		}
		return "available"
	case "decoder":
		_, err := rt.codecs.DecoderFactory(codecID)
		return codecFactoryStatus(err)
	case "encoder":
		_, err := rt.codecs.EncoderFactory(codecID)
		return codecFactoryStatus(err)
	case "filter":
		if transform == "" {
			return "unknown"
		}
		if _, err := rt.filters.Factory(transform); err != nil {
			return "missing"
		}
		return "available"
	default:
		return "required"
	}
}

func filterAdapterRequirement(rt *Runtime, name string, requiredBy string) plan.AdapterRequirement {
	requirement := plan.AdapterRequirement{
		Kind:       "filter",
		Name:       name,
		Transform:  name,
		RequiredBy: requiredBy,
		Status:     adapterRequirementRuntimeStatus(rt, "filter", "", "", name),
	}
	if rt == nil {
		return requirement
	}
	desc, err := rt.filters.Descriptor(name)
	if err != nil {
		return requirement
	}
	requirement.Input = desc.Input
	requirement.Output = desc.Output
	requirement.PixelFormats = append([]string(nil), desc.PixelFormats...)
	requirement.SampleFormats = append([]string(nil), desc.SampleFormats...)
	requirement.ResizeModes = append([]filter.ResizeMode(nil), desc.ResizeModes...)
	requirement.Realtime = desc.Realtime
	requirement.Stateless = desc.Stateless
	requirement.Metadata = compile.CloneMetadata(desc.Metadata)
	return requirement
}

func formatAdapterRequirement(rt *Runtime, kind string, formatID av.FormatID, requiredBy string) plan.AdapterRequirement {
	requirement := plan.AdapterRequirement{
		Kind:       kind,
		Name:       string(formatID),
		Format:     formatID,
		RequiredBy: requiredBy,
		Status:     adapterRequirementRuntimeStatus(rt, kind, formatID, "", ""),
	}
	if rt == nil {
		return requirement
	}
	var desc format.Descriptor
	var err error
	switch kind {
	case "demuxer":
		desc, err = rt.formats.DemuxerDescriptor(formatID)
	case "muxer":
		desc, err = rt.formats.MuxerDescriptor(formatID)
	default:
		return requirement
	}
	if err != nil {
		return requirement
	}
	requirement.Media = append([]av.MediaType(nil), desc.Media...)
	requirement.Codecs = append([]av.CodecID(nil), desc.Codecs...)
	requirement.MinStreams = desc.MinStreams
	requirement.MaxStreams = desc.MaxStreams
	requirement.Realtime = desc.Realtime
	requirement.Metadata = compile.CloneMetadata(desc.Metadata)
	return requirement
}

func codecFactoryStatus(err error) string {
	if err == nil {
		return "available"
	}
	if errors.Is(err, codec.ErrUnavailable) {
		return "unavailable"
	}
	return "missing"
}

func codecAdapterRequirement(rt *Runtime, kind string, codecID av.CodecID, requiredBy string, spec codec.Spec) plan.AdapterRequirement {
	requirement := plan.AdapterRequirement{
		Kind:       kind,
		Name:       string(codecID),
		Codec:      codecID,
		RequiredBy: requiredBy,
		Status:     adapterRequirementRuntimeStatus(rt, kind, "", codecID, ""),
	}
	descriptors := codecDescriptorsForRequirement(rt, kind, codecID)
	requirement = applyCodecDescriptorRequirement(requirement, descriptors)
	if rt == nil {
		return requirement
	}
	mode := codec.ModeDecode
	if kind == "encoder" {
		mode = codec.ModeEncode
	}
	spec.ID = codecID
	request := compile.EncodeAdapterRequestFromPreparedStream(spec, av.Stream{})
	if mode == codec.ModeDecode {
		request = compile.CodecAdapterRequest{Codec: codecID, Backend: spec.Settings.Backend}
	}
	if choice, err := rt.codecs.Select(request.Need(mode)); err == nil {
		requirement.PixelFormats, requirement.SampleFormats = nil, nil
		requirement = applyCodecDescriptorRequirement(requirement, []codec.Descriptor{choice.Descriptor})
		requirement.Backend = choice.Descriptor.Backend.Label()
		requirement.BackendReason = choice.Reason
		for i := range choice.Alternatives {
			requirement.Alternatives = append(requirement.Alternatives, choice.Alternatives[i].Backend.Label())
		}
		for i := range choice.Descriptor.Knobs {
			requirement.Knobs = append(requirement.Knobs, choice.Descriptor.Knobs[i].String())
		}
	}
	return requirement
}

func codecDescriptorsForRequirement(rt *Runtime, kind string, codecID av.CodecID) []codec.Descriptor {
	if rt == nil || codecID == "" {
		return nil
	}
	mode := codec.ModeDecode
	if kind == "encoder" {
		mode = codec.ModeEncode
	}
	descriptors, err := rt.codecs.Find(codecID, mode)
	if err != nil {
		return nil
	}
	return descriptors
}

func applyCodecDescriptorRequirement(requirement plan.AdapterRequirement, descriptors []codec.Descriptor) plan.AdapterRequirement {
	for i := range descriptors {
		if descriptors[i].Type != "" && !compile.MediaAllowed(requirement.Media, descriptors[i].Type) {
			requirement.Media = append(requirement.Media, descriptors[i].Type)
		}
		requirement.SampleFormats = compile.MergeStringList(requirement.SampleFormats, descriptors[i].Capabilities.SampleFormats)
		requirement.PixelFormats = compile.MergeStringList(requirement.PixelFormats, descriptors[i].Capabilities.PixelFormats)
		requirement.Realtime = requirement.Realtime || descriptors[i].Realtime
	}
	return requirement
}

func appendAdapterRequirement(requirements []plan.AdapterRequirement, requirement plan.AdapterRequirement) []plan.AdapterRequirement {
	key := adapterRequirementKey(requirement)
	for i := range requirements {
		if adapterRequirementKey(requirements[i]) == key {
			return requirements
		}
	}
	return append(requirements, requirement)
}

func upsertAdapterRequirement(requirements []plan.AdapterRequirement, requirement plan.AdapterRequirement) []plan.AdapterRequirement {
	key := adapterRequirementKey(requirement)
	for i := range requirements {
		if adapterRequirementKey(requirements[i]) == key {
			requirements[i] = requirement
			return requirements
		}
	}
	return append(requirements, requirement)
}

func adapterRequirementKey(requirement plan.AdapterRequirement) string {
	return requirement.Kind + "|" + string(requirement.Format) + "|" + string(requirement.Codec) + "|" + requirement.Transform + "|" + requirement.RequiredBy
}

func annotatePlanReportError(report *plan.Report, err error) {
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr == nil {
		report.Warnings = appendPlanDiagnostics(report.Warnings, plan.Diagnostic{
			Code:    compile.DiagnosticExplainPreflightError,
			Message: err.Error(),
		})
		return
	}
	report.Warnings = appendPlanDiagnostics(report.Warnings, plan.Diagnostic{
		Code:        string(buildErr.Code),
		Node:        buildErr.Node,
		Message:     buildErr.Reason,
		Details:     append([]string(nil), buildErr.detailLines()...),
		Suggestions: append([]string(nil), buildErr.suggestionLines()...),
	})
	requirement, ok := adapterRequirementFromBuildError(buildErr)
	if !ok {
		if buildErr.Code == destinationMuxIncompatibleCode {
			return
		}
		report.Missing = append(report.Missing, plan.Requirement{
			Kind:       firstNonEmpty(string(buildErr.Code), "requirement"),
			Name:       firstNonEmpty(buildErr.Node, buildErr.Reason),
			RequiredBy: buildErr.Node,
			Status:     "failed",
		})
		return
	}
	report.RequiredAdapters = upsertAdapterRequirement(report.RequiredAdapters, requirement)
	report.Missing = appendMissingRequirement(report.Missing, plan.Requirement{
		Kind:       requirement.Kind,
		Name:       firstNonEmpty(requirement.Name, string(requirement.Format), string(requirement.Codec), requirement.Transform),
		RequiredBy: requirement.RequiredBy,
		Status:     requirement.Status,
	})
}

func adapterRequirementFromBuildError(err *BuildError) (plan.AdapterRequirement, bool) {
	details := buildErrorDetailMap(err)
	status := adapterRequirementStatus(err.Code)
	requiredBy := firstNonEmpty(err.Node, err.Operation)
	switch err.Code {
	case inputDemuxerMissingCode:
		formatID := av.FormatID(details["format"])
		return plan.AdapterRequirement{
			Kind:       "demuxer",
			Name:       string(formatID),
			Format:     formatID,
			RequiredBy: requiredBy,
			Status:     status,
		}, formatID != ""
	case outputMuxerMissingCode, destinationMuxerMissingCode:
		formatID := av.FormatID(details["format"])
		return plan.AdapterRequirement{
			Kind:       "muxer",
			Name:       string(formatID),
			Format:     formatID,
			RequiredBy: requiredBy,
			Status:     status,
		}, formatID != ""
	case decodeAdapterMissingCode, decodeAdapterUnavailableCode, decodeAdapterIncompatibleCode:
		codecID := av.CodecID(details["codec"])
		requirement := plan.AdapterRequirement{
			Kind:       "decoder",
			Name:       string(codecID),
			Codec:      codecID,
			RequiredBy: requiredBy,
			Status:     status,
		}
		applyCodecDetailsFromBuildError(&requirement, details)
		return requirement, codecID != ""
	case encodeAdapterMissingCode, encodeAdapterUnavailableCode, encodeAdapterIncompatibleCode:
		codecID := av.CodecID(details["codec"])
		requirement := plan.AdapterRequirement{
			Kind:       "encoder",
			Name:       string(codecID),
			Codec:      codecID,
			RequiredBy: requiredBy,
			Status:     status,
		}
		applyCodecDetailsFromBuildError(&requirement, details)
		return requirement, codecID != ""
	case transformAdapterMissingCode, transformAdapterIncompatibleCode:
		name := details["transform"]
		requirement := filterAdapterRequirement(nil, name, requiredBy)
		requirement.Status = status
		if details["actual_input"] != "" {
			requirement.Input = av.MediaType(details["actual_input"])
		}
		if details["actual_output"] != "" {
			requirement.Output = av.MediaType(details["actual_output"])
		}
		if details["field"] == "pixel_format" {
			requirement.PixelFormats = splitDetailCSV(details["supported"])
		}
		if details["field"] == "sample_format" {
			requirement.SampleFormats = splitDetailCSV(details["supported"])
		}
		if details["field"] == "resize_mode" {
			requirement.ResizeModes = resizeModesFromStrings(splitDetailCSV(details["supported"]))
		}
		return requirement, name != ""
	default:
		if strings.HasSuffix(string(err.Code), "_format_unknown") {
			return plan.AdapterRequirement{
				Kind:       "format-prober",
				Name:       firstNonEmpty(err.Node, "format"),
				RequiredBy: requiredBy,
				Status:     "unknown",
			}, true
		}
		return plan.AdapterRequirement{}, false
	}
}

func applyCodecDetailsFromBuildError(requirement *plan.AdapterRequirement, details map[string]string) {
	if requirement == nil {
		return
	}
	if details["supported_media"] != "" {
		requirement.Media = mediaTypesFromStrings(splitDetailCSV(details["supported_media"]))
	}
	if details["field"] == "media" && details["supported"] != "" && len(requirement.Media) == 0 {
		requirement.Media = mediaTypesFromStrings(splitDetailCSV(details["supported"]))
	}
	if details["supported_sample_formats"] != "" {
		requirement.SampleFormats = splitDetailCSV(details["supported_sample_formats"])
	}
	if details["field"] == "sample_format" && details["supported"] != "" && len(requirement.SampleFormats) == 0 {
		requirement.SampleFormats = splitDetailCSV(details["supported"])
	}
	if details["supported_pixel_formats"] != "" {
		requirement.PixelFormats = splitDetailCSV(details["supported_pixel_formats"])
	}
	if details["field"] == "pixel_format" && details["supported"] != "" && len(requirement.PixelFormats) == 0 {
		requirement.PixelFormats = splitDetailCSV(details["supported"])
	}
}

func adapterRequirementStatus(code errcode.Code) string {
	switch {
	case strings.HasSuffix(string(code), "_incompatible"):
		return "incompatible"
	case strings.HasSuffix(string(code), "_unavailable"):
		return "unavailable"
	case strings.HasSuffix(string(code), "_unknown"):
		return "unknown"
	default:
		return "missing"
	}
}

func buildErrorDetailMap(err *BuildError) map[string]string {
	if err == nil {
		return nil
	}
	out := make(map[string]string, len(err.fields))
	for i := range err.fields {
		if err.fields[i].Key == "" || err.fields[i].Value == nil {
			continue
		}
		out[err.fields[i].Key] = fmt.Sprint(err.fields[i].Value)
	}
	return out
}

func splitDetailCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func resizeModesFromStrings(values []string) []filter.ResizeMode {
	if len(values) == 0 {
		return nil
	}
	out := make([]filter.ResizeMode, 0, len(values))
	for i := range values {
		if values[i] != "" {
			out = append(out, filter.ResizeMode(values[i]))
		}
	}
	return out
}

func mediaTypesFromStrings(values []string) []av.MediaType {
	if len(values) == 0 {
		return nil
	}
	out := make([]av.MediaType, 0, len(values))
	for i := range values {
		if values[i] != "" {
			out = append(out, av.MediaType(values[i]))
		}
	}
	return out
}

func appendMissingRequirement(requirements []plan.Requirement, requirement plan.Requirement) []plan.Requirement {
	for i := range requirements {
		if requirements[i].Kind == requirement.Kind &&
			requirements[i].Name == requirement.Name &&
			requirements[i].RequiredBy == requirement.RequiredBy {
			requirements[i] = requirement
			return requirements
		}
	}
	return append(requirements, requirement)
}

func appendPlanDiagnostics(diagnostics []plan.Diagnostic, next ...plan.Diagnostic) []plan.Diagnostic {
	for i := range next {
		found := false
		for j := range diagnostics {
			if diagnostics[j].Code == next[i].Code &&
				diagnostics[j].Node == next[i].Node &&
				diagnostics[j].Message == next[i].Message {
				found = true
				break
			}
		}
		if !found {
			diagnostics = append(diagnostics, next[i])
		}
	}
	return diagnostics
}

func explainSummary(recipeName string, report plan.Report) string {
	name := firstNonEmpty(recipeName, "job")
	input := "input"
	if len(report.Inputs) == 1 {
		input = firstNonEmpty(report.Inputs[0].Name, report.Inputs[0].URI, input)
	} else if len(report.Inputs) > 1 {
		input = fmt.Sprintf("%d inputs", len(report.Inputs))
	}
	target := "target"
	if len(report.Destinations) == 1 {
		target = firstNonEmpty(report.Destinations[0].Name, report.Destinations[0].URI, target)
	} else if len(report.Destinations) > 1 {
		target = fmt.Sprintf("%d destinations", len(report.Destinations))
	}
	if len(report.Streams) == 0 {
		return fmt.Sprintf("%s %s to %s", name, input, target)
	}
	return fmt.Sprintf("%s %s through %d stream branch(es) to %s", name, input, len(report.Streams), target)
}

// explainExecutionDecision reports when the planner runs the graph buffered
// although the runtime's own policy is direct: live inputs, a join that needs
// concurrent arms, a declared branch queue, or a fanout of goav-owned encode
// work. Buffered execution gives every node its own worker and queue, so
// branches and join arms run in parallel. Describe's graph shape is the same
// either way, so Explain says which one the build will run.
func explainExecutionDecision(resolved recipeResolved) (plan.Decision, bool) {
	gp := resolved.graphPlan
	if gp.lowerer == nil || gp.runtime == nil || !gp.runtime.buffer.IsDirect() {
		return plan.Decision{}, false
	}
	config, err := gp.lowerer.graphConfig(gp)
	if err != nil || config.Buffer.IsDirect() {
		return plan.Decision{}, false
	}
	return plan.Decision{
		Code:    compile.DiagnosticBufferedExecution,
		Message: "the graph runs buffered: every node gets its own worker and queue, so branches and join arms run in parallel",
	}, true
}

func explainStreamRuleFacts(rules []recipeir.StreamRule) []plan.Decision {
	if len(rules) == 0 {
		return nil
	}
	out := make([]plan.Decision, 0, len(rules))
	for i := range rules {
		names := make([]string, 0, len(rules[i].Branches))
		destinations := make([]string, 0)
		for j := range rules[i].Branches {
			names = append(names, rules[i].Branches[j].Name+"-<stream>")
			destinations = append(destinations, rules[i].Branches[j].Destinations...)
		}
		match := firstNonEmpty(rules[i].MatchDescription, "none")
		out = append(out, plan.Decision{
			Code:   compile.DiagnosticStreamRule,
			Branch: strings.Join(names, "+"),
			Message: fmt.Sprintf("on discovered stream (%s): attach %s to %s per matched stream",
				match, strings.Join(names, ", "), strings.Join(destinations, ", ")),
		})
	}
	return out
}
