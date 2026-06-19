package goav

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// Explain reports the planned workflow before any resource opens: inputs,
// branches, destinations, taps, stream and operation shapes, adapter
// requirements, decisions, and warnings. On a refusal it returns both the
// structured BuildError and a partial report naming what is missing.
func (j *Job) Explain(ctx context.Context) (plan.Report, error) {
	resolved, err := compileJobRecipeForBuildContext(ctx, j)
	if err == nil {
		return newPlanReport("build job", resolved)
	}
	fallback, fallbackErr := compileJobRecipe(j)
	if fallbackErr != nil {
		return plan.Report{}, err
	}
	report, reportErr := newPlanReport("build job", fallback)
	if reportErr != nil {
		return plan.Report{}, err
	}
	annotatePlanReportError(&report, err)
	return report, err
}

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
	report.Destinations = explainDestinations(resolved.intent.Destinations, resolved.outputFormats, work.Destinations)
	report.Decisions = explainDecisions(work.Decisions)
	report.Decisions = append(report.Decisions, explainStreamRules(resolved.streamRules)...)
	report.RequiredAdapters, report.Warnings = explainRequirements(resolved, work, report)
	report.Warnings = appendPlanDiagnostics(report.Warnings, work.Diagnostics...)
	report.Warnings = appendPlanDiagnostics(report.Warnings, muxCompatibilityDiagnostics(resolvedMuxCompatibilityIssues(resolved))...)
	report.Summary = explainSummary(resolved.intent.Name, report)
	return report, nil
}

func explainTaps(taps []workTap) []plan.Tap {
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
	inputs := resolved.intent.Inputs
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
			report.Streams = liveIntentStreams([]inputIntent{input})
		}
		reports = append(reports, report)
	}
	return reports
}

func (r recipeResolved) inputProbe(index int) (format.ProbeResult, bool) {
	if r.branchInputProbeReady {
		if index == 0 {
			return r.branchInputProbe, true
		}
		return format.ProbeResult{}, false
	}
	if index < 0 || index >= len(r.inputProbes) {
		return format.ProbeResult{}, false
	}
	probe := r.inputProbes[index]
	if probe.Format == "" && len(probe.Streams) == 0 && probe.Score == 0 && probe.Reason == "" {
		return format.ProbeResult{}, false
	}
	return probe, true
}

func explainStreams(streams []workStream) []plan.Stream {
	reports := make([]plan.Stream, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		reports = append(reports, plan.Stream{
			Name:         stream.Name,
			Select:       stream.Select,
			Decode:       chainHasDecode(stream.Operations),
			Operations:   explainOperationSpecs(stream.Operations),
			Encode:       chainEncodeSpec(stream.Operations),
			Destinations: append([]string(nil), stream.Destinations...),
		})
	}
	return reports
}

func explainOperationSpecs(operations []operationSpec) []plan.Operation {
	reports := make([]plan.Operation, 0, len(operations))
	for i := range operations {
		reports = append(reports, plan.Operation{
			Kind:      operations[i].Kind,
			Component: operations[i].Component,
			Detail:    operationSpecDetail(operations[i]),
			Shape:     operationSpecShape(operations[i]),
			Shared:    operations[i].Shared,
		})
	}
	return reports
}

func operationSpecShape(operation operationSpec) shape.Spec {
	switch operation.Kind {
	case plan.OpTransform:
		return mediaShapeFromTransform(operation.Transform)
	case plan.OpShape:
		if operation.Require != nil {
			return *operation.Require
		}
		return operation.Shape
	case plan.OpEncode:
		return mediaShapeFromCodecSpec(operation.Encode, shape.DomainPacket)
	default:
		return shape.Spec{}
	}
}

func operationSpecDetail(operation operationSpec) string {
	switch operation.Kind {
	case plan.OpTransform:
		return firstNonEmpty(transformFactoryName(operation.Transform), "transform frames")
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

func explainBranches(work workPlan) []plan.Branch {
	operations := workOperationsByID(work.Operations)
	reports := make([]plan.Branch, 0, len(work.Branches))
	for i := range work.Branches {
		branch := work.Branches[i]
		destinations := make([]string, 0, len(branch.Destinations))
		for _, id := range branch.Destinations {
			destinations = append(destinations, workDestinationNameByID(work.Destinations, id))
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
func explainBranchOperations(branch workBranch, operations map[string]workOperation) []plan.Operation {
	reports := make([]plan.Operation, 0, len(branch.Operations))
	for _, id := range branch.Operations {
		operation, ok := operations[id]
		if !ok || workOperationTerminal(operation.Kind) {
			continue
		}
		reports = append(reports, plan.Operation{
			Kind:      operation.Kind,
			Component: operation.Component,
			Detail:    operation.Detail,
			Shape:     operation.ShapeOut,
			Shared:    operation.Shared,
		})
	}
	return reports
}

func explainDecisions(decisions []planDecision) []plan.Decision {
	reports := make([]plan.Decision, 0, len(decisions))
	for i := range decisions {
		reports = append(reports, plan.Decision(decisions[i]))
	}
	return reports
}

func explainDestinations(destinations []destinationIntent, outputFormats map[string]av.FormatID, workDestinations []workDestination) []plan.Destination {
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

func workDestinationBranches(destinations []workDestination) map[string][]string {
	branches := make(map[string][]string, len(destinations))
	for i := range destinations {
		if len(destinations[i].Branches) == 0 {
			continue
		}
		branches[destinations[i].Name] = append([]string(nil), destinations[i].Branches...)
	}
	return branches
}

func explainRequirements(resolved recipeResolved, work workPlan, report plan.Report) ([]plan.AdapterRequirement, []plan.Diagnostic) {
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
	operations := workOperationsByID(work.Operations)
	for i := range work.Branches {
		branch := work.Branches[i]
		var branchWarnings []plan.Diagnostic
		requirements, branchWarnings = appendWorkBranchOperationRequirements(requirements, resolved, branch, operations)
		warnings = append(warnings, branchWarnings...)
	}
	return requirements, warnings
}

func appendWorkBranchOperationRequirements(requirements []plan.AdapterRequirement, resolved recipeResolved, branch workBranch, operations map[string]workOperation) ([]plan.AdapterRequirement, []plan.Diagnostic) {
	var warnings []plan.Diagnostic
	requiredBy := firstNonEmpty(branch.Name, "branch")
	for _, id := range branch.Operations {
		operation, ok := operations[id]
		if !ok {
			continue
		}
		switch operation.Kind {
		case plan.OpDecode:
			codecID, ok := workOperationDecodeCodec(resolved, branch, operation)
			if !ok || codecID == "" {
				warnings = append(warnings, plan.Diagnostic{
					Code:    string(errcode.DecodeCodecDeferred),
					Node:    requiredBy,
					Message: "decode codec will be resolved when the input opens",
					Suggestions: []string{
						"declare the provider codec intent when the receive codec is already known",
						"use input metadata or a format adapter that reports streams during probing",
					},
				})
				continue
			}
			requirements = appendAdapterRequirement(requirements, codecAdapterRequirement(resolved.runtime, "decoder", codecID, requiredBy))
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
			requirements = appendAdapterRequirement(requirements, codecAdapterRequirement(resolved.runtime, "encoder", codecID, requiredBy))
		}
	}
	return requirements, warnings
}

func workOperationDecodeCodec(resolved recipeResolved, branch workBranch, operation workOperation) (av.CodecID, bool) {
	if operation.Codec.ID != "" {
		return operation.Codec.ID, true
	}
	if codecID, _, _, ok := copyMuxStreamFacts(branch, resolved.intent, resolved.inputProbes, resolved.branchInputProbe, resolved.branchInputProbeReady); ok {
		return codecID, true
	}
	codecID := av.CodecID(operation.Component)
	if codecID == "" || codecID == "decoder" {
		return "", false
	}
	return codecID, true
}

func workOperationEncodeCodec(operation workOperation) av.CodecID {
	if operation.Codec.ID != "" {
		return operation.Codec.ID
	}
	codecID := av.CodecID(operation.Component)
	if codecID == "" || codecID == "encoder" {
		return ""
	}
	return codecID
}

func adapterRequirementRuntimeStatus(rt Runtime, kind string, formatID av.FormatID, codecID av.CodecID, transform string) string {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return "required"
	}
	switch kind {
	case "demuxer":
		if formatID == "" {
			return "unknown"
		}
		if _, err := standard.formats.DemuxerFactory(formatID); err != nil {
			return "missing"
		}
		return "available"
	case "muxer":
		if formatID == "" {
			return "unknown"
		}
		if _, err := standard.formats.MuxerFactory(formatID); err != nil {
			return "missing"
		}
		return "available"
	case "decoder":
		_, err := standard.codecs.DecoderFactory(codecID)
		return codecFactoryStatus(err)
	case "encoder":
		_, err := standard.codecs.EncoderFactory(codecID)
		return codecFactoryStatus(err)
	case "filter":
		if transform == "" {
			return "unknown"
		}
		if _, err := standard.filters.Factory(transform); err != nil {
			return "missing"
		}
		return "available"
	default:
		return "required"
	}
}

func filterAdapterRequirement(rt Runtime, name string, requiredBy string) plan.AdapterRequirement {
	requirement := plan.AdapterRequirement{
		Kind:       "filter",
		Name:       name,
		Transform:  name,
		RequiredBy: requiredBy,
		Status:     adapterRequirementRuntimeStatus(rt, "filter", "", "", name),
	}
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return requirement
	}
	desc, err := standard.filters.Descriptor(name)
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
	requirement.Metadata = cloneMetadata(desc.Metadata)
	return requirement
}

func formatAdapterRequirement(rt Runtime, kind string, formatID av.FormatID, requiredBy string) plan.AdapterRequirement {
	requirement := plan.AdapterRequirement{
		Kind:       kind,
		Name:       string(formatID),
		Format:     formatID,
		RequiredBy: requiredBy,
		Status:     adapterRequirementRuntimeStatus(rt, kind, formatID, "", ""),
	}
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return requirement
	}
	var desc format.Descriptor
	var err error
	switch kind {
	case "demuxer":
		desc, err = standard.formats.DemuxerDescriptor(formatID)
	case "muxer":
		desc, err = standard.formats.MuxerDescriptor(formatID)
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
	requirement.Metadata = cloneMetadata(desc.Metadata)
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

func codecAdapterRequirement(rt Runtime, kind string, codecID av.CodecID, requiredBy string) plan.AdapterRequirement {
	requirement := plan.AdapterRequirement{
		Kind:       kind,
		Name:       string(codecID),
		Codec:      codecID,
		RequiredBy: requiredBy,
		Status:     adapterRequirementRuntimeStatus(rt, kind, "", codecID, ""),
	}
	descriptors := codecDescriptorsForRequirement(rt, kind, codecID)
	return applyCodecDescriptorRequirement(requirement, descriptors)
}

func codecDescriptorsForRequirement(rt Runtime, kind string, codecID av.CodecID) []codec.Descriptor {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil || codecID == "" {
		return nil
	}
	mode := codec.ModeDecode
	if kind == "encoder" {
		mode = codec.ModeEncode
	}
	descriptors, err := standard.codecs.Find(codecID, mode)
	if err != nil {
		return nil
	}
	return descriptors
}

func applyCodecDescriptorRequirement(requirement plan.AdapterRequirement, descriptors []codec.Descriptor) plan.AdapterRequirement {
	for i := range descriptors {
		if descriptors[i].Type != "" && !mediaAllowed(requirement.Media, descriptors[i].Type) {
			requirement.Media = append(requirement.Media, descriptors[i].Type)
		}
		requirement.SampleFormats = mergeStringList(requirement.SampleFormats, descriptors[i].Capabilities.SampleFormats)
		requirement.PixelFormats = mergeStringList(requirement.PixelFormats, descriptors[i].Capabilities.PixelFormats)
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
			Code:    string(errcode.ExplainPreflightError),
			Message: err.Error(),
		})
		return
	}
	report.Warnings = appendPlanDiagnostics(report.Warnings, plan.Diagnostic{
		Code:        string(buildErr.Code),
		Node:        buildErr.Node,
		Message:     buildErr.Reason,
		Details:     append([]string(nil), buildErr.Details...),
		Suggestions: append([]string(nil), buildErr.Suggestions...),
	})
	requirement, ok := adapterRequirementFromBuildError(buildErr)
	if !ok {
		if buildErr.Code == errcode.DestinationMuxIncompatible {
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
	details := buildErrorDetailMap(err.Details)
	status := adapterRequirementStatus(err.Code)
	requiredBy := firstNonEmpty(err.Node, err.Operation)
	switch err.Code {
	case errcode.InputDemuxerMissing:
		formatID := av.FormatID(details["format"])
		return plan.AdapterRequirement{
			Kind:       "demuxer",
			Name:       string(formatID),
			Format:     formatID,
			RequiredBy: requiredBy,
			Status:     status,
		}, formatID != ""
	case errcode.OutputMuxerMissing, errcode.DestinationMuxerMissing:
		formatID := av.FormatID(details["format"])
		return plan.AdapterRequirement{
			Kind:       "muxer",
			Name:       string(formatID),
			Format:     formatID,
			RequiredBy: requiredBy,
			Status:     status,
		}, formatID != ""
	case errcode.DecodeAdapterMissing, errcode.DecodeAdapterUnavailable, errcode.DecodeAdapterIncompatible:
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
	case errcode.EncodeAdapterMissing, errcode.EncodeAdapterUnavailable, errcode.EncodeAdapterIncompatible:
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
	case errcode.TransformAdapterMissing, errcode.TransformAdapterIncompatible:
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

func cloneMetadata(metadata av.Metadata) av.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(av.Metadata, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
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

func buildErrorDetailMap(details []string) map[string]string {
	out := make(map[string]string, len(details))
	for i := range details {
		key, value, ok := strings.Cut(details[i], "=")
		if !ok || key == "" {
			continue
		}
		out[key] = value
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

func explainSummary(intentName string, report plan.Report) string {
	name := firstNonEmpty(intentName, "job")
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

func cloneOperationSpecs(operations []operationSpec) []operationSpec {
	if len(operations) == 0 {
		return nil
	}
	out := make([]operationSpec, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Transform = cloneTransformSpec(operation.Transform)
		if gate, ok := operation.Stage.(*syncGate); ok {
			operation.Stage = newSyncGate(gate.policy)
		}
		if operation.Auto != nil {
			policy := *operation.Auto
			operation.Auto = &policy
		}
		if operation.Require != nil {
			required := *operation.Require
			operation.Require = &required
		}
		if operation.Prefer != nil {
			preferred := *operation.Prefer
			operation.Prefer = &preferred
		}
		out = append(out, operation)
	}
	return out
}
