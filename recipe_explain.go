package goav

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type PlanReport struct {
	Summary          string
	Operation        string
	Intent           Intent
	Inputs           []InputReport
	Streams          []StreamReport
	Taps             []TapReport
	Branches         []BranchReport
	Targets          []TargetReport
	Decisions        []Decision
	Missing          []Requirement
	RequiredAdapters []AdapterRequirement
	Warnings         []PlanDiagnostic
	Graph            pipeline.Spec
}

type InputReport struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Realtime bool
	Format   av.FormatID
	Codec    av.CodecID
	Streams  []av.Stream
}

type StreamReport struct {
	Name        string
	Select      StreamSelect
	Decode      bool
	Operations  []OperationReport
	Transforms  []TransformReport
	Encode      CodecSpec
	CodecChange CodecChangePolicy
	Targets     []string
}

type TransformReport struct {
	Kind     string
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
}

type TargetReport struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
	Kind     string
	Branches []string
}

type BranchReport struct {
	Name       string
	Input      string
	Stream     StreamSelect
	Operations []OperationReport
	Targets    []string
}

type OperationReport struct {
	Kind      OperationKind
	Component string
	Detail    string
}

type Decision struct {
	Code    string
	Branch  string
	Message string
}

type Requirement struct {
	Kind       string
	Name       string
	RequiredBy string
	Status     string
}

type AdapterRequirement struct {
	Kind       string
	Name       string
	Format     av.FormatID
	Codec      av.CodecID
	Transform  string
	Input      av.MediaType
	Output     av.MediaType
	Realtime   bool
	Stateless  bool
	Metadata   av.Metadata
	RequiredBy string
	Status     string
}

type PlanDiagnostic struct {
	Code        string
	Node        string
	Message     string
	Details     []string
	Suggestions []string
}

func (j *Job) Explain(ctx context.Context) (PlanReport, error) {
	resolved, err := compileJobRecipeForBuildContext(ctx, j)
	if err == nil {
		return newPlanReport("build job", resolved)
	}
	fallback, fallbackErr := compileJobRecipe(j)
	if fallbackErr != nil {
		return PlanReport{}, err
	}
	report, reportErr := newPlanReport("build job", fallback)
	if reportErr != nil {
		return PlanReport{}, err
	}
	annotatePlanReportError(&report, err)
	return report, err
}

func newPlanReport(operation string, resolved recipeResolved) (PlanReport, error) {
	graph, err := resolved.Describe()
	if err != nil {
		return PlanReport{}, err
	}
	report := PlanReport{
		Operation: operation,
		Intent:    cloneIntent(resolved.intent),
		Graph:     graph,
	}
	report.Inputs = explainInputs(resolved)
	report.Streams = explainStreams(resolved.intent.Streams)
	report.Taps = explainTaps(resolved.mediaPlan.Taps)
	report.Branches = explainBranches(resolved.mediaPlan.Branches)
	report.Targets = explainTargets(resolved.intent.Targets, resolved.outputFormats, resolved.mediaPlan.Outputs)
	report.Decisions = explainDecisions(resolved.mediaPlan.Decisions)
	report.RequiredAdapters, report.Warnings = explainRequirements(resolved, report)
	report.Warnings = appendPlanDiagnostics(report.Warnings, muxCompatibilityDiagnostics(resolvedMuxCompatibilityIssues(resolved))...)
	report.Summary = explainSummary(report)
	return report, nil
}

func explainTaps(taps []planTap) []TapReport {
	reports := make([]TapReport, 0, len(taps))
	for i := range taps {
		reports = append(reports, TapReport{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			Caps:      taps[i].Caps,
			Node:      taps[i].Node,
		})
	}
	return reports
}

func explainInputs(resolved recipeResolved) []InputReport {
	inputs := resolved.intent.Inputs
	reports := make([]InputReport, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		report := InputReport{
			Name:     input.Name,
			URI:      input.URI,
			Protocol: input.Protocol,
			MIMEType: input.MIMEType,
			Realtime: input.Realtime,
			Codec:    input.Codec.ID,
		}
		if input.Protocol == av.ProtocolRTP {
			report.Format = av.FormatRTP
		}
		if input.Protocol == av.ProtocolWebRTC {
			report.Format = av.FormatWebRTC
		}
		probe, ok := resolved.inputProbe(i)
		if ok {
			report.Format = probe.Format
			report.Streams = append([]av.Stream(nil), probe.Streams...)
		}
		if len(report.Streams) == 0 && input.Realtime && input.Codec.ID != "" {
			report.Streams = liveIntentStreams([]InputIntent{input})
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

func explainStreams(streams []StreamIntent) []StreamReport {
	reports := make([]StreamReport, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		reports = append(reports, StreamReport{
			Name:        stream.Name,
			Select:      stream.Select,
			Decode:      stream.Decode,
			Operations:  explainStreamOperations(stream.Operations),
			Transforms:  explainTransforms(stream.Transforms),
			Encode:      stream.Encode,
			CodecChange: stream.CodecChange,
			Targets:     append([]string(nil), stream.Targets...),
		})
	}
	return reports
}

func explainStreamOperations(operations []StreamOperation) []OperationReport {
	reports := make([]OperationReport, 0, len(operations))
	for i := range operations {
		reports = append(reports, OperationReport{
			Kind:      operations[i].Kind,
			Component: operations[i].Component,
			Detail:    streamOperationDetail(operations[i]),
		})
	}
	return reports
}

func streamOperationDetail(operation StreamOperation) string {
	switch operation.Kind {
	case OpTransform:
		return firstNonEmpty(transformFactoryName(operation.Transform), "transform frames")
	case OpStage:
		return "custom stage"
	case OpTap:
		return "named media outlet"
	case OpEncode:
		return "frames to packets"
	case OpDecode:
		return "packets to frames"
	default:
		return ""
	}
}

func explainBranches(branches []planBranch) []BranchReport {
	reports := make([]BranchReport, 0, len(branches))
	for i := range branches {
		branch := branches[i]
		reports = append(reports, BranchReport{
			Name:       branch.Name,
			Input:      branch.Input,
			Stream:     branch.Stream,
			Operations: explainOperations(branch.Operations),
			Targets:    append([]string(nil), branch.Outputs...),
		})
	}
	return reports
}

func explainOperations(operations []planOperation) []OperationReport {
	reports := make([]OperationReport, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		reports = append(reports, OperationReport{
			Kind:      operation.Kind,
			Component: operation.Component,
			Detail:    operation.Detail,
		})
	}
	return reports
}

func explainDecisions(decisions []planDecision) []Decision {
	reports := make([]Decision, 0, len(decisions))
	for i := range decisions {
		decision := decisions[i]
		reports = append(reports, Decision{
			Code:    decision.Code,
			Branch:  decision.Branch,
			Message: decision.Message,
		})
	}
	return reports
}

func explainTransforms(transforms []TransformSpec) []TransformReport {
	reports := make([]TransformReport, 0, len(transforms))
	for i := range transforms {
		transform := transforms[i]
		report := TransformReport{}
		if transform.Resize != nil {
			resize := *transform.Resize
			report.Kind = filter.FactoryResize
			report.Resize = &resize
		}
		if transform.Resample != nil {
			resample := *transform.Resample
			report.Kind = filter.FactoryResample
			report.Resample = &resample
		}
		reports = append(reports, report)
	}
	return reports
}

func explainTargets(targets []TargetIntent, outputFormats map[string]av.FormatID, planOutputs []planOutput) []TargetReport {
	reports := make([]TargetReport, 0, len(targets))
	branchesByTarget := planOutputBranches(planOutputs)
	for i := range targets {
		target := targets[i]
		name := firstNonEmpty(target.Name, target.URI, fmt.Sprintf("target-%d", i))
		formatID := target.Format
		if resolved := outputFormats[name]; resolved != "" {
			formatID = resolved
		}
		kind := "sink"
		if target.URI != "" || target.Protocol != "" || target.MIMEType != "" || formatID != "" {
			kind = "mux"
		}
		reports = append(reports, TargetReport{
			Name:     name,
			URI:      target.URI,
			Protocol: target.Protocol,
			MIMEType: target.MIMEType,
			Format:   formatID,
			Kind:     kind,
			Branches: append([]string(nil), branchesByTarget[name]...),
		})
	}
	return reports
}

func planOutputBranches(outputs []planOutput) map[string][]string {
	branches := make(map[string][]string, len(outputs))
	for i := range outputs {
		if len(outputs[i].BranchRefs) == 0 {
			continue
		}
		branches[outputs[i].Name] = append([]string(nil), outputs[i].BranchRefs...)
	}
	return branches
}

func explainRequirements(resolved recipeResolved, report PlanReport) ([]AdapterRequirement, []PlanDiagnostic) {
	var requirements []AdapterRequirement
	var warnings []PlanDiagnostic
	for i := range report.Inputs {
		input := report.Inputs[i]
		switch {
		case input.Format != "" && input.Format != av.FormatRTP && input.Format != av.FormatWebRTC:
			requirements = appendAdapterRequirement(requirements, AdapterRequirement{
				Kind:       "demuxer",
				Name:       string(input.Format),
				Format:     input.Format,
				RequiredBy: firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", i)),
				Status:     adapterRequirementRuntimeStatus(resolved.runtime, "demuxer", input.Format, "", ""),
			})
		case input.Realtime && input.Codec != "":
			requirements = appendAdapterRequirement(requirements, AdapterRequirement{
				Kind:       "rtp-depacketizer",
				Name:       string(input.Codec),
				Codec:      input.Codec,
				RequiredBy: firstNonEmpty(input.Name, fmt.Sprintf("input-%d", i)),
				Status:     "built-in",
			})
		}
	}
	for i := range report.Targets {
		output := report.Targets[i]
		if output.Kind != "mux" || output.Format == "" {
			continue
		}
		requirements = appendAdapterRequirement(requirements, AdapterRequirement{
			Kind:       "muxer",
			Name:       string(output.Format),
			Format:     output.Format,
			RequiredBy: firstNonEmpty(output.Name, output.URI, fmt.Sprintf("output-%d", i)),
			Status:     adapterRequirementRuntimeStatus(resolved.runtime, "muxer", output.Format, "", ""),
		})
	}
	for i := range report.Branches {
		branch := report.Branches[i]
		stream, streamOK := reportStreamForBranch(resolved.intent.Streams, branch, i)
		var branchWarnings []PlanDiagnostic
		requirements, branchWarnings = appendBranchOperationRequirements(requirements, resolved, branch, stream, streamOK)
		warnings = append(warnings, branchWarnings...)
	}
	return requirements, warnings
}

func appendBranchOperationRequirements(requirements []AdapterRequirement, resolved recipeResolved, branch BranchReport, stream StreamIntent, streamOK bool) ([]AdapterRequirement, []PlanDiagnostic) {
	var warnings []PlanDiagnostic
	requiredBy := firstNonEmpty(branch.Name, "branch")
	for i := range branch.Operations {
		operation := branch.Operations[i]
		switch operation.Kind {
		case OpDecode:
			codecID, ok := operationDecodeCodec(resolved, stream, streamOK, operation)
			if !ok || codecID == "" {
				warnings = append(warnings, PlanDiagnostic{
					Code:    "decode_codec_deferred",
					Node:    requiredBy,
					Message: "decode codec will be resolved when the input opens",
					Suggestions: []string{
						"use RTP/WebRTC codec intent when the receive codec is already known",
						"use input metadata or a format adapter that reports streams during probing",
					},
				})
				continue
			}
			requirements = appendAdapterRequirement(requirements, AdapterRequirement{
				Kind:       "decoder",
				Name:       string(codecID),
				Codec:      codecID,
				RequiredBy: requiredBy,
				Status:     adapterRequirementRuntimeStatus(resolved.runtime, "decoder", "", codecID, ""),
			})
		case OpTransform:
			name := operation.Component
			if name == "" || name == "transform" {
				continue
			}
			requirements = appendAdapterRequirement(requirements, filterAdapterRequirement(resolved.runtime, name, requiredBy))
		case OpEncode:
			codecID := operationEncodeCodec(stream, streamOK, operation)
			if codecID == "" {
				continue
			}
			requirements = appendAdapterRequirement(requirements, AdapterRequirement{
				Kind:       "encoder",
				Name:       string(codecID),
				Codec:      codecID,
				RequiredBy: requiredBy,
				Status:     adapterRequirementRuntimeStatus(resolved.runtime, "encoder", "", codecID, ""),
			})
		}
	}
	return requirements, warnings
}

func reportStreamForBranch(streams []StreamIntent, branch BranchReport, index int) (StreamIntent, bool) {
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

func reportBranchNameForStream(stream StreamIntent, index int) string {
	return firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("branch-%d", index))
}

func operationDecodeCodec(resolved recipeResolved, stream StreamIntent, streamOK bool, operation OperationReport) (av.CodecID, bool) {
	if streamOK {
		if codecID, ok := reportDecodeCodec(resolved, stream); ok {
			return codecID, true
		}
	}
	codecID := av.CodecID(operation.Component)
	if codecID == "" || codecID == "decoder" {
		return "", false
	}
	return codecID, true
}

func operationEncodeCodec(stream StreamIntent, streamOK bool, operation OperationReport) av.CodecID {
	if streamOK && stream.Encode.ID != "" {
		return stream.Encode.ID
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

func filterAdapterRequirement(rt Runtime, name string, requiredBy string) AdapterRequirement {
	requirement := AdapterRequirement{
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
	requirement.Realtime = desc.Realtime
	requirement.Stateless = desc.Stateless
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

func reportDecodeCodec(resolved recipeResolved, stream StreamIntent) (av.CodecID, bool) {
	if codecID, ok := liveDecodeCodec(resolved.intent.Inputs, stream); ok {
		return codecID, true
	}
	if resolved.branchInputProbeReady {
		return knownProbeDecodeCodec([]format.ProbeResult{resolved.branchInputProbe}, stream)
	}
	return knownProbeDecodeCodec(resolved.inputProbes, stream)
}

func appendAdapterRequirement(requirements []AdapterRequirement, requirement AdapterRequirement) []AdapterRequirement {
	key := adapterRequirementKey(requirement)
	for i := range requirements {
		if adapterRequirementKey(requirements[i]) == key {
			return requirements
		}
	}
	return append(requirements, requirement)
}

func upsertAdapterRequirement(requirements []AdapterRequirement, requirement AdapterRequirement) []AdapterRequirement {
	key := adapterRequirementKey(requirement)
	for i := range requirements {
		if adapterRequirementKey(requirements[i]) == key {
			requirements[i] = requirement
			return requirements
		}
	}
	return append(requirements, requirement)
}

func adapterRequirementKey(requirement AdapterRequirement) string {
	return requirement.Kind + "|" + string(requirement.Format) + "|" + string(requirement.Codec) + "|" + requirement.Transform + "|" + requirement.RequiredBy
}

func annotatePlanReportError(report *PlanReport, err error) {
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr == nil {
		report.Warnings = appendPlanDiagnostics(report.Warnings, PlanDiagnostic{
			Code:    "explain_preflight_error",
			Message: err.Error(),
		})
		return
	}
	report.Warnings = appendPlanDiagnostics(report.Warnings, PlanDiagnostic{
		Code:        buildErr.Code,
		Node:        buildErr.Node,
		Message:     buildErr.Reason,
		Details:     append([]string(nil), buildErr.Details...),
		Suggestions: append([]string(nil), buildErr.Suggestions...),
	})
	requirement, ok := adapterRequirementFromBuildError(buildErr)
	if !ok {
		if buildErr.Code == "target_mux_incompatible" {
			return
		}
		report.Missing = append(report.Missing, Requirement{
			Kind:       firstNonEmpty(buildErr.Code, "requirement"),
			Name:       firstNonEmpty(buildErr.Node, buildErr.Reason),
			RequiredBy: buildErr.Node,
			Status:     "failed",
		})
		return
	}
	report.RequiredAdapters = upsertAdapterRequirement(report.RequiredAdapters, requirement)
	report.Missing = appendMissingRequirement(report.Missing, Requirement{
		Kind:       requirement.Kind,
		Name:       firstNonEmpty(requirement.Name, string(requirement.Format), string(requirement.Codec), requirement.Transform),
		RequiredBy: requirement.RequiredBy,
		Status:     requirement.Status,
	})
}

func adapterRequirementFromBuildError(err *BuildError) (AdapterRequirement, bool) {
	details := buildErrorDetailMap(err.Details)
	status := adapterRequirementStatus(err.Code)
	requiredBy := firstNonEmpty(err.Node, err.Operation)
	switch err.Code {
	case "input_demuxer_missing":
		formatID := av.FormatID(details["format"])
		return AdapterRequirement{
			Kind:       "demuxer",
			Name:       string(formatID),
			Format:     formatID,
			RequiredBy: requiredBy,
			Status:     status,
		}, formatID != ""
	case "output_muxer_missing", "target_muxer_missing":
		formatID := av.FormatID(details["format"])
		return AdapterRequirement{
			Kind:       "muxer",
			Name:       string(formatID),
			Format:     formatID,
			RequiredBy: requiredBy,
			Status:     status,
		}, formatID != ""
	case "decode_adapter_missing", "decode_adapter_unavailable":
		codecID := av.CodecID(details["codec"])
		return AdapterRequirement{
			Kind:       "decoder",
			Name:       string(codecID),
			Codec:      codecID,
			RequiredBy: requiredBy,
			Status:     status,
		}, codecID != ""
	case "encode_adapter_missing", "encode_adapter_unavailable":
		codecID := av.CodecID(details["codec"])
		return AdapterRequirement{
			Kind:       "encoder",
			Name:       string(codecID),
			Codec:      codecID,
			RequiredBy: requiredBy,
			Status:     status,
		}, codecID != ""
	case "transform_adapter_missing":
		name := details["transform"]
		requirement := filterAdapterRequirement(nil, name, requiredBy)
		requirement.Status = status
		return requirement, name != ""
	default:
		if strings.HasSuffix(err.Code, "_format_unknown") {
			return AdapterRequirement{
				Kind:       "format-prober",
				Name:       firstNonEmpty(err.Node, "format"),
				RequiredBy: requiredBy,
				Status:     "unknown",
			}, true
		}
		return AdapterRequirement{}, false
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

func adapterRequirementStatus(code string) string {
	switch {
	case strings.HasSuffix(code, "_unavailable"):
		return "unavailable"
	case strings.HasSuffix(code, "_unknown"):
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

func appendMissingRequirement(requirements []Requirement, requirement Requirement) []Requirement {
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

func appendPlanDiagnostics(diagnostics []PlanDiagnostic, next ...PlanDiagnostic) []PlanDiagnostic {
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

func explainSummary(report PlanReport) string {
	name := firstNonEmpty(report.Intent.Name, "job")
	input := "input"
	if len(report.Inputs) == 1 {
		input = firstNonEmpty(report.Inputs[0].Name, report.Inputs[0].URI, input)
	} else if len(report.Inputs) > 1 {
		input = fmt.Sprintf("%d inputs", len(report.Inputs))
	}
	target := "target"
	if len(report.Targets) == 1 {
		target = firstNonEmpty(report.Targets[0].Name, report.Targets[0].URI, target)
	} else if len(report.Targets) > 1 {
		target = fmt.Sprintf("%d targets", len(report.Targets))
	}
	if len(report.Streams) == 0 {
		return fmt.Sprintf("%s %s to %s", name, input, target)
	}
	return fmt.Sprintf("%s %s through %d stream branch(es) to %s", name, input, len(report.Streams), target)
}

func cloneIntent(intent Intent) Intent {
	clone := intent
	clone.Inputs = append([]InputIntent(nil), intent.Inputs...)
	clone.Streams = append([]StreamIntent(nil), intent.Streams...)
	for i := range clone.Streams {
		clone.Streams[i].Operations = cloneStreamOperations(intent.Streams[i].Operations)
		clone.Streams[i].Transforms = cloneTransformSpecs(intent.Streams[i].Transforms)
		clone.Streams[i].Taps = cloneTapIntents(intent.Streams[i].Taps)
		clone.Streams[i].Targets = append([]string(nil), intent.Streams[i].Targets...)
	}
	clone.Targets = append([]TargetIntent(nil), intent.Targets...)
	return clone
}

func cloneStreamOperations(operations []StreamOperation) []StreamOperation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]StreamOperation, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		operation.Transform = cloneTransformSpec(operation.Transform)
		out = append(out, operation)
	}
	return out
}

func cloneTapIntents(taps []TapIntent) []TapIntent {
	if len(taps) == 0 {
		return nil
	}
	return append([]TapIntent(nil), taps...)
}
