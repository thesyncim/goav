package goav

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav/av"
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
	Outputs          []OutputReport
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
	Transforms  []TransformReport
	Encode      CodecSpec
	CodecChange CodecChangePolicy
	RouteTo     []string
}

type TransformReport struct {
	Kind     string
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
}

type OutputReport struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
	Kind     string
}

type AdapterRequirement struct {
	Kind       string
	Name       string
	Format     av.FormatID
	Codec      av.CodecID
	Transform  string
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
	if err != nil {
		return PlanReport{}, err
	}
	return newPlanReport("build job", resolved)
}

func (j *TranscodeJob) Explain(ctx context.Context) (PlanReport, error) {
	resolved, err := compileTranscodeRecipeForBuildContext(ctx, j)
	if err != nil {
		return PlanReport{}, err
	}
	return newPlanReport(transcodeRecipeOperation, resolved)
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
	report.Outputs = explainOutputs(resolved.intent.Outputs, resolved.outputFormats)
	report.RequiredAdapters, report.Warnings = explainRequirements(resolved, report)
	report.Summary = explainSummary(report)
	return report, nil
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
	if r.transcodeInputProbeReady {
		if index == 0 {
			return r.transcodeInputProbe, true
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
			Transforms:  explainTransforms(stream.Transforms),
			Encode:      stream.Encode,
			CodecChange: stream.CodecChange,
			RouteTo:     append([]string(nil), stream.RouteTo...),
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

func explainOutputs(outputs []OutputIntent, outputFormats map[string]av.FormatID) []OutputReport {
	reports := make([]OutputReport, 0, len(outputs))
	for i := range outputs {
		output := outputs[i]
		formatID := output.Format
		if resolved := outputFormats[output.Name]; resolved != "" {
			formatID = resolved
		}
		kind := "sink"
		if output.URI != "" || output.Protocol != "" || output.MIMEType != "" || formatID != "" {
			kind = "mux"
		}
		reports = append(reports, OutputReport{
			Name:     output.Name,
			URI:      output.URI,
			Protocol: output.Protocol,
			MIMEType: output.MIMEType,
			Format:   formatID,
			Kind:     kind,
		})
	}
	return reports
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
				Status:     "available",
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
	for i := range report.Outputs {
		output := report.Outputs[i]
		if output.Kind != "mux" || output.Format == "" {
			continue
		}
		requirements = appendAdapterRequirement(requirements, AdapterRequirement{
			Kind:       "muxer",
			Name:       string(output.Format),
			Format:     output.Format,
			RequiredBy: firstNonEmpty(output.Name, output.URI, fmt.Sprintf("output-%d", i)),
			Status:     "available",
		})
	}
	for i := range resolved.intent.Streams {
		stream := resolved.intent.Streams[i]
		requiredBy := firstNonEmpty(stream.Name, string(stream.Select.Type), fmt.Sprintf("stream-%d", i))
		for j := range stream.Transforms {
			name := transformFactoryName(stream.Transforms[j])
			if name == "" {
				continue
			}
			requirements = appendAdapterRequirement(requirements, AdapterRequirement{
				Kind:       "filter",
				Name:       name,
				Transform:  name,
				RequiredBy: requiredBy,
				Status:     "available",
			})
		}
		if stream.Encode.ID != "" {
			requirements = appendAdapterRequirement(requirements, AdapterRequirement{
				Kind:       "encoder",
				Name:       string(stream.Encode.ID),
				Codec:      stream.Encode.ID,
				RequiredBy: requiredBy,
				Status:     "available",
			})
		}
		if !streamNeedsDecode(stream) {
			continue
		}
		codecID, ok := reportDecodeCodec(resolved, stream)
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
			Status:     "available",
		})
	}
	return requirements, warnings
}

func reportDecodeCodec(resolved recipeResolved, stream StreamIntent) (av.CodecID, bool) {
	if codecID, ok := liveDecodeCodec(resolved.intent.Inputs, stream); ok {
		return codecID, true
	}
	if resolved.transcodeInputProbeReady {
		return knownProbeDecodeCodec([]format.ProbeResult{resolved.transcodeInputProbe}, stream)
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

func adapterRequirementKey(requirement AdapterRequirement) string {
	return requirement.Kind + "|" + string(requirement.Format) + "|" + string(requirement.Codec) + "|" + requirement.Transform + "|" + requirement.RequiredBy
}

func explainSummary(report PlanReport) string {
	name := firstNonEmpty(report.Intent.Name, "job")
	input := "input"
	if len(report.Inputs) == 1 {
		input = firstNonEmpty(report.Inputs[0].Name, report.Inputs[0].URI, input)
	} else if len(report.Inputs) > 1 {
		input = fmt.Sprintf("%d inputs", len(report.Inputs))
	}
	output := "output"
	if len(report.Outputs) == 1 {
		output = firstNonEmpty(report.Outputs[0].Name, report.Outputs[0].URI, output)
	} else if len(report.Outputs) > 1 {
		output = fmt.Sprintf("%d outputs", len(report.Outputs))
	}
	if len(report.Streams) == 0 {
		return fmt.Sprintf("%s %s to %s", name, input, output)
	}
	return fmt.Sprintf("%s %s through %d stream branch(es) to %s", name, input, len(report.Streams), output)
}

func cloneIntent(intent Intent) Intent {
	clone := intent
	clone.Inputs = append([]InputIntent(nil), intent.Inputs...)
	clone.Streams = append([]StreamIntent(nil), intent.Streams...)
	for i := range clone.Streams {
		clone.Streams[i].Transforms = cloneTransformSpecs(intent.Streams[i].Transforms)
		clone.Streams[i].RouteTo = append([]string(nil), intent.Streams[i].RouteTo...)
	}
	clone.Outputs = append([]OutputIntent(nil), intent.Outputs...)
	return clone
}
