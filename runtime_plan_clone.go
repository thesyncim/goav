package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
)

// planTapRows projects live task taps onto the plan report's tap rows.
func planTapRows(taps []TapSnapshot) []plan.Tap {
	rows := make([]plan.Tap, 0, len(taps))
	for i := range taps {
		rows = append(rows, plan.Tap{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			Shape:     taps[i].Shape,
			Node:      taps[i].Node,
		})
	}
	return rows
}

func clonePlanReport(in plan.Report) plan.Report {
	out := in
	out.Inputs = clonePlanInputs(in.Inputs)
	out.Streams = clonePlanStreams(in.Streams)
	out.Taps = append([]plan.Tap(nil), in.Taps...)
	out.Branches = clonePlanBranches(in.Branches)
	out.Destinations = clonePlanDestinations(in.Destinations)
	out.Decisions = append([]plan.Decision(nil), in.Decisions...)
	out.Missing = append([]plan.Requirement(nil), in.Missing...)
	out.RequiredAdapters = clonePlanAdapterRequirements(in.RequiredAdapters)
	out.Warnings = compile.ClonePlanDiagnostics(in.Warnings)
	out.Graph = clonePipelineSpec(in.Graph)
	return out
}

func clonePlanInputs(inputs []plan.Input) []plan.Input {
	out := make([]plan.Input, len(inputs))
	for i := range inputs {
		out[i] = inputs[i]
		out[i].Streams = append([]av.Stream(nil), inputs[i].Streams...)
	}
	return out
}

func clonePlanStreams(streams []plan.Stream) []plan.Stream {
	out := make([]plan.Stream, len(streams))
	for i := range streams {
		out[i] = streams[i]
		out[i].Operations = append([]plan.Operation(nil), streams[i].Operations...)
		out[i].Destinations = append([]string(nil), streams[i].Destinations...)
		out[i].Encode.Settings.Custom = compile.CloneMetadata(streams[i].Encode.Settings.Custom)
	}
	return out
}

func clonePlanBranches(branches []plan.Branch) []plan.Branch {
	out := make([]plan.Branch, len(branches))
	for i := range branches {
		out[i] = branches[i]
		out[i].Operations = append([]plan.Operation(nil), branches[i].Operations...)
		out[i].Destinations = append([]string(nil), branches[i].Destinations...)
	}
	return out
}

func clonePlanDestinations(destinations []plan.Destination) []plan.Destination {
	out := make([]plan.Destination, len(destinations))
	for i := range destinations {
		out[i] = destinations[i]
		out[i].Branches = append([]string(nil), destinations[i].Branches...)
	}
	return out
}

func clonePlanAdapterRequirements(requirements []plan.AdapterRequirement) []plan.AdapterRequirement {
	out := make([]plan.AdapterRequirement, len(requirements))
	for i := range requirements {
		out[i] = requirements[i]
		out[i].Media = append([]av.MediaType(nil), requirements[i].Media...)
		out[i].Codecs = append([]av.CodecID(nil), requirements[i].Codecs...)
		out[i].PixelFormats = append([]av.PixelFormat(nil), requirements[i].PixelFormats...)
		out[i].SampleFormats = append([]av.SampleFormat(nil), requirements[i].SampleFormats...)
		out[i].ResizeModes = append([]filter.ResizeMode(nil), requirements[i].ResizeModes...)
		out[i].Metadata = compile.CloneMetadata(requirements[i].Metadata)
	}
	return out
}

func clonePipelineSpec(spec pipeline.Spec) pipeline.Spec {
	spec.Nodes = append([]pipeline.NodeSpec(nil), spec.Nodes...)
	spec.Edges = append([]pipeline.EdgeSpec(nil), spec.Edges...)
	return spec
}
