package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/transcode"
)

type branchComposePlan struct {
	Name         string
	Input        format.Input
	Branches     []branchComposeBranch
	Destinations []branchComposeTarget
	Metadata     av.Metadata
}

type branchComposeBranch struct {
	Name              string
	Selector          av.StreamSelector
	Decode            bool
	Copy              bool
	Operations        []OperationSpec
	SharedOperations  []OperationSpec
	PrivateOperations []OperationSpec
	Resize            *filter.ResizeConfig
	Resample          *filter.ResampleConfig
	DecodeConfig      CodecSpec
	CodecChange       CodecChangePolicy
	Encode            codec.EncodeConfig
	Labels            []string
	Metadata          av.Metadata
}

type branchComposeTarget struct {
	Name           string
	Destination    destinationSpec
	Target         format.Output
	Sink           pipeline.Sink
	Format         av.FormatID
	Branches       []string
	Metadata       av.Metadata
	resolvedFormat av.FormatID
}

func (t branchComposeTarget) OpenFormat() av.FormatID {
	if t.resolvedFormat != "" {
		return t.resolvedFormat
	}
	return t.Format
}

func resolveBranchComposeTargetFormat(target branchComposeTarget, format av.FormatID) branchComposeTarget {
	target.resolvedFormat = format
	return target
}

func branchComposePlanFromTranscode(plan transcode.Plan) branchComposePlan {
	branches := make([]branchComposeBranch, 0, len(plan.Branches))
	for i := range plan.Branches {
		branch := plan.Branches[i]
		operations := branchOperationSpecsFromTranscodeSteps(branch.Steps)
		branches = append(branches, branchComposeBranch{
			Name:              branch.Name,
			Selector:          branch.Selector,
			Decode:            branch.Decode,
			Copy:              false,
			Operations:        cloneOperationSpecs(operations),
			PrivateOperations: operations,
			Resize:            branch.Resize,
			Resample:          branch.Resample,
			Encode:            branch.Encode,
			Labels:            append([]string(nil), branch.Labels...),
			Metadata:          branch.Metadata,
		})
	}
	destinations := make([]branchComposeTarget, 0, len(plan.Outputs))
	for i := range plan.Outputs {
		output := plan.Outputs[i]
		destinations = append(destinations, branchComposeTarget{
			Name:           output.Name,
			Target:         output.Target,
			Sink:           output.Sink,
			Format:         output.Format,
			Branches:       append([]string(nil), output.Branches...),
			Metadata:       output.Metadata,
			resolvedFormat: output.OpenFormat(),
		})
	}
	return branchComposePlan{
		Name:         plan.Name,
		Input:        plan.Input,
		Branches:     branches,
		Destinations: destinations,
		Metadata:     plan.Metadata,
	}
}

func branchOperationSpecsFromTranscodeSteps(steps []transcode.Step) []OperationSpec {
	if len(steps) == 0 {
		return nil
	}
	out := make([]OperationSpec, 0, len(steps))
	for i := range steps {
		transform := cloneTransformSpec(TransformSpec{
			Resize:   steps[i].Resize,
			Resample: steps[i].Resample,
		})
		switch {
		case steps[i].Stage != nil:
			out = append(out, operationSpecForStage(steps[i].Stage))
		case transform.Resize != nil || transform.Resample != nil:
			out = append(out, operationSpecForTransform(transform))
		default:
			out = append(out, OperationSpec{Kind: OpTransform, Component: "transform"})
		}
	}
	return out
}
