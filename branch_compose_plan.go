package goav

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
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
