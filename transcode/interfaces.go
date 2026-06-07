package transcode

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type Plan struct {
	Name     string
	Input    format.Input
	Branches []Branch
	Outputs  []Output
	Metadata av.Metadata
}

type Branch struct {
	Name     string
	Selector av.StreamSelector
	Decode   bool
	Steps    []Step
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
	Encode   codec.EncodeConfig
	Labels   []string
	Metadata av.Metadata
}

type Step struct {
	Stage    pipeline.Stage
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
}

type Output struct {
	Name           string
	Target         format.Output
	Sink           pipeline.Sink
	Format         av.FormatID
	Branches       []string
	Metadata       av.Metadata
	resolvedFormat av.FormatID
}

// ResolveOutputFormat returns output with an internal mux-open format resolved
// by a recipe compiler while preserving Format for explicit user intent.
func ResolveOutputFormat(output Output, format av.FormatID) Output {
	output.resolvedFormat = format
	return output
}

// OpenFormat returns the concrete mux format to use when opening the output.
func (o Output) OpenFormat() av.FormatID {
	if o.resolvedFormat != "" {
		return o.resolvedFormat
	}
	return o.Format
}
