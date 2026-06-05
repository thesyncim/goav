package transcode

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type Plan struct {
	Name       string
	Input      format.Input
	Renditions []Rendition
	Outputs    []Output
	Metadata   av.Metadata
}

type Rendition struct {
	Name     string
	Selector av.StreamSelector
	Decode   bool
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
	Encode   codec.EncodeConfig
	Labels   []string
	Metadata av.Metadata
}

type Output struct {
	Name       string
	Target     format.Output
	Format     av.FormatID
	Renditions []string
	Metadata   av.Metadata
}

type Planner interface {
	Plan(context.Context, Request) (Plan, error)
}

type Request struct {
	Input    format.Input
	Ladder   Ladder
	Outputs  []format.Output
	Realtime bool
	Metadata av.Metadata
}

type Ladder struct {
	Name       string
	Renditions []Rendition
}

type Compiler interface {
	Compile(context.Context, Plan) (pipeline.Graph, error)
}
