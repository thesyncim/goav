package transcode

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
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
