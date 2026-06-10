// Package plan holds the structured Explain tree: the report a job or task
// returns about what it will do, which adapters it needs, and the decisions
// and diagnostics behind the plan. The write-side grammar (From, Mix,
// Branch, To, Control, constructors, options) lives in the goav root;
// everything Explain reports back lives here.
package plan

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Report is the structured report Explain returns: what the job will do,
// which adapters it needs, and the decisions and diagnostics behind the plan.
type Report struct {
	Summary          string
	Operation        string
	Inputs           []Input
	Streams          []Stream
	Taps             []Tap
	Branches         []Branch
	Destinations     []Destination
	Decisions        []Decision
	Missing          []Requirement
	RequiredAdapters []AdapterRequirement
	Warnings         []Diagnostic
	Graph            pipeline.Spec
}

// Input reports one job input: where it comes from and what it carries.
type Input struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Realtime bool
	Format   av.FormatID
	Codec    av.CodecID
	Streams  []av.Stream
}

// Stream reports one declared stream chain: its selection, decode intent,
// ordered operations, encode target, and destination bindings.
type Stream struct {
	Name         string
	Select       StreamSelect
	Decode       bool
	Operations   []Operation
	Encode       codec.CodecSpec
	Destinations []string
}

// Destination reports one planned destination and the branches feeding it.
type Destination struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
	Kind     string
	Branches []string
}

// Branch reports one planned branch: its source binding, media shape, and
// ordered operations.
type Branch struct {
	Name         string
	Input        string
	Stream       StreamSelect
	Shape        shape.Spec
	Operations   []Operation
	Destinations []string
}

// Operation reports one planned operation row inside a stream or branch.
type Operation struct {
	Kind      OperationKind
	Component string
	Detail    string
	Shape     shape.Spec
	Shared    bool
}

// Tap reports one named media outlet the plan declares for runtime branches.
type Tap struct {
	Name      string
	MediaKind av.MediaType
	Domain    shape.MediaDomain
	Shape     shape.Spec
	Node      pipeline.NodeRef
}

// Decision records one planning decision and the branch it applies to.
type Decision struct {
	Code    string
	Branch  string
	Message string
}

// Requirement reports one unmet prerequisite that blocks the plan.
type Requirement struct {
	Kind       string
	Name       string
	RequiredBy string
	Status     string
}

// AdapterRequirement reports one adapter (demuxer, muxer, codec, filter) the
// plan needs, with the capability metadata known about it.
type AdapterRequirement struct {
	Kind          string
	Name          string
	Format        av.FormatID
	Codec         av.CodecID
	Media         []av.MediaType
	Codecs        []av.CodecID
	MinStreams    int
	MaxStreams    int
	Transform     string
	Input         av.MediaType
	Output        av.MediaType
	PixelFormats  []string
	SampleFormats []string
	ResizeModes   []filter.ResizeMode
	Realtime      bool
	Stateless     bool
	Metadata      av.Metadata
	RequiredBy    string
	Status        string
}

// Diagnostic is one structured warning attached to a plan.
type Diagnostic struct {
	Code        string
	Node        string
	Message     string
	Details     []string
	Suggestions []string
}
