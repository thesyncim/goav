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
// Every field is typed; Report and its members marshal to stable JSON so
// machine consumers can read the plan directly. Detail strings are a display
// fallback, never the machine surface.
type Report struct {
	Summary          string               `json:"summary,omitempty"`
	Operation        string               `json:"operation,omitempty"`
	Inputs           []Input              `json:"inputs,omitempty"`
	Streams          []Stream             `json:"streams,omitempty"`
	Taps             []Tap                `json:"taps,omitempty"`
	Branches         []Branch             `json:"branches,omitempty"`
	Destinations     []Destination        `json:"destinations,omitempty"`
	Decisions        []Decision           `json:"decisions,omitempty"`
	Missing          []Requirement        `json:"missing,omitempty"`
	RequiredAdapters []AdapterRequirement `json:"requiredAdapters,omitempty"`
	Warnings         []Diagnostic         `json:"warnings,omitempty"`
	Graph            pipeline.Spec        `json:"graph"`
}

// Input reports one job input: where it comes from and what it carries.
type Input struct {
	Name     string        `json:"name,omitempty"`
	URI      string        `json:"uri,omitempty"`
	Protocol av.ProtocolID `json:"protocol,omitempty"`
	MIMEType string        `json:"mimeType,omitempty"`
	Realtime bool          `json:"realtime,omitempty"`
	Format   av.FormatID   `json:"format,omitempty"`
	Codec    av.CodecID    `json:"codec,omitempty"`
	Streams  []av.Stream   `json:"streams,omitempty"`
}

// Stream reports one declared stream chain: its selection, decode intent,
// ordered operations, encode target, and destination bindings.
type Stream struct {
	Name         string       `json:"name,omitempty"`
	Select       StreamSelect `json:"select,omitzero"`
	Decode       bool         `json:"decode,omitempty"`
	Operations   []Operation  `json:"operations,omitempty"`
	Encode       codec.Spec   `json:"encode,omitzero"`
	Destinations []string     `json:"destinations,omitempty"`
}

// Destination reports one planned destination and the branches feeding it.
type Destination struct {
	Name     string        `json:"name,omitempty"`
	URI      string        `json:"uri,omitempty"`
	Protocol av.ProtocolID `json:"protocol,omitempty"`
	MIMEType string        `json:"mimeType,omitempty"`
	Format   av.FormatID   `json:"format,omitempty"`
	Kind     string        `json:"kind,omitempty"`
	Branches []string      `json:"branches,omitempty"`
}

// Branch reports one planned branch: its source binding, media shape, and
// ordered operations.
type Branch struct {
	Name         string       `json:"name,omitempty"`
	Input        string       `json:"input,omitempty"`
	Stream       StreamSelect `json:"stream,omitzero"`
	Shape        shape.Spec   `json:"shape,omitzero"`
	Operations   []Operation  `json:"operations,omitempty"`
	Destinations []string     `json:"destinations,omitempty"`
}

// Operation reports one planned operation row inside a stream or branch. Kind,
// Component, Shape, Adapter, and Config are the typed machine surface; Detail
// is the display fallback. Shape carries media kind, codec, format, and
// geometry; Adapter names the implementation the operation lowers to (a codec
// id, filter factory, or component name); Config carries small typed settings
// such as bitrate or geometry as stable key/value strings.
type Operation struct {
	Kind      OperationKind     `json:"kind,omitempty"`
	Component string            `json:"component,omitempty"`
	Detail    string            `json:"detail,omitempty"`
	Shape     shape.Spec        `json:"shape,omitzero"`
	Adapter   string            `json:"adapter,omitempty"`
	Config    map[string]string `json:"config,omitempty"`
	Shared    bool              `json:"shared,omitempty"`
}

// Tap reports one named media outlet the plan declares for runtime branches.
type Tap struct {
	Name      string            `json:"name,omitempty"`
	MediaKind av.MediaType      `json:"mediaKind,omitempty"`
	Domain    shape.MediaDomain `json:"domain,omitempty"`
	Shape     shape.Spec        `json:"shape,omitzero"`
	Node      pipeline.NodeRef  `json:"node,omitempty"`
}

// Decision records one planning decision and the branch it applies to.
type Decision struct {
	Code    string `json:"code,omitempty"`
	Branch  string `json:"branch,omitempty"`
	Message string `json:"message,omitempty"`
}

// Requirement reports one unmet prerequisite that blocks the plan.
type Requirement struct {
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name,omitempty"`
	RequiredBy string `json:"requiredBy,omitempty"`
	Status     string `json:"status,omitempty"`
}

// AdapterRequirement reports one adapter (demuxer, muxer, codec, filter) the
// plan needs, with the capability metadata known about it.
type AdapterRequirement struct {
	Kind          string              `json:"kind,omitempty"`
	Name          string              `json:"name,omitempty"`
	Format        av.FormatID         `json:"format,omitempty"`
	Codec         av.CodecID          `json:"codec,omitempty"`
	Media         []av.MediaType      `json:"media,omitempty"`
	Codecs        []av.CodecID        `json:"codecs,omitempty"`
	MinStreams    int                 `json:"minStreams,omitempty"`
	MaxStreams    int                 `json:"maxStreams,omitempty"`
	Transform     string              `json:"transform,omitempty"`
	Input         av.MediaType        `json:"input,omitempty"`
	Output        av.MediaType        `json:"output,omitempty"`
	PixelFormats  []string            `json:"pixelFormats,omitempty"`
	SampleFormats []string            `json:"sampleFormats,omitempty"`
	ResizeModes   []filter.ResizeMode `json:"resizeModes,omitempty"`
	Realtime      bool                `json:"realtime,omitempty"`
	Stateless     bool                `json:"stateless,omitempty"`
	Metadata      av.Metadata         `json:"metadata,omitempty"`
	RequiredBy    string              `json:"requiredBy,omitempty"`
	Status        string              `json:"status,omitempty"`
}

// Diagnostic is one structured warning attached to a plan.
type Diagnostic struct {
	Code        string   `json:"code,omitempty"`
	Node        string   `json:"node,omitempty"`
	Message     string   `json:"message,omitempty"`
	Details     []string `json:"details,omitempty"`
	Suggestions []string `json:"suggestions,omitempty"`
}
