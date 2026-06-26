// Package recipeir contains immutable data snapshots of the public recipe
// grammar. It deliberately imports only leaf vocabulary packages, so root
// builders can lower into this package before the planner starts.
package recipeir

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// Kind identifies the public grammar form captured by a Recipe.
type Kind string

const (
	// KindJob is a normal From(...).Audio/Video/... recipe.
	KindJob Kind = "job"
	// KindJoin is a Mix/Composite/Select/custom join recipe.
	KindJoin Kind = "join"
	// KindBranchComposition is a planned-branch fanout recipe.
	KindBranchComposition Kind = "branch-composition"
)

// Recipe is the immutable recipe data boundary between fluent grammar builders
// and planner passes.
type Recipe struct {
	Kind         Kind
	Name         string
	Inputs       []Input
	Streams      []Stream
	Destinations []Destination
	Join         Join
	StreamRules  []StreamRule
	Policies     Policies
	Copy         bool
}

// InputKind identifies the planner-visible behavior of an input without
// carrying the concrete reader, source provider, or callback across the recipe
// boundary.
type InputKind string

const (
	InputKindUnknown      InputKind = ""
	InputKindByteStream   InputKind = "byte-stream"
	InputKindProvider     InputKind = "provider"
	InputKindCustomSource InputKind = "custom-source"
)

// Input is the planner-visible data for one recipe input.
type Input struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Codec    codec.CodecSpec
	Realtime bool
	Kind     InputKind
	// SourceShape is set for provider and custom-source inputs whose stream
	// shape is known before opening runtime resources.
	SourceShape shape.Spec
}

// Stream is one selected stream chain with ordered operations and output refs.
type Stream struct {
	Name        string
	Selector    plan.StreamSelect
	From        TapRef
	Operations  []Operation
	CodecChange CodecChangePolicy
	Outputs     []OutputRef
}

// OutputRef names a destination declared on the recipe.
type OutputRef string

// DestinationKind identifies the planner-visible behavior of a destination
// without carrying the concrete writer, sink, or provider object across the
// recipe boundary.
type DestinationKind string

const (
	DestinationKindUnknown    DestinationKind = ""
	DestinationKindByteStream DestinationKind = "byte-stream"
	DestinationKindSink       DestinationKind = "sink"
)

// Destination is the planner-visible data for one output.
type Destination struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
	Kind     DestinationKind
}

// StreamRule is the planner-visible summary of a dynamic-stream rule. It does
// not carry executable branch specs or custom matcher functions; the runtime
// still owns those until mutation patches cross the recipe boundary.
type StreamRule struct {
	MatchDescription  string
	Branches          []StreamRuleBranch
	RemoveDisposition string
}

// StreamRuleBranch is one branch template visible to Explain/validation.
type StreamRuleBranch struct {
	Name         string
	Destinations []string
}

// Join summarizes the planner-visible shape of a convergence recipe. It keeps
// validation and diagnostics off the executable root joinSpec while the lowerer
// continues to own concrete arms, stages, and destination handles.
type Join struct {
	Kind             string
	ArmCount         int
	InputCount       int
	DestinationCount int
	BranchCount      int
	OperationCount   int
	TapCount         int
	HasEncode        bool
	Custom           bool
}

// Policies are recipe-wide planning flags.
type Policies struct {
	Realtime bool
}

// TransformKind identifies the concrete frame transform carried by Transform.
type TransformKind string

const (
	TransformNone     TransformKind = ""
	TransformResize   TransformKind = "resize"
	TransformResample TransformKind = "resample"
)

// Transform is one planner-visible transform config without depending on the
// root transformSpec wrapper.
type Transform struct {
	Kind     TransformKind
	Resize   filter.ResizeConfig
	Resample filter.ResampleConfig
}

// Operation is one normalized stream operation.
type Operation struct {
	Kind      plan.OperationKind
	Component string
	Detail    string
	Stage     pipeline.Stage
	Shape     shape.Spec
	Transform Transform
	Tap       Tap
	Decode    codec.CodecSpec
	Encode    codec.CodecSpec
	Shared    bool
	Auto      *shape.Policy
	Require   *shape.Spec
	Prefer    *shape.Spec
	Path      Path
}

// Tap is the typed anchor data carried by tap operations.
type Tap struct {
	Name      string
	MediaKind av.MediaType
	Domain    shape.MediaDomain
	After     plan.OperationKind
}

// TapRef identifies a stream source tap.
type TapRef struct {
	Name   string
	Domain shape.MediaDomain
}

// CodecChangePolicy mirrors the recipe codec-change data without importing
// the root package.
type CodecChangePolicy struct {
	RebindCompatible     bool
	RequestKeyframe      bool
	DropUntilSync        bool
	FailOnDifferentCodec bool
}

// Path is a stable recipe location for future diagnostics.
type Path string
