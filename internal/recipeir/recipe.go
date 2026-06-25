// Package recipeir contains immutable data snapshots of the public recipe
// grammar. It deliberately imports only leaf vocabulary packages, so root
// builders can lower into this package before the planner starts.
package recipeir

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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
	Policies     Policies
	Copy         bool
}

// Input is the planner-visible data for one recipe input.
type Input struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Codec    codec.CodecSpec
	Realtime bool
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

// Destination is the planner-visible data for one output.
type Destination struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
}

// Policies are recipe-wide planning flags.
type Policies struct {
	Realtime bool
}

// Operation is one normalized stream operation. Transform stays as an opaque
// config during the transition because the current root TransformSpec is still
// a pre-v1 exported pointer-union slated for the next simplification phases.
type Operation struct {
	Kind      plan.OperationKind
	Component string
	Detail    string
	Stage     pipeline.Stage
	Shape     shape.Spec
	Transform any
	Tap       Tap
	Decode    codec.CodecSpec
	Encode    codec.CodecSpec
	Shared    bool
	Auto      *shape.Policy
	Require   *shape.Spec
	Prefer    *shape.Spec
	Domain    shape.MediaDomain
	Config    any
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
