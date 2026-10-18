// Package recipeir is the recipe IR the value grammar compiles to and the
// planner reads: a DAG of inputs, selected streams, operations, joins, and
// destinations (Graph), and the branches derived from it (Streams). It
// deliberately imports only leaf vocabulary packages, so the planner's input
// is plain data; the concrete readers, writers, sinks, and providers stay in
// the root package, indexed by the IR.
package recipeir

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// Recipe is one compiled value recipe.
type Recipe struct {
	Inputs       []Input
	Destinations []Destination
	// Graph is the recipe as its Outputs built it.
	Graph Graph
	// Streams are the branches the planner derives from Graph: one per
	// distinct Stream the Outputs route, its operations from its root (an
	// input's selected stream or a join's output) in order, each carrying the
	// node it runs as. A node that serves several branches appears in each
	// of them, Shared.
	Streams     []Stream
	StreamRules []StreamRule
	Policies    Policies
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
	Codec    codec.Spec
	Realtime bool
	Kind     InputKind
	// SourceShape is set for provider and custom-source inputs whose stream
	// shape is known before opening runtime resources.
	SourceShape shape.Spec
}

// Stream is one branch: a root, its ordered operations, and the
// destinations it feeds.
type Stream struct {
	Name string
	// Named marks a branch whose Name is its identity (a fork, or a stream
	// named with As): its encoder output takes Name as its stream id. A lone
	// unnamed stream keeps its source stream's identity.
	Named bool
	// Root is the branch's root node: a NodeSelect or a NodeJoin.
	Root NodeID
	// Input indexes Recipe.Inputs for a branch of an input's stream; -1 for
	// a branch of a join's output.
	Input      int
	Selector   plan.StreamSelect
	Operations []Operation
	Outputs    []OutputRef
	// Buffer is the branch's declared queue (.Buffer(...)); zero for
	// branches that declare none.
	Buffer flow.BranchBuffer
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
// not carry executable destination handles or custom matcher functions; the
// runtime still owns those while mutation patches cross the recipe boundary.
type StreamRule struct {
	MatchDescription string
	Branches         []StreamRuleBranch
	// RequiresRuntime records whether any rule branch needs an explicit
	// runtime (it decodes, encodes, transforms, or writes to an
	// adapter-backed destination).
	RequiresRuntime bool
}

// StreamRuleBranch is one branch template visible to Explain/validation.
type StreamRuleBranch struct {
	Name         string
	Media        av.MediaType
	Operations   []Operation
	Destinations []string
	Buffer       flow.BranchBuffer
}

// RuntimeBranch is the immutable branch recipe captured for runtime mutation
// before patch planning opens stages or mutates a graph.
type RuntimeBranch struct {
	Name       string
	Media      av.MediaType
	Source     RuntimeBranchSource
	Operations []Operation
	Buffer     flow.BranchBuffer
}

// RuntimeBranchSource is the planner-visible source anchor for a runtime
// branch. It can name an existing graph node, a typed tap, or one discovered
// source stream.
type RuntimeBranchSource struct {
	From          string
	Tap           TapRef
	Policy        pipeline.RoutePolicy
	Label         string
	Stream        av.Stream
	StreamPresent bool
	StreamDomain  shape.MediaDomain
}

// RuntimeDestination is the planner-visible destination data captured for a
// runtime branch before graph patch planning. Concrete writer/sink handles stay
// outside this DTO and are opened only at the mutation edge.
type RuntimeDestination struct {
	Name     string
	Kind     DestinationKind
	Format   av.FormatID
	ShareKey string
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

// Transform is one planner-visible transform config.
type Transform struct {
	Kind     TransformKind
	Resize   filter.ResizeConfig
	Resample filter.ResampleConfig
}

// Operation is one stream operation: the one record the planner reads.
type Operation struct {
	Kind      plan.OperationKind
	Component string
	Detail    string
	Stage     pipeline.Stage
	Shape     shape.Spec
	Transform Transform
	Tap       Tap
	Decode    codec.Spec
	Encode    codec.Spec
	Shared    bool
	Auto      *shape.Policy
	Require   *shape.Spec
	Prefer    *shape.Spec
	// Name is an OpName operation's name; Buffer an OpBuffer operation's
	// queue.
	Name   string
	Buffer flow.BranchBuffer
	// Node is the graph node the operation runs as. Operations the planner
	// inserts (a packet copy, a shape conversion) get nodes of their own.
	Node NodeID
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

// Graph is a value recipe as a DAG: every Stream the recipe's Outputs reach
// is one node, so a Stream used twice is one node with several children (a
// fork) and a join is one node with several parents. Nodes are ordered so
// that every parent precedes its children; NodeID indexes Nodes.
type Graph struct {
	Nodes []Node
	// Rules names the stream matches of NodeRule roots, by index.
	Rules []string
}

// NodeID identifies one node of a Graph.
type NodeID int

// NodeKind is what a Graph node is.
type NodeKind uint8

const (
	// NodeInput is one recipe input's source; Input indexes Recipe.Inputs.
	NodeInput NodeKind = iota + 1
	// NodeSelect selects one stream of its parent input.
	NodeSelect
	// NodeRule is each stream its parent input discovers while it runs
	// (OnStream); Rule indexes Graph.Rules.
	NodeRule
	// NodeAnchor is a stream of a running task an attach recipe starts from.
	NodeAnchor
	// NodeOp is one operation on its parent's stream.
	NodeOp
	// NodeJoin converges its parents, in arm order, into one stream.
	NodeJoin
	// NodeOutput is a destination its parents feed; Output indexes
	// Recipe.Destinations. A parent that is a NodeInput copies every stream
	// of that input.
	NodeOutput
)

// Node is one node of a Graph.
type Node struct {
	Kind    NodeKind
	Parents []NodeID
	Input   int
	Select  plan.StreamSelect
	Rule    int
	Anchor  Anchor
	Op      Operation
	Join    Join
	Output  int
}

// Anchor is where an attach recipe's stream comes from: a tap, one stream of
// an input (Track), or a graph route.
type Anchor struct {
	Tap   string
	Track *av.Stream
	Input int
	Route *pipeline.Route
}

// Join is a convergence node's kind and per-arm placement.
type Join struct {
	// Kind is mix, composite, or select, or a custom join's name.
	Kind string
	// Stage is a custom join's convergence stage.
	Stage pipeline.Stage
	// Sync aligns the arms by presentation time.
	Sync bool
	// At places each arm (by parent index) on a Composite canvas.
	At []Point
}

// Point is one arm's canvas position; Set is false for an arm placed at the
// origin by default.
type Point struct {
	X, Y int
	Set  bool
}

// Graph operation kinds that carry stream facts rather than work: a name
// (As) and a fork queue (Buffer). They lower to no node.
const (
	OpName   plan.OperationKind = "name"
	OpBuffer plan.OperationKind = "buffer"
)
