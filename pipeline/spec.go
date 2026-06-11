package pipeline

// NodeKind classifies a spec node as a source, stage, or sink.
type NodeKind string

// The node kinds appearing in a Spec.
const (
	NodeSource NodeKind = "source"
	NodeStage  NodeKind = "stage"
	NodeSink   NodeKind = "sink"
)

// NodeSpec describes one graph node: its unique name, kind, and a free-form
// detail line (codec, format, configuration) for rendering.
type NodeSpec struct {
	Name   string
	Kind   NodeKind
	Detail string
}

// EdgeSpec describes one graph edge: its endpoints and the route policy and
// label filtering it.
type EdgeSpec struct {
	From   NodeRef
	To     NodeRef
	Policy RoutePolicy
	Label  string
}

// Spec is the inspectable structure of a graph: every node and edge, in
// deterministic order. Describe on a job or task returns it; rendering lives
// outside core (graphrender).
type Spec struct {
	Name     string
	Realtime bool
	Nodes    []NodeSpec
	Edges    []EdgeSpec
}
