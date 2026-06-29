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
// detail line (codec, format, configuration) for rendering. Spec is a stable
// JSON document; Detail is the display fallback, while machine consumers read
// the typed per-operation metadata in plan.Report (Explain).
type NodeSpec struct {
	Name   string   `json:"name"`
	Kind   NodeKind `json:"kind"`
	Detail string   `json:"detail,omitempty"`
}

// EdgeSpec describes one graph edge: its endpoints and the route policy and
// label filtering it.
type EdgeSpec struct {
	From   NodeRef     `json:"from"`
	To     NodeRef     `json:"to"`
	Policy RoutePolicy `json:"policy,omitempty"`
	Label  string      `json:"label,omitempty"`
}

// Spec is the inspectable structure of a graph: every node and edge, in
// deterministic order. Describe on a job or task returns it; rendering lives
// outside core (graphrender). It marshals to stable JSON for machine consumers.
type Spec struct {
	Name     string     `json:"name"`
	Realtime bool       `json:"realtime,omitempty"`
	Nodes    []NodeSpec `json:"nodes,omitempty"`
	Edges    []EdgeSpec `json:"edges,omitempty"`
}
