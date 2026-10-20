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
	Name      string        `json:"name"`
	Kind      NodeKind      `json:"kind"`
	Detail    string        `json:"detail,omitempty"`
	Execution NodeExecution `json:"execution,omitzero"`
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
	Name      string        `json:"name"`
	Realtime  bool          `json:"realtime,omitempty"`
	Nodes     []NodeSpec    `json:"nodes,omitempty"`
	Edges     []EdgeSpec    `json:"edges,omitempty"`
	Execution ExecutionSpec `json:"execution,omitzero"`
}

// ExecutionSpec describes the selected runner and its scheduling contract.
// Buffered workers may overlap, but a full blocking queue paces its producer
// and every sibling that producer has yet to visit.
type ExecutionSpec struct {
	// Admission is "timestamp" when eligible offline sources take turns by
	// packet time, "arrival" otherwise. A pre-open plan reports "runtime"
	// when provider or decorator capabilities must first be resolved.
	Admission string `json:"admission,omitempty"`
	Mode      string `json:"mode,omitempty"`   // "direct" or "buffered"
	Fanout    string `json:"fanout,omitempty"` // "shared_producer"
}

// NodeExecution reports a source goroutine, inline handler, or serial worker.
// Queue is the actual normalized policy; a source and an inline handler have
// no queue. BlocksProducer includes inline work and full blocking queues.
type NodeExecution struct {
	Kind           string       `json:"kind,omitempty"` // "source", "inline", "worker"
	Queue          BufferPolicy `json:"queue,omitzero"`
	BlocksProducer bool         `json:"blocksProducer,omitempty"`
}

// DescribeExecution uses the same normalized queue policy as NewGraph.
func DescribeExecution(config GraphConfig) ExecutionSpec {
	mode := "direct"
	if !config.Buffer.IsDirect() {
		mode = "buffered"
	}
	return ExecutionSpec{Mode: mode, Fanout: "shared_producer", Admission: "arrival"}
}

// DescribeNodeExecution describes one node's actual delivery boundary.
func DescribeNodeExecution(kind NodeKind, config GraphConfig, policy BufferPolicy) NodeExecution {
	if kind == NodeSource {
		return NodeExecution{Kind: "source"}
	}
	if config.Buffer.IsDirect() {
		return NodeExecution{Kind: "inline", BlocksProducer: true}
	}
	if policy.IsDirect() {
		policy = config.Buffer
	}
	policy = normalizeBufferedPolicy(policy)
	return NodeExecution{Kind: "worker", Queue: policy, BlocksProducer: policy.Drop == DropBlock}
}
