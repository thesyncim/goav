package pipeline

type NodeKind string

const (
	NodeSource NodeKind = "source"
	NodeStage  NodeKind = "stage"
	NodeSink   NodeKind = "sink"
)

type NodeSpec struct {
	Name   string
	Kind   NodeKind
	Detail string
}

type EdgeSpec struct {
	From   NodeRef
	To     NodeRef
	Policy RoutePolicy
	Label  string
}

type Spec struct {
	Name     string
	Realtime bool
	Nodes    []NodeSpec
	Edges    []EdgeSpec
}
