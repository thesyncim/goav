package pipeline

import "strings"

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

func (s Spec) String() string {
	var out strings.Builder
	_ = s.writeText(&out)
	return out.String()
}

func (s Spec) writeText(w interface{ Write([]byte) (int, error) }) error {
	if err := writeStrings(w, "pipeline ", specName(s.Name), "\n"); err != nil {
		return err
	}
	for i := range s.Nodes {
		node := &s.Nodes[i]
		if err := writeStrings(w, "  ", string(node.Kind), " ", node.Name); err != nil {
			return err
		}
		if node.Detail != "" {
			if err := writeStrings(w, " [", node.Detail, "]"); err != nil {
				return err
			}
		}
		if err := writeStrings(w, "\n"); err != nil {
			return err
		}
	}
	for i := range s.Edges {
		edge := &s.Edges[i]
		if err := writeStrings(w, "  ", edge.From.String(), " -> ", edge.To.String()); err != nil {
			return err
		}
		label := edgeTextLabel(edge)
		if label != "" {
			if err := writeStrings(w, " [", label, "]"); err != nil {
				return err
			}
		}
		if err := writeStrings(w, "\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeStrings(w interface{ Write([]byte) (int, error) }, values ...string) error {
	for i := range values {
		if _, err := w.Write([]byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

func specName(name string) string {
	if name == "" {
		return "goav"
	}
	return name
}

func edgeTextLabel(edge *EdgeSpec) string {
	if edge.Policy == "" || edge.Policy == RouteAll {
		return edge.Label
	}
	switch edge.Policy {
	case RouteByStream:
		return routedEdgeLabel("stream", edge.Label)
	case RouteByEvent:
		return routedEdgeLabel("event", edge.Label)
	default:
		if edge.Label == "" {
			return string(edge.Policy)
		}
		return string(edge.Policy) + "=" + edge.Label
	}
}

func routedEdgeLabel(kind string, label string) string {
	if label == "" {
		return kind
	}
	return kind + "=" + label
}
