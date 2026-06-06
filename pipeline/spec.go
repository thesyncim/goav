package pipeline

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

var ErrUnsupportedSpecFormat = errors.New("pipeline: unsupported spec format")

// SpecFormat selects a textual graph rendering format.
type SpecFormat string

const (
	SpecText    SpecFormat = "text"
	SpecDOT     SpecFormat = "dot"
	SpecMermaid SpecFormat = "mermaid"
)

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
	return s.Render(SpecText)
}

// Render returns the graph spec in the requested format.
func (s Spec) Render(format SpecFormat) string {
	var out strings.Builder
	_ = s.Write(&out, format)
	return out.String()
}

// Write renders the graph spec to w in the requested format.
func (s Spec) Write(w io.Writer, format SpecFormat) error {
	switch format {
	case "", SpecText:
		return s.writeText(w)
	case SpecDOT:
		return s.writeDOT(w)
	case SpecMermaid:
		return s.writeMermaid(w)
	default:
		return ErrUnsupportedSpecFormat
	}
}

func (s Spec) writeText(w io.Writer) error {
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

func (s Spec) writeDOT(w io.Writer) error {
	if err := writeStrings(w, "digraph ", quoteDOT(specName(s.Name)), " {\n  rankdir=LR;\n"); err != nil {
		return err
	}
	for i := range s.Nodes {
		node := &s.Nodes[i]
		shape := "box"
		switch node.Kind {
		case NodeSource:
			shape = "oval"
		case NodeSink:
			shape = "doublecircle"
		}
		if err := writeStrings(w, "  ", quoteDOT(node.Name), " [shape=", shape, ", label=", quoteDOT(nodeLabel(node)), "];\n"); err != nil {
			return err
		}
	}
	for i := range s.Edges {
		edge := &s.Edges[i]
		if err := writeStrings(w, "  ", quoteDOT(edge.From.String()), " -> ", quoteDOT(edge.To.String())); err != nil {
			return err
		}
		label := edgeTextLabel(edge)
		if label != "" {
			if err := writeStrings(w, " [label=", quoteDOT(label), "]"); err != nil {
				return err
			}
		}
		if err := writeStrings(w, ";\n"); err != nil {
			return err
		}
	}
	return writeStrings(w, "}\n")
}

func (s Spec) writeMermaid(w io.Writer) error {
	if err := writeStrings(w, "flowchart LR\n"); err != nil {
		return err
	}
	ids := make(map[string]string, len(s.Nodes))
	for i := range s.Nodes {
		node := &s.Nodes[i]
		id := "n" + strconv.Itoa(i)
		ids[node.Name] = id
		label := nodeLabel(node)
		switch node.Kind {
		case NodeSource:
			if err := writeStrings(w, "  ", id, "([", quoteMermaid(label), "])\n"); err != nil {
				return err
			}
		case NodeSink:
			if err := writeStrings(w, "  ", id, "((", quoteMermaid(label), "))\n"); err != nil {
				return err
			}
		default:
			if err := writeStrings(w, "  ", id, "[", quoteMermaid(label), "]\n"); err != nil {
				return err
			}
		}
	}
	for i := range s.Edges {
		edge := &s.Edges[i]
		from := mermaidNodeID(ids, edge.From.String())
		to := mermaidNodeID(ids, edge.To.String())
		label := edgeTextLabel(edge)
		if label == "" {
			if err := writeStrings(w, "  ", from, " --> ", to, "\n"); err != nil {
				return err
			}
			continue
		}
		if err := writeStrings(w, "  ", from, " -- ", quoteMermaid(label), " --> ", to, "\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeStrings(w io.Writer, values ...string) error {
	for i := range values {
		if _, err := io.WriteString(w, values[i]); err != nil {
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

func nodeLabel(node *NodeSpec) string {
	label := node.Name + "\n" + string(node.Kind)
	if node.Detail != "" {
		label += "\n" + node.Detail
	}
	return label
}

func quoteDOT(value string) string {
	return strconv.Quote(value)
}

func quoteMermaid(value string) string {
	return strconv.Quote(value)
}

func mermaidNodeID(ids map[string]string, name string) string {
	if id, ok := ids[name]; ok {
		return id
	}
	return "missing_" + mermaidSafeID(name)
}

func mermaidSafeID(value string) string {
	if value == "" {
		return "node"
	}
	var out strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			out.WriteRune(r)
			continue
		}
		out.WriteByte('_')
	}
	return out.String()
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
