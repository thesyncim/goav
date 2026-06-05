package pipeline

import (
	"io"
	"strconv"
	"strings"
)

type NodeKind string

const (
	NodeSource NodeKind = "source"
	NodeStage  NodeKind = "stage"
	NodeSink   NodeKind = "sink"
)

type NodeSpec struct {
	Name string
	Kind NodeKind
}

type EdgeSpec struct {
	From   PadRef
	To     PadRef
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
	_ = s.WriteText(&out)
	return out.String()
}

func (s Spec) DOT() string {
	var out strings.Builder
	_ = s.WriteDOT(&out)
	return out.String()
}

func (s Spec) WriteText(w io.Writer) error {
	if err := writeStrings(w, "pipeline ", specName(s.Name), "\n"); err != nil {
		return err
	}
	for i := range s.Nodes {
		node := &s.Nodes[i]
		if err := writeStrings(w, "  ", string(node.Kind), " ", node.Name, "\n"); err != nil {
			return err
		}
	}
	for i := range s.Edges {
		edge := &s.Edges[i]
		if err := writeStrings(w, "  ", edge.From.Node, ":", edge.From.Pad, " -> ", edge.To.Node, ":", edge.To.Pad); err != nil {
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

func (s Spec) WriteDOT(w io.Writer) error {
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
		if err := writeStrings(w, "  ", quoteDOT(node.Name), " [shape=", shape, ", label=", quoteDOT(node.Name+"\\n"+string(node.Kind)), "];\n"); err != nil {
			return err
		}
	}
	for i := range s.Edges {
		edge := &s.Edges[i]
		if err := writeStrings(w, "  ", quoteDOT(edge.From.Node), " -> ", quoteDOT(edge.To.Node)); err != nil {
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

func quoteDOT(value string) string {
	return strconv.Quote(value)
}

func edgeTextLabel(edge *EdgeSpec) string {
	if edge.Label == "" {
		if edge.Policy == "" || edge.Policy == RouteAll {
			return ""
		}
		return string(edge.Policy)
	}
	if edge.Policy == "" || edge.Policy == RouteAll {
		return edge.Label
	}
	return string(edge.Policy) + ":" + edge.Label
}
