package graphrender

import (
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/pipeline"
)

var ErrUnsupportedFormat = errors.New("graphrender: unsupported format")

type Format string

const (
	Text    Format = "text"
	DOT     Format = "dot"
	Mermaid Format = "mermaid"
)

func Render(spec pipeline.Spec, format Format) string {
	var out strings.Builder
	_ = Write(&out, spec, format)
	return out.String()
}

func Write(w io.Writer, spec pipeline.Spec, format Format) error {
	switch format {
	case "", Text:
		_, err := io.WriteString(w, spec.String())
		return err
	case DOT:
		return writeDOT(w, spec)
	case Mermaid:
		return writeMermaid(w, spec)
	default:
		return ErrUnsupportedFormat
	}
}

func writeDOT(w io.Writer, spec pipeline.Spec) error {
	if err := writeStrings(w, "digraph ", quoteDOT(specName(spec.Name)), " {\n  rankdir=LR;\n"); err != nil {
		return err
	}
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		shape := "box"
		switch node.Kind {
		case pipeline.NodeSource:
			shape = "oval"
		case pipeline.NodeSink:
			shape = "doublecircle"
		}
		if err := writeStrings(w, "  ", quoteDOT(node.Name), " [shape=", shape, ", label=", quoteDOT(nodeLabel(node)), "];\n"); err != nil {
			return err
		}
	}
	for i := range spec.Edges {
		edge := &spec.Edges[i]
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

func writeMermaid(w io.Writer, spec pipeline.Spec) error {
	if err := writeStrings(w, "flowchart LR\n"); err != nil {
		return err
	}
	ids := make(map[string]string, len(spec.Nodes))
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
		id := "n" + strconv.Itoa(i)
		ids[node.Name] = id
		label := nodeLabel(node)
		switch node.Kind {
		case pipeline.NodeSource:
			if err := writeStrings(w, "  ", id, "([", quoteMermaid(label), "])\n"); err != nil {
				return err
			}
		case pipeline.NodeSink:
			if err := writeStrings(w, "  ", id, "((", quoteMermaid(label), "))\n"); err != nil {
				return err
			}
		default:
			if err := writeStrings(w, "  ", id, "[", quoteMermaid(label), "]\n"); err != nil {
				return err
			}
		}
	}
	for i := range spec.Edges {
		edge := &spec.Edges[i]
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

func nodeLabel(node *pipeline.NodeSpec) string {
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

func edgeTextLabel(edge *pipeline.EdgeSpec) string {
	if edge.Policy == "" || edge.Policy == pipeline.RouteAll {
		return edge.Label
	}
	switch edge.Policy {
	case pipeline.RouteByStream:
		return routedEdgeLabel("stream", edge.Label)
	case pipeline.RouteByEvent:
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
