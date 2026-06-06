package graphrender

import (
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/pipeline"
)

var ErrUnsupportedFormat = errors.New("graphrender: unsupported format")
var ErrUnsupportedURI = errors.New("graphrender: unsupported uri")

type format string

const (
	textFormat    format = "text"
	dotFormat     format = "dot"
	mermaidFormat format = "mermaid"
)

func RenderURI(spec pipeline.Spec, target string) (string, error) {
	var out strings.Builder
	if err := writeURI(&out, spec, target); err != nil {
		return "", err
	}
	return out.String(), nil
}

func write(w io.Writer, spec pipeline.Spec, format format) error {
	switch format {
	case "", textFormat:
		return writeText(w, spec)
	case dotFormat:
		return writeDOT(w, spec)
	case mermaidFormat:
		return writeMermaid(w, spec)
	default:
		return ErrUnsupportedFormat
	}
}

func writeURI(w io.Writer, spec pipeline.Spec, target string) error {
	format, err := formatURI(target)
	if err != nil {
		return err
	}
	return write(w, spec, format)
}

func formatURI(target string) (format, error) {
	if target == "" {
		return textFormat, nil
	}
	uri, err := url.Parse(target)
	if err != nil {
		return "", err
	}
	if uri.Scheme != "goav" || !isGraphURI(uri) {
		return "", ErrUnsupportedURI
	}
	format := graphURIFormat(uri)
	if format == "" {
		return textFormat, nil
	}
	return parseFormat(format)
}

func writeText(w io.Writer, spec pipeline.Spec) error {
	if err := writeStrings(w, "pipeline ", specName(spec.Name), "\n"); err != nil {
		return err
	}
	for i := range spec.Nodes {
		node := &spec.Nodes[i]
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
	for i := range spec.Edges {
		edge := &spec.Edges[i]
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

func isGraphURI(uri *url.URL) bool {
	return uri.Opaque == "graph" ||
		uri.Host == "graph" ||
		(uri.Host == "" && strings.Trim(uri.Path, "/") == "graph")
}

func graphURIFormat(uri *url.URL) string {
	if format := uri.Query().Get("format"); format != "" {
		return format
	}
	if uri.Host != "graph" {
		return ""
	}
	return strings.Trim(uri.Path, "/")
}

func parseFormat(value string) (format, error) {
	switch format(strings.ToLower(value)) {
	case "", textFormat:
		return textFormat, nil
	case dotFormat:
		return dotFormat, nil
	case mermaidFormat:
		return mermaidFormat, nil
	default:
		return "", ErrUnsupportedFormat
	}
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
