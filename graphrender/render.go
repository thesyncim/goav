// Package graphrender renders a pipeline.Spec — the graph Describe returns
// for planned and built jobs — as human-readable text, Graphviz DOT, or
// Mermaid. It is a diagnostics leaf outside the goav core: the root package
// never imports it, and it reads only the public Spec vocabulary, so any
// graph a recipe or expert builder describes can be rendered.
//
// The format is selected by a goav:graph URI:
//
//	out, err := graphrender.RenderURI(spec, "goav:graph")          // text
//	out, err := graphrender.RenderURI(spec, "goav://graph/dot")    // DOT
//	out, err := graphrender.RenderURI(spec, "goav://graph?format=mermaid")
//	flow, err := graphrender.RenderTaskFlowchart(task)             // live task
//	flow, err := graphrender.RenderSnapshotFlowchart(task.Snapshot())
//	flow, err := graphrender.RenderBranchFlowchart(attachment.Snapshot())
package graphrender

import (
	"errors"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

// ErrUnsupportedFormat reports a goav:graph URI naming a format other than
// text, dot, or mermaid.
var ErrUnsupportedFormat = errors.New("graphrender: unsupported format")

// ErrUnsupportedURI reports a render target that is not a goav:graph URI.
var ErrUnsupportedURI = errors.New("graphrender: unsupported uri")

type format string

const (
	textFormat    format = "text"
	dotFormat     format = "dot"
	mermaidFormat format = "mermaid"
)

// RenderURI renders spec in the format the goav:graph target URI selects:
// "goav:graph" (or an empty target) renders text, "goav://graph/dot" and
// "goav://graph/mermaid" select by path, and a format query parameter
// ("goav://graph?format=dot") wins over the path. Unknown formats return
// ErrUnsupportedFormat; non-goav:graph targets return ErrUnsupportedURI.
func RenderURI(spec pipeline.Spec, target string) (string, error) {
	var out strings.Builder
	if err := writeURI(&out, spec, target); err != nil {
		return "", err
	}
	return out.String(), nil
}

// RenderTaskFlowchart renders the current snapshot of a running task-like
// value as a Mermaid flowchart. Runtime branch-owned nodes are annotated with
// branch names and lifecycle state.
func RenderTaskFlowchart(task interface{ Snapshot() snapshot.Task }) (string, error) {
	return RenderTaskURI(task, "goav:graph?format=mermaid")
}

// RenderTaskURI renders the current snapshot of a running task-like value in
// the format selected by target. The task only needs to expose Snapshot, so
// this package stays outside the core import graph.
func RenderTaskURI(task interface{ Snapshot() snapshot.Task }, target string) (string, error) {
	if task == nil {
		return RenderSnapshotURI(snapshot.Task{}, target)
	}
	return RenderSnapshotURI(task.Snapshot(), target)
}

// RenderSnapshotFlowchart renders a task snapshot as a Mermaid flowchart.
// Runtime branch-owned nodes are annotated with branch names and lifecycle
// state, so callers can render a running task after capturing Snapshot().
func RenderSnapshotFlowchart(snap snapshot.Task) (string, error) {
	return RenderSnapshotURI(snap, "goav:graph?format=mermaid")
}

// RenderSnapshotURI renders a task snapshot in the format selected by target.
// It is useful when a host wants to persist, diff, or render a point-in-time
// task view without keeping a task handle in scope.
func RenderSnapshotURI(snap snapshot.Task, target string) (string, error) {
	return RenderURI(specFromSnapshot(snap), target)
}

// RenderBranchFlowchart renders an attachment/branch snapshot as a Mermaid
// flowchart. The branch's nodes are annotated with the branch name and
// lifecycle state.
func RenderBranchFlowchart(branch snapshot.Branch) (string, error) {
	return RenderBranchURI(branch, "goav:graph?format=mermaid")
}

// RenderBranchURI renders an attachment/branch snapshot in the format selected
// by target.
func RenderBranchURI(branch snapshot.Branch, target string) (string, error) {
	return RenderURI(specFromBranchSnapshot(branch), target)
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

func specFromSnapshot(snap snapshot.Task) pipeline.Spec {
	spec := cloneSpec(snap.Spec)
	labels := labelsByNode(snap)
	if len(labels) == 0 {
		return spec
	}
	for i := range spec.Nodes {
		nodeLabels := labels[spec.Nodes[i].Name]
		if len(nodeLabels) == 0 {
			continue
		}
		spec.Nodes[i].Detail = appendNodeDetail(spec.Nodes[i].Detail, strings.Join(nodeLabels, ", "))
	}
	return spec
}

func specFromBranchSnapshot(branch snapshot.Branch) pipeline.Spec {
	spec := cloneSpec(branch.Spec)
	if spec.Name == "" {
		spec.Name = branchSortKey(branch)
	}
	return specFromSnapshot(snapshot.Task{
		Spec:     spec,
		Taps:     branch.Taps,
		Branches: []snapshot.Branch{branch},
	})
}

func cloneSpec(spec pipeline.Spec) pipeline.Spec {
	spec.Nodes = append([]pipeline.NodeSpec(nil), spec.Nodes...)
	spec.Edges = append([]pipeline.EdgeSpec(nil), spec.Edges...)
	return spec
}

func branchLabelsByNode(branches []snapshot.Branch) map[string][]string {
	labels := make(map[string][]string)
	sorted := append([]snapshot.Branch(nil), branches...)
	sort.Slice(sorted, func(i, j int) bool {
		return branchSortKey(sorted[i]) < branchSortKey(sorted[j])
	})
	for i := range sorted {
		branch := &sorted[i]
		label := branchRenderLabel(branch)
		if label == "" {
			continue
		}
		for _, node := range branchNodeNames(branch) {
			labels[node] = append(labels[node], label)
		}
	}
	return labels
}

func labelsByNode(snap snapshot.Task) map[string][]string {
	labels := tapLabelsByNode(snap)
	for node, branchLabels := range branchLabelsByNode(snap.Branches) {
		labels[node] = append(labels[node], branchLabels...)
	}
	return labels
}

func tapLabelsByNode(snap snapshot.Task) map[string][]string {
	labels := make(map[string][]string)
	taps := append([]snapshot.Tap(nil), snap.Taps...)
	for i := range snap.Branches {
		taps = append(taps, snap.Branches[i].Taps...)
	}
	sort.Slice(taps, func(i, j int) bool {
		left := tapSortKey(taps[i])
		right := tapSortKey(taps[j])
		return left < right
	})
	seen := make(map[string]struct{}, len(taps))
	for i := range taps {
		label := tapRenderLabel(taps[i])
		node := taps[i].Node.String()
		if label == "" || node == "" {
			continue
		}
		key := node + "\x00" + label
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		labels[node] = append(labels[node], label)
	}
	return labels
}

func tapSortKey(tap snapshot.Tap) string {
	return tap.Node.String() + "\x00" + tap.Name + "\x00" + string(tap.Domain) + "\x00" + string(tap.MediaKind)
}

func tapRenderLabel(tap snapshot.Tap) string {
	if tap.Name == "" {
		return ""
	}
	var attrs []string
	if tap.Domain != "" {
		attrs = append(attrs, string(tap.Domain))
	}
	if tap.MediaKind != "" {
		attrs = append(attrs, string(tap.MediaKind))
	}
	if len(attrs) == 0 {
		return "tap=" + tap.Name
	}
	return "tap=" + tap.Name + " (" + strings.Join(attrs, " ") + ")"
}

func branchSortKey(branch snapshot.Branch) string {
	if branch.Name != "" {
		return branch.Name
	}
	return branch.ID
}

func branchRenderLabel(branch *snapshot.Branch) string {
	if branch == nil {
		return ""
	}
	name := branch.Name
	if name == "" {
		name = branch.ID
	}
	if name == "" {
		return ""
	}
	if branch.State == "" {
		return "branch=" + name
	}
	return "branch=" + name + " (" + string(branch.State) + ")"
}

func branchNodeNames(branch *snapshot.Branch) []string {
	if branch == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(branch.Nodes)+len(branch.Spec.Nodes))
	var out []string
	for i := range branch.Nodes {
		name := branch.Nodes[i].String()
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	for i := range branch.Spec.Nodes {
		name := branch.Spec.Nodes[i].Name
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func appendNodeDetail(detail string, annotation string) string {
	if annotation == "" {
		return detail
	}
	if detail == "" {
		return annotation
	}
	return detail + "\n" + annotation
}
