// Package graphrender renders a pipeline.Spec — the graph Runtime.Describe
// plans and LiveTask.Spec reports once built — as human-readable text,
// Graphviz DOT, or Mermaid, and annotates goav.TaskSnapshot and
// goav.BranchSnapshot views with branch and tap labels. It is diagnostics
// outside the goav core: the root package never imports it, so any graph a
// recipe or expert builder describes can be rendered.
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
	"sort"
	"strings"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/internal/specrender"
	"github.com/thesyncim/goav/pipeline"
)

// ErrUnsupportedFormat reports a goav:graph URI naming a format other than
// text, dot, or mermaid.
var ErrUnsupportedFormat = specrender.ErrUnsupportedFormat

// ErrUnsupportedURI reports a render target that is not a goav:graph URI.
var ErrUnsupportedURI = specrender.ErrUnsupportedURI

// RenderURI renders spec in the format the goav:graph target URI selects:
// "goav:graph" (or an empty target) renders text, "goav://graph/dot" and
// "goav://graph/mermaid" select by path, and a format query parameter
// ("goav://graph?format=dot") wins over the path. Unknown formats return
// ErrUnsupportedFormat; non-goav:graph targets return ErrUnsupportedURI.
func RenderURI(spec pipeline.Spec, target string) (string, error) {
	return specrender.RenderURI(spec, target)
}

// branch names and lifecycle state.
func RenderTaskFlowchart(task interface{ Snapshot() goav.TaskSnapshot }) (string, error) {
	return RenderTaskURI(task, "goav:graph?format=mermaid")
}

// RenderTaskURI renders the current snapshot of a running task-like value in
// the format selected by target. The task only needs to expose Snapshot, so
// this package stays outside the core import graph.
func RenderTaskURI(task interface{ Snapshot() goav.TaskSnapshot }, target string) (string, error) {
	if task == nil {
		return RenderSnapshotURI(goav.TaskSnapshot{}, target)
	}
	return RenderSnapshotURI(task.Snapshot(), target)
}

// RenderSnapshotFlowchart renders a task snapshot as a Mermaid flowchart.
// Runtime branch-owned nodes are annotated with branch names and lifecycle
// state, so callers can render a running task after capturing Snapshot().
func RenderSnapshotFlowchart(snap goav.TaskSnapshot) (string, error) {
	return RenderSnapshotURI(snap, "goav:graph?format=mermaid")
}

// RenderSnapshotURI renders a task snapshot in the format selected by target.
// It is useful when a host wants to persist, diff, or render a point-in-time
// task view without keeping a task handle in scope.
func RenderSnapshotURI(snap goav.TaskSnapshot, target string) (string, error) {
	return RenderURI(specFromSnapshot(snap), target)
}

// RenderBranchFlowchart renders an attachment/branch snapshot as a Mermaid
// flowchart. The branch's nodes are annotated with the branch name and
// lifecycle state.
func RenderBranchFlowchart(branch goav.BranchSnapshot) (string, error) {
	return RenderBranchURI(branch, "goav:graph?format=mermaid")
}

// RenderBranchURI renders an attachment/branch snapshot in the format selected
// by target.
func RenderBranchURI(branch goav.BranchSnapshot, target string) (string, error) {
	return RenderURI(specFromBranchSnapshot(branch), target)
}

func specFromSnapshot(snap goav.TaskSnapshot) pipeline.Spec {
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

func specFromBranchSnapshot(branch goav.BranchSnapshot) pipeline.Spec {
	spec := cloneSpec(branch.Spec)
	if spec.Name == "" {
		spec.Name = branchSortKey(branch)
	}
	return specFromSnapshot(goav.TaskSnapshot{
		Spec:     spec,
		Taps:     branch.Taps,
		Branches: []goav.BranchSnapshot{branch},
	})
}

func cloneSpec(spec pipeline.Spec) pipeline.Spec {
	spec.Nodes = append([]pipeline.NodeSpec(nil), spec.Nodes...)
	spec.Edges = append([]pipeline.EdgeSpec(nil), spec.Edges...)
	return spec
}

func branchLabelsByNode(branches []goav.BranchSnapshot) map[string][]string {
	labels := make(map[string][]string)
	sorted := append([]goav.BranchSnapshot(nil), branches...)
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

func labelsByNode(snap goav.TaskSnapshot) map[string][]string {
	labels := tapLabelsByNode(snap)
	for node, branchLabels := range branchLabelsByNode(snap.Branches) {
		labels[node] = append(labels[node], branchLabels...)
	}
	return labels
}

func tapLabelsByNode(snap goav.TaskSnapshot) map[string][]string {
	labels := make(map[string][]string)
	taps := append([]goav.TapSnapshot(nil), snap.Taps...)
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

func tapSortKey(tap goav.TapSnapshot) string {
	return tap.Node.String() + "\x00" + tap.Name + "\x00" + string(tap.Domain) + "\x00" + string(tap.MediaKind)
}

func tapRenderLabel(tap goav.TapSnapshot) string {
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

func branchSortKey(branch goav.BranchSnapshot) string {
	if branch.Name != "" {
		return branch.Name
	}
	return branch.ID
}

func branchRenderLabel(branch *goav.BranchSnapshot) string {
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

func branchNodeNames(branch *goav.BranchSnapshot) []string {
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
