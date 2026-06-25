package inspect

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/snapshot"
)

// Subscribe calls task.Watch(filters...) and returns an independent disposable
// event subscription. A nil task returns an already closed subscription.
func Subscribe(task interface {
	Watch(...EventFilter) Subscription
}, filters ...EventFilter) Subscription {
	if task == nil {
		events := make(chan av.Event)
		close(events)
		return closedSubscription{events: events}
	}
	return task.Watch(filters...)
}

// Snapshot returns task.Snapshot(), or the zero snapshot for a nil task.
func Snapshot(task interface{ Snapshot() snapshot.Task }) snapshot.Task {
	if task == nil {
		return snapshot.Task{}
	}
	return task.Snapshot()
}

// Stats returns task.Stats(), or zero counters for a nil task.
func Stats(task interface{ Stats() pipeline.GraphStats }) pipeline.GraphStats {
	if task == nil {
		return pipeline.GraphStats{}
	}
	return task.Stats()
}

// Render renders the task's current snapshot as a small Mermaid flowchart.
func Render(task interface{ Snapshot() snapshot.Task }) (string, error) {
	spec := Snapshot(task).Spec
	var out strings.Builder
	out.WriteString("flowchart LR\n")
	if len(spec.Nodes) == 0 {
		out.WriteString("  empty[\"empty\"]\n")
		return out.String(), nil
	}
	ids := make(map[pipeline.NodeRef]string, len(spec.Nodes))
	for i := range spec.Nodes {
		node := spec.Nodes[i]
		id := fmt.Sprintf("n%d", i)
		ids[pipeline.NodeRef(node.Name)] = id
		label := renderLabel(node.Name, string(node.Kind), node.Detail)
		fmt.Fprintf(&out, "  %s[\"%s\"]\n", id, escapeMermaidLabel(label))
	}
	for i := range spec.Edges {
		edge := spec.Edges[i]
		from, okFrom := ids[edge.From]
		to, okTo := ids[edge.To]
		if !okFrom || !okTo {
			continue
		}
		if edge.Label != "" {
			fmt.Fprintf(&out, "  %s -- \"%s\" --> %s\n", from, escapeMermaidLabel(edge.Label), to)
			continue
		}
		out.WriteString("  ")
		out.WriteString(from)
		out.WriteString(" --> ")
		out.WriteString(to)
		out.WriteByte('\n')
	}
	return out.String(), nil
}

type closedSubscription struct {
	events <-chan av.Event
}

func (s closedSubscription) Events() <-chan av.Event { return s.events }

func (closedSubscription) Close() error { return nil }

func renderLabel(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for i := range parts {
		if parts[i] != "" {
			filtered = append(filtered, parts[i])
		}
	}
	return strings.Join(filtered, "\\n")
}

func escapeMermaidLabel(label string) string {
	label = strings.ReplaceAll(label, "\\", "\\\\")
	return strings.ReplaceAll(label, `"`, `\"`)
}
