package goav

import (
	"context"

	"github.com/thesyncim/goav/pipeline"
)

// removeBranchNode removes one of a detaching branch's nodes and records its
// final counters in final: a removed node leaves the graph's Stats, so a
// detached branch reports what it did from these.
func removeBranchNode(ctx context.Context, graph pipeline.Graph, node pipeline.NodeRef, final map[string]pipeline.NodeStats) error {
	if remover, ok := graph.(interface {
		RemoveNodeContext(context.Context, pipeline.NodeRef) (pipeline.NodeStats, error)
	}); ok {
		stats, err := remover.RemoveNodeContext(ctx, node)
		if err == nil {
			final[node.String()] = stats
		}
		return err
	}
	remover, ok := graph.(pipeline.NodeRemover)
	if !ok {
		return graph.Remove(node)
	}
	stats, err := remover.RemoveNode(node)
	if err == nil {
		final[node.String()] = stats
	}
	return err
}

// withRemovedStats lists a detached branch's final node counters in stats,
// for that branch's own view.
func withRemovedStats(stats pipeline.GraphStats, removed *map[string]pipeline.NodeStats) pipeline.GraphStats {
	if removed == nil || len(*removed) == 0 {
		return stats
	}
	if stats.Nodes == nil {
		stats.Nodes = make(map[string]pipeline.NodeStats, len(*removed))
	}
	// A later node may reuse a removed name; the branch reports its own.
	for name, node := range *removed {
		stats.Nodes[name] = node
	}
	return stats
}
