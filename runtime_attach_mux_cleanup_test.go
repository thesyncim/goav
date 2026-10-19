package goav

import (
	"slices"
	"testing"

	"github.com/thesyncim/goav/pipeline"
)

// TestRuntimeAttachmentStopsUpstreamFirst pins the detach order removal's
// drain relies on: each node goes before the nodes it feeds, so its queue
// drains into nodes still attached, whatever order they were attached in.
func TestRuntimeAttachmentStopsUpstreamFirst(t *testing.T) {
	nodes := []pipeline.NodeRef{"mux", "encode", "sink", "resize"}
	routes := []pipeline.Route{
		{From: "tap", To: []string{"resize"}},
		{From: "resize", To: []string{"encode", "sink"}},
		{From: "encode", To: []string{"mux"}},
	}
	got := upstreamFirst(nodes, routes)
	want := []pipeline.NodeRef{"resize", "encode", "mux", "sink"}
	if !slices.Equal(got, want) {
		t.Fatalf("stop order = %v, want %v", got, want)
	}
}
