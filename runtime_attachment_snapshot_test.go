package goav

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/snapshot"
)

func TestRuntimeAttachmentNilAndOwnerlessSnapshotAccessors(t *testing.T) {
	ctx := context.Background()
	var nilAttachment *runtimeAttachment
	if got := nilAttachment.ID(); got != "" {
		t.Fatalf("nil ID = %q, want empty", got)
	}
	if got := nilAttachment.Name(); got != "" {
		t.Fatalf("nil Name = %q, want empty", got)
	}
	if got := nilAttachment.Spec(); !reflect.DeepEqual(got, pipeline.Spec{}) {
		t.Fatalf("nil Spec = %#v, want empty", got)
	}
	if got := nilAttachment.Stats(); !reflect.DeepEqual(got, pipeline.GraphStats{}) {
		t.Fatalf("nil Stats = %#v, want empty", got)
	}
	if got := nilAttachment.Snapshot(); !reflect.DeepEqual(got, snapshot.Branch{}) {
		t.Fatalf("nil Snapshot = %#v, want empty", got)
	}
	if got := nilAttachment.allAnchorTaps(); got != nil {
		t.Fatalf("nil allAnchorTaps = %#v, want nil", got)
	}
	if got := nilAttachment.allAnchorNodes(); got != nil {
		t.Fatalf("nil allAnchorNodes = %#v, want nil", got)
	}
	if err := nilAttachment.setPaused(ctx, true); err != nil {
		t.Fatalf("nil setPaused: %v", err)
	}
	if err := nilAttachment.Close(ctx); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	if got := nilAttachment.specFromGraph(pipeline.Spec{Name: "ignored"}); !reflect.DeepEqual(got, pipeline.Spec{}) {
		t.Fatalf("nil specFromGraph = %#v, want empty", got)
	}

	unanchored := &runtimeAttachment{}
	if got := unanchored.allAnchorTaps(); got != nil {
		t.Fatalf("blank allAnchorTaps = %#v, want nil", got)
	}
	if got := unanchored.allAnchorNodes(); got != nil {
		t.Fatalf("blank allAnchorNodes = %#v, want nil", got)
	}

	attachment := &runtimeAttachment{
		id:         "branch-42",
		name:       "preview",
		anchorTap:  "legacy-tap",
		anchorNode: "legacy-node",
		nodes:      []pipeline.NodeRef{"preview-copy", "preview-sink"},
		taps: []snapshot.Tap{{
			Name: "preview-packets",
			Node: pipeline.NodeRef("preview-copy"),
		}},
		work: workPatch{Destinations: []workDestination{{
			Name:      "preview.ivf",
			Operation: plan.OpMux,
			Component: "custom-mux",
			Format:    av.FormatIVF,
			Branches:  []string{"preview"},
		}}},
	}
	if got := attachment.ID(); got != "branch-42" {
		t.Fatalf("ID = %q, want branch-42", got)
	}
	if got := attachment.Name(); got != "preview" {
		t.Fatalf("Name = %q, want preview", got)
	}
	if got := attachment.Spec(); !reflect.DeepEqual(got, pipeline.Spec{}) {
		t.Fatalf("ownerless Spec = %#v, want empty", got)
	}
	if got := attachment.Stats(); !reflect.DeepEqual(got, pipeline.GraphStats{}) {
		t.Fatalf("ownerless Stats = %#v, want empty", got)
	}
	if err := attachment.setPaused(ctx, true); err != nil {
		t.Fatalf("ownerless setPaused: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := attachment.setPaused(canceled, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled setPaused = %v, want context.Canceled", err)
	}

	got := attachment.Snapshot()
	want := snapshot.Branch{
		ID:          "branch-42",
		Name:        "preview",
		State:       lifecycle.BranchAttached,
		AnchorTaps:  []string{"legacy-tap"},
		AnchorNodes: []string{"legacy-node"},
		Nodes:       []pipeline.NodeRef{"preview-copy", "preview-sink"},
		Taps: []snapshot.Tap{{
			Name: "preview-packets",
			Node: pipeline.NodeRef("preview-copy"),
		}},
		Destinations: []snapshot.Destination{{
			Name:      "preview.ivf",
			Operation: plan.OpMux,
			Component: "custom-mux",
			Format:    av.FormatIVF,
			Branches:  []string{"preview"},
			State:     lifecycle.DestinationOpen,
			Open:      true,
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot = %#v, want %#v", got, want)
	}

	got.AnchorTaps[0] = "mutated"
	got.AnchorNodes[0] = "mutated"
	got.Nodes[0] = "mutated"
	got.Taps[0].Name = "mutated"
	got.Destinations[0].Branches[0] = "mutated"
	if attachment.anchorTap != "legacy-tap" ||
		attachment.anchorNode != "legacy-node" ||
		attachment.nodes[0] != "preview-copy" ||
		attachment.taps[0].Name != "preview-packets" ||
		attachment.work.Destinations[0].Branches[0] != "preview" {
		t.Fatalf("snapshot mutation leaked into attachment: %#v", attachment)
	}

	attachment.anchorTaps = []string{"tap-a", "tap-b"}
	attachment.anchorNodes = []string{"node-a", "node-b"}
	if got := attachment.allAnchorTaps(); !reflect.DeepEqual(got, []string{"tap-a", "tap-b"}) {
		t.Fatalf("allAnchorTaps = %#v, want plural anchors", got)
	}
	if got := attachment.allAnchorNodes(); !reflect.DeepEqual(got, []string{"node-a", "node-b"}) {
		t.Fatalf("allAnchorNodes = %#v, want plural anchors", got)
	}

	attachment.stopped = true
	attachment.detachOutcome.Store(lifecycle.DestinationAborted)
	detached := attachment.Snapshot()
	if detached.State != lifecycle.BranchDetached {
		t.Fatalf("detached State = %q, want %q", detached.State, lifecycle.BranchDetached)
	}
	if len(detached.Destinations) != 1 || detached.Destinations[0].State != lifecycle.DestinationAborted || detached.Destinations[0].Open {
		t.Fatalf("detached destination = %#v, want aborted and closed", detached.Destinations)
	}
	if err := attachment.setPaused(ctx, true); err == nil {
		t.Fatal("setPaused on detached ownerless branch succeeded, want error")
	}
	if err := attachment.Close(ctx); err != nil {
		t.Fatalf("ownerless Close: %v", err)
	}
}

func TestRuntimeAttachmentSpecAndStatsFiltering(t *testing.T) {
	attachment := &runtimeAttachment{
		name:  "preview",
		nodes: []pipeline.NodeRef{"preview-copy", "preview-sink", "missing"},
	}
	graph := pipeline.Spec{
		Name:     "root",
		Realtime: true,
		Nodes: []pipeline.NodeSpec{
			{Name: "input", Kind: pipeline.NodeSource, Detail: "custom source"},
			{Name: "preview-copy", Kind: pipeline.NodeStage, Detail: "copy packets"},
			{Name: "preview-sink", Kind: pipeline.NodeSink, Detail: "custom sink"},
			{Name: "main-sink", Kind: pipeline.NodeSink, Detail: "main output"},
		},
		Edges: []pipeline.EdgeSpec{
			{From: pipeline.NodeRef("input"), To: pipeline.NodeRef("preview-copy"), Policy: pipeline.RouteAll},
			{From: pipeline.NodeRef("preview-copy"), To: pipeline.NodeRef("preview-sink"), Policy: pipeline.RouteByStream, Label: "audio"},
			{From: pipeline.NodeRef("input"), To: pipeline.NodeRef("main-sink"), Policy: pipeline.RouteAll},
		},
	}
	wantSpec := pipeline.Spec{
		Name:     "preview",
		Realtime: true,
		Nodes: []pipeline.NodeSpec{
			{Name: "preview-copy", Kind: pipeline.NodeStage, Detail: "copy packets"},
			{Name: "preview-sink", Kind: pipeline.NodeSink, Detail: "custom sink"},
		},
		Edges: []pipeline.EdgeSpec{
			{From: pipeline.NodeRef("input"), To: pipeline.NodeRef("preview-copy"), Policy: pipeline.RouteAll},
			{From: pipeline.NodeRef("preview-copy"), To: pipeline.NodeRef("preview-sink"), Policy: pipeline.RouteByStream, Label: "audio"},
		},
	}
	if got := attachment.specFromGraph(graph); !reflect.DeepEqual(got, wantSpec) {
		t.Fatalf("specFromGraph = %#v, want %#v", got, wantSpec)
	}

	copyStats := pipeline.NodeStats{
		InMessages:       3,
		OutMessages:      5,
		OutPackets:       4,
		OutFrames:        1,
		Dropped:          2,
		DropReasons:      map[pipeline.DropPolicy]uint64{pipeline.DropOldest: 2},
		LastEvent:        av.Event{Type: av.EventBackpressure, StreamID: "audio"},
		LastEventPresent: true,
	}
	sinkStats := pipeline.NodeStats{
		InMessages:       5,
		OutMessages:      1,
		OutEvents:        1,
		Dropped:          1,
		DropReasons:      map[pipeline.DropPolicy]uint64{pipeline.DropNewest: 1},
		LastEvent:        av.Event{Type: av.EventEndOfStream, StreamID: "audio"},
		LastEventPresent: true,
	}
	sourceStats := pipeline.NodeStats{
		OutMessages: 99,
		Dropped:     99,
		DropReasons: map[pipeline.DropPolicy]uint64{pipeline.DropBlock: 99},
	}
	stats := pipeline.GraphStats{Nodes: map[string]pipeline.NodeStats{
		"preview-copy": copyStats,
		"preview-sink": sinkStats,
		"main-sink":    sourceStats,
	}}
	wantStats := pipeline.GraphStats{
		Messages:         6,
		Packets:          4,
		Frames:           1,
		Events:           1,
		Dropped:          3,
		DropReasons:      map[pipeline.DropPolicy]uint64{pipeline.DropOldest: 2, pipeline.DropNewest: 1},
		Delivered:        8,
		LastEvent:        sinkStats.LastEvent,
		LastEventPresent: true,
		Nodes: map[string]pipeline.NodeStats{
			"preview-copy": copyStats,
			"preview-sink": sinkStats,
		},
	}
	gotStats := branchStatsForNodes(stats, attachment.nodes)
	if !reflect.DeepEqual(gotStats, wantStats) {
		t.Fatalf("branchStatsForNodes = %#v, want %#v", gotStats, wantStats)
	}
	gotStats.Nodes["preview-copy"].DropReasons[pipeline.DropOldest] = 99
	if stats.Nodes["preview-copy"].DropReasons[pipeline.DropOldest] != 2 {
		t.Fatalf("branchStatsForNodes reused node drop-reason map: %#v", stats.Nodes["preview-copy"].DropReasons)
	}
	if got := branchStatsForNodes(stats, nil); !reflect.DeepEqual(got, pipeline.GraphStats{}) {
		t.Fatalf("branchStatsForNodes(nil nodes) = %#v, want empty", got)
	}
	if got := branchStatsForNodes(pipeline.GraphStats{}, attachment.nodes); !reflect.DeepEqual(got, pipeline.GraphStats{}) {
		t.Fatalf("branchStatsForNodes(empty stats) = %#v, want empty", got)
	}
	noDropStats := branchStatsForNodes(pipeline.GraphStats{Nodes: map[string]pipeline.NodeStats{
		"preview-copy": {OutMessages: 1},
	}}, []pipeline.NodeRef{"preview-copy"})
	if noDropStats.DropReasons != nil {
		t.Fatalf("branchStatsForNodes(no drops) DropReasons = %#v, want nil", noDropStats.DropReasons)
	}
}
