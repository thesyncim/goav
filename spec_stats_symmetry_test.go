package goav_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/goav/pipeline"
)

func statsNodeNames(stats pipeline.GraphStats) []string {
	names := make([]string, 0, len(stats.Nodes))
	for name := range stats.Nodes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func requireSpecStatsAgree(t *testing.T, what string, spec pipeline.Spec, stats pipeline.GraphStats) {
	t.Helper()
	specNames, statsNames := specNodeNames(spec), statsNodeNames(stats)
	slices.Sort(specNames)
	if !slices.Equal(specNames, statsNames) {
		t.Fatalf("%s: Spec nodes %v, Stats nodes %v; want the same node set", what, specNames, statsNames)
	}
	if len(specNames) == 0 {
		t.Fatalf("%s: Spec and Stats are both empty", what)
	}
}

// TestSpecAndStatsAgreeAcrossTaskBranchAndSnapshot pins that Spec and Stats
// report one node set at every level: the task, each attachment, and both
// snapshot views, while branches churn and after a branch detaches (whose
// Spec and Stats are then its final ones).
func TestSpecAndStatsAgreeAcrossTaskBranchAndSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	main := &frameCountSink{name: "main"}
	task := decodedVideoTapTask(t, ctx, liveMutableVideoPackets("camera", 64, 64, time.Millisecond), main)
	defer task.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- task.Run(ctx) }()
	waitFrameCount(ctx, &main.count, 1)

	for i := range 4 {
		sink := &frameCountSink{name: fmt.Sprintf("churn-%d", i)}
		churn := attachDropBranch(t, ctx, task, sink.name, sink)
		waitFrameCount(ctx, &sink.count, 1)
		if err := task.Detach(ctx, churn); err != nil {
			t.Fatalf("Detach(%s) error = %v", sink.name, err)
		}
		requireSpecStatsAgree(t, "detached branch "+sink.name, churn.Spec(), churn.Stats())
		snap := churn.Snapshot()
		requireSpecStatsAgree(t, "detached branch snapshot "+sink.name, snap.Spec, snap.Stats)
	}

	live := &frameCountSink{name: "live"}
	attachment := attachDropBranch(t, ctx, task, "live", live)
	waitFrameCount(ctx, &live.count, 1)

	requireSpecStatsAgree(t, "task", task.Describe(), task.Stats())
	snap := task.Snapshot()
	requireSpecStatsAgree(t, "task snapshot", snap.Spec, snap.Stats)
	requireSpecStatsAgree(t, "branch", attachment.Spec(), attachment.Stats())
	branch := attachment.Snapshot()
	requireSpecStatsAgree(t, "branch snapshot", branch.Spec, branch.Stats)
	for i := range snap.Branches {
		requireSpecStatsAgree(t, "task snapshot branch "+snap.Branches[i].Name, snap.Branches[i].Spec, snap.Branches[i].Stats)
	}

	cancel()
	if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
}
