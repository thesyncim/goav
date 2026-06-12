package main

import (
	"context"
	"testing"
)

func TestPlannerScenariosCoverAudioAndExpectedError(t *testing.T) {
	results := runPlannerScenarios(context.Background())
	if len(results) != 3 {
		t.Fatalf("results = %d, want 3", len(results))
	}
	status := map[string]string{}
	for _, result := range results {
		status[result.ID] = result.Status
		if result.GeneratedAt.IsZero() {
			t.Fatalf("%s missing GeneratedAt", result.ID)
		}
	}
	if status["opus-ladder"] != "ok" {
		t.Fatalf("opus-ladder status = %q", status["opus-ladder"])
	}
	if status["mix-resample"] != "ok" {
		t.Fatalf("mix-resample status = %q", status["mix-resample"])
	}
	if status["invalid-shape"] != "expected-error" {
		t.Fatalf("invalid-shape status = %q", status["invalid-shape"])
	}
}
