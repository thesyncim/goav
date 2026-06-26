package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestCompatibilityPolicyPinsReleaseDecisionEvidence(t *testing.T) {
	body, err := os.ReadFile("docs/COMPATIBILITY.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"# Compatibility policy",
		"## Current promise",
		"## V1 promise draft",
		"## Release compatibility note template",
		"## Release blocking rule",
		"go 1.26",
		"docs/API_SURFACE.md",
		"docs/ROADMAP.md",
		"not the entire current governed inventory",
		"Runtime mutation (`Attach`, `Detach`, `Rebranch`)",
		"are advanced/non-v1 unless the release compatibility note",
		"records the exception to `docs/SIMPLIFICATION_TARGET.md`",
		"Minimum Go version:",
		"Module scope:",
		"API surface:",
		"Behavior changes:",
		"Structured error code/catalog changes:",
		"Advanced/non-v1 exceptions retained in this release:",
		"Adapter and extension changes:",
		"Migration notes:",
		"Evidence:",
		"Benchmark/perf-lab artifacts:",
		"Deferred / not claimed:",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/COMPATIBILITY.md missing %q", required)
		}
	}
}

func TestCompatibilityPolicyIsLinkedFromReleaseDocs(t *testing.T) {
	for _, file := range []string{"README.md", "docs/RELEASING.md", "docs/ROADMAP.md", "docs/V1_CREDIBILITY_AUDIT.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/COMPATIBILITY.md") {
			t.Fatalf("%s should link docs/COMPATIBILITY.md", file)
		}
	}
}
