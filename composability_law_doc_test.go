package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestComposabilityLawsMapToExecutableEvidence(t *testing.T) {
	doc, err := os.ReadFile("docs/COMPOSABILITY_LAWS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, required := range []string{
		"# Composability laws",
		"direct stream chain",
		"`Flow` owns ordered operations only",
		"runtime `Mutable.Attach`",
		"`Describe()`",
		"`Explain()`",
		"Destination grouping is explicit",
		"Branch-local policy",
		"Runtime attach failures roll back",
		"External adapters and joins",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/COMPOSABILITY_LAWS.md missing %q", required)
		}
	}
}

func TestComposabilityLawsAreLinkedFromSurfaceDocs(t *testing.T) {
	for _, file := range []string{"docs/API_SURFACE.md", "docs/ROADMAP.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/COMPOSABILITY_LAWS.md") {
			t.Fatalf("%s should link docs/COMPOSABILITY_LAWS.md", file)
		}
	}
}
