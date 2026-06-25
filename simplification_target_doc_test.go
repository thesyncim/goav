package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestSimplificationTargetPinsPreV1Scope(t *testing.T) {
	body, err := os.ReadFile("docs/SIMPLIFICATION_TARGET.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"# Simplification Target",
		"Recipe AST -> Plan -> Graph -> Task",
		"Input -> Stream -> Operations -> Output",
		"## Supported v1 workflows",
		"Reader/file-like packet copy to writer.",
		"Decode -> transform -> encode -> mux.",
		"Explicit fanout branches.",
		"Describe/Explain before Build.",
		"Structured build errors.",
		"## Experimental or non-v1",
		"Expert graph.",
		"Runtime Attach/Rebranch.",
		"Control-plane socket host.",
		"Live provider transcode until compiler support is real.",
		"## Delete or demote before v1",
		"Same-handle destination grouping as a semantic rule.",
		"Prefix-based tap inference.",
		"## API budget",
		"| Root package exported identifiers | <= 40 |",
		"| `errcode` exported identifiers | <= 40 |",
		"| README | <= 120 lines |",
		"docs/API_INVENTORY.md",
		"## Architecture target",
		"grammar builders -> immutable recipe data -> planner -> work plan -> graph",
		"## Release gate",
		"Do not cut v1 just because the current docs and pins are green.",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/SIMPLIFICATION_TARGET.md missing %q", required)
		}
	}
}

func TestSimplificationTargetIsLinkedFromReleaseDocs(t *testing.T) {
	for _, file := range []string{
		"docs/API_REDUCTION_PLAN.md",
		"docs/COMPATIBILITY.md",
		"docs/PROGRESS.md",
		"docs/ROADMAP.md",
	} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/SIMPLIFICATION_TARGET.md") {
			t.Fatalf("%s should link docs/SIMPLIFICATION_TARGET.md", file)
		}
	}
}
