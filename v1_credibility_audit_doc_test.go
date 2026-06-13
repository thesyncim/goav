package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestV1CredibilityAuditMapsRequestedEvidence(t *testing.T) {
	body, err := os.ReadFile("docs/V1_CREDIBILITY_AUDIT.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"# V1 Credibility Audit",
		"## Front Door",
		"README.md",
		"TestReadmeStaysFrontDoorSized",
		"TestReadmeGoBlocksCompileAsExternalConsumer",
		"## Machine-Checked Docs",
		"docs/ERROR_CATALOG.md",
		"docs/OPERATIONS.md",
		"docs/EXTENSION_COOKBOOK.md",
		"## External Extension Proof",
		"examples/custom-source",
		"examples/custom-filter",
		"examples/transactional-writer",
		"examples/custom-codec",
		"examples/custom-join",
		"examples/control-plane-host",
		"testdata/expected.txt",
		"## Performance Evidence",
		"bench-results/README.md",
		"docs/PERFORMANCE.md",
		"## CI And Release Trust",
		".github/workflows/ci.yml",
		".github/workflows/codeql.yml",
		".github/workflows/release.yml",
		"docs/REPOSITORY_TRUST.md",
		"## Composability And API Restraint",
		"docs/COMPOSABILITY_LAWS.md",
		".github/PULL_REQUEST_TEMPLATE.md",
		"docs/V1_CREDIBILITY_PR.md",
		"## Remaining Release Decision",
		"docs/ROADMAP.md",
		"docs/COMPATIBILITY.md",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/V1_CREDIBILITY_AUDIT.md missing %q", required)
		}
	}
}

func TestV1CredibilityAuditIsLinked(t *testing.T) {
	for _, file := range []string{"README.md", "docs/ROADMAP.md", "docs/PROGRESS.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/V1_CREDIBILITY_AUDIT.md") {
			t.Fatalf("%s should link docs/V1_CREDIBILITY_AUDIT.md", file)
		}
	}
}
