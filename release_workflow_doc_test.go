package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseWorkflowAndDocsStayAligned(t *testing.T) {
	workflow, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	releasing, err := os.ReadFile("docs/RELEASING.md")
	if err != nil {
		t.Fatal(err)
	}
	changelog, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	wf := string(workflow)
	for _, required := range []string{
		"name: release",
		"workflow_dispatch",
		"v[0-9]*.[0-9]*.[0-9]*",
		"rtpav/v[0-9]*.[0-9]*.[0-9]*",
		"webrtcav/v[0-9]*.[0-9]*.[0-9]*",
		"module_dir=\"rtpav\"",
		"module_dir=\"webrtcav\"",
		"working-directory: ${{ steps.release.outputs.module_dir }}",
		"CGO_ENABLED=0 go test -count=1 ./...",
		"CGO_ENABLED=0 go vet ./...",
		"git verify-tag",
		"go build -trimpath",
		"SHA256SUMS",
		"sbom-go-modules.json",
		"provenance.txt",
		"go version -m",
		"gh \"${args[@]}\"",
		"--generate-notes",
	} {
		if !strings.Contains(wf, required) {
			t.Fatalf(".github/workflows/release.yml missing %q", required)
		}
	}
	doc := string(releasing)
	for _, required := range []string{
		"git tag -s v0.1.0",
		"rtpav/v0.1.0",
		"webrtcav/v0.1.0",
		"Tag order follows dependencies",
		"verifies the signed tag",
		"tagged module directory",
		"sbom-go-modules.json",
		"provenance.txt",
		"`*.buildinfo.txt`",
		"Manual dispatch defaults to a draft release",
		"matching nested module",
		"docs/COMPATIBILITY.md",
		"## Acceptance Gate Matrix",
		"Pure-Go runtime tests",
		"CGO_ENABLED=0 go test ./...",
		"Race coverage",
		"CGO_ENABLED=1 go test -race",
		"staticcheck ./...",
		"govulncheck ./...",
		"test -z \"$(gofmt -l .)\"",
		"examples/*/go.mod",
		"TestReadmeGoBlocksCompileAsExternalConsumer",
		"error_catalog_pin_test.go",
		"Hot-path allocations",
		"Public API restraint",
		"CI artifacts",
		"Performance claims",
	} {
		if !strings.Contains(doc, required) {
			t.Fatalf("docs/RELEASING.md missing %q", required)
		}
	}
	if !strings.Contains(string(changelog), "## Unreleased") {
		t.Fatalf("CHANGELOG.md should keep an Unreleased section for release notes")
	}
}

func TestReleasingDocsAreLinked(t *testing.T) {
	for _, file := range []string{"README.md", "docs/ROADMAP.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/RELEASING.md") {
			t.Fatalf("%s should link docs/RELEASING.md", file)
		}
	}
}

func TestRoadmapReleaseAndErrorEvidenceStaysCurrent(t *testing.T) {
	body, err := os.ReadFile("docs/ROADMAP.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"complete acceptance coverage rows in `docs/ERROR_CATALOG.md`",
		"every current errcode names a",
		"existing signed tags",
		"tagged module directory",
		"docs/COMPATIBILITY.md",
		"## Governed pre-v1 surface",
		"not normal v1 promises unless the",
		"advanced/non-v1 unless explicitly retained",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/ROADMAP.md missing current evidence phrase %q", required)
		}
	}
	for _, stale := range []string{
		"+ 10 acceptance snippets",
		"runs root checks",
	} {
		if strings.Contains(text, stale) {
			t.Fatalf("docs/ROADMAP.md still contains stale phrase %q", stale)
		}
	}
}

func TestPullRequestTemplateCapturesV1CredibilityEvidence(t *testing.T) {
	body, err := os.ReadFile(".github/PULL_REQUEST_TEMPLATE.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"## What Changed",
		"## Why It Improves V1 Credibility",
		"## Tests Run",
		"## Benchmarks Run",
		"## New Risks",
		"## Deferred Work",
		"## API Restraint",
		"why the existing grammar cannot",
		"operation record appended",
		"shape facts",
		"Explain/Describe/Snapshot",
		"Build and",
		"Attach behavior",
		"pre-resource failure mode",
		"external adapter/test",
		"No separate workflow path",
		"CHANGELOG.md",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf(".github/PULL_REQUEST_TEMPLATE.md missing %q", required)
		}
	}
}

func TestV1CredibilityPRDraftCapturesRequiredEvidence(t *testing.T) {
	body, err := os.ReadFile("docs/V1_CREDIBILITY_PR.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"## What Changed",
		"## Why It Improves V1 Credibility",
		"## Tests Run",
		"go test ./...",
		"CGO_ENABLED=0 go vet ./...",
		"examples/custom-source",
		"examples/provider-source",
		"examples/custom-destination",
		"examples/control-plane-host",
		"root-module compile-only sweep",
		"nested transport module compile-only sweep",
		"nested transport module runtime checks",
		"standalone example module runtime checks",
		"gh repo view",
		"## Benchmarks Run",
		"bench-results/baseline",
		"bench-results/pressure",
		"SourcePush pressure",
		"real Opus encode/decode",
		"## New Risks",
		"## Deferred Work",
		"## API Restraint",
		"No public API growth",
		"docs/REPOSITORY_TRUST.md",
		"docs/COMPATIBILITY.md",
		"Local full runtime `go test ./...` is now part of the safe-point check",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/V1_CREDIBILITY_PR.md missing %q", required)
		}
	}
}
