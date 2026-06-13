package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestRepositoryTrustDocPinsExternalMetadata(t *testing.T) {
	body, err := os.ReadFile("docs/REPOSITORY_TRUST.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"# Repository trust surface",
		"Pure-Go media workflow runtime for validated, inspectable, in-process audio/video pipelines",
		"https://pkg.go.dev/github.com/thesyncim/goav",
		"`audio`, `codecs`, `go`, `media`, `pure-go`, `realtime`, `rtp`",
		"`streaming`, `video`, `webrtc`",
		"gh repo view thesyncim/goav",
		"latestRelease",
		"gh release list --repo thesyncim/goav --limit 10",
		"No release should be published until the maintainer intentionally cuts a signed",
		"CHANGELOG.md",
		"docs/COMPATIBILITY.md",
		".github/workflows/release.yml",
		".github/PULL_REQUEST_TEMPLATE.md",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/REPOSITORY_TRUST.md missing %q", required)
		}
	}
}

func TestRepositoryTrustDocIsLinked(t *testing.T) {
	for _, file := range []string{"README.md", "docs/V1_CREDIBILITY_AUDIT.md", "docs/ROADMAP.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/REPOSITORY_TRUST.md") {
			t.Fatalf("%s should link docs/REPOSITORY_TRUST.md", file)
		}
	}
}
