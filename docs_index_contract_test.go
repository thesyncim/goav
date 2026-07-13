package goav

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocsIndexAndHistoryReferences(t *testing.T) {
	index, err := os.ReadFile(filepath.Join("docs", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	indexText := string(index)
	for _, fragment := range []string{
		"[`ROADMAP.md`](ROADMAP.md)",
		"[`NORTH_STAR.md`](NORTH_STAR.md)",
		"[`CLI.md`](CLI.md)",
		"[`CONTROL_PLANE.md`](CONTROL_PLANE.md)",
		"[`history/`](history/)",
	} {
		if !strings.Contains(indexText, fragment) {
			t.Fatalf("docs/README.md missing %q", fragment)
		}
	}

	historical := []string{
		"API_REDUCTION_PLAN.md",
		"API_INVENTORY.md",
		"SIMPLIFICATION_TARGET.md",
		"PROGRESS.md",
		"V1_CREDIBILITY_AUDIT.md",
		"V1_CREDIBILITY_PR.md",
	}
	entries, err := os.ReadDir("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		path := filepath.Join("docs", entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(data), "\n")
		for lineNo, line := range lines {
			for _, name := range historical {
				if strings.Contains(line, name) && !strings.Contains(line, "history/"+name) {
					t.Fatalf("%s:%d references historical %s without history/ path: %s", path, lineNo+1, name, line)
				}
			}
		}
	}
}

func TestRoadmapDocumentsH264DecodeOnlyDecision(t *testing.T) {
	roadmap, err := os.ReadFile(filepath.Join("docs", "ROADMAP.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(roadmap)
	for _, fragment := range []string{
		"H264 recipe encode",
		"deliberately descriptor-only",
		"vetted pure-Go H.264 encoder backend",
		"`codec.ErrUnavailable`",
		"`runconfig.WithEncoder`",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("docs/ROADMAP.md missing H264 decode-only decision fragment %q", fragment)
		}
	}
}
