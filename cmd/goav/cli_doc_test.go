package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestCLIDocumentationPinsTheLanguage keeps docs/CLI.md in step with the
// commands: the synopsis, the refusal it shows, and the run summary are what
// the CLI prints.
func TestCLIDocumentationPinsTheLanguage(t *testing.T) {
	doc, err := os.ReadFile("../../docs/CLI.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, fragment := range []string{
		"goav run      [--runtime default|demo|test] [--control unix://PATH] [-f FILE] '<program>'",
		"goav describe [--runtime ...] [--format text|dot|mermaid|json] [-f FILE] '<program>'",
		"goav elements",
		"ctlserver.CapabilitySet",
		"Hot-path media logic belongs in\nthe Go library.",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("docs/CLI.md missing %q", fragment)
		}
	}
	var stdout, stderr bytes.Buffer
	run([]string{"run", "file clip.webm ! video ! decod ! file x.webm"}, &stdout, &stderr)
	for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
		if !strings.Contains(text, line) {
			t.Fatalf("docs/CLI.md refusal sample lacks %q, which goav prints", line)
		}
	}
}
