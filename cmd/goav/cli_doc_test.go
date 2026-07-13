package main

import (
	"os"
	"strings"
	"testing"
)

func TestCLIDocumentationPinsRunSchema(t *testing.T) {
	doc, err := os.ReadFile("../../docs/CLI.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, fragment := range []string{
		"goav run [--runtime demo|default|test] [--control unix://PATH] '<pipeline>'",
		"testsrc video [name=<id>] width=<px> height=<px>",
		"tap name=<tap-name>",
		"resize width=<px> height=<px>",
		"encode codec=<id> media=<video|audio>",
		"filesink location=<path> [format=<id>]",
		"ctlserver.CapabilitySet",
		"Hot-path media logic belongs",
		"the Go library.",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("docs/CLI.md missing %q", fragment)
		}
	}
	if !strings.Contains(runPipelineHelp(), "schema: docs/CLI.md") {
		t.Fatal("goav run help should point to docs/CLI.md")
	}
}
