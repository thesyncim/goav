package goav_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
)

const readmeThirtySecondRecordingSnippet = `return goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(goav.File("recording.ivf", out)).
    Run(ctx)`

// readmeThirtySecondRecording is the compiled counterpart to the README's
// 30-second example. TestReadmeGoSnippetsAreCompiledAndPinned keeps the prose
// snippet in lockstep with this function.
func readmeThirtySecondRecording(ctx context.Context, in io.Reader, out io.Writer) error {
	return goav.From(goav.FileInput("input.ivf", in)).
		Copy().
		To(goav.File("recording.ivf", out)).
		Run(ctx)
}

func TestReadmeGoSnippetsAreCompiledAndPinned(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	snippets := markdownCodeBlocks(string(body), "go")
	if len(snippets) != 1 {
		t.Fatalf("README go snippet count = %d, want 1 adoption-front-door example", len(snippets))
	}
	if normalizeSnippet(snippets[0]) != normalizeSnippet(readmeThirtySecondRecordingSnippet) {
		t.Fatalf("README 30-second snippet drifted:\n%s", snippets[0])
	}
}

func markdownCodeBlocks(text, lang string) []string {
	var blocks []string
	var current strings.Builder
	inBlock := false
	for _, line := range strings.SplitAfter(text, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case !inBlock && trimmed == "```"+lang:
			inBlock = true
			current.Reset()
		case inBlock && trimmed == "```":
			blocks = append(blocks, strings.TrimRight(current.String(), "\n"))
			inBlock = false
		case inBlock:
			current.WriteString(line)
		}
	}
	return blocks
}

func normalizeSnippet(text string) string {
	return strings.Join(strings.Fields(text), " ")
}
