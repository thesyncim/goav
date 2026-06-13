package goav_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReadmeGoBlocksCompileAsExternalConsumer(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	snippets := markdownCodeBlocks(string(body), "go")
	if len(snippets) != 1 {
		t.Fatalf("README go snippet count = %d, want one adoption-front-door example", len(snippets))
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	moduleDir := t.TempDir()
	writeReadmeConsumerModule(t, moduleDir, root, snippets)

	runReadmeConsumerGo(t, moduleDir, "test", "-mod=readonly", "-run", "^TestReadmeSnippetsCompile$", "./...")
}

func runReadmeConsumerGo(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("external README consumer go %s failed:\n%s", strings.Join(args, " "), output)
	}
}

func writeReadmeConsumerModule(t *testing.T, dir, root string, snippets []string) {
	t.Helper()
	mod := fmt.Sprintf(`module github.com/thesyncim/goav-readme-consumer

go 1.26.4

require github.com/thesyncim/goav v0.0.0

replace github.com/thesyncim/goav => %s
`, strconv.Quote(filepath.ToSlash(root)))
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0o644); err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile("go.sum")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "readme_test.go"), []byte(readmeConsumerTestSource(snippets)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readmeConsumerTestSource(snippets []string) string {
	var out strings.Builder
	out.WriteString(`package readmeconsumer

import (
	"context"
	"io"
	"testing"

	"github.com/thesyncim/goav"
)

`)
	for i, snippet := range snippets {
		fmt.Fprintf(&out, "func readmeSnippet%d(ctx context.Context, in io.Reader, out io.Writer) error {\n%s\n}\n\n", i, indentSnippet(snippet))
	}
	out.WriteString("func TestReadmeSnippetsCompile(t *testing.T) {\n")
	for i := range snippets {
		fmt.Fprintf(&out, "\t_ = readmeSnippet%d\n", i)
	}
	out.WriteString("}\n")
	return out.String()
}

func indentSnippet(snippet string) string {
	lines := strings.Split(strings.TrimSpace(snippet), "\n")
	for i := range lines {
		lines[i] = "\t" + strings.TrimRight(lines[i], " \t")
	}
	return strings.Join(lines, "\n")
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
