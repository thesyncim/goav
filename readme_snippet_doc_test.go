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
	if len(snippets) == 0 {
		t.Fatal("README should include at least one adoption-front-door Go example")
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	moduleDir := t.TempDir()
	writeReadmeConsumerModule(t, moduleDir, root, snippets)

	runReadmeConsumerGo(t, moduleDir, "test", "-mod=mod", "-run", "^TestReadmeSnippetsCompile$", "./...")
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

go 1.26

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
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/std"
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

func TestReadmeLiveExamplesUseLiveSourceSemantics(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, section := range []struct {
		summary  string
		required []string
	}{
		{
			summary: "Live camera track: archive steadily, keep preview low-latency",
			required: []string{
				"goav.Source(",
				"make(chan *av.Packet)",
				"goav.Sync(",
				"goav.Codec(codec.VP8())",
			},
		},
		{
			summary: "Dynamic WebRTC/RTP tracks: attach branches as streams appear",
			required: []string{
				"av.EventStreamAdded",
				"av.EventStreamRemoved",
				"packet.StreamID = camera.ID",
				"goav.OnRemove(goav.DrainBranch())",
			},
		},
	} {
		block := readmeDetailsBlock(t, text, section.summary)
		if strings.Contains(block, "goav.FileInput(") {
			t.Fatalf("%s example uses FileInput; live/runtime examples should model live sources", section.summary)
		}
		for _, required := range section.required {
			if !strings.Contains(block, required) {
				t.Fatalf("%s example missing %q", section.summary, required)
			}
		}
	}
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

func readmeDetailsBlock(t *testing.T, text string, summary string) string {
	t.Helper()
	start := strings.Index(text, "<summary><strong>"+summary+"</strong></summary>")
	if start < 0 {
		t.Fatalf("README details block %q not found", summary)
	}
	end := strings.Index(text[start:], "</details>")
	if end < 0 {
		t.Fatalf("README details block %q is missing </details>", summary)
	}
	return text[start : start+end]
}
