package goav

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var readmeGoBlockPattern = regexp.MustCompile("(?s)```go\n(.*?)```")

// TestReadmeGoExamplesCompile pins the README's Go blocks to the current API:
// each block is wrapped in a harness package inside a throwaway module that
// replaces github.com/thesyncim/goav with this checkout, then built with the
// real toolchain. A README example that drifts from the API fails here instead
// of in a reader's first five minutes. Blocks are fragments by design, so the
// harness provides the context, reader, and writer the front-door snippets
// use; a future block needing more vocabulary should extend the harness — that
// forced look is the pin working.
func TestReadmeGoExamplesCompile(t *testing.T) {
	if testing.Short() {
		t.Skip("compiling README examples shells out to go build")
	}
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	blocks := readmeGoBlockPattern.FindAllStringSubmatch(string(readme), -1)
	if len(blocks) == 0 {
		t.Fatal("README.md contains no ```go blocks; update this pin if that is deliberate")
	}
	repoRoot, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}

	for i, block := range blocks {
		body := block[1]
		src := body
		if !strings.Contains(body, "package ") {
			src = "package readmecheck\n\n" +
				"import (\n" +
				"\t\"context\"\n" +
				"\t\"io\"\n\n" +
				"\t\"github.com/thesyncim/goav\"\n" +
				"\t\"github.com/thesyncim/goav/bundle\"\n" +
				"\t\"github.com/thesyncim/goav/codec\"\n" +
				")\n\n" +
				"func compileReadmeBlock(ctx context.Context, in io.Reader, out io.Writer) error {\n" +
				body +
				"\treturn err\n" +
				"}\n\n" +
				"var _ = codec.Opus // keep the harness imports referenced even if a block narrows\n" +
				"var _ = goav.From\n" +
				"var _ = bundle.Run\n"
		}

		dir := t.TempDir()
		gomod := "module readmecheck\n\ngo 1.26\n\nrequire github.com/thesyncim/goav v0.0.0\n\nreplace github.com/thesyncim/goav => " + repoRoot + "\n"
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "block.go"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("go", "build", "./...")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod", "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("README go block %d does not compile against the current API:\n%s\n--- block ---\n%s", i+1, out, body)
		}
	}
}
