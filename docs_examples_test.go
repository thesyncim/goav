package goav_test

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// docSnippetPackages are the packages a doc snippet may name; a snippet
// imports the ones it references.
var docSnippetPackages = map[string]string{
	"goav":        "github.com/thesyncim/goav",
	"av":          "github.com/thesyncim/goav/av",
	"bundle":      "github.com/thesyncim/goav/bundle",
	"codec":       "github.com/thesyncim/goav/codec",
	"component":   "github.com/thesyncim/goav/component",
	"control":     "github.com/thesyncim/goav/control",
	"ctlserver":   "github.com/thesyncim/goav/ctlserver",
	"errcode":     "github.com/thesyncim/goav/errcode",
	"expert":      "github.com/thesyncim/goav/expert",
	"filter":      "github.com/thesyncim/goav/filter",
	"flow":        "github.com/thesyncim/goav/flow",
	"format":      "github.com/thesyncim/goav/format",
	"goavtest":    "github.com/thesyncim/goav/goavtest",
	"graphrender": "github.com/thesyncim/goav/graphrender",
	"launch":      "github.com/thesyncim/goav/launch",
	"pipeline":    "github.com/thesyncim/goav/pipeline",
	"plan":        "github.com/thesyncim/goav/plan",
	"provider":    "github.com/thesyncim/goav/provider",
	"runconfig":   "github.com/thesyncim/goav/runconfig",
	"shape":       "github.com/thesyncim/goav/shape",
	"source":      "github.com/thesyncim/goav/source",
	"bytes":       "bytes",
	"context":     "context",
	"errors":      "errors",
	"fmt":         "fmt",
	"http":        "net/http",
	"io":          "io",
	"os":          "os",
	"strings":     "strings",
	"sync":        "sync",
	"time":        "time",
}

// docSnippetPrelude gives the names snippets use without declaring them
// their real types, so calls through them are checked too.
const docSnippetPrelude = `
	var ctx context.Context
	var rt *goav.Runtime
	var task goav.LiveTask
	var input goav.InputSpec
	_, _, _, _ = ctx, rt, task, input
`

var (
	docGoBlockPattern   = regexp.MustCompile("(?s)```go\n(.*?)```")
	docUndefinedName    = regexp.MustCompile(`^undefined: \w+$`)
	docSnippetBenignMsg = []string{"declared and not used", "imported and not used", "missing return"}
)

// TestDocsGoExamplesTypeCheck type-checks every Go block in docs/ against the
// compiled API. A block is a fragment of the reader's program: a name it
// leaves undefined (the reader's writer, track, or helper) is context and is
// ignored, and so is an unused variable; everything else — a method the API
// lacks, a wrong argument type, a package-level name that does not exist —
// fails here instead of in a reader's editor. Nested-module packages (rtpav,
// webrtcav) are not importable from the root module, so calls into them are
// unchecked context.
func TestDocsGoExamplesTypeCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checking doc examples shells out to go list")
	}
	check := newDocSnippetChecker(t)
	t.Run("catches API drift", func(t *testing.T) {
		if errs := check.snippet("err := rt.Run(ctx, input.Video().Branches())"); len(errs) == 0 {
			t.Fatal("a call to a method the API lacks type-checked clean; the checker is not checking")
		}
		if errs := check.snippet("var _ = goav.From"); len(errs) == 0 {
			t.Fatal("a missing package-level name type-checked clean; the checker is not checking")
		}
	})
	files, err := filepath.Glob(filepath.Join("docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range docGoBlockPattern.FindAllSubmatchIndex(src, -1) {
			line := bytes.Count(src[:m[2]], []byte("\n")) + 1
			if errs := check.snippet(string(src[m[2]:m[3]])); len(errs) > 0 {
				t.Errorf("%s:%d: Go block does not type-check against the current API:\n  %s", file, line, strings.Join(errs, "\n  "))
			}
		}
	}
}

type docSnippetChecker struct {
	fset     *token.FileSet
	importer types.Importer
}

func newDocSnippetChecker(t *testing.T) *docSnippetChecker {
	t.Helper()
	paths := make([]string, 0, len(docSnippetPackages))
	for _, path := range docSnippetPackages {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	cmd := exec.Command("go", append([]string{"list", "-export", "-deps", "-f", "{{.ImportPath}} {{.Export}}"}, paths...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -export: %v\n%s", err, stderr.String())
	}
	exports := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		if fields := strings.Fields(scanner.Text()); len(fields) == 2 {
			exports[fields[0]] = fields[1]
		}
	}
	fset := token.NewFileSet()
	return &docSnippetChecker{
		fset: fset,
		importer: importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
			file, ok := exports[path]
			if !ok {
				return nil, fmt.Errorf("no export data for %s", path)
			}
			return os.Open(file)
		}),
	}
}

// snippet returns the type errors of one Go block: a whole file when it
// declares a package, else statements inside a function (or, when they do
// not parse as statements, top-level declarations).
func (c *docSnippetChecker) snippet(body string) []string {
	src := body
	if !strings.Contains(body, "package ") {
		imports := docSnippetImports(body + docSnippetPrelude)
		if _, err := parser.ParseFile(token.NewFileSet(), "", "package p\nfunc _() {\n"+body+"\n}", 0); err == nil {
			src = "package snippet\n" + imports + "func _() error {\n" + docSnippetPrelude + "{\n" + body + "\n}\nreturn nil\n}\n"
		} else {
			src = "package snippet\n" + docSnippetImports(body) + body
		}
	}
	file, err := parser.ParseFile(c.fset, "snippet.go", src, 0)
	if err != nil {
		return []string{"parse: " + err.Error()}
	}
	var errs []string
	conf := types.Config{Importer: c.importer, Error: func(err error) {
		msg := err.Error()
		if typeErr, ok := err.(types.Error); ok {
			msg = typeErr.Msg
		}
		if docUndefinedName.MatchString(msg) {
			return
		}
		for _, benign := range docSnippetBenignMsg {
			if strings.Contains(msg, benign) {
				return
			}
		}
		errs = append(errs, msg)
	}}
	_, _ = conf.Check("snippet", c.fset, []*ast.File{file}, nil)
	return errs
}

func docSnippetImports(code string) string {
	var b strings.Builder
	b.WriteString("import (\n")
	for name, path := range docSnippetPackages {
		if regexp.MustCompile(`\b` + name + `\.`).MatchString(code) {
			fmt.Fprintf(&b, "\t%s %q\n", name, path)
		}
	}
	b.WriteString(")\n")
	return b.String()
}
