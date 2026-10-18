package goav

import (
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestRuntimeMethodsInGodoc pins that Runtime is a real exported type: godoc
// (go doc, pkg.go.dev) lists New as its constructor and Run, Build, Describe,
// and Explain as its methods. An alias to an unexported type lists none.
func TestRuntimeMethodsInGodoc(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	pkg, err := doc.NewFromFiles(fset, files, "github.com/thesyncim/goav")
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range pkg.Types {
		if typ.Name != "Runtime" {
			continue
		}
		if typ.Doc == "" {
			t.Fatal("Runtime has no doc comment")
		}
		listed := map[string]bool{}
		for _, fn := range typ.Funcs {
			listed[fn.Name] = true
		}
		for _, method := range typ.Methods {
			listed[method.Name] = true
		}
		for _, want := range []string{"New", "Run", "Build", "Describe", "Explain"} {
			if !listed[want] {
				t.Errorf("go doc Runtime does not list %s", want)
			}
		}
		return
	}
	t.Fatal("go doc lists no Runtime type")
}
