package compile

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestExportedSurfaceGolden pins the compiler's exported names: what the
// root package may reach. Growing it is a deliberate edit of
// testdata/surface.golden (regenerate with GOAV_UPDATE_GOLDEN=1), under the
// same rule as the public API surface: keep the waist narrow.
func TestExportedSurfaceGolden(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv == nil && decl.Name.IsExported() {
					names = append(names, "func "+decl.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if spec.Name.IsExported() {
							names = append(names, "type "+spec.Name.Name)
						}
					case *ast.ValueSpec:
						for _, n := range spec.Names {
							if n.IsExported() {
								names = append(names, decl.Tok.String()+" "+n.Name)
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(names)
	got := []byte(strings.Join(names, "\n") + "\n")
	path := filepath.Join("testdata", "surface.golden")
	if os.Getenv("GOAV_UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (regenerate with GOAV_UPDATE_GOLDEN=1): %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("internal/compile exported surface changed (regenerate with GOAV_UPDATE_GOLDEN=1 after a deliberate change):\n%s", got)
	}
}
