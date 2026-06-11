package goav_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// docPinPackages are the public packages whose exported surface must stay
// documented. Adapter and container packages are implementation details and
// are not pinned here.
var docPinPackages = []string{
	".",
	"av",
	"codec",
	"codes",
	"filter",
	"flow",
	"format",
	"lifecycle",
	"pipeline",
	"plan",
	"rtpav",
	"shape",
	"snapshot",
	"webrtcav",
}

// TestExportedSymbolsAreDocumented enforces godoc completeness at the source
// level: every public package carries a package comment, and every exported
// package-level symbol — types, funcs, methods on exported types, consts, and
// vars — carries a doc comment (its own, its spec group's, or its
// declaration group's). The contract is what makes the pkg.go.dev surface
// complete; this pin makes silent regressions impossible.
func TestExportedSymbolsAreDocumented(t *testing.T) {
	for _, dir := range docPinPackages {
		files := parsePackageDir(t, dir)
		if len(files) == 0 {
			t.Errorf("%s: no Go source files found", dir)
			continue
		}
		hasPackageDoc := false
		for _, file := range files {
			if docPresent(file.Doc) {
				hasPackageDoc = true
			}
		}
		if !hasPackageDoc {
			t.Errorf("%s: package has no package comment", dir)
		}
		for filename, file := range files {
			checkFileDocs(t, dir, filename, file)
		}
	}
}

func parsePackageDir(t *testing.T, dir string) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = file
	}
	return files
}

func checkFileDocs(t *testing.T, dir string, filename string, file *ast.File) {
	t.Helper()
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			checkFuncDoc(t, dir, filename, d)
		case *ast.GenDecl:
			checkGenDeclDocs(t, dir, filename, d)
		}
	}
}

func checkFuncDoc(t *testing.T, dir string, filename string, d *ast.FuncDecl) {
	t.Helper()
	if !d.Name.IsExported() {
		return
	}
	if d.Recv != nil {
		recv := docPinReceiverName(d.Recv)
		if recv != "" && !ast.IsExported(recv) {
			return
		}
		if !docPresent(d.Doc) {
			t.Errorf("%s/%s: method %s.%s has no doc comment", dir, filename, recv, d.Name.Name)
		}
		return
	}
	if !docPresent(d.Doc) {
		t.Errorf("%s/%s: func %s has no doc comment", dir, filename, d.Name.Name)
	}
}

// checkGenDeclDocs accepts a doc comment on the spec itself, a line comment on
// the spec, or a doc comment on the surrounding declaration group — grouped
// constants with one group comment are idiomatic godoc.
func checkGenDeclDocs(t *testing.T, dir string, filename string, d *ast.GenDecl) {
	t.Helper()
	declDoc := docPresent(d.Doc)
	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.TypeSpec:
			if !s.Name.IsExported() {
				continue
			}
			if !declDoc && !docPresent(s.Doc) {
				t.Errorf("%s/%s: type %s has no doc comment", dir, filename, s.Name.Name)
			}
		case *ast.ValueSpec:
			specDoc := docPresent(s.Doc) || docPresent(s.Comment)
			for _, name := range s.Names {
				if !name.IsExported() {
					continue
				}
				if !declDoc && !specDoc {
					t.Errorf("%s/%s: %s has no doc comment", dir, filename, name.Name)
				}
			}
		}
	}
}

func docPresent(group *ast.CommentGroup) bool {
	return group != nil && strings.TrimSpace(group.Text()) != ""
}

func docPinReceiverName(recv *ast.FieldList) string {
	if len(recv.List) == 0 {
		return ""
	}
	expr := recv.List[0].Type
	for {
		switch typed := expr.(type) {
		case *ast.StarExpr:
			expr = typed.X
		case *ast.IndexExpr:
			expr = typed.X
		case *ast.IndexListExpr:
			expr = typed.X
		case *ast.Ident:
			return typed.Name
		default:
			return ""
		}
	}
}
