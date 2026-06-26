package goav_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// docPinImplementationSubtrees are the only public package subtrees outside
// the documentation pin: concrete codec/container implementations behind the
// codec/format seams, registered by bundle.New and explicitly outside
// the governed surface (docs/API_SURFACE.md). Everything else the module
// discovery finds is doc-pinned automatically — there is no package list to
// forget to extend.
var docPinImplementationSubtrees = []string{"adapters", "container"}

func TestControlPlaneCustomCodecDocsUseBootstrapRuntime(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("docs", "CONTROL_PLANE.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, fragment := range []string{
		"UseRuntime(bundle.MustNew(",
		"goavruntime.WithFormatAdapter",
		"The destination container must accept the selected codec.",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("CONTROL_PLANE.md missing %q", fragment)
		}
	}
	if strings.Contains(text, "goav.New(\n        goavruntime.WithFilterAdapter(") {
		t.Fatal("CONTROL_PLANE.md custom-codec bootstrap drifted back to a bare runtime")
	}
}

// docPinGoverned reports whether a discovered package directory falls under
// the documentation pin (true for everything outside the implementation
// subtrees).
func docPinGoverned(dir string) bool {
	for _, subtree := range docPinImplementationSubtrees {
		if dir == subtree || strings.HasPrefix(dir, subtree+"/") {
			return false
		}
	}
	return true
}

// modulePublicPackages discovers every Go package directory in this module by
// walking from the module root: hidden directories, testdata, internal trees,
// and nested modules (their own go.mod — examples/) are skipped, test-only
// directories (no non-test .go files — adapterproof) drop out naturally, and
// every remaining directory with Go sources is returned. The doc and surface
// pins iterate this list instead of a hardcoded one, so a package added
// tomorrow is governed the day it lands — a manual list once shipped
// graphrender ungoverned and failed silently.
func modulePublicPackages(t *testing.T) []string {
	t.Helper()
	if _, err := os.Stat("go.mod"); err != nil {
		t.Fatalf("package discovery must run from the module root: %v", err)
	}
	var dirs []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if path != "." {
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "testdata" || name == "internal" {
				return fs.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return fs.SkipDir // a nested module governs itself
			}
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			dirs = append(dirs, filepath.ToSlash(path))
			break
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(dirs)
	if len(dirs) == 0 || dirs[0] != "." {
		t.Fatalf("package discovery is broken: root package not found in %v", dirs)
	}
	return dirs
}

// TestExportedSymbolsAreDocumented enforces godoc completeness at the source
// level: every discovered public package carries a package comment, and every
// exported package-level symbol — types, funcs, methods on exported types,
// consts, and vars — carries a doc comment (its own, its spec group's, or its
// declaration group's). The contract is what makes the pkg.go.dev surface
// complete; dynamic discovery makes silent regressions impossible even for
// packages added later.
func TestExportedSymbolsAreDocumented(t *testing.T) {
	for _, dir := range modulePublicPackages(t) {
		if !docPinGoverned(dir) {
			continue
		}
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
