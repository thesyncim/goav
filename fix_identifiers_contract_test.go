package goav_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestStringLiteralAPIReferencesResolve keeps error fixes and diagnostics
// honest: every pkg.Name a string literal spells for one of goav's own
// packages (goav.Branch, codec.Opus, flow.Blocking, ...) and every task.Name
// must be an exported symbol that exists today. A fix that names a removed API
// (Inspectable.Taps, Mutable.Attach) sends users to code that cannot compile.
func TestStringLiteralAPIReferencesResolve(t *testing.T) {
	exported := map[string]map[string]bool{}
	packages := map[string]string{"goav": "."}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			packages[entry.Name()] = entry.Name()
		}
	}
	fset := token.NewFileSet()
	files := map[string][]*ast.File{}
	for name, dir := range packages {
		parsed := parseDir(t, fset, dir)
		if len(parsed) == 0 {
			continue
		}
		files[name] = parsed
		names := map[string]bool{}
		for _, file := range parsed {
			collectExported(file, names)
		}
		exported[name] = names
	}
	// task.X names a method of the task a Build returns.
	exported["task"] = map[string]bool{}
	for _, file := range files["goav"] {
		ast.Inspect(file, func(n ast.Node) bool {
			spec, ok := n.(*ast.TypeSpec)
			if !ok || (spec.Name.Name != "Task" && spec.Name.Name != "LiveTask") {
				return true
			}
			if iface, ok := spec.Type.(*ast.InterfaceType); ok {
				for _, method := range iface.Methods.List {
					for _, name := range method.Names {
						exported["task"][name.Name] = true
					}
				}
			}
			return false
		})
	}

	reference := regexp.MustCompile(`\b([a-z][a-z0-9]*)\.([A-Z][A-Za-z0-9]*)`)
	// Type.Method( must name a type some goav package exports.
	methodCall := regexp.MustCompile(`\b([A-Z][A-Za-z0-9]*)\.[A-Z][A-Za-z0-9]*\(`)
	types := map[string]bool{}
	for _, names := range exported {
		for name, ok := range names {
			types[name] = types[name] || ok
		}
	}
	for _, parsed := range files {
		for _, file := range parsed {
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				for _, match := range reference.FindAllStringSubmatch(value, -1) {
					names, known := exported[match[1]]
					if known && !names[match[2]] {
						t.Errorf("%s: %q names %s.%s, which does not exist", fset.Position(lit.Pos()), value, match[1], match[2])
					}
				}
				for _, match := range methodCall.FindAllStringSubmatch(value, -1) {
					if !types[match[1]] {
						t.Errorf("%s: %q calls a method on %s, which no goav package exports", fset.Position(lit.Pos()), value, match[1])
					}
				}
				return true
			})
		}
	}
}

func parseDir(t *testing.T, fset *token.FileSet, dir string) []*ast.File {
	t.Helper()
	var out []*ast.File
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != dir {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		out = append(out, file)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func collectExported(file *ast.File, names map[string]bool) {
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if decl.Name.IsExported() {
				names[decl.Name.Name] = true
			}
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					names[spec.Name.Name] = spec.Name.IsExported()
					if iface, ok := spec.Type.(*ast.InterfaceType); ok {
						for _, method := range iface.Methods.List {
							for _, name := range method.Names {
								names[name.Name] = true
							}
						}
					}
					if st, ok := spec.Type.(*ast.StructType); ok {
						for _, field := range st.Fields.List {
							for _, name := range field.Names {
								names[name.Name] = names[name.Name] || name.IsExported()
							}
						}
					}
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						if name.IsExported() {
							names[name.Name] = true
						}
					}
				}
			}
		}
	}
}
