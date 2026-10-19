package goav

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// grammarCompileBoundaryFiles are the value grammar: it builds Stream values
// and Outputs, which the compiler reads; it never reads the compiler's state.
var grammarCompileBoundaryFiles = []string{
	"stream.go",
	"source.go",
	"stream_rule.go",
}

var plannerStateIdentifiers = map[string]bool{
	"recipeCompileState":       true,
	"branchCompositionPresent": true,
	"branchInputAttachment":    true,
	"branchInputProbe":         true,
	"branchInputProbeReady":    true,
	"inputAttachments":         true,
	"inputFacts":               true,
	"inputProbes":              true,
	"joinFacts":                true,
	"joinTree":                 true,
	"streamRuleFacts":          true,
}

// TestGrammarFilesDoNotReadPlannerState pins that the grammar files never read
// the compiler's state: the compiler reads the recipe DAG the grammar builds.
func TestGrammarFilesDoNotReadPlannerState(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string
	for _, file := range grammarCompileBoundaryFiles {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if !ok || !plannerStateIdentifiers[ident.Name] {
				return true
			}
			violations = append(violations,
				file+":"+strconv.Itoa(fset.Position(ident.Pos()).Line)+" references "+ident.Name)
			return true
		})
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("grammar files read planner compile state:\n%s\n"+
			"read compiler state in the compile files, not the grammar",
			strings.Join(violations, "\n"))
	}
}

// TestRecipeIRImportsOnlyLeafPackages pins that the immutable recipe IR package
// imports only leaf vocabulary packages — never the root goav package or a
// builder. That import discipline is what lets builders lower into the IR
// before the planner runs, with no cycle and no builder coupling.
func TestRecipeIRImportsOnlyLeafPackages(t *testing.T) {
	const root = "github.com/thesyncim/goav"
	dir := filepath.Join("internal", "recipeir")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("bad import in %s: %v", entry.Name(), err)
			}
			if path == root {
				t.Fatalf("%s imports the root package %q; the recipe IR must stay a leaf boundary",
					entry.Name(), root)
			}
			if strings.HasPrefix(path, root+"/internal/") && !strings.HasPrefix(path, dir) {
				t.Fatalf("%s imports internal package %q; the recipe IR must depend only on leaf vocabulary packages",
					entry.Name(), path)
			}
		}
	}
}

// TestCompileRefusalsCrossThroughPublicError pins the one way a compiler
// refusal reaches a caller: every root call of an internal/compile function
// that returns an error is the direct argument of publicError (or refuse,
// refuse2 for calls that also return values), so the caller sees
// *BuildError and never the internal builderr.Refusal.
func TestCompileRefusalsCrossThroughPublicError(t *testing.T) {
	fset := token.NewFileSet()
	refusing := map[string]bool{}
	for _, file := range parsePackageFiles(t, fset, filepath.Join("internal", "compile")) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !fn.Name.IsExported() || fn.Type.Results == nil {
				continue
			}
			results := fn.Type.Results.List
			if last, ok := results[len(results)-1].Type.(*ast.Ident); ok && last.Name == "error" {
				refusing[fn.Name.Name] = true
			}
		}
	}
	if len(refusing) == 0 {
		t.Fatal("found no refusing compile functions; the scan is broken")
	}
	converters := map[string]bool{"publicError": true, "refuse": true, "refuse2": true}
	var violations []string
	for _, file := range parsePackageFiles(t, fset, ".") {
		var stack []ast.Node
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			defer func() { stack = append(stack, n) }()
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "compile" || !refusing[sel.Sel.Name] {
				return true
			}
			if len(stack) != 0 {
				if outer, ok := stack[len(stack)-1].(*ast.CallExpr); ok {
					if fn, ok := outer.Fun.(*ast.Ident); ok && converters[fn.Name] && len(outer.Args) == 1 {
						return true
					}
				}
			}
			violations = append(violations, fset.Position(call.Pos()).String()+" compile."+sel.Sel.Name)
			return true
		})
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("compile calls whose refusal is not converted; wrap them in publicError, refuse, or refuse2:\n%s",
			strings.Join(violations, "\n"))
	}
}

// parsePackageFiles parses the non-test Go files directly in dir.
func parsePackageFiles(t *testing.T, fset *token.FileSet, dir string) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, file)
	}
	return files
}
