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

// plannerLowererFiles are the planner/lowerer source files that must consume the
// immutable recipe IR (internal/recipeir) and never reach back into the mutable
// fluent-builder structs. They are pinned by the boundary tests below.
//
// join_build.go is deliberately NOT in this list: it carries both the join
// grammar (a builder that constructs BranchSpec values) and the join planner in
// one file, so a file-level pin cannot separate the two. Moving the join
// planner off BranchSpec is tracked as remaining boundary work in
// docs/ARCHITECTURE.md.
var plannerLowererFiles = []string{
	"recipe_compile.go",
	"media_plan_build.go",
	"media_plan_spec.go",
	"work_plan.go",
	"work_patch.go",
	"shape_solver.go",
	"branch_compose_plan.go",
	"branch_compose_build.go",
}

// builderInternalTypes are the mutable fluent-builder structs whose internals
// the planner/lowerer must not read; they only cross the boundary as immutable
// recipeir data.
var builderInternalTypes = map[string]bool{
	"BranchSpec":     true,
	"jobStreamBuild": true,
}

// TestPlannerFilesDoNotReadBuilderInternals fails when a planner/lowerer file
// references a mutable builder struct in code (comments are exempt: they are not
// in the AST). This is the executable boundary pin the architecture target asks
// for: planner passes consume recipe IR, not builder internals.
func TestPlannerFilesDoNotReadBuilderInternals(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string
	for _, name := range plannerLowererFiles {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("planner file %s not found (rename it in plannerLowererFiles): %v", name, err)
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if !ok || !builderInternalTypes[ident.Name] {
				return true
			}
			violations = append(violations, name+":"+
				strconv.Itoa(fset.Position(ident.Pos()).Line)+" references "+ident.Name)
			return true
		})
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("planner/lowerer files reach into builder internals:\n%s\n"+
			"consume the immutable recipe IR (internal/recipeir) instead",
			strings.Join(violations, "\n"))
	}
}

// TestJoinPlannerReadsRecipeIRNotBuilderInternals pins that the join planner —
// every method on joinPlan — consumes the captured recipe IR (recipeir.Join
// branches and the joinBranchSnapshot record) and never the mutable BranchSpec
// builder struct. join_build.go mixes the join grammar/capture (which legitimately
// constructs and reads BranchSpec) with the planner in one file, so this is a
// receiver-scoped pin rather than a file-scoped one: the planner's state machine
// must stay BranchSpec-free even though the file is not physically split.
func TestJoinPlannerReadsRecipeIRNotBuilderInternals(t *testing.T) {
	const file = "join_build.go"
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	methods := 0
	var violations []string
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
			continue
		}
		recv := fn.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		ident, ok := recv.(*ast.Ident)
		if !ok || ident.Name != "joinPlan" {
			continue
		}
		methods++
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if ok && id.Name == "BranchSpec" {
				violations = append(violations,
					fn.Name.Name+":"+strconv.Itoa(fset.Position(id.Pos()).Line))
			}
			return true
		})
	}
	if methods == 0 {
		t.Fatal("found no joinPlan methods; the scanner is broken")
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("join planner methods reach into BranchSpec (read the captured recipe IR instead):\n%s",
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
