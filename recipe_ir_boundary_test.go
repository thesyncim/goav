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
// join_plan.go has a dedicated file-scope pin below. The join planner still
// carries a few root-only operation/destination handles while the remaining
// join boundary slices are tracked separately from this general file set.
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

// TestPlannerNeverReadsDestinationSpecFields pins the A2 boundary slice:
// planner files consume captured destination facts and carry concrete
// destination handles opaquely. Opening a writer or sink still needs
// destinationSpec later, but planner decisions must not inspect its fields.
func TestPlannerNeverReadsDestinationSpecFields(t *testing.T) {
	files := map[string]bool{
		"branch_compose_build.go": true,
		"branch_compose_plan.go":  true,
		"media_plan_build.go":     true,
		"media_plan_spec.go":      true,
	}
	destinationSpecFields := map[string]bool{
		"custom":         true,
		"err":            true,
		"format":         true,
		"group":          true,
		"id":             true,
		"name":           true,
		"output":         true,
		"resolvedFormat": true,
		"sink":           true,
	}
	fset := token.NewFileSet()
	var violations []string
	for file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			selector, ok := n.(*ast.SelectorExpr)
			if !ok || !destinationSpecFields[selector.Sel.Name] {
				return true
			}
			if !destinationSpecSelectorLooksConcrete(file, selector.X) {
				return true
			}
			violations = append(violations,
				file+":"+strconv.Itoa(fset.Position(selector.Sel.Pos()).Line)+" reads ."+selector.Sel.Name)
			return true
		})
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("planner files read destinationSpec internals:\n%s\n"+
			"capture destination facts at the builder/compile edge instead",
			strings.Join(violations, "\n"))
	}
}

// TestPlannerLowersOperationsViaIR pins A3: planner/lowerer files read recipe
// IR operation facts through the resolver layer, not root operationSpec
// directly. Root operation compatibility stays in operation_facts.go while
// planner files consume the lowered DTO/facts surface.
func TestPlannerLowersOperationsViaIR(t *testing.T) {
	files := plannerLowererFiles
	fset := token.NewFileSet()
	var violations []string
	for _, file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if !ok || ident.Name != "operationSpec" {
				return true
			}
			violations = append(violations, file+":"+strconv.Itoa(fset.Position(ident.Pos()).Line))
			return true
		})
	}
	if len(violations) != 0 {
		t.Fatalf("planner/lowerer files read operationSpec directly:\n%s\n"+
			"consume recipe IR operation facts instead",
			strings.Join(violations, "\n"))
	}
}

var grammarCompileBoundaryFiles = []string{
	"recipe.go",
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

// TestGrammarFilesDoNotReadPlannerState pins the reverse A4 boundary:
// grammar files may declare recipes and small query interfaces, but concrete
// recipeCompileState internals live in recipe_compile.go.
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
			"move concrete recipeCompileState reads behind query methods in recipe_compile.go",
			strings.Join(violations, "\n"))
	}
}

func destinationSpecSelectorLooksConcrete(file string, expr ast.Expr) bool {
	if selectorPathContains(expr, "details") {
		return false
	}
	if selectorPathContains(expr, "Destination") {
		return true
	}
	root, ok := selectorRootIdent(expr)
	if !ok {
		return false
	}
	switch root {
	case "destination", "destinations", "namedOutputs":
		return true
	case "output", "outputs":
		return file != "branch_compose_build.go"
	default:
		return false
	}
}

func selectorRootIdent(expr ast.Expr) (string, bool) {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name, true
	case *ast.IndexExpr:
		return selectorRootIdent(expr.X)
	case *ast.SelectorExpr:
		return selectorRootIdent(expr.X)
	default:
		return "", false
	}
}

func selectorPathContains(expr ast.Expr, name string) bool {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name == name
	case *ast.IndexExpr:
		return selectorPathContains(expr.X, name)
	case *ast.SelectorExpr:
		return expr.Sel.Name == name || selectorPathContains(expr.X, name)
	default:
		return false
	}
}

// TestJoinPlannerReadsRecipeIRNotBuilderInternals pins that the join planner
// file consumes the captured recipe IR (recipeir.Join branches and the
// joinBranchSnapshot record) and never the mutable BranchSpec builder struct.
// join_build.go owns grammar/capture; join_plan.go owns planner/lowering.
func TestJoinPlannerReadsRecipeIRNotBuilderInternals(t *testing.T) {
	const file = "join_plan.go"
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	var violations []string
	ast.Inspect(parsed, func(n ast.Node) bool {
		id, ok := n.(*ast.Ident)
		if ok && id.Name == "BranchSpec" {
			violations = append(violations,
				file+":"+strconv.Itoa(fset.Position(id.Pos()).Line))
		}
		return true
	})
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("join planner file reaches into BranchSpec (read the captured recipe IR instead):\n%s",
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
