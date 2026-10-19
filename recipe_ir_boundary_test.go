package goav

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
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
}

var plannerStateIdentifiers = map[string]bool{
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
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "compile" {
					violations = append(violations,
						file+":"+strconv.Itoa(fset.Position(sel.Pos()).Line)+" references compile."+sel.Sel.Name)
				}
			}
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

// internalLeaves are the internal packages the root builds on, each with the
// internal packages it may import. None imports the root: the root imports
// them, so recipeir (the DAG), builderr (the refusal vocabulary), and compile
// (the compiler) form a one-way stack below it.
var internalLeaves = map[string][]string{
	"recipeir": nil,
	"builderr": nil,
	"compile":  {"internal/builderr", "internal/recipeir"},
}

// TestInternalLeavesNeverImportRoot pins the import discipline of the
// packages below the root: none imports the root package, and each imports
// only goav's public vocabulary packages and the internal packages its row
// allows. The import cycle already forbids compile -> root; this also stops
// the escape route of a new internal package both sides import.
func TestInternalLeavesNeverImportRoot(t *testing.T) {
	const root = "github.com/thesyncim/goav"
	fset := token.NewFileSet()
	for leaf, allowed := range internalLeaves {
		dir := filepath.Join("internal", leaf)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
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
					t.Errorf("%s/%s imports the root package; internal/%s must stay below it", dir, entry.Name(), leaf)
				}
				if rest, ok := strings.CutPrefix(path, root+"/internal/"); ok && !slices.Contains(allowed, "internal/"+rest) {
					t.Errorf("%s/%s imports %s; internal/%s may import only %v", dir, entry.Name(), path, leaf, allowed)
				}
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

// compileBoundary is every internal/compile name the root may use, with the
// root files that may use it: the narrow interface between the two. A new
// use is a deliberate edit here; a name the root stops using must leave too.
var compileBoundary = map[string][]string{
	// entry: the request, the result, and the attach entries
	"AttachBranchPlan": {"compile_attach.go"},
	"AttachGroup":      {"attach_plan.go"},
	"AttachPlan":       {"attach_plan.go"},
	"Compile":          {"compile.go"},
	"Destination":      {"recipe_graph.go"},
	"Env":              {"runtime.go"},
	"Input":            {"recipe_graph.go"},
	"LowerAttach":      {"attach_plan.go"},
	"Output":           {"recipe_graph.go"},
	"PlanAttach":       {"compile_attach.go"},
	"Request":          {"recipe_graph.go"},
	"StreamRulePlan":   {"compile_attach.go"},
	// the work plan Describe, Explain, and the task read
	"ClonePlanDiagnostics":            {"runtime.go"},
	"CloneWorkDestinations":           {"runtime.go"},
	"CloneWorkPlan":                   {"graph_plan.go"},
	"DiagnosticBufferedExecution":     {"recipe_explain.go"},
	"DiagnosticDecodeCodecDeferred":   {"recipe_explain.go"},
	"DiagnosticExplainPreflightError": {"recipe_explain.go"},
	"DiagnosticStreamRule":            {"recipe_explain.go"},
	"PlanDecision":                    {"recipe_explain.go"},
	"WorkBranch":                      {"recipe_explain.go"},
	"WorkDestination":                 {"attach_plan.go", "recipe_explain.go", "runtime.go", "runtime_attach.go"},
	"WorkDestinationID":               {"attach_plan.go"},
	"WorkDestinationNameByID":         {"recipe_explain.go"},
	"WorkInput":                       {"recipe_explain.go"},
	"WorkOperation":                   {"recipe_explain.go"},
	"WorkOperationTerminal":           {"recipe_explain.go"},
	"WorkOperationsByID":              {"recipe_explain.go"},
	"WorkPlan":                        {"compile.go", "graph_plan.go", "recipe_explain.go"},
	"WorkStream":                      {"recipe_explain.go"},
	"WorkTap":                         {"compile.go", "recipe_explain.go"},
	// the compose plan the build opens
	"AddPlannedNode":               {"runtime_plan.go"},
	"AnchorRoute":                  {"attach_plan.go"},
	"ComposeArmBind":               {"join_lower.go"},
	"ComposeArmConform":            {"join_lower.go"},
	"ComposeArmDecode":             {"compile_lower.go", "join_lower.go"},
	"ComposeDecode":                {"attach_plan.go", "compile_lower.go"},
	"ComposeEncode":                {"attach_plan.go", "compile_lower.go"},
	"ComposeGraph":                 {"compile_lower.go"},
	"ComposeJoin":                  {"join_lower.go"},
	"ComposeMux":                   {"attach_plan.go", "compile_lower.go"},
	"ComposeNode":                  {"attach_plan.go", "compile_lower.go", "join_lower.go"},
	"ComposeRemuxMux":              {"compile_lower.go"},
	"ComposeRemuxSink":             {"compile_lower.go"},
	"ComposeSelect":                {"compile_lower.go"},
	"ComposeSink":                  {"attach_plan.go", "compile_lower.go"},
	"ComposeSource":                {"compile_lower.go"},
	"ComposeStep":                  {"attach_plan.go", "compile_lower.go"},
	"ComposeTransform":             {"join_lower.go"},
	"CompositeLayout":              {"video_composite.go"},
	"ConformedStream":              {"join_lower.go"},
	"DecodeRequest":                {"compile_lower.go", "join_lower.go", "runtime_decode.go"},
	"DecodeRequestDetail":          {"runtime_decode.go"},
	"EncodeRequest":                {"runtime_encode.go"},
	"JoinComposite":                {"join_lower.go"},
	"JoinMix":                      {"join_lower.go"},
	"JoinPlan":                     {"compile_lower.go", "join_lower.go"},
	"JoinSyncMode":                 {"audio_mix.go", "join_sync.go", "video_composite.go"},
	"JoinSyncPTS":                  {"join_sync.go"},
	"MediaTransform":               {"join_arm_file.go", "media_transform.go"},
	"PlannedNode":                  {"runtime_plan.go"},
	"RealtimeRecipeBufferCapacity": {"buffer_policy.go"},
	"SelectBufferCapacity":         {"join_lower.go"},
	"TapRef":                       {"attach_plan.go", "tap_ref.go"},
	"TransformResample":            {"join_arm_file.go"},
	"TransformResize":              {"join_arm_file.go"},
	// node names and describe details the build and the plan share
	"DecodeNodeName":       {"runtime_decode.go"},
	"DescribedNodeDetail":  {"provider.go", "recipe_graph.go", "runtime_plan.go"},
	"EncodeNodeDetail":     {"runtime_encode.go"},
	"EncodeNodeName":       {"runtime_encode.go"},
	"HumanizeShape":        {"join_arm_file.go"},
	"JoinSpecDetail":       {"runtime_graph_detail.go"},
	"JoinSyncNodeDetail":   {"audio_mix.go", "video_composite.go"},
	"MediaTransformDetail": {"media_transform.go"},
	"MuxNodeName":          {"destination.go", "mux_destination.go", "runtime_format_error.go"},
	"ReadableSelector":     {"runtime_decode.go"},
	"StreamDiagnostic":     {"runtime_decode.go", "runtime_encode.go"},
	// refusals the root raises with the compiler
	"DestinationInvalidError":                    {"destination.go"},
	"DestinationNameClashError":                  {"recipe_graph.go"},
	"DestinationShapeMismatchError":              {"runtime_attach.go"},
	"EmptyTransformSpecError":                    {"transform.go"},
	"EncodeAdapterIncompatibleErrorAt":           {"runtime_encode.go"},
	"GraphPlanInvalidError":                      {"graph_plan.go"},
	"JoinArmError":                               {"join_arm_file.go", "join_lower.go"},
	"MuxCompatibilityBuildError":                 {"attach_plan.go"},
	"RecipeDecodeAdapterError":                   {"attach_plan.go", "runtime_decode.go"},
	"RecipeEncodeAdapterCodecError":              {"attach_plan.go"},
	"RecipeGraphUnsupportedRecipeError":          {"compile.go", "compile_lower.go"},
	"RecipeTransformAdapterError":                {"attach_plan.go"},
	"StreamInvalidError":                         {"compile_attach.go", "recipe_graph.go"},
	"StreamRuleInvalidError":                     {"runtime_stream_rule.go"},
	"ValidateDecodeAdapterDescriptors":           {"attach_plan.go", "runtime_decode.go"},
	"ValidateEncodeAdapterDescriptors":           {"attach_plan.go"},
	"ValidateRecipeEncode":                       {"attach_plan.go"},
	"ValidateRecipeIROperationShapes":            {"runtime_attach.go"},
	"ValidateRecipeIRTransformAdapterDescriptor": {"attach_plan.go"},
	"ValidateShapeAnnotationDomain":              {"attach_plan.go"},
	"ValidateStageComponent":                     {"graph.go", "runtime.go", "runtime_plan.go"},
	// mux and adapter facts Explain and attach check
	"CheckKnownMuxCompatibility":  {"runtime_attach.go"},
	"CopyMuxStreamFacts":          {"recipe_explain.go"},
	"LiveInputStreams":            {"recipe_explain.go"},
	"MediaAllowed":                {"recipe_explain.go"},
	"MergeStringList":             {"recipe_explain.go"},
	"MuxCompatibilityDiagnostics": {"recipe_explain.go"},
	"MuxCompatibilityIssue":       {"recipe_mux_compat.go", "runtime_attach.go"},
	"MuxCompatibilityIssues":      {"recipe_mux_compat.go"},
	"MuxStreamTimeBase":           {"runtime_attach.go"},
	"PlannedMuxStream":            {"runtime_attach.go"},
	// stream, codec, and shape helpers
	"CloneBuffer":                            {"mux_destination.go"},
	"CloneCodecSettings":                     {"runtime_decode.go"},
	"CloneCodecSpec":                         {"input.go", "operation.go", "recipe_explain.go"},
	"CloneMetadata":                          {"destination.go", "mux_destination.go", "recipe_explain.go", "runtime.go"},
	"CodecAdapterRequest":                    {"runtime_encode.go"},
	"CodecDescriptorSupports":                {"runtime_encode.go"},
	"CodecMedia":                             {"attach_plan.go", "runtime_attach.go"},
	"CodecSpecHasParameters":                 {"runtime_decode.go"},
	"ComponentValidator":                     {"functions.go"},
	"DecodeAdapterRequestFromStream":         {"attach_plan.go", "runtime_decode.go"},
	"EncodeAdapterRequestFromPreparedStream": {"attach_plan.go", "runtime_encode.go"},
	"FirstNonEmptyCodec":                     {"attach_plan.go"},
	"FirstNonEmptyMedia":                     {"attach_plan.go", "runtime_attach.go"},
	"MediaShapeEmpty":                        {"attach_plan.go"},
	"MediaShapeFromCodecSpec":                {"recipe_explain.go"},
	"MediaShapeFromRecipeIRTransform":        {"recipe_explain.go"},
	"NormalizeTapShape":                      {"runtime_attach.go"},
	"OperationsEncodeSpec":                   {"recipe_explain.go"},
	"OperationsHaveDecode":                   {"recipe_explain.go"},
	"OutputNodeDetailWithFormat":             {"mux_destination.go"},
	"PlanShapeConversion":                    {"join_arm_file.go"},
	"RecipeIROperationOutputShape":           {"runtime_attach.go"},
	"RecipeIRTransformEmpty":                 {"attach_plan.go"},
	"RecipeIRTransformFactoryName":           {"operation.go", "recipe_explain.go"},
	"SelectDecodeStream":                     {"compile_lower.go"},
	"SelectStream":                           {"compile_lower.go", "join_lower.go"},
	"SolveRecipeIROperationShapes":           {"runtime_stream_rule.go"},
	"StreamMatchesSelector":                  {"runtime_encode.go", "runtime_select.go"},
	"StreamSelectFromAV":                     {"recipe_graph.go", "runtime_decode.go"},
	"StreamSelectFromStream":                 {"attach_plan.go"},
}

// TestRootUsesCompileOnlyThroughEntry pins the boundary to internal/compile:
// every compile.X a root file names is in compileBoundary for that file, and
// every entry there is still used.
func TestRootUsesCompileOnlyThroughEntry(t *testing.T) {
	fset := token.NewFileSet()
	used := map[string]map[string]bool{}
	var violations []string
	for _, file := range parsePackageFiles(t, fset, ".") {
		name := filepath.Base(fset.Position(file.Pos()).Filename)
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "compile" {
				return true
			}
			symbol := sel.Sel.Name
			if !slices.Contains(compileBoundary[symbol], name) {
				violations = append(violations, fset.Position(sel.Pos()).String()+" uses compile."+symbol)
			}
			if used[symbol] == nil {
				used[symbol] = map[string]bool{}
			}
			used[symbol][name] = true
			return true
		})
	}
	for symbol, files := range compileBoundary {
		for _, file := range files {
			if !used[symbol][file] {
				violations = append(violations, "compileBoundary lists compile."+symbol+" for "+file+", which no longer uses it")
			}
		}
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("root uses of internal/compile outside the boundary:\n%s\n"+
			"reach the compiler through its entries; widen compileBoundary only deliberately",
			strings.Join(violations, "\n"))
	}
}

// builderrAliasFiles are the root files that may spell builderr's vocabulary
// by the root's historical names.
var builderrAliasFiles = []string{"build_error.go", "internal_errcodes.go", "runtime.go"}

// TestRootDeclaresNoCompileAliases pins that the root keeps no shim of the
// compiler: no type alias, var, or const of a compile name anywhere, and
// builderr aliases only in builderrAliasFiles.
func TestRootDeclaresNoCompileAliases(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string
	aliasOf := func(expr ast.Expr) string {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return ""
		}
		if pkg, ok := sel.X.(*ast.Ident); ok {
			return pkg.Name
		}
		return ""
	}
	for _, file := range parsePackageFiles(t, fset, ".") {
		name := filepath.Base(fset.Position(file.Pos()).Filename)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				var targets []ast.Expr
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if spec.Assign.IsValid() {
						targets = append(targets, spec.Type)
					}
				case *ast.ValueSpec:
					targets = append(targets, spec.Values...)
				}
				for _, target := range targets {
					switch pkg := aliasOf(target); {
					case pkg == "compile":
						violations = append(violations, fset.Position(target.Pos()).String()+" aliases a compile name")
					case pkg == "builderr" && !slices.Contains(builderrAliasFiles, name):
						violations = append(violations, fset.Position(target.Pos()).String()+" aliases a builderr name outside "+strings.Join(builderrAliasFiles, ", "))
					}
				}
			}
		}
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("root shims of internal packages:\n%s", strings.Join(violations, "\n"))
	}
}

// TestOnlyRecipeGraphBuildsCompileRequest pins that one root file projects
// a recipe into the compiler's request: the DAG builder, recipe_graph.go.
func TestOnlyRecipeGraphBuildsCompileRequest(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string
	for _, file := range parsePackageFiles(t, fset, ".") {
		name := filepath.Base(fset.Position(file.Pos()).Filename)
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Request" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "compile" && name != "recipe_graph.go" {
				violations = append(violations, fset.Position(lit.Pos()).String())
			}
			return true
		})
	}
	if len(violations) != 0 {
		t.Fatalf("compile.Request built outside recipe_graph.go:\n%s", strings.Join(violations, "\n"))
	}
}
