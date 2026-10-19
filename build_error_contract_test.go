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

// buildErrorLiteral is one &BuildError{...} composite literal found in the
// package source, with the field keys it sets and its raw source text.
type buildErrorLiteral struct {
	pos    string
	fields map[string]bool
	text   string
}

// internalInvariant reports whether the literal is an internal-invariant error
// (a programming bug, not a user-actionable refusal), detected by its marker
// detail. Such errors are exempt from the user-actionable fix requirement.
func (l buildErrorLiteral) internalInvariant() bool {
	return strings.Contains(l.text, "internal invariant")
}

// scanBuildErrorLiterals parses every non-test .go file in the root package and
// returns each BuildError composite literal with the set of keyed fields it
// sets and its source text. It is the shared engine behind the BuildError
// contract pins.
func scanBuildErrorLiterals(t *testing.T) []buildErrorLiteral {
	t.Helper()
	fset := token.NewFileSet()
	var literals []buildErrorLiteral
	for _, name := range refusalSourceFiles(t) {
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if !isRefusalType(lit.Type) {
				return true
			}
			fields := map[string]bool{}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				fields[refusalKey(key.Name)] = true
			}
			literals = append(literals, buildErrorLiteral{
				pos:    fset.Position(lit.Pos()).String(),
				fields: fields,
				text:   string(src[fset.Position(lit.Pos()).Offset:fset.Position(lit.End()).Offset]),
			})
			return true
		})
	}
	if len(literals) == 0 {
		t.Fatal("no BuildError literals found; the scanner is broken")
	}
	compiled := 0
	for _, lit := range literals {
		if strings.HasPrefix(lit.pos, filepath.Join("internal", "compile")) {
			compiled++
		}
	}
	if compiled == 0 {
		t.Fatal("no builderr.Refusal literals found in internal/compile; the scanner is broken")
	}
	return literals
}

// TestErrNoteCarriesNoKeyedDetail pins that errNote (a keyless detail) is never
// handed a "key=value" string: those must use errDetail(key, value) so the value
// stays reachable through BuildError.Detail(key). It guards against the typed
// migration silently dropping a queryable key into a rendered-only note.
func TestErrNoteCarriesNoKeyedDetail(t *testing.T) {
	keyedNote := func(format string) bool {
		key, _, ok := strings.Cut(format, "=")
		if !ok || key == "" {
			return false
		}
		for _, r := range key {
			if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
				return false
			}
		}
		return true
	}
	fset := token.NewFileSet()
	var violations []string
	for _, name := range refusalSourceFiles(t) {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if !isNoteCall(call.Fun) || len(call.Args) != 1 {
				return true
			}
			// The note's rendered string comes either from a string literal or a
			// fmt.Sprintf format string; both reveal a "key=" prefix statically.
			var format string
			switch arg := call.Args[0].(type) {
			case *ast.BasicLit:
				if arg.Kind == token.STRING {
					format, _ = strconv.Unquote(arg.Value)
				}
			case *ast.CallExpr:
				if len(arg.Args) != 0 {
					if lit, ok := arg.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						format, _ = strconv.Unquote(lit.Value)
					}
				}
			}
			if keyedNote(format) {
				violations = append(violations,
					name+":"+strconv.Itoa(fset.Position(call.Pos()).Line)+" errNote("+strconv.Quote(format)+")")
			}
			return true
		})
	}
	if len(violations) != 0 {
		sort.Strings(violations)
		t.Fatalf("errNote carries a key=value string; use errDetail(key, value) so Detail(key) works:\n%s",
			strings.Join(violations, "\n"))
	}
}

// TestEveryUserActionableBuildErrorHasFix pins that every user-actionable
// BuildError carries at least one concrete fix. Internal-invariant errors
// (programming bugs, marked by an "internal invariant" detail) are exempt: a
// user cannot act on them.
func TestEveryUserActionableBuildErrorHasFix(t *testing.T) {
	var missing []string
	for _, lit := range scanBuildErrorLiterals(t) {
		if lit.fields["fixes"] || lit.internalInvariant() {
			continue
		}
		missing = append(missing, lit.pos)
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("%d user-actionable BuildError literals have no fix (set fixes, or mark internal-invariant):\n%s",
			len(missing), strings.Join(missing, "\n"))
	}
}

// TestEveryBuildErrorHasPhaseFamilyCodeReason pins that every BuildError raised
// by the package sets Phase, Family, Code, and Reason explicitly. Phase is the
// load-bearing addition: it removes the old inference-from-operation-string
// fallback, so the lifecycle phase of a refusal is a stated fact, not a guess.
func TestEveryBuildErrorHasPhaseFamilyCodeReason(t *testing.T) {
	required := []string{"Phase", "Family", "Code", "Reason"}
	var missing []string
	for _, lit := range scanBuildErrorLiterals(t) {
		var gaps []string
		for _, field := range required {
			if !lit.fields[field] {
				gaps = append(gaps, field)
			}
		}
		if len(gaps) != 0 {
			missing = append(missing, lit.pos+" missing "+strings.Join(gaps, ","))
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("%d BuildError literals are missing required fields:\n%s",
			len(missing), strings.Join(missing, "\n"))
	}
}

// refusalSourceFiles are the non-test sources that raise refusals: the root
// package and the internal compiler and refusal vocabulary.
func refusalSourceFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	for _, dir := range []string{".", filepath.Join("internal", "compile"), filepath.Join("internal", "builderr")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			files = append(files, filepath.Join(dir, name))
		}
	}
	return files
}

// isRefusalType reports a BuildError or builderr.Refusal composite literal.
func isRefusalType(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name == "BuildError"
	case *ast.SelectorExpr:
		pkg, ok := x.X.(*ast.Ident)
		return ok && pkg.Name == "builderr" && x.Sel.Name == "Refusal"
	}
	return false
}

// refusalKey names a builderr.Refusal key as the BuildError field it fills.
func refusalKey(key string) string {
	switch key {
	case "Details":
		return "fields"
	case "Fixes":
		return "fixes"
	case "Cause":
		return "cause"
	}
	return key
}

// isNoteCall reports a call of errNote or builderr.Note.
func isNoteCall(fun ast.Expr) bool {
	switch x := fun.(type) {
	case *ast.Ident:
		return x.Name == "errNote"
	case *ast.SelectorExpr:
		pkg, ok := x.X.(*ast.Ident)
		return ok && pkg.Name == "builderr" && x.Sel.Name == "Note"
	}
	return false
}
