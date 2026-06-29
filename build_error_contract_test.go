package goav

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
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
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	var literals []buildErrorLiteral
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
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
			ident, ok := lit.Type.(*ast.Ident)
			if !ok || ident.Name != "BuildError" {
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
				fields[key.Name] = true
			}
			literals = append(literals, buildErrorLiteral{
				pos:    filepath.Base(fset.Position(lit.Pos()).String()),
				fields: fields,
				text:   string(src[fset.Position(lit.Pos()).Offset:fset.Position(lit.End()).Offset]),
			})
			return true
		})
	}
	if len(literals) == 0 {
		t.Fatal("no BuildError literals found; the scanner is broken")
	}
	return literals
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
