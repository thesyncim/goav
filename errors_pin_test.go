package goav_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// TestBuildErrorContractPinned enforces the error contract at the source
// level: every &BuildError{...} literal in the package carries a Code from
// the errors.go catalog (never a raw string), an Operation, a Reason, and
// either Suggestions (a user-fixable refusal's concrete fixes) or Details
// (an internal invariant's explanation). The contract is what makes goav
// errors uniformly actionable; this pin makes regressions impossible.
func TestBuildErrorContractPinned(t *testing.T) {
	files := parsePackageSourceFiles(t)
	for filename, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			ident, ok := lit.Type.(*ast.Ident)
			if !ok || ident.Name != "BuildError" {
				return true
			}
			fields := make(map[string]ast.Expr, len(lit.Elts))
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				fields[key.Name] = kv.Value
			}
			code, ok := fields["Code"]
			if !ok {
				t.Errorf("%s: BuildError literal without Code", filename)
				return true
			}
			if reason := describeForbiddenCodeExpr(code); reason != "" {
				t.Errorf("%s: BuildError Code must come from the errors.go catalog: %s", filename, reason)
			}
			if _, ok := fields["Operation"]; !ok {
				t.Errorf("%s: BuildError literal without Operation", filename)
			}
			if _, ok := fields["Reason"]; !ok {
				t.Errorf("%s: BuildError literal without Reason", filename)
			}
			_, hasSuggestions := fields["Suggestions"]
			_, hasDetails := fields["Details"]
			if !hasSuggestions && !hasDetails {
				t.Errorf("%s: BuildError literal carries neither Suggestions (the fix) nor Details (what happened)", filename)
			}
			return true
		})
	}
}

// describeForbiddenCodeExpr rejects Code expressions that bypass the catalog:
// raw string literals, string concatenations, and inline ErrorCode("...")
// conversions. Catalog constants (CodeX), typed code parameters, struct
// fields, and the joinErrorCode derivation are the allowed forms; the
// compiler guarantees they are typed ErrorCode.
func describeForbiddenCodeExpr(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.BasicLit:
		return fmt.Sprintf("raw string %s", e.Value)
	case *ast.BinaryExpr:
		return "string concatenation"
	case *ast.CallExpr:
		if fn, ok := e.Fun.(*ast.Ident); ok && fn.Name == "ErrorCode" {
			return "inline ErrorCode(...) conversion"
		}
	}
	return ""
}

// TestErrorCodeCatalogPinned keeps the catalog itself healthy: every
// ErrorCode constant lives in errors.go, is documented, uses a snake_case
// value, and no value is declared twice.
func TestErrorCodeCatalogPinned(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errors.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	value := regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*$`)
	seen := make(map[string]string)
	count := 0
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typeIdent, ok := vs.Type.(*ast.Ident)
			if !ok || typeIdent.Name != "ErrorCode" {
				continue
			}
			for i, name := range vs.Names {
				count++
				if !strings.HasPrefix(name.Name, "Code") {
					t.Errorf("errors.go: ErrorCode constant %s should be named Code...", name.Name)
				}
				if vs.Doc == nil || strings.TrimSpace(vs.Doc.Text()) == "" {
					t.Errorf("errors.go: %s has no doc comment saying when it fires", name.Name)
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok {
					t.Errorf("errors.go: %s must be a string literal", name.Name)
					continue
				}
				code := strings.Trim(lit.Value, `"`)
				if !value.MatchString(code) {
					t.Errorf("errors.go: %s value %q is not snake_case", name.Name, code)
				}
				if prev, dup := seen[code]; dup {
					t.Errorf("errors.go: value %q declared by both %s and %s", code, prev, name.Name)
				}
				seen[code] = name.Name
			}
		}
	}
	if count < 140 {
		t.Errorf("errors.go declares %d ErrorCode constants; the catalog should stay complete (>= 140)", count)
	}
}

// TestErrorCodeLiteralsStayInCatalog sweeps every non-test source file for
// stray code-shaped string literals assigned to Code fields outside errors.go
// — diagnostics and decisions included — so new codes land in the catalog
// instead of drifting back to scattered strings.
func TestErrorCodeLiteralsStayInCatalog(t *testing.T) {
	files := parsePackageSourceFiles(t)
	for filename, file := range files {
		if filename == "errors.go" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			kv, ok := n.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "Code" {
				return true
			}
			if lit, ok := kv.Value.(*ast.BasicLit); ok {
				t.Errorf("%s: Code field assigned raw string %s; declare it in errors.go and use the constant", filename, lit.Value)
			}
			return true
		})
	}
}
