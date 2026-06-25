package goav_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"

	"github.com/thesyncim/goav/errcode"
)

// TestBuildErrorContractPinned enforces the error contract at the source
// level: every &BuildError{...} literal in the package carries a Code from
// the errcode catalog (never a raw string), an Operation, a Reason, and
// either Suggestions/Fixes (a user-fixable refusal's concrete fixes) or
// Details/Fields (an internal invariant's explanation). The contract is what
// makes goav errors uniformly actionable; this pin makes regressions
// impossible.
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
				t.Errorf("%s: BuildError Code must come from the errcode catalog: %s", filename, reason)
			}
			if _, ok := fields["Operation"]; !ok {
				t.Errorf("%s: BuildError literal without Operation", filename)
			}
			if _, ok := fields["Reason"]; !ok {
				t.Errorf("%s: BuildError literal without Reason", filename)
			}
			_, hasSuggestions := fields["Suggestions"]
			_, hasFixes := fields["Fixes"]
			_, hasDetails := fields["Details"]
			_, hasFields := fields["Fields"]
			if !hasSuggestions && !hasFixes && !hasDetails && !hasFields {
				t.Errorf("%s: BuildError literal carries neither Suggestions/Fixes (the fix) nor Details/Fields (what happened)", filename)
			}
			return true
		})
	}
}

// describeForbiddenCodeExpr rejects Code expressions that bypass the catalog:
// raw string literals, string concatenations, and inline errcode.Code("...")
// conversions. Catalog constants (errcode.X), typed code parameters, struct
// fields, and the joinErrorCode derivation are the allowed forms; the
// compiler guarantees they are typed errcode.Code.
func describeForbiddenCodeExpr(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.BasicLit:
		return fmt.Sprintf("raw string %s", e.Value)
	case *ast.BinaryExpr:
		return "string concatenation"
	case *ast.CallExpr:
		if fn, ok := e.Fun.(*ast.SelectorExpr); ok && fn.Sel.Name == "Code" {
			if pkg, ok := fn.X.(*ast.Ident); ok && pkg.Name == "errcode" {
				return "inline errcode.Code(...) conversion"
			}
		}
	}
	return ""
}

// TestErrorCodeCatalogPinned keeps the catalog itself healthy: every Code
// constant lives in errcode/errcode.go, is documented, drops the package name
// from its own (no errcode.CodeX stutter), uses a snake_case value, and no
// value is declared twice.
func TestErrorCodeCatalogPinned(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errcode/errcode.go", nil, parser.ParseComments)
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
			if !ok || typeIdent.Name != "Code" {
				continue
			}
			for i, name := range vs.Names {
				count++
				if rest, ok := strings.CutPrefix(name.Name, "Code"); ok && rest != "" && rest[0] >= 'A' && rest[0] <= 'Z' {
					t.Errorf("errcode/errcode.go: constant %s stutters as errcode.%s; drop the Code prefix", name.Name, name.Name)
				}
				if vs.Doc == nil || strings.TrimSpace(vs.Doc.Text()) == "" {
					t.Errorf("errcode/errcode.go: %s has no doc comment saying when it fires", name.Name)
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok {
					t.Errorf("errcode/errcode.go: %s must be a string literal", name.Name)
					continue
				}
				code := strings.Trim(lit.Value, `"`)
				if !value.MatchString(code) {
					t.Errorf("errcode/errcode.go: %s value %q is not snake_case", name.Name, code)
				}
				if prev, dup := seen[code]; dup {
					t.Errorf("errcode/errcode.go: value %q declared by both %s and %s", code, prev, name.Name)
				}
				seen[code] = name.Name
			}
		}
	}
	if count < 140 {
		t.Errorf("errcode/errcode.go declares %d Code constants; the catalog should stay complete (>= 140)", count)
	}
}

func TestReservedEncodeWorkInProgressCodeStaysExplicit(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errcode/errcode.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
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
			for i, name := range vs.Names {
				if name.Name != "EncodeWorkInProgress" {
					continue
				}
				if i >= len(vs.Values) {
					t.Fatal("EncodeWorkInProgress has no explicit value")
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || strings.Trim(lit.Value, `"`) != string(errcode.EncodeWorkInProgress) {
					t.Fatalf("EncodeWorkInProgress literal = %v, want %q", vs.Values[i], errcode.EncodeWorkInProgress)
				}
				doc := ""
				if vs.Doc != nil {
					doc = vs.Doc.Text()
				}
				if !strings.Contains(doc, "reserved") ||
					!strings.Contains(doc, "Normal codec availability is reported by the encode adapter error codes") {
					t.Fatalf("EncodeWorkInProgress doc = %q, want reserved-code guidance", doc)
				}
				return
			}
		}
	}
	t.Fatal("EncodeWorkInProgress not found")
}

// TestErrorCodeLiteralsStayInCatalog sweeps every non-test source file for
// stray code-shaped string literals assigned to Code fields — diagnostics and
// decisions included — so new codes land in the errcode catalog instead of
// drifting back to scattered strings.
func TestErrorCodeLiteralsStayInCatalog(t *testing.T) {
	files := parsePackageSourceFiles(t)
	for filename, file := range files {
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
				t.Errorf("%s: Code field assigned raw string %s; declare it in errcode/errcode.go and use the constant", filename, lit.Value)
			}
			return true
		})
	}
}
