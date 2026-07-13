package goav

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// errcodeConstPattern matches gofmt-shaped Code constant declarations in
// errcode/errcode.go: `\tName Code = "value"`.
var errcodeConstPattern = regexp.MustCompile(`(?m)^\t([A-Z][A-Za-z0-9]*) Code = "([a-z0-9_]+)"`)

// derivedJoinCodePattern matches the derived-code Constant cells in the
// catalog's Full Code Index: joinErrorCode("mix", "inputs").
var derivedJoinCodePattern = regexp.MustCompile(`^joinErrorCode\("([a-z]+)", "([a-z_]+)"\)$`)

// TestErrorCatalogDocMatchesErrcodeCatalog keeps docs/ERROR_CATALOG.md and
// errcode/errcode.go in lockstep, both directions: every Code constant in the
// source appears in the doc's Full Code Index with its constant name, every
// doc row resolves to a declared constant or a derived join code, and every
// named coverage test exists in this repository. The doc's header cites this
// test by name; if the test is renamed, update the header.
func TestErrorCatalogDocMatchesErrcodeCatalog(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("errcode", "errcode.go"))
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]string{} // code value -> constant name
	for _, match := range errcodeConstPattern.FindAllStringSubmatch(string(source), -1) {
		declared[match[2]] = match[1]
	}
	if len(declared) == 0 {
		t.Fatal("no Code constants found in errcode/errcode.go; the declaration pattern may have drifted")
	}

	doc, err := os.ReadFile(filepath.Join("docs", "ERROR_CATALOG.md"))
	if err != nil {
		t.Fatal(err)
	}
	docText := string(doc)
	if !strings.Contains(docText, "TestErrorCatalogDocMatchesErrcodeCatalog") {
		t.Fatal("docs/ERROR_CATALOG.md header no longer names this test")
	}

	testIndex := repoTestFunctionIndex(t)

	indexed := map[string]string{} // code value -> constant cell
	inIndex := false
	for lineNo, line := range strings.Split(docText, "\n") {
		if strings.HasPrefix(line, "## ") {
			inIndex = strings.Contains(line, "Full Code Index")
			continue
		}
		if !inIndex || !strings.HasPrefix(line, "| ") || strings.HasPrefix(line, "|---") || strings.HasPrefix(line, "| Section") {
			continue
		}
		cells := splitTableRow(line)
		if len(cells) != 6 {
			t.Fatalf("docs/ERROR_CATALOG.md:%d: Full Code Index row has %d cells, want 6: %s", lineNo+1, len(cells), line)
		}
		constant := strings.Trim(cells[1], "`")
		code := strings.Trim(cells[2], "`")
		coverage := cells[5]
		indexed[code] = constant

		switch {
		case strings.HasPrefix(constant, "errcode."):
			name := strings.TrimPrefix(constant, "errcode.")
			if declared[code] != name {
				t.Errorf("docs/ERROR_CATALOG.md:%d: row %q cites %s but errcode/errcode.go declares %q for that code", lineNo+1, code, constant, declared[code])
			}
		case derivedJoinCodePattern.MatchString(constant):
			m := derivedJoinCodePattern.FindStringSubmatch(constant)
			if want := m[1] + "_" + m[2]; want != code {
				t.Errorf("docs/ERROR_CATALOG.md:%d: derived constant %s does not produce code %q", lineNo+1, constant, code)
			}
		default:
			t.Errorf("docs/ERROR_CATALOG.md:%d: constant cell %q is neither an errcode constant nor a derived join code", lineNo+1, constant)
		}

		if !testIndex[coverage] {
			t.Errorf("docs/ERROR_CATALOG.md:%d: coverage test %q does not exist in the repository", lineNo+1, coverage)
		}
	}

	for code, name := range declared {
		if _, ok := indexed[code]; !ok {
			t.Errorf("errcode.%s (%q) is missing from the Full Code Index in docs/ERROR_CATALOG.md", name, code)
		}
	}
}

// citationTestFilePattern matches backtick-quoted test file citations in docs.
var citationTestFilePattern = regexp.MustCompile("`([a-z0-9_/]+_test\\.go)`")

// citationTestFuncPattern matches backtick-quoted test function citations in
// docs; the leading boundary keeps file citations and qualified names out.
var citationTestFuncPattern = regexp.MustCompile("`(Test[A-Z][A-Za-z0-9_]*)`")

// citationScriptPattern matches cited helper script paths.
var citationScriptPattern = regexp.MustCompile("`(scripts/[a-z0-9/._-]+\\.sh)`")

// TestDocsCiteOnlyLivingArtifacts is the link-rot pin: every test file, test
// function, or script a living document cites as evidence must exist in the
// repository. Docs under docs/history/ are exempt — they describe the past.
// This pin exists because six docs once cited enforcement tests that had been
// deleted weeks earlier, and nothing noticed.
func TestDocsCiteOnlyLivingArtifacts(t *testing.T) {
	testFiles, testFuncs, scripts := repoCitationIndex(t)

	var docPaths []string
	entries, err := os.ReadDir("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
			docPaths = append(docPaths, filepath.Join("docs", entry.Name()))
		}
	}
	docPaths = append(docPaths, "README.md", "CHANGELOG.md", "CONTRIBUTING.md")

	for _, path := range docPaths {
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for lineNo, line := range strings.Split(string(data), "\n") {
			for _, match := range citationTestFilePattern.FindAllStringSubmatch(line, -1) {
				if !testFiles[filepath.Base(match[1])] {
					t.Errorf("%s:%d cites test file %q which does not exist", path, lineNo+1, match[1])
				}
			}
			for _, match := range citationTestFuncPattern.FindAllStringSubmatch(line, -1) {
				if !testFuncs[match[1]] {
					t.Errorf("%s:%d cites test %q which does not exist", path, lineNo+1, match[1])
				}
			}
			for _, match := range citationScriptPattern.FindAllStringSubmatch(line, -1) {
				if !scripts[match[1]] {
					t.Errorf("%s:%d cites script %q which does not exist", path, lineNo+1, match[1])
				}
			}
		}
	}
}

func splitTableRow(line string) []string {
	trimmed := strings.Trim(line, "|")
	parts := strings.Split(trimmed, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

var testFuncDeclPattern = regexp.MustCompile(`(?m)^func (Test[A-Z][A-Za-z0-9_]*)\(`)

// repoCitationIndex walks the repository once and returns the sets of test
// file base names, test function names, and script paths that exist. Nested
// modules and examples are included: docs may cite their tests as evidence.
func repoCitationIndex(t *testing.T) (testFiles, testFuncs, scripts map[string]bool) {
	t.Helper()
	testFiles = map[string]bool{}
	testFuncs = map[string]bool{}
	scripts = map[string]bool{}
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".idea", ".claude", "vendor", "bench-results", "history":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".sh") && strings.HasPrefix(path, "scripts/") {
			scripts[path] = true
		}
		if strings.HasSuffix(path, "_test.go") {
			testFiles[filepath.Base(path)] = true
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, match := range testFuncDeclPattern.FindAllStringSubmatch(string(data), -1) {
				testFuncs[match[1]] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return testFiles, testFuncs, scripts
}

func repoTestFunctionIndex(t *testing.T) map[string]bool {
	t.Helper()
	_, funcs, _ := repoCitationIndex(t)
	return funcs
}
