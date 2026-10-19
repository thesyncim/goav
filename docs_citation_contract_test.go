package goav

import (
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav/errcode"
)

// errcodeConstPattern matches gofmt-shaped Code constant declarations in
// errcode/errcode.go: `\tName Code = "value"`.
var errcodeConstPattern = regexp.MustCompile(`(?m)^\t([A-Z][A-Za-z0-9]*) Code = "([a-z0-9_]+)"`)

// builderrCodePattern matches the refusal codes internal/builderr declares:
// `\tNameCode errcode.Code = "value"`.
var builderrCodePattern = regexp.MustCompile(`(?m)^\t[A-Z][A-Za-z0-9]*Code +errcode\.Code = "([a-z0-9_]+)"`)

// catalogCodes is every code a goav refusal can carry, from the source: the
// public errcode constants, the internal refusal codes, and the codes the
// built-in joins derive (<kind>_inputs, <kind>_arm). It refuses a code with
// no family, which would leave callers nothing stable to switch on.
func catalogCodes(t *testing.T) map[errcode.Family][]string {
	t.Helper()
	var codes []string
	for path, pattern := range map[string]*regexp.Regexp{
		filepath.Join("errcode", "errcode.go"):            errcodeConstPattern,
		filepath.Join("internal", "builderr", "codes.go"): builderrCodePattern,
	} {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		matches := pattern.FindAllStringSubmatch(string(source), -1)
		if len(matches) == 0 {
			t.Fatalf("no codes found in %s; the declaration pattern may have drifted", path)
		}
		for _, match := range matches {
			codes = append(codes, match[len(match)-1])
		}
	}
	for _, kind := range []string{"mix", "composite", "select"} {
		codes = append(codes, kind+"_inputs", kind+"_arm")
	}
	families := map[errcode.Family][]string{}
	for _, code := range codes {
		family := errcode.FamilyForCode(errcode.Code(code))
		if family == errcode.FamilyExternal {
			t.Errorf("code %q has no family in errcode.FamilyForCode", code)
		}
		if !slices.Contains(families[family], code) {
			families[family] = append(families[family], code)
		}
	}
	return families
}

// TestErrorCatalogDocMatchesErrcodeCatalog pins docs/ERROR_CATALOG.md's
// code table to the source: it lists every code a goav refusal can carry
// (catalogCodes), by family, and nothing else. GOAV_UPDATE_GOLDEN=1
// regenerates it. The doc's header cites this test by name.
func TestErrorCatalogDocMatchesErrcodeCatalog(t *testing.T) {
	const begin, end = "<!-- BEGIN GENERATED CODES -->\n", "<!-- END GENERATED CODES -->"
	families := catalogCodes(t)
	var b strings.Builder
	b.WriteString(begin + "| Family | Codes |\n| --- | --- |\n")
	for _, family := range slices.Sorted(maps.Keys(families)) {
		codes := slices.Sorted(slices.Values(families[family]))
		b.WriteString("| `" + string(family) + "` | `" + strings.Join(codes, "`, `") + "` |\n")
	}
	path := filepath.Join("docs", "ERROR_CATALOG.md")
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	if !strings.Contains(text, "TestErrorCatalogDocMatchesErrcodeCatalog") {
		t.Fatal("docs/ERROR_CATALOG.md no longer names this test")
	}
	start, stop := strings.Index(text, begin), strings.Index(text, end)
	if start < 0 || stop < start {
		t.Fatalf("docs/ERROR_CATALOG.md has no %q ... %q block", strings.TrimSpace(begin), end)
	}
	if got, want := text[start:stop], b.String(); got != want {
		if os.Getenv("GOAV_UPDATE_GOLDEN") == "1" {
			if err := os.WriteFile(path, []byte(text[:start]+want+text[stop:]), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		t.Errorf("docs/ERROR_CATALOG.md code table differs from the source (regenerate with GOAV_UPDATE_GOLDEN=1):\n got:\n%s\nwant:\n%s", got, want)
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
