package goav_test

import (
	"bufio"
	"go/ast"
	"os"
	"sort"
	"strings"
	"testing"
)

// surfacePinPackages are the packages whose exported package-level identifiers
// are governed by testdata/api_surface.txt: the root grammar plus the
// near-frozen vocabulary packages (control, errcode, inspect, lifecycle, plan,
// snapshot) and the diagnostics renderer (graphrender). The extension-seam packages grow with
// capabilities and are governed by the doc pin instead
// (surfaceSeamPackages); TestEveryPublicPackageIsGoverned asserts the two
// lists cover everything the module discovery finds. See docs/API_SURFACE.md
// for the contract.
var surfacePinPackages = []struct {
	dir    string
	prefix string
}{
	{dir: ".", prefix: "goav"},
	{dir: "control", prefix: "control"},
	{dir: "errcode", prefix: "errcode"},
	{dir: "graphrender", prefix: "graphrender"},
	{dir: "inspect", prefix: "inspect"},
	{dir: "lifecycle", prefix: "lifecycle"},
	{dir: "plan", prefix: "plan"},
	{dir: "snapshot", prefix: "snapshot"},
}

// surfaceSeamPackages are the extension-seam packages governed by the doc pin
// instead of the approved list: their surfaces grow with capabilities (new
// codecs, events, options), so freezing them would turn every capability into
// surface churn. They still cannot regress silently — every exported symbol
// must be documented (TestExportedSymbolsAreDocumented).
//
// rtpav, webrtcav, and the examples/* modules are nested modules now (their
// own go.mod, their own dependencies); they govern themselves with their own
// tests and are outside this module's walk by construction.
var surfaceSeamPackages = []string{
	"av",
	"bundle",
	"cmd/goav",
	"codec",
	"ctl",
	"expert",
	"filter",
	"flow",
	"format",
	"goavtest",
	"pipeline",
	"provider",
	"runtime",
	"shape",
}

// TestEveryPublicPackageIsGoverned closes the governance gap hardcoded lists
// leave open: it discovers every package in the module dynamically
// (modulePublicPackages) and requires each one to be classified — frozen
// surface (surfacePinPackages, also doc-pinned), extension seam
// (surfaceSeamPackages, doc-pinned), or implementation subtree behind the
// codec/format seams (docPinImplementationSubtrees). A package added tomorrow
// fails this test until a reviewer places it; a classified package that
// disappears fails until the stale entry is removed.
func TestEveryPublicPackageIsGoverned(t *testing.T) {
	frozen := make(map[string]bool, len(surfacePinPackages))
	for _, pin := range surfacePinPackages {
		frozen[pin.dir] = true
	}
	seam := make(map[string]bool, len(surfaceSeamPackages))
	for _, dir := range surfaceSeamPackages {
		seam[dir] = true
	}
	discovered := make(map[string]bool)
	for _, dir := range modulePublicPackages(t) {
		discovered[dir] = true
		switch {
		case !docPinGoverned(dir):
			if frozen[dir] || seam[dir] {
				t.Errorf("%s is an implementation subtree package and must not also be surface-classified", dir)
			}
		case frozen[dir] && seam[dir]:
			t.Errorf("%s is classified both frozen and seam; pick one", dir)
		case !frozen[dir] && !seam[dir]:
			t.Errorf("%s is a public package with no governance: add it to surfacePinPackages (frozen, approved-list pinned) or surfaceSeamPackages (doc-pin governed)", dir)
		}
	}
	for _, pin := range surfacePinPackages {
		if !discovered[pin.dir] {
			t.Errorf("surfacePinPackages lists %s but no such package exists; remove the stale entry", pin.dir)
		}
	}
	for _, dir := range surfaceSeamPackages {
		if !discovered[dir] {
			t.Errorf("surfaceSeamPackages lists %s but no such package exists; remove the stale entry", dir)
		}
	}
}

// TestPublicSurfaceIsApproved makes silent API growth impossible: it extracts
// every exported package-level identifier (funcs, types, consts, vars) from
// the governed packages and compares the set against the checked-in approved
// list. A new exported symbol fails until a reviewer adds it to
// testdata/api_surface.txt — every addition to the public contract becomes a
// deliberate, diff-visible act. The reverse direction holds too: an approved
// symbol that disappears must be removed from the list, so the list IS the
// surface.
func TestPublicSurfaceIsApproved(t *testing.T) {
	approved := readApprovedSurface(t)
	exported := make(map[string]bool)
	for _, pin := range surfacePinPackages {
		for _, name := range exportedPackageIdentifiers(t, pin.dir) {
			exported[pin.prefix+"."+name] = true
		}
	}
	for _, symbol := range sortedSurface(exported) {
		if !approved[symbol] {
			t.Errorf("%s is exported but not approved; exporting a symbol is a deliberate act — add it to testdata/api_surface.txt in sorted position", symbol)
		}
	}
	for _, symbol := range sortedSurface(approved) {
		if !exported[symbol] {
			t.Errorf("%s is approved but no longer exported; remove it from testdata/api_surface.txt", symbol)
		}
	}
}

// readApprovedSurface loads testdata/api_surface.txt, skipping blank lines and
// # comments, and enforces the file's own hygiene: sorted order (stable diffs)
// and no duplicates.
func readApprovedSurface(t *testing.T) map[string]bool {
	t.Helper()
	file, err := os.Open("testdata/api_surface.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	approved := make(map[string]bool)
	previous := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if approved[line] {
			t.Errorf("testdata/api_surface.txt lists %s twice", line)
		}
		if previous != "" && line < previous {
			t.Errorf("testdata/api_surface.txt is not sorted: %s after %s", line, previous)
		}
		approved[line] = true
		previous = line
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return approved
}

// exportedPackageIdentifiers returns the exported package-level identifiers —
// funcs (not methods), types, consts, and vars — declared by the non-test
// sources of one package directory.
func exportedPackageIdentifiers(t *testing.T, dir string) []string {
	t.Helper()
	names := make(map[string]bool)
	for _, file := range parsePackageDir(t, dir) {
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					names[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							names[s.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, name := range s.Names {
							if name.IsExported() {
								names[name.Name] = true
							}
						}
					}
				}
			}
		}
	}
	return sortedSurface(names)
}

func sortedSurface(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for symbol := range set {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// TestReadmeFirstScreenAvoidsGraphInternals enforces the front-door rule on
// the README's first screen — everything through the close of the fifth Go
// example. The first thing a reader learns must be the grammar alone: no
// pipeline node types, no registries or runtime adapter options, no buffer
// ownership, no expert graph handles. The whole-file guard
// (TestReadmeKeepsAdvancedRuntimeKnobsOutOfFrontDoor) stays broader; this one
// is absolute for the opening screen.
func TestReadmeFirstScreenAvoidsGraphInternals(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	screen := readmeFirstScreen(string(body), 5)
	for _, forbidden := range []string{
		"pipeline.",
		"Registry",
		"WithCodecAdapter",
		"WithFormatAdapter",
		"WithFilterAdapter",
		"WithBufferPolicy",
		"av.Buffer",
		"BufferOwner",
		"Expert(",
		"expert.Graph",
		"GraphBuilder",
		"GraphNode",
		"GraphInlet",
		"GraphOutlet",
		"NewGraph",
		"NodeRef",
	} {
		if strings.Contains(screen, forbidden) {
			t.Errorf("README first screen mentions %q; the first five examples must stay on the From/To grammar", forbidden)
		}
	}
}

func TestReadmeStaysFrontDoorSized(t *testing.T) {
	body, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	lines := strings.Count(text, "\n")
	if lines > 360 {
		t.Fatalf("README has %d lines, want <= 360; move advanced material into docs/", lines)
	}
	for _, required := range []string{
		"## Install",
		"## 30-Second Examples",
		"## Common Recipes",
		"## Why goav",
		"## Capability Matrix",
		"## Stability Matrix",
		"## Deep Dives",
		"img.shields.io/badge/go-1.26%2B",
		"CHANGELOG.md",
		"docs/EXTENSION_COOKBOOK.md",
		"docs/PERFORMANCE.md",
		"docs/RELEASING.md",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README front door missing %q", required)
		}
	}
}

// readmeFirstScreen returns the README prefix through the closing fence of the
// n-th ```go code block (the whole file when fewer exist).
func readmeFirstScreen(text string, n int) string {
	lines := strings.SplitAfter(text, "\n")
	inGoFence := false
	closed := 0
	var screen strings.Builder
	for _, line := range lines {
		screen.WriteString(line)
		trimmed := strings.TrimSpace(line)
		switch {
		case !inGoFence && strings.HasPrefix(trimmed, "```go"):
			inGoFence = true
		case inGoFence && trimmed == "```":
			inGoFence = false
			closed++
			if closed == n {
				return screen.String()
			}
		}
	}
	return screen.String()
}
