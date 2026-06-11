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
// near-frozen vocabulary packages (errcode, lifecycle, plan, snapshot) and the
// diagnostics renderer (graphrender). The extension-seam packages (av, codec,
// format, filter, pipeline, flow, shape, rtpav, webrtcav, goavtest) grow with
// capabilities and are governed by the doc pin instead. See
// docs/API_SURFACE.md for the contract.
var surfacePinPackages = []struct {
	dir    string
	prefix string
}{
	{dir: ".", prefix: "goav"},
	{dir: "errcode", prefix: "errcode"},
	{dir: "graphrender", prefix: "graphrender"},
	{dir: "lifecycle", prefix: "lifecycle"},
	{dir: "plan", prefix: "plan"},
	{dir: "snapshot", prefix: "snapshot"},
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
