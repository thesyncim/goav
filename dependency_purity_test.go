package goav_test

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// rootLicenses is every module the root module may require, with the license
// its LICENSE file must state. Importing goav links only permissive code by
// default; the one copyleft entry, goh264 (LGPL-2.1), is linked only by
// builds with -tags goav_goh264. GPL backends live in the gplcodec module.
var rootLicenses = map[string]struct {
	license, marker, buildTag string
}{
	"github.com/thesyncim/goav1":  {"BSD-2-Clause", "Redistribution and use in source and binary forms", ""},
	"github.com/thesyncim/gopus":  {"BSD-3-Clause", "BSD 3-Clause License", ""},
	"github.com/thesyncim/govpx":  {"BSD-3-Clause", "BSD 3-Clause License", ""},
	"github.com/thesyncim/goh264": {"LGPL-2.1", "GNU LESSER GENERAL PUBLIC LICENSE", "goav_goh264"},
}

// TestRootModuleDependencyPurity pins the root module's dependency and
// license policy: go.mod requires only the modules in rootLicenses, each
// LICENSE file states the license recorded there, no GPL module is reachable
// from any root package or test (with or without build tags), and a module
// recorded behind a build tag is linked only under it. Third-party code
// (pion, purego, the FAAD2 runtime) lives in nested modules — rtpav,
// webrtcav, playoutav, player, libcodec, gplcodec, examples — so importing
// goav pulls none of it into the consumer's module graph.
//
// Replace directives are refused too: the root module must be consumable as
// published, with no local-path indirection.
func TestRootModuleDependencyPurity(t *testing.T) {
	mod := readRootModuleFile(t)
	for _, replacement := range mod.Replace {
		t.Errorf("go.mod replace directive %s => %s; the root module must build as published", moduleRef(replacement.Old), moduleRef(replacement.New))
	}
	for _, requirement := range mod.Require {
		want, ok := rootLicenses[requirement.Path]
		if !ok {
			t.Errorf("go.mod requires %s, which has no reviewed license entry; the root module carries only permissively licensed code (copyleft goes in gplcodec), so add it to rootLicenses after review or move it to a nested module", requirement.Path)
			continue
		}
		license := moduleLicense(t, requirement.Path)
		if !strings.Contains(license, want.marker) {
			t.Errorf("%s LICENSE no longer states %s; review the dependency's new license", requirement.Path, want.license)
		}
		if strings.Contains(license, "GNU GENERAL PUBLIC LICENSE") && !strings.Contains(license, "GNU LESSER") {
			t.Errorf("%s is GPL-licensed; GPL backends belong in the gplcodec module", requirement.Path)
		}
	}
	for _, tags := range []string{"", "goav_goh264"} {
		for module := range linkedModules(t, tags) {
			want, ok := rootLicenses[module]
			if !ok {
				t.Errorf("root packages (tags %q) link %s, which go.mod policy does not allow", tags, module)
				continue
			}
			if want.buildTag != "" && want.buildTag != tags {
				t.Errorf("root packages link %s (%s) without -tags %s; that dependency must stay opt-in", module, want.license, want.buildTag)
			}
		}
	}
}

// linkedModules is the set of dependency modules any root package or test
// imports under tags.
func linkedModules(t *testing.T, tags string) map[string]bool {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-test", "-tags="+tags, "-f", "{{with .Module}}{{if not .Main}}{{.Path}}{{end}}{{end}}", "./...")
	out, err := cmd.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list -deps failed: %v\n%s", err, exit.Stderr)
		}
		t.Fatalf("go list -deps failed: %v", err)
	}
	modules := map[string]bool{}
	for line := range strings.Lines(string(out)) {
		if line = strings.TrimSpace(line); line != "" {
			modules[line] = true
		}
	}
	return modules
}

// moduleLicense is the text of a required module's LICENSE file.
func moduleLicense(t *testing.T, module string) string {
	t.Helper()
	// go mod download fetches a module no default build links (goh264).
	out, err := exec.Command("go", "mod", "download", "-json", module).Output()
	var downloaded struct{ Dir string }
	if err != nil || json.Unmarshal(out, &downloaded) != nil || downloaded.Dir == "" {
		t.Fatalf("download %s: %v\n%s", module, err, out)
	}
	dir := downloaded.Dir
	data, err := os.ReadFile(filepath.Join(dir, "LICENSE"))
	if err != nil {
		t.Fatalf("%s has no LICENSE file: %v", module, err)
	}
	return string(data)
}

type moduleEditFile struct {
	Require []moduleRequirement `json:"Require"`
	Replace []moduleReplacement `json:"Replace"`
}

type moduleRequirement struct {
	Path string `json:"Path"`
}

type moduleReplacement struct {
	Old modulePathVersion `json:"Old"`
	New modulePathVersion `json:"New"`
}

type modulePathVersion struct {
	Path    string `json:"Path"`
	Version string `json:"Version"`
}

func readRootModuleFile(t *testing.T) moduleEditFile {
	t.Helper()
	cmd := exec.Command("go", "mod", "edit", "-json")
	out, err := cmd.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go mod edit -json failed: %v\n%s", err, exit.Stderr)
		}
		t.Fatalf("go mod edit -json failed: %v", err)
	}
	var mod moduleEditFile
	if err := json.Unmarshal(out, &mod); err != nil {
		t.Fatalf("decode go mod edit -json: %v", err)
	}
	return mod
}

func moduleRef(module modulePathVersion) string {
	if module.Version == "" {
		return module.Path
	}
	return module.Path + " " + module.Version
}

// TestNoRepositoryAssembly pins the asm-free policy shared with the
// thesyncim/* codec backends: vector kernels are Go SIMD behind
// GOEXPERIMENT=simd with a scalar fallback, so every build configuration is
// plain, checkable Go. The walk is filesystem-wide, not per-GOARCH, so a
// kernel for any architecture is caught.
func TestNoRepositoryAssembly(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "testdata" || entry.Name() == "bench-results") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && (strings.HasSuffix(path, ".s") || strings.HasSuffix(path, ".S")) {
			t.Errorf("assembly file %s; write the kernel with the Go simd package (GOEXPERIMENT=simd) and keep a scalar fallback", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRootImportDoesNotPullBundledAdapters(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "github.com/thesyncim/goav")
	out, err := cmd.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list -deps failed: %v\n%s", err, exit.Stderr)
		}
		t.Fatalf("go list -deps failed: %v", err)
	}
	deps := string(out)
	for _, forbidden := range []string{
		"github.com/thesyncim/goav/adapters/annexb",
		"github.com/thesyncim/goav/adapters/goav1",
		"github.com/thesyncim/goav/adapters/goh264",
		"github.com/thesyncim/goav/adapters/gopus",
		"github.com/thesyncim/goav/adapters/govpx",
		"github.com/thesyncim/goav/adapters/ivf",
		"github.com/thesyncim/goav/adapters/resample",
		"github.com/thesyncim/goav/adapters/resize",
		"github.com/thesyncim/goav/container/matroska",
		"github.com/thesyncim/goav/container/mp4",
		"github.com/thesyncim/goav/container/webm",
		"github.com/thesyncim/goaac",
		"github.com/thesyncim/goav1",
		"github.com/thesyncim/gopus",
		"github.com/thesyncim/govpx",
	} {
		if strings.Contains(deps, forbidden) {
			t.Fatalf("root import pulls bundled adapter/backend dependency %s", forbidden)
		}
	}
}

// TestNoCommittedBinaries keeps compiled programs out of the repository: a
// tracked file starting with an ELF, Mach-O, or PE header fails, and the
// helper binaries the scripts build stay ignored.
func TestNoCommittedBinaries(t *testing.T) {
	out, err := exec.Command("git", "ls-files", "-z").Output()
	if err != nil {
		t.Skipf("not a git checkout: %v", err)
	}
	magics := [][]byte{
		{0x7f, 'E', 'L', 'F'},
		{0xfe, 0xed, 0xfa, 0xce}, {0xfe, 0xed, 0xfa, 0xcf}, {0xce, 0xfa, 0xed, 0xfe}, {0xcf, 0xfa, 0xed, 0xfe}, {0xca, 0xfe, 0xba, 0xbe},
		{'M', 'Z'},
	}
	for name := range strings.SplitSeq(strings.TrimSuffix(string(out), "\x00"), "\x00") {
		file, err := os.Open(name)
		if err != nil {
			continue // deleted in the working tree
		}
		head := make([]byte, 4)
		n, _ := file.Read(head)
		file.Close()
		for _, magic := range magics {
			if bytes.HasPrefix(head[:n], magic) {
				t.Errorf("%s is a compiled binary; remove it with git rm --cached and ignore it in .gitignore", name)
			}
		}
	}
	for _, built := range []string{"scripts/bench/internal/benchbaseline/benchbaseline", "cmd/goav/goav"} {
		if err := exec.Command("git", "check-ignore", "-q", built).Run(); err != nil {
			t.Errorf("%s is not ignored; add it to .gitignore", built)
		}
	}
}
