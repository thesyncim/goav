package goav_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// TestRootModuleDependencyPurity pins the root module's dependency policy:
// the core may depend on github.com/thesyncim/* modules, the standard library,
// and the narrow third-party runtime set required by the built-in pure-Go AAC
// backend. Other third-party code (pion, compression backends, comparison
// libraries) lives in nested modules — rtpav, webrtcav, playoutav, examples —
// each with its own go.mod, so importing goav never pulls a transport
// ecosystem into the consumer's module graph.
//
// The allowlist below is intentionally exact: if the AAC backend's generated
// runtime changes its transitive set, this test asks for an explicit review
// instead of letting dependency sprawl slip in via go.mod.
//
// Replace directives are refused too: the root module must be consumable as
// published, with no local-path indirection.
func TestRootModuleDependencyPurity(t *testing.T) {
	mod := readRootModuleFile(t)

	const allowedPrefix = "github.com/thesyncim/"
	allowedThirdParty := map[string]string{
		"github.com/dustin/go-humanize":    "goaac modernc runtime dependency",
		"github.com/google/uuid":           "goaac modernc runtime dependency",
		"github.com/mattn/go-isatty":       "goaac modernc runtime dependency",
		"github.com/ncruces/go-strftime":   "goaac modernc runtime dependency",
		"github.com/remyoudompheng/bigfft": "goaac modernc runtime dependency",
		"golang.org/x/sys":                 "goaac modernc runtime dependency",
		"modernc.org/libc":                 "goaac pure-Go FAAD2 runtime",
		"modernc.org/mathutil":             "goaac modernc runtime dependency",
		"modernc.org/memory":               "goaac modernc runtime dependency",
	}

	for _, replacement := range mod.Replace {
		t.Errorf("go.mod replace directive %s => %s; the root module must build as published", moduleRef(replacement.Old), moduleRef(replacement.New))
	}

	for _, requirement := range mod.Require {
		module := requirement.Path
		if !strings.HasPrefix(module, allowedPrefix) {
			if _, ok := allowedThirdParty[module]; ok {
				continue
			}
			t.Errorf("go.mod requires third-party dependency %s; the root module may require only %s* plus the reviewed AAC runtime allowlist", module, allowedPrefix)
		}
	}
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
		"github.com/thesyncim/goav/adapters/goaac",
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
