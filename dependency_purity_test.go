package goav_test

import (
	"os"
	"strings"
	"testing"
)

// TestRootModuleDependencyPurity pins the root module's dependency policy:
// the core may depend on github.com/thesyncim/* modules, the standard library,
// and the narrow third-party runtime set required by the built-in pure-Go AAC
// backend. Other third-party code (pion, compression backends, comparison
// libraries) lives in nested modules — rtpav, webrtcav, examples — each with
// its own go.mod, so importing goav never pulls a transport ecosystem into the
// consumer's module graph.
//
// The allowlist below is intentionally exact: if the AAC backend's generated
// runtime changes its transitive set, this test asks for an explicit review
// instead of letting dependency sprawl slip in via go.mod.
//
// Replace directives are refused too: the root module must be consumable as
// published, with no local-path indirection.
func TestRootModuleDependencyPurity(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}

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

	inRequire := false
	inReplace := false
	for lineNumber, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if comment := strings.Index(line, "//"); comment >= 0 {
			line = strings.TrimSpace(line[:comment])
		}
		if line == "" {
			continue
		}
		switch {
		case line == ")":
			inRequire = false
			inReplace = false
			continue
		case strings.HasPrefix(line, "require ("):
			inRequire = true
			continue
		case strings.HasPrefix(line, "replace (") || strings.HasPrefix(line, "replace "):
			t.Errorf("go.mod line %d: replace directive %q; the root module must build as published", lineNumber+1, line)
			if strings.HasPrefix(line, "replace (") {
				inReplace = true
			}
			continue
		case inReplace:
			t.Errorf("go.mod line %d: replace directive %q; the root module must build as published", lineNumber+1, line)
			continue
		case strings.HasPrefix(line, "require "):
			line = strings.TrimSpace(strings.TrimPrefix(line, "require "))
		case !inRequire:
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Errorf("go.mod line %d: unexpected require line %q", lineNumber+1, raw)
			continue
		}
		module := fields[0]
		if !strings.HasPrefix(module, allowedPrefix) {
			if _, ok := allowedThirdParty[module]; ok {
				continue
			}
			t.Errorf("go.mod line %d: third-party dependency %s; the root module may require only %s* plus the reviewed AAC runtime allowlist", lineNumber+1, module, allowedPrefix)
		}
	}
}
