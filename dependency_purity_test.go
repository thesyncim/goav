package goav_test

import (
	"os"
	"strings"
	"testing"
)

// TestRootModuleDependencyPurity pins the root module's dependency policy:
// the core may depend only on github.com/thesyncim/* modules and the
// standard library. Third-party code (pion, compression backends,
// comparison libraries) lives in nested modules — rtpav, webrtcav,
// examples — each with its own go.mod, so importing goav never pulls a
// transport or codec ecosystem into the consumer's module graph.
//
// There is no allowlist beyond the thesyncim prefix: golang.org/x/* is also
// refused. If a tool dependency ever becomes genuinely necessary, it must be
// added here explicitly with a rationale, not slipped in via go.mod.
//
// Replace directives are refused too: the root module must be consumable as
// published, with no local-path indirection.
func TestRootModuleDependencyPurity(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}

	const allowedPrefix = "github.com/thesyncim/"

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
			t.Errorf("go.mod line %d: third-party dependency %s; the root module may require only %s* (move the consumer into a nested module)", lineNumber+1, module, allowedPrefix)
		}
	}
}
