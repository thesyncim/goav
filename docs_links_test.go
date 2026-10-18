package goav_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// docLinkTargetPattern matches the target of a markdown link or image,
// `[text](target)` or `[text](target "title")`. Matching on the `](` that
// closes the text keeps nested forms such as `[![badge](img)](target)` whole.
var docLinkTargetPattern = regexp.MustCompile(`\]\(([^)\s]+)(?:\s+"[^"]*")?\)`)

// docInlineCodePattern matches inline code spans, which are not links.
var docInlineCodePattern = regexp.MustCompile("`[^`]*`")

// TestDocsRelativeLinksResolve is the link-rot pin for living markdown: every
// relative link in the front-door files, docs/*.md, and the example READMEs must name a file or
// directory that exists, resolved against the linking file. External links
// and pure #anchors are skipped, a #fragment is stripped before the check,
// and docs/history/ is exempt because it describes the past.
func TestDocsRelativeLinksResolve(t *testing.T) {
	paths := []string{"README.md", "CONTRIBUTING.md", "SECURITY.md"}
	docs, err := filepath.Glob(filepath.Join("docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, docs...)
	examples, err := filepath.Glob(filepath.Join("examples", "*", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, examples...)
	checked := 0
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		fenced := false
		for lineNo, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			if fenced {
				continue
			}
			line = docInlineCodePattern.ReplaceAllString(line, "")
			for _, match := range docLinkTargetPattern.FindAllStringSubmatch(line, -1) {
				target := match[1]
				if docLinkIsExternal(target) || strings.HasPrefix(target, "#") {
					continue
				}
				if i := strings.IndexByte(target, '#'); i >= 0 {
					target = target[:i]
				}
				resolved := filepath.Join(filepath.Dir(path), filepath.FromSlash(target))
				if strings.HasPrefix(target, "/") {
					resolved = filepath.FromSlash(strings.TrimPrefix(target, "/"))
				}
				checked++
				if _, err := os.Stat(resolved); err != nil {
					t.Errorf("%s:%d links %q, which does not resolve: %v", path, lineNo+1, match[1], err)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no relative links found; the link pattern may have drifted")
	}
}

func docLinkIsExternal(target string) bool {
	for _, scheme := range []string{"http://", "https://", "mailto:"} {
		if strings.HasPrefix(target, scheme) {
			return true
		}
	}
	return false
}
