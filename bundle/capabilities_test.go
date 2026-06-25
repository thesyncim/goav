package bundle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	bundleCapabilitiesStart = "<!-- BEGIN GENERATED BUNDLE CAPABILITIES -->"
	bundleCapabilitiesEnd   = "<!-- END GENERATED BUNDLE CAPABILITIES -->"
)

func TestBundledAdapterCapabilityDocsMatchDescriptors(t *testing.T) {
	want, err := bundledCapabilityMarkdown()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join("..", "docs", "ADAPTERS.md"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := generatedBundleCapabilitiesSection(string(data))
	if !ok {
		t.Fatalf("docs/ADAPTERS.md missing generated bundle capability markers")
	}
	if got != want {
		t.Fatalf("generated bundled adapter capability docs drifted\nwant:\n%s\n\ngot:\n%s", want, got)
	}
}

func generatedBundleCapabilitiesSection(text string) (string, bool) {
	start := strings.Index(text, bundleCapabilitiesStart)
	if start < 0 {
		return "", false
	}
	end := strings.Index(text[start:], bundleCapabilitiesEnd)
	if end < 0 {
		return "", false
	}
	end += start + len(bundleCapabilitiesEnd)
	return strings.TrimSpace(text[start:end]), true
}
