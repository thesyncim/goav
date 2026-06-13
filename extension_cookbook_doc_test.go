package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestExtensionCookbookCoversPublicExtensionPoints(t *testing.T) {
	body, err := os.ReadFile("docs/EXTENSION_COOKBOOK.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"# Extension cookbook",
		"## Choose the seam",
		"## Custom Source",
		"## Custom Destination",
		"## Transactional Writer",
		"## Custom Filter",
		"## Custom Codec",
		"## Custom Join",
		"## Control-Plane Host",
		"## Checklist",
		"`goav.Source(name, shape, fn)`",
		"`provider.Source`",
		"`goav.Writer(name, open, opts...)`",
		"`provider.TransactionalWriter`",
		"`filter.Factory`",
		"`codec.EncoderFactory`",
		"`goav.Join(name, pipeline.Stage, arms...)`",
		"`ctl.CommandSpec`",
		"examples/custom-source",
		"examples/provider-source",
		"examples/custom-destination",
		"examples/custom-filter",
		"examples/transactional-writer",
		"examples/custom-codec",
		"examples/custom-join",
		"examples/control-plane-host",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/EXTENSION_COOKBOOK.md missing %q", required)
		}
	}
}

func TestExtensionCookbookIsLinkedFromFrontDoorDocs(t *testing.T) {
	for _, file := range []string{"README.md", "docs/API_SURFACE.md", "docs/ADAPTER_AUTHORING.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/EXTENSION_COOKBOOK.md") {
			t.Fatalf("%s should link docs/EXTENSION_COOKBOOK.md", file)
		}
	}
}
