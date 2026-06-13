package goav_test

import (
	"os"
	"strings"
	"testing"
)

func TestOperationsReferenceCoversFrontDoorGrammar(t *testing.T) {
	body, err := os.ReadFile("docs/OPERATIONS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"# Operations",
		"Input shape",
		"Output shape",
		"Allowed domain",
		"Inserted conversions",
		"Primary refusals",
		"Runtime attach",
		"`.Apply(flow)`",
		"`.Decode(options...)`",
		"`.Copy()`",
		"`.Shape(spec)`",
		"`.Auto(policies...)`",
		"`.Require(spec)`",
		"`.Prefer(spec)`",
		"`.Resize(width, height, options...)`",
		"`.Resample(sampleRate, channels, options...)`",
		"`.Do(stage...)`",
		"`.Tap(tap)`",
		"`.Encode(codecSpec)`",
		"`.Branches(branches...)`",
		"`.To(destinations...)`",
		"`Branch(name).From(anchor)`",
		"`Branch(name).Buffer(policy)`",
		"## Join Constructors",
		"`Mix(arms...)`",
		"`Composite(arms...)`",
		"`Select(arms...)`",
		"`Join(name, stage, arms...)`",
		"`SelectActive`",
		"`join_name_invalid`",
		"`join_stage_invalid`",
		"## Joined Stream Operations",
		"`.SyncByPTS()`",
		"`.Region(x, y)`",
		"`shape_conversion_inserted`",
		"`docs/ERROR_CATALOG.md`",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/OPERATIONS.md missing %q", required)
		}
	}
}

func TestOperationsReferenceIsLinkedFromFrontDoorDocs(t *testing.T) {
	for _, file := range []string{"README.md", "docs/API_SURFACE.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/OPERATIONS.md") {
			t.Fatalf("%s should link docs/OPERATIONS.md", file)
		}
	}
}
