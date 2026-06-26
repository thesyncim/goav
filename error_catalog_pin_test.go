package goav_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type errorCatalogEntry struct {
	Section  string
	Constant string
	Code     string
	Kind     string
	When     string
}

type errorCatalogExample struct {
	Code          string
	Test          string
	BadRecipe     string
	RenderedError string
	Fix           string
	Cause         string
}

var derivedJoinErrorCatalogEntries = []errorCatalogEntry{
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("mix", "inputs")`,
		Code:     "mix_inputs",
		Kind:     "refusal",
		When:     "Mix is given fewer than two arms.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("mix", "arm")`,
		Code:     "mix_arm",
		Kind:     "refusal",
		When:     "A Mix arm is invalid: wrong media, duplicate stream ids, an unconvertible format, or a nested arm carrying .Encode.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("mix", "destination")`,
		Code:     "mix_destination",
		Kind:     "refusal",
		When:     "Mix without .Encode is routed to a non-sink destination.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("mix", "tap_arm")`,
		Code:     "mix_tap_arm",
		Kind:     "refusal",
		When:     "A Mix tap arm references a tap no earlier arm declares.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("composite", "inputs")`,
		Code:     "composite_inputs",
		Kind:     "refusal",
		When:     "Composite is given fewer than two arms.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("composite", "arm")`,
		Code:     "composite_arm",
		Kind:     "refusal",
		When:     "A Composite arm is invalid: wrong media, duplicate stream ids, an unconvertible format, or unsupported nested operations.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("composite", "destination")`,
		Code:     "composite_destination",
		Kind:     "refusal",
		When:     "Composite without .Encode is routed to a non-sink destination.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("composite", "tap_arm")`,
		Code:     "composite_tap_arm",
		Kind:     "refusal",
		When:     "A Composite tap arm references a tap no earlier arm declares.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("select", "inputs")`,
		Code:     "select_inputs",
		Kind:     "refusal",
		When:     "Select is given fewer than two arms.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("select", "arm")`,
		Code:     "select_arm",
		Kind:     "refusal",
		When:     "A Select arm is invalid: duplicate stream ids, incompatible media facts, or unsupported nested operations.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("select", "destination")`,
		Code:     "select_destination",
		Kind:     "refusal",
		When:     "Select's output cannot reach the declared destination kind.",
	},
	{
		Section:  "Derived join codes (built-in Mix, Composite, Select profiles).",
		Constant: `joinErrorCode("select", "tap_arm")`,
		Code:     "select_tap_arm",
		Kind:     "refusal",
		When:     "A Select tap arm references a tap no earlier arm declares.",
	},
}

var errorCatalogExamples = []errorCatalogExample{
	{
		Code:          "tap_domain_mismatch",
		Test:          "TestErrorAcceptanceTypedTapAtWrongDomain",
		BadRecipe:     `.Encode(codec.Opus()).Tap(goav.FrameTap("post-encode"))`,
		RenderedError: "domain details and typed-tap suggestions are asserted by the test",
		Fix:           "use goav.PacketTap(name) after packet-domain operations, or goav.FrameTap(name) after decode",
		Cause:         "goav.ErrUnsupportedBuild",
	},
}

var errorCatalogAdditionalExamples = []errorCatalogExample{
	{
		Code:          "tap_invalid",
		Test:          "TestStreamChainRejectsInvalidPostEncodeAndTapContracts",
		BadRecipe:     `.Tap(goav.FrameTap(""))`,
		RenderedError: "empty tap name refusal is asserted by the test",
		Fix:           "use a non-empty typed tap such as goav.FrameTap(\"audio.frames\")",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "join_name_invalid",
		Test:          "TestJoinRejectsInvalidNames",
		BadRecipe:     `goav.Join("Cross Fade", stage, arms...)`,
		RenderedError: "invalid custom join name variants are asserted by the test",
		Fix:           "use a snake-safe non-reserved custom join name",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "join_stage_invalid",
		Test:          "TestJoinRejectsStageMismatch",
		BadRecipe:     `goav.Join("funnel", nil, arms...)` + " or stage.Name() mismatch",
		RenderedError: "nil/mismatched join stage refusal is asserted by the test",
		Fix:           "pass a non-nil stage whose Name() equals the join name",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_inputs",
		Test:          "TestMixRequiresTwoArms",
		BadRecipe:     `goav.Mix(oneArm).To(...)`,
		RenderedError: "Mix minimum arm count is asserted by the test",
		Fix:           "pass at least two Mix arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_arm",
		Test:          "TestJoinArmsRejectMultiInputChains",
		BadRecipe:     "a Mix arm has invalid media, duplicate stream ids, or unsupported nested operations",
		RenderedError: "Mix arm validation details are asserted by the test",
		Fix:           "make every Mix arm an audio chain with a distinct stream id and supported operations",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_destination",
		Test:          "TestMixRawFramesRequireSinkDestination",
		BadRecipe:     `goav.Mix(a, b).To(goav.Write("mix.ogg", writer))` + " without Encode",
		RenderedError: "raw Mix destination refusal and encode-or-sink guidance are asserted by the test",
		Fix:           "call .Encode(codec.Opus(...)) before file output or route raw frames to goav.Sink(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "mix_tap_arm",
		Test:          "TestMixTapArmUnknownTapListsDeclaredTaps",
		BadRecipe:     `goav.Mix(chain.Tap(goav.FrameTap("dry")), goav.FrameTap("nope"))`,
		RenderedError: "unknown Mix tap arm and declared taps are asserted by the test",
		Fix:           "declare the tap on an earlier arm or reorder the arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_inputs",
		Test:          "TestCompositeRequiresTwoArms",
		BadRecipe:     `goav.Composite(oneArm).To(...)`,
		RenderedError: "Composite minimum arm count is asserted by the test",
		Fix:           "pass at least two Composite arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_arm",
		Test:          "TestCompositeRejectsDuplicateStreamIDs",
		BadRecipe:     "Composite arms declare duplicate stream ids or invalid video arms",
		RenderedError: "Composite arm validation details are asserted by the test",
		Fix:           "make every Composite arm a valid video chain with a distinct stream id",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_destination",
		Test:          "TestCompositeRawFramesRequireSinkDestination",
		BadRecipe:     `goav.Composite(a, b).To(goav.Write("canvas.ivf", writer))` + " without Encode",
		RenderedError: "raw Composite destination refusal and encode-or-sink guidance are asserted by the test",
		Fix:           "call .Encode(codec.VP8(...)) before file output or route raw frames to goav.Sink(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "composite_tap_arm",
		Test:          "TestCompositeTapArmUnknownTapListsDeclaredTaps",
		BadRecipe:     `goav.Composite(chain.Tap(goav.FrameTap("cam.frames")), goav.FrameTap("missing").Region(...))`,
		RenderedError: "unknown Composite tap arm and declared taps are asserted by the test",
		Fix:           "declare the tap on an earlier Composite arm or reorder the arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_inputs",
		Test:          "TestSelectRequiresTwoArms",
		BadRecipe:     `goav.Select(oneArm).To(...)`,
		RenderedError: "Select minimum arm count is asserted by the test",
		Fix:           "pass at least two Select arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_arm",
		Test:          "TestSelectRequiresDistinctArmIDs",
		BadRecipe:     "Select arms declare duplicate stream ids or invalid arms",
		RenderedError: "Select arm validation details are asserted by the test",
		Fix:           "make every Select arm valid and give each a distinct stream id",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_destination",
		Test:          "TestSelectRequiresSinkDestination",
		BadRecipe:     `goav.Select(a, b).To(goav.Write("selected.ogg", writer))`,
		RenderedError: "Select sink-only destination refusal is asserted by the test",
		Fix:           "deliver selected frames to goav.Sink(...) or use .Branches(...) for muxed outputs",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "select_tap_arm",
		Test:          "TestSelectTapArmUnknownTapListsDeclaredTaps",
		BadRecipe:     `goav.Select(chain.Tap(goav.FrameTap("selected.frames")), goav.FrameTap("missing"))`,
		RenderedError: "unknown Select tap arm and declared taps are asserted by the test",
		Fix:           "declare the tap on an earlier Select arm or reorder the arms",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "stream_rule_invalid",
		Test:          "TestOnStreamValidation",
		BadRecipe:     `goav.OnStream(...)` + " with no matcher, no branch, or malformed stream",
		RenderedError: "invalid OnStream rule variants are asserted by the test",
		Fix:           "provide a stream matcher and a valid branch spec",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "job_invalid",
		Test:          "TestZeroJobRejectsPublicConstruction",
		BadRecipe:     "compile a nil job, zero goav.Job, or nil join intent",
		RenderedError: "zero-job constructor guidance is asserted by the test",
		Fix:           "start from goav.From(...), goav.Mix(...), goav.Composite(...), or goav.Select(...)",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "runtime_missing",
		Test:          "TestRecipeReportsOmittedRuntime",
		BadRecipe:     "goav.From(file).Copy().To(file).Build(ctx) without .UseRuntime(...) or bundle.Build/bundle.Run",
		RenderedError: "adapter-backed omitted runtime guidance is asserted by the test",
		Fix:           "attach a runtime with .UseRuntime(...) or use bundle.Build/bundle.Run for the bundled runtime",
		Cause:         "goav.ErrUnsupportedBuild",
	},
	{
		Code:          "recipe_graph_unsupported",
		Test:          "TestRecipeResolvedUnsupportedContracts",
		BadRecipe:     "describe/build a recipe shape that has no graph plan",
		RenderedError: "unsupported recipe graph helper is asserted by the test",
		Fix:           "use a supported front-door recipe shape",
		Cause:         "goav.ErrUnsupportedBuild",
	},
}

func allErrorCatalogExamples() []errorCatalogExample {
	examples := append([]errorCatalogExample(nil), errorCatalogExamples...)
	examples = append(examples, errorCatalogAdditionalExamples...)
	return examples
}

func TestErrorCatalogDocMatchesErrcodeCatalog(t *testing.T) {
	entries := readErrorCatalogEntries(t)
	generated := []byte(renderErrorCatalogDoc(entries))
	const path = "docs/ERROR_CATALOG.md"
	if os.Getenv("UPDATE_ERROR_CATALOG") == "1" {
		if err := os.WriteFile(path, generated, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v; run UPDATE_ERROR_CATALOG=1 go test -run TestErrorCatalogDocMatchesErrcodeCatalog .", path, err)
	}
	if string(current) != string(generated) {
		t.Fatalf("%s is out of date with errcode/errcode.go; run UPDATE_ERROR_CATALOG=1 go test -run TestErrorCatalogDocMatchesErrcodeCatalog .", path)
	}
}

func TestErrorCatalogEveryErrcodeHasExampleCoverage(t *testing.T) {
	entries := readErrorCatalogEntries(t)
	coverage := errorCatalogCoverage()
	for _, entry := range entries {
		if len(coverage[entry.Code]) == 0 {
			t.Fatalf("%s (%s) has no acceptance snippet coverage", entry.Constant, entry.Code)
		}
	}
}

func TestErrorCatalogCoverageMetadataIsComplete(t *testing.T) {
	knownCodes := make(map[string]bool)
	for _, entry := range readErrorCatalogEntries(t) {
		knownCodes[entry.Code] = true
	}
	knownTests := testFunctionNames(t)
	seen := make(map[string]bool)
	for _, example := range allErrorCatalogExamples() {
		label := example.Code + "/" + example.Test
		if seen[label] {
			t.Fatalf("duplicate error catalog coverage row for %s", label)
		}
		seen[label] = true
		if !knownCodes[example.Code] {
			t.Fatalf("coverage row names unknown errcode %q", example.Code)
		}
		if !knownTests[example.Test] {
			t.Fatalf("coverage row for %s names missing test %q", example.Code, example.Test)
		}
		for field, value := range map[string]string{
			"bad recipe":     example.BadRecipe,
			"rendered error": example.RenderedError,
			"fix":            example.Fix,
			"cause":          example.Cause,
		} {
			if strings.TrimSpace(value) == "" {
				t.Fatalf("coverage row for %s/%s has empty %s", example.Code, example.Test, field)
			}
			if strings.Contains(strings.ToLower(value), "todo") {
				t.Fatalf("coverage row for %s/%s leaves TODO in %s: %s", example.Code, example.Test, field, value)
			}
		}
	}
}

func TestErrorGuideDescribesCompleteCatalogCoverage(t *testing.T) {
	body, err := os.ReadFile("docs/ERRORS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "Every current catalog row names coverage") ||
		!strings.Contains(text, "If a future row appears as `catalog-only`") {
		t.Fatal("docs/ERRORS.md should describe the complete catalog coverage contract")
	}
	if strings.Contains(text, "Rows marked\n`catalog-only` still need") {
		t.Fatal("docs/ERRORS.md still describes catalog-only rows as current work")
	}
}

func readErrorCatalogEntries(t *testing.T) []errorCatalogEntry {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "errcode/errcode.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var entries []errorCatalogEntry
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		section := firstDocLine(gen.Doc)
		if strings.HasPrefix(section, "Diagnostic and decision") {
			t.Fatalf("errcode/errcode.go: non-failing diagnostic codes should stay out of the public error catalog")
		}
		kind := "refusal"
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || !isErrcodeCodeSpec(valueSpec) {
				continue
			}
			when := compactDoc(valueSpec.Doc)
			if when == "" {
				when = compactDoc(valueSpec.Comment)
			}
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					t.Fatalf("errcode/errcode.go: %s has no explicit value", name.Name)
				}
				value, ok := valueSpec.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("errcode/errcode.go: %s value is not a literal", name.Name)
				}
				entries = append(entries, errorCatalogEntry{
					Section:  section,
					Constant: "errcode." + name.Name,
					Code:     strings.Trim(value.Value, `"`),
					Kind:     kind,
					When:     when,
				})
			}
		}
		if strings.HasPrefix(section, "Join codes") {
			entries = append(entries, derivedJoinErrorCatalogEntries...)
		}
	}
	if len(entries) == 0 {
		t.Fatal("no errcode.Code constants found")
	}
	return entries
}

func isErrcodeCodeSpec(spec *ast.ValueSpec) bool {
	ident, ok := spec.Type.(*ast.Ident)
	return ok && ident.Name == "Code"
}

func testFunctionNames(t *testing.T) map[string]bool {
	t.Helper()
	names := make(map[string]bool)
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "bench-results":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			if strings.HasPrefix(fn.Name.Name, "Test") {
				names[fn.Name.Name] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("no Test functions discovered")
	}
	return names
}

func renderErrorCatalogDoc(entries []errorCatalogEntry) string {
	coverage := errorCatalogCoverage()
	var b strings.Builder
	b.WriteString("# Error Catalog\n\n")
	b.WriteString("<!-- Code generated from errcode/errcode.go by TestErrorCatalogDocMatchesErrcodeCatalog; DO NOT EDIT BY HAND. -->\n\n")
	b.WriteString("This catalog is the checked index of goav's public BuildError refusal codes. ")
	b.WriteString("The `Code`, `Constant`, `Section`, `Kind`, and `When it fires` columns are generated from `errcode/errcode.go` plus checked derived-code tables, so a new code must update the source catalog and this checked document together.\n\n")
	b.WriteString("Every current catalog row names coverage. If a future row is marked `catalog-only`, it still needs a dedicated bad recipe, rendered golden error, fixed recipe, sentinel/cause, and test name before the v1 error catalog is complete. ")
	b.WriteString("Rows naming tests already have public grammar snippets, rendered-error assertions, fix coverage, or sentinel checks in the named test.\n\n")
	b.WriteString("## Acceptance Snippet Coverage\n\n")
	b.WriteString("| Code | Test | Bad recipe | Rendered error | Fix coverage | Cause |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, example := range allErrorCatalogExamples() {
		b.WriteString(fmt.Sprintf("| `%s` | `%s` | %s | %s | %s | `%s` |\n",
			escapeMarkdown(example.Code),
			escapeMarkdown(example.Test),
			escapeMarkdown(example.BadRecipe),
			escapeMarkdown(example.RenderedError),
			escapeMarkdown(example.Fix),
			escapeMarkdown(example.Cause),
		))
	}
	b.WriteString("\n## Full Code Index\n\n")
	b.WriteString("| Section | Constant | Code | Kind | When it fires | Example coverage |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, entry := range entries {
		examples := coverage[entry.Code]
		if len(examples) == 0 {
			examples = []string{"catalog-only"}
		}
		b.WriteString(fmt.Sprintf("| %s | `%s` | `%s` | %s | %s | %s |\n",
			escapeMarkdown(entry.Section),
			escapeMarkdown(entry.Constant),
			escapeMarkdown(entry.Code),
			escapeMarkdown(entry.Kind),
			escapeMarkdown(entry.When),
			escapeMarkdown(strings.Join(examples, ", ")),
		))
	}
	return b.String()
}

func errorCatalogCoverage() map[string][]string {
	coverage := make(map[string][]string)
	for _, example := range allErrorCatalogExamples() {
		coverage[example.Code] = append(coverage[example.Code], example.Test)
	}
	for code := range coverage {
		sort.Strings(coverage[code])
	}
	return coverage
}

func firstDocLine(group *ast.CommentGroup) string {
	text := compactDoc(group)
	if text == "" {
		return ""
	}
	if i := strings.Index(text, ". "); i >= 0 {
		return text[:i+1]
	}
	return text
}

func compactDoc(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	return strings.Join(strings.Fields(group.Text()), " ")
}

func escapeMarkdown(text string) string {
	text = strings.ReplaceAll(text, "|", `\|`)
	text = strings.ReplaceAll(text, "\n", " ")
	return text
}
