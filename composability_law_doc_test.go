package goav_test

import (
	"os"
	"strings"
	"testing"
)

var composabilityLawEvidence = map[string]string{
	"north_star_acceptance_test.go":       "TestNorthStarDirectChainEqualsExplicitMainBranch",
	"recipe_compile_test.go":              "TestReusableRecipeAndBranchChainsStoreOperationSpecsOnly",
	"recipe_api_test.go":                  "TestFlowBranchesStayOnJobAndBuildIntent",
	"recipe_runtime_test.go":              "TestTaskAttachRuntimeDecodeResampleEncodeMuxBranchFromPacketTap",
	"join_plan_test.go":                   "TestJoinDescribeEqualsBuildMix",
	"shape_solver_test.go":                "TestAutoInsertsFormatConvertThroughRegisteredAdapter",
	"shape_require_prefer_test.go":        "TestPreferUnsatisfiableIgnoredWithDiagnostic",
	"multi_input_test.go":                 "TestFromMultiInputPlanDedupesSharedDestination",
	"copy_contract_test.go":               "TestCopyContractMutableFanoutBranchCannotCorruptSibling",
	"cross_feature_test.go":               "TestFromMultiInputChainsKeepIndependentAutoPolicies",
	"graph_test.go":                       "TestTaskAttachRuntimeBranchGroupRollsBackOnLaterFailure",
	"adapterproof/adapter_compat_test.go": "TestExternalAdaptersComposeThroughPublicGrammar",
	"adapterproof/join_proof_test.go":     "TestExternalJoinComposesThroughPublicGrammar",
}

func TestComposabilityLawsMapToExecutableEvidence(t *testing.T) {
	doc, err := os.ReadFile("docs/COMPOSABILITY_LAWS.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, required := range []string{
		"# Composability laws",
		"direct stream chain",
		"`Flow` owns ordered operations only",
		"runtime `Task.Attach`",
		"`Describe()`",
		"`Explain()`",
		"Destination reuse groups",
		"Branch-local policy",
		"Runtime attach failures roll back",
		"External adapters and joins",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("docs/COMPOSABILITY_LAWS.md missing %q", required)
		}
	}
	for file, testName := range composabilityLawEvidence {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), testName) {
			t.Fatalf("%s missing evidence test %s", file, testName)
		}
		if !strings.Contains(text, testName) {
			t.Fatalf("docs/COMPOSABILITY_LAWS.md should cite %s", testName)
		}
	}
}

func TestComposabilityLawsAreLinkedFromSurfaceDocs(t *testing.T) {
	for _, file := range []string{"docs/API_SURFACE.md", "docs/ROADMAP.md"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "docs/COMPOSABILITY_LAWS.md") {
			t.Fatalf("%s should link docs/COMPOSABILITY_LAWS.md", file)
		}
	}
}
