package goav

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/errcode"
)

func TestPrepareBranchComposePlanEmptyContracts(t *testing.T) {
	_, _, err := prepareBranchComposePlan(branchComposePlan{})
	assertBranchComposeGraphPlanBuildError(t, err, branchComposePlanEmptyCode, "no branches")

	_, _, err = prepareBranchComposePlan(branchComposePlan{
		Branches: []branchComposeBranch{{Name: "preview"}},
	})
	assertBranchComposeGraphPlanBuildError(t, err, branchComposePlanEmptyCode, "no destinations")
}

func assertBranchComposeGraphPlanBuildError(t *testing.T, err error, code errcode.Code, reason string) {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	if buildErr.Code != code || !strings.Contains(buildErr.Reason, reason) {
		t.Fatalf("BuildError = %+v, want code=%s reason containing %q", buildErr, code, reason)
	}
}
