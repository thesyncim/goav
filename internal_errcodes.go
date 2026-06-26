package goav

import "github.com/thesyncim/goav/errcode"

const (
	branchComposePlanEmptyCode   errcode.Code = "branch_compose_plan_empty"
	compilerPassInvalidCode      errcode.Code = "compiler_pass_invalid"
	compilerPassFailedCode       errcode.Code = "compiler_pass_failed"
	recipeAttachmentMismatchCode errcode.Code = "recipe_attachment_mismatch"
	graphPlanInvalidCode         errcode.Code = "graph_plan_invalid"
	bufferBudgetMissingCode      errcode.Code = "buffer_budget_missing"
)
