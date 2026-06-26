package goav

import "github.com/thesyncim/goav/errcode"

const (
	branchComposePlanEmptyCode   errcode.Code = "branch_compose_plan_empty"
	compilerPassInvalidCode      errcode.Code = "compiler_pass_invalid"
	compilerPassFailedCode       errcode.Code = "compiler_pass_failed"
	recipeAttachmentMismatchCode errcode.Code = "recipe_attachment_mismatch"
	graphPlanInvalidCode         errcode.Code = "graph_plan_invalid"
	bufferBudgetMissingCode      errcode.Code = "buffer_budget_missing"

	flowInvalidCode              errcode.Code = "flow_invalid"
	flowMediaMismatchCode        errcode.Code = "flow_media_mismatch"
	flowDecodeDuplicateCode      errcode.Code = "flow_decode_duplicate"
	flowDecodeOrderInvalidCode   errcode.Code = "flow_decode_order_invalid"
	flowDecodeDomainMismatchCode errcode.Code = "flow_decode_domain_mismatch"
	flowCopyDomainMismatchCode   errcode.Code = "flow_copy_domain_mismatch"

	runtimeBranchInvalidCode              errcode.Code = "runtime_branch_invalid"
	runtimeBranchAnchorMissingCode        errcode.Code = "runtime_branch_anchor_missing"
	runtimeBranchTapMissingCode           errcode.Code = "runtime_branch_tap_missing"
	runtimeBranchTapDuplicateCode         errcode.Code = "runtime_branch_tap_duplicate"
	runtimeBranchNodeDuplicateCode        errcode.Code = "runtime_branch_node_duplicate"
	runtimeBranchEncodeMissingCode        errcode.Code = "runtime_branch_encode_missing"
	runtimeBranchEncodeDomainMismatchCode errcode.Code = "runtime_branch_encode_domain_mismatch"
	runtimeBranchDecodeDomainMismatchCode errcode.Code = "runtime_branch_decode_domain_mismatch"
	runtimeBranchDecodeCodecMissingCode   errcode.Code = "runtime_branch_decode_codec_missing"
	runtimeBranchCopyDomainMismatchCode   errcode.Code = "runtime_branch_copy_domain_mismatch"
	runtimeBranchMuxCodecMissingCode      errcode.Code = "runtime_branch_mux_codec_missing"
	runtimeBranchTransformErrorCode       errcode.Code = "runtime_branch_transform_error"
	runtimeBranchGraphErrorCode           errcode.Code = "runtime_branch_graph_error"
)
