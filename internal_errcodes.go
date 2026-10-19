package goav

import "github.com/thesyncim/goav/internal/builderr"

// The root spells the internal refusal codes of internal/builderr by these
// names.
const (
	compilerPassFailedCode              = builderr.CompilerPassFailedCode
	graphPlanInvalidCode                = builderr.GraphPlanInvalidCode
	branchDecodeDuplicateCode           = builderr.BranchDecodeDuplicateCode
	branchDecodeDomainMismatchCode      = builderr.BranchDecodeDomainMismatchCode
	branchTransformMediaMismatchCode    = builderr.BranchTransformMediaMismatchCode
	branchOperationChainUnsupportedCode = builderr.BranchOperationChainUnsupportedCode
	branchBufferInvalidCode             = builderr.BranchBufferInvalidCode
	branchBufferUnsupportedCode         = builderr.BranchBufferUnsupportedCode
	decodeConfigConflictCode            = builderr.DecodeConfigConflictCode

	outputInvalidCode              = builderr.OutputInvalidCode
	outputDuplicateCode            = builderr.OutputDuplicateCode
	outputKindMixedCode            = builderr.OutputKindMixedCode
	outputWriterMissingCode        = builderr.OutputWriterMissingCode
	outputFormatMissingCode        = builderr.OutputFormatMissingCode
	outputFormatUnknownCode        = builderr.OutputFormatUnknownCode
	outputMuxerMissingCode         = builderr.OutputMuxerMissingCode
	outputDestinationMissingCode   = builderr.OutputDestinationMissingCode
	outputSchemeUnsupportedCode    = builderr.OutputSchemeUnsupportedCode
	destinationInvalidCode         = builderr.DestinationInvalidCode
	destinationDuplicateCode       = builderr.DestinationDuplicateCode
	destinationFormatUnknownCode   = builderr.DestinationFormatUnknownCode
	destinationMuxerMissingCode    = builderr.DestinationMuxerMissingCode
	destinationMuxIncompatibleCode = builderr.DestinationMuxIncompatibleCode
	destinationShapeMismatchCode   = builderr.DestinationShapeMismatchCode

	transformInvalidCode             = builderr.TransformInvalidCode
	transformMediaMismatchCode       = builderr.TransformMediaMismatchCode
	transformAdapterMissingCode      = builderr.TransformAdapterMissingCode
	transformAdapterIncompatibleCode = builderr.TransformAdapterIncompatibleCode
	transcodeResizeInvalidCode       = builderr.TranscodeResizeInvalidCode
	operationShapeMismatchCode       = builderr.OperationShapeMismatchCode
	shapeRequirementUnmetCode        = builderr.ShapeRequirementUnmetCode
	shapeConversionRefusedCode       = builderr.ShapeConversionRefusedCode
	shapeAdapterMissingCode          = builderr.ShapeAdapterMissingCode
	shapeAdapterAmbiguousCode        = builderr.ShapeAdapterAmbiguousCode
	decodeAdapterMissingCode         = builderr.DecodeAdapterMissingCode
	decodeAdapterUnavailableCode     = builderr.DecodeAdapterUnavailableCode
	decodeAdapterIncompatibleCode    = builderr.DecodeAdapterIncompatibleCode
	encodeAdapterMissingCode         = builderr.EncodeAdapterMissingCode
	encodeAdapterUnavailableCode     = builderr.EncodeAdapterUnavailableCode
	encodeAdapterIncompatibleCode    = builderr.EncodeAdapterIncompatibleCode
	encodeMissingCode                = builderr.EncodeMissingCode
	encodeDuplicateCode              = builderr.EncodeDuplicateCode
	encodeParameterInvalidCode       = builderr.EncodeParameterInvalidCode
	encodeAutoUnresolvedCode         = builderr.EncodeAutoUnresolvedCode
	encodeStreamMismatchCode         = builderr.EncodeStreamMismatchCode
	encodeDestinationMissingCode     = builderr.EncodeDestinationMissingCode

	inputInvalidCode               = builderr.InputInvalidCode
	inputDuplicateCode             = builderr.InputDuplicateCode
	inputUnknownCode               = builderr.InputUnknownCode
	inputFormatUnknownCode         = builderr.InputFormatUnknownCode
	inputDemuxerMissingCode        = builderr.InputDemuxerMissingCode
	inputSchemeUnsupportedCode     = builderr.InputSchemeUnsupportedCode
	sourceCallbackMissingCode      = builderr.SourceCallbackMissingCode
	sourceShapeInvalidCode         = builderr.SourceShapeInvalidCode
	sourceShapeUnsupportedCode     = builderr.SourceShapeUnsupportedCode
	sourceShapeMismatchCode        = builderr.SourceShapeMismatchCode
	explicitGraphSourceMissingCode = builderr.ExplicitGraphSourceMissingCode
	streamMissingCode              = builderr.StreamMissingCode
	streamAmbiguousCode            = builderr.StreamAmbiguousCode
	streamSelectorInvalidCode      = builderr.StreamSelectorInvalidCode
	streamCodecMissingCode         = builderr.StreamCodecMissingCode
	streamStepAfterEncodeCode      = builderr.StreamStepAfterEncodeCode
	streamInvalidCode              = builderr.StreamInvalidCode
	stageMissingCode               = builderr.StageMissingCode

	runtimeBranchInvalidCode              = builderr.RuntimeBranchInvalidCode
	runtimeBranchAnchorMissingCode        = builderr.RuntimeBranchAnchorMissingCode
	runtimeBranchTapMissingCode           = builderr.RuntimeBranchTapMissingCode
	runtimeBranchTapDuplicateCode         = builderr.RuntimeBranchTapDuplicateCode
	runtimeBranchNodeDuplicateCode        = builderr.RuntimeBranchNodeDuplicateCode
	runtimeBranchEncodeMissingCode        = builderr.RuntimeBranchEncodeMissingCode
	runtimeBranchEncodeDomainMismatchCode = builderr.RuntimeBranchEncodeDomainMismatchCode
	runtimeBranchDecodeDomainMismatchCode = builderr.RuntimeBranchDecodeDomainMismatchCode
	runtimeBranchDecodeCodecMissingCode   = builderr.RuntimeBranchDecodeCodecMissingCode
	runtimeBranchCopyDomainMismatchCode   = builderr.RuntimeBranchCopyDomainMismatchCode
	runtimeBranchMuxCodecMissingCode      = builderr.RuntimeBranchMuxCodecMissingCode
	runtimeBranchTransformErrorCode       = builderr.RuntimeBranchTransformErrorCode
	runtimeBranchGraphErrorCode           = builderr.RuntimeBranchGraphErrorCode
)
