package goav

import "github.com/thesyncim/goav/internal/builderr"

// The root spells the internal refusal codes of internal/builderr by these
// names.
const (
	graphPlanInvalidCode             = builderr.GraphPlanInvalidCode
	branchDecodeDuplicateCode        = builderr.BranchDecodeDuplicateCode
	branchTransformMediaMismatchCode = builderr.BranchTransformMediaMismatchCode

	outputInvalidCode              = builderr.OutputInvalidCode
	outputDuplicateCode            = builderr.OutputDuplicateCode
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

	transformInvalidCode             = builderr.TransformInvalidCode
	transformMediaMismatchCode       = builderr.TransformMediaMismatchCode
	transformAdapterMissingCode      = builderr.TransformAdapterMissingCode
	transformAdapterIncompatibleCode = builderr.TransformAdapterIncompatibleCode
	transcodeResizeInvalidCode       = builderr.TranscodeResizeInvalidCode
	operationShapeMismatchCode       = builderr.OperationShapeMismatchCode
	shapeAdapterMissingCode          = builderr.ShapeAdapterMissingCode
	decodeAdapterMissingCode         = builderr.DecodeAdapterMissingCode
	decodeAdapterUnavailableCode     = builderr.DecodeAdapterUnavailableCode
	decodeAdapterIncompatibleCode    = builderr.DecodeAdapterIncompatibleCode
	encodeAdapterMissingCode         = builderr.EncodeAdapterMissingCode
	encodeAdapterUnavailableCode     = builderr.EncodeAdapterUnavailableCode
	encodeAdapterIncompatibleCode    = builderr.EncodeAdapterIncompatibleCode
	encodeMissingCode                = builderr.EncodeMissingCode
	encodeParameterInvalidCode       = builderr.EncodeParameterInvalidCode
	encodeStreamMismatchCode         = builderr.EncodeStreamMismatchCode
	encodeDestinationMissingCode     = builderr.EncodeDestinationMissingCode

	inputInvalidCode               = builderr.InputInvalidCode
	inputDuplicateCode             = builderr.InputDuplicateCode
	inputFormatUnknownCode         = builderr.InputFormatUnknownCode
	inputDemuxerMissingCode        = builderr.InputDemuxerMissingCode
	inputSchemeUnsupportedCode     = builderr.InputSchemeUnsupportedCode
	sourceCallbackMissingCode      = builderr.SourceCallbackMissingCode
	sourceShapeInvalidCode         = builderr.SourceShapeInvalidCode
	sourceShapeUnsupportedCode     = builderr.SourceShapeUnsupportedCode
	sourceShapeMismatchCode        = builderr.SourceShapeMismatchCode
	explicitGraphSourceMissingCode = builderr.ExplicitGraphSourceMissingCode
	streamAmbiguousCode            = builderr.StreamAmbiguousCode
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
