package goav

import (
	"errors"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
)

func inputFormatProbeError(input format.Input, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"give file or URI inputs a name, URI, or MIME type a registered prober can recognize",
		"register a format adapter with goav.MustNew(goavruntime.WithFormatAdapter(...))",
		"use goav.Input(provider) for realtime packet receive",
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.InputFormatUnknown),
		Code:      errcode.InputFormatUnknown,
		Operation: "open input",
		Node:      demuxNodeName(input),
		Reason:    "input format could not be detected",
		fields:    inputFormatFields(input),
		fixes:     buildErrorFixes(suggestions),
		Cause:     cause,
	}
}

func inputDemuxerMissingError(input format.Input, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"register a format adapter that provides a " + string(id) + " demuxer",
		"choose an input container supported by the runtime",
		"call .UseRuntime(goav.MustNew(goavruntime.WithFormatAdapter(...))) when using a custom adapter bundle",
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.InputDemuxerMissing),
		Code:      errcode.InputDemuxerMissing,
		Operation: "open input",
		Node:      demuxNodeName(input),
		Reason:    "format " + quoteFormat(id) + " was detected but no demuxer is registered",
		fields:    append(inputFormatFields(input), buildErrorDetail{Key: "format", Value: id}),
		fixes:     buildErrorFixes(suggestions),
		Cause:     cause,
	}
}

func outputFormatProbeError(output format.Output, index int, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"give file outputs a name or MIME type a registered prober can recognize",
		"pass goav.Format(...) to goav.Write(...) or goav.Writer(...) when the writer has no filename",
		"register a format adapter with goav.MustNew(goavruntime.WithFormatAdapter(...))",
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(outputFormatUnknownCode),
		Code:      outputFormatUnknownCode,
		Operation: "open output",
		Node:      muxNodeName(output, index),
		Reason:    "output format could not be detected",
		fields:    outputFormatFields(output),
		fixes:     buildErrorFixes(suggestions),
		Cause:     cause,
	}
}

func outputMuxerMissingError(output format.Output, index int, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"register a " + string(id) + " muxer with goav.MustNew(goavruntime.WithMuxer(...)) or a format adapter that provides one",
		"choose an output container supported by the runtime, such as .ivf for VP8/VP9/AV1 packet recording or .h264 for H264 packet recording",
		"call .UseRuntime(goav.MustNew(goavruntime.WithFormatAdapter(...))) when using a custom adapter bundle",
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(outputMuxerMissingCode),
		Code:      outputMuxerMissingCode,
		Operation: "open output",
		Node:      muxNodeName(output, index),
		Reason:    "format " + quoteFormat(id) + " was selected but no muxer is registered",
		fields:    append(outputFormatFields(output), buildErrorDetail{Key: "format", Value: id}),
		fixes:     buildErrorFixes(suggestions),
		Cause:     cause,
	}
}

func destinationFormatProbeError(node string, output format.Output, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"give file destinations a name or MIME type a registered prober can recognize",
		"pass goav.Format(...) to the destination constructor when the writer has no filename",
		"register a format adapter with goav.MustNew(goavruntime.WithFormatAdapter(...))",
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationFormatUnknownCode),
		Code:      destinationFormatUnknownCode,
		Operation: "open destination",
		Node:      node,
		Reason:    "destination format could not be detected",
		fields:    outputFormatFields(output),
		fixes:     buildErrorFixes(suggestions),
		Cause:     cause,
	}
}

func destinationMuxerMissingError(node string, output format.Output, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"register a " + string(id) + " muxer with goav.MustNew(goavruntime.WithMuxer(...)) or a format adapter that provides one",
		"choose a destination container supported by the runtime, such as .ivf for VP8/VP9/AV1 packet recording or .h264 for H264 packet recording",
		"call .UseRuntime(goav.MustNew(goavruntime.WithFormatAdapter(...))) when using a custom adapter bundle",
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(destinationMuxerMissingCode),
		Code:      destinationMuxerMissingCode,
		Operation: "open destination",
		Node:      node,
		Reason:    "format " + quoteFormat(id) + " was selected for destination but no muxer is registered",
		fields:    append(outputFormatFields(output), buildErrorDetail{Key: "format", Value: id}),
		fixes:     buildErrorFixes(suggestions),
		Cause:     cause,
	}
}

func inputFormatFields(input format.Input) []buildErrorDetail {
	var fields []buildErrorDetail
	if input.Name != "" {
		fields = append(fields, buildErrorDetail{Key: "name", Value: input.Name})
	}
	if input.URI != "" {
		fields = append(fields, buildErrorDetail{Key: "uri", Value: input.URI})
	}
	if input.Protocol != "" {
		fields = append(fields, buildErrorDetail{Key: "protocol", Value: input.Protocol})
	}
	if input.MIMEType != "" {
		fields = append(fields, buildErrorDetail{Key: "mime", Value: input.MIMEType})
	}
	return fields
}

func outputFormatFields(output format.Output) []buildErrorDetail {
	var fields []buildErrorDetail
	if output.Name != "" {
		fields = append(fields, buildErrorDetail{Key: "name", Value: output.Name})
	}
	if output.URI != "" {
		fields = append(fields, buildErrorDetail{Key: "uri", Value: output.URI})
	}
	if output.Protocol != "" {
		fields = append(fields, buildErrorDetail{Key: "protocol", Value: output.Protocol})
	}
	if output.MIMEType != "" {
		fields = append(fields, buildErrorDetail{Key: "mime", Value: output.MIMEType})
	}
	return fields
}

func quoteFormat(id av.FormatID) string {
	return strconv.Quote(string(id))
}
