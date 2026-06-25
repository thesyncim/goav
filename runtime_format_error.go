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
		"register a format adapter with goav.MustNew(goav.WithFormatAdapter(...))",
		"use goav.Input(provider) for realtime packet receive",
	}
	return &BuildError{
		Family:      errcode.FamilyForCode(errcode.InputFormatUnknown),
		Code:        errcode.InputFormatUnknown,
		Operation:   "open input",
		Node:        demuxNodeName(input),
		Reason:      "input format could not be detected",
		Fields:      inputFormatFields(input),
		Details:     inputFormatDetails(input),
		Fixes:       fixesFromSuggestions(suggestions),
		Suggestions: suggestions,
		Cause:       cause,
	}
}

func inputDemuxerMissingError(input format.Input, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"register a format adapter that provides a " + string(id) + " demuxer",
		"choose an input container supported by the runtime",
		"call .UseRuntime(goav.MustNew(goav.WithFormatAdapter(...))) when using a custom adapter bundle",
	}
	return &BuildError{
		Family:      errcode.FamilyForCode(errcode.InputDemuxerMissing),
		Code:        errcode.InputDemuxerMissing,
		Operation:   "open input",
		Node:        demuxNodeName(input),
		Reason:      "format " + quoteFormat(id) + " was detected but no demuxer is registered",
		Fields:      append(inputFormatFields(input), Detail{Key: "format", Value: id}),
		Details:     append(inputFormatDetails(input), "format="+string(id)),
		Fixes:       fixesFromSuggestions(suggestions),
		Suggestions: suggestions,
		Cause:       cause,
	}
}

func outputFormatProbeError(output format.Output, index int, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"give file outputs a name or MIME type a registered prober can recognize",
		"pass goav.Format(...) to goav.File(...) or goav.Writer(...) when the writer has no filename",
		"register a format adapter with goav.MustNew(goav.WithFormatAdapter(...))",
	}
	return &BuildError{
		Family:      errcode.FamilyForCode(errcode.OutputFormatUnknown),
		Code:        errcode.OutputFormatUnknown,
		Operation:   "open output",
		Node:        muxNodeName(output, index),
		Reason:      "output format could not be detected",
		Fields:      outputFormatFields(output),
		Details:     outputFormatDetails(output),
		Fixes:       fixesFromSuggestions(suggestions),
		Suggestions: suggestions,
		Cause:       cause,
	}
}

func outputMuxerMissingError(output format.Output, index int, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"register a " + string(id) + " muxer with goav.MustNew(goav.WithMuxer(...)) or a format adapter that provides one",
		"choose an output container supported by the runtime, such as .ivf for VP8/VP9/AV1 packet recording or .h264 for H264 packet recording",
		"call .UseRuntime(goav.MustNew(goav.WithFormatAdapter(...))) when using a custom adapter bundle",
	}
	return &BuildError{
		Family:      errcode.FamilyForCode(errcode.OutputMuxerMissing),
		Code:        errcode.OutputMuxerMissing,
		Operation:   "open output",
		Node:        muxNodeName(output, index),
		Reason:      "format " + quoteFormat(id) + " was selected but no muxer is registered",
		Fields:      append(outputFormatFields(output), Detail{Key: "format", Value: id}),
		Details:     append(outputFormatDetails(output), "format="+string(id)),
		Fixes:       fixesFromSuggestions(suggestions),
		Suggestions: suggestions,
		Cause:       cause,
	}
}

func destinationFormatProbeError(node string, output format.Output, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"give file destinations a name or MIME type a registered prober can recognize",
		"pass goav.Format(...) to the destination constructor when the writer has no filename",
		"register a format adapter with goav.MustNew(goav.WithFormatAdapter(...))",
	}
	return &BuildError{
		Family:      errcode.FamilyForCode(errcode.DestinationFormatUnknown),
		Code:        errcode.DestinationFormatUnknown,
		Operation:   "open destination",
		Node:        node,
		Reason:      "destination format could not be detected",
		Fields:      outputFormatFields(output),
		Details:     outputFormatDetails(output),
		Fixes:       fixesFromSuggestions(suggestions),
		Suggestions: suggestions,
		Cause:       cause,
	}
}

func destinationMuxerMissingError(node string, output format.Output, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	suggestions := []string{
		"register a " + string(id) + " muxer with goav.MustNew(goav.WithMuxer(...)) or a format adapter that provides one",
		"choose a destination container supported by the runtime, such as .ivf for VP8/VP9/AV1 packet recording or .h264 for H264 packet recording",
		"call .UseRuntime(goav.MustNew(goav.WithFormatAdapter(...))) when using a custom adapter bundle",
	}
	return &BuildError{
		Family:      errcode.FamilyForCode(errcode.DestinationMuxerMissing),
		Code:        errcode.DestinationMuxerMissing,
		Operation:   "open destination",
		Node:        node,
		Reason:      "format " + quoteFormat(id) + " was selected for destination but no muxer is registered",
		Fields:      append(outputFormatFields(output), Detail{Key: "format", Value: id}),
		Details:     append(outputFormatDetails(output), "format="+string(id)),
		Fixes:       fixesFromSuggestions(suggestions),
		Suggestions: suggestions,
		Cause:       cause,
	}
}

func fixesFromSuggestions(suggestions []string) []Fix {
	if len(suggestions) == 0 {
		return nil
	}
	fixes := make([]Fix, 0, len(suggestions))
	for i := range suggestions {
		if suggestions[i] == "" {
			continue
		}
		fixes = append(fixes, Fix{Message: suggestions[i]})
	}
	return fixes
}

func inputFormatDetails(input format.Input) []string {
	return detailsToLines(inputFormatFields(input))
}

func inputFormatFields(input format.Input) []Detail {
	var fields []Detail
	if input.Name != "" {
		fields = append(fields, Detail{Key: "name", Value: input.Name})
	}
	if input.URI != "" {
		fields = append(fields, Detail{Key: "uri", Value: input.URI})
	}
	if input.Protocol != "" {
		fields = append(fields, Detail{Key: "protocol", Value: input.Protocol})
	}
	if input.MIMEType != "" {
		fields = append(fields, Detail{Key: "mime", Value: input.MIMEType})
	}
	return fields
}

func outputFormatDetails(output format.Output) []string {
	return detailsToLines(outputFormatFields(output))
}

func outputFormatFields(output format.Output) []Detail {
	var fields []Detail
	if output.Name != "" {
		fields = append(fields, Detail{Key: "name", Value: output.Name})
	}
	if output.URI != "" {
		fields = append(fields, Detail{Key: "uri", Value: output.URI})
	}
	if output.Protocol != "" {
		fields = append(fields, Detail{Key: "protocol", Value: output.Protocol})
	}
	if output.MIMEType != "" {
		fields = append(fields, Detail{Key: "mime", Value: output.MIMEType})
	}
	return fields
}

func quoteFormat(id av.FormatID) string {
	return strconv.Quote(string(id))
}
