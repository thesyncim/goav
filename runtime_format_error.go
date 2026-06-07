package goav

import (
	"errors"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func inputFormatProbeError(input format.Input, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "input_format_unknown",
		Operation: "open input",
		Node:      demuxNodeName(input),
		Reason:    "input format could not be detected",
		Details:   inputFormatDetails(input),
		Suggestions: []string{
			"give file or URI inputs a name, URI, or MIME type a registered prober can recognize",
			"register a format adapter with goav.New(goav.WithFormatAdapter(...))",
			"use goav.RTP(...) or goav.WebRTCTrack(...) for realtime packet receive",
		},
		Cause: cause,
	}
}

func inputDemuxerMissingError(input format.Input, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "input_demuxer_missing",
		Operation: "open input",
		Node:      demuxNodeName(input),
		Reason:    "format " + quoteFormat(id) + " was detected but no demuxer is registered",
		Details:   append(inputFormatDetails(input), "format="+string(id)),
		Suggestions: []string{
			"register a format adapter that provides a " + string(id) + " demuxer",
			"choose an input container supported by the runtime",
			"call .UseRuntime(goav.New(goav.WithFormatAdapter(...))) when using a custom adapter bundle",
		},
		Cause: cause,
	}
}

func outputFormatProbeError(output format.Output, index int, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "output_format_unknown",
		Operation: "open output",
		Node:      muxNodeName(output, index),
		Reason:    "output format could not be detected",
		Details:   outputFormatDetails(output),
		Suggestions: []string{
			"give file outputs a name or MIME type a registered prober can recognize",
			"pass goav.Format(...) to goav.File(...) or goav.Writer(...) when the writer has no filename",
			"register a format adapter with goav.New(goav.WithFormatAdapter(...))",
		},
		Cause: cause,
	}
}

func outputMuxerMissingError(output format.Output, index int, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "output_muxer_missing",
		Operation: "open output",
		Node:      muxNodeName(output, index),
		Reason:    "format " + quoteFormat(id) + " was selected but no muxer is registered",
		Details:   append(outputFormatDetails(output), "format="+string(id)),
		Suggestions: []string{
			"register a format adapter that provides a " + string(id) + " muxer",
			"choose an output container supported by the runtime, such as .ivf for VP8/VP9/AV1 packet recording or .h264 for H264 packet recording",
			"call .UseRuntime(goav.New(goav.WithFormatAdapter(...))) when using a custom adapter bundle",
		},
		Cause: cause,
	}
}

func targetFormatProbeError(node string, output format.Output, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "target_format_unknown",
		Operation: "open target",
		Node:      node,
		Reason:    "target format could not be detected",
		Details:   outputFormatDetails(output),
		Suggestions: []string{
			"give file targets a name or MIME type a registered prober can recognize",
			"pass goav.Format(...) to the destination constructor when the writer has no filename",
			"register a format adapter with goav.New(goav.WithFormatAdapter(...))",
		},
		Cause: cause,
	}
}

func targetMuxerMissingError(node string, output format.Output, id av.FormatID, cause error) error {
	if !errors.Is(cause, format.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "target_muxer_missing",
		Operation: "open target",
		Node:      node,
		Reason:    "format " + quoteFormat(id) + " was selected for target but no muxer is registered",
		Details:   append(outputFormatDetails(output), "format="+string(id)),
		Suggestions: []string{
			"register a format adapter that provides a " + string(id) + " muxer",
			"choose a target container supported by the runtime, such as .ivf for VP8/VP9/AV1 packet recording or .h264 for H264 packet recording",
			"call .UseRuntime(goav.New(goav.WithFormatAdapter(...))) when using a custom adapter bundle",
		},
		Cause: cause,
	}
}

func inputFormatDetails(input format.Input) []string {
	var details []string
	if input.Name != "" {
		details = append(details, "name="+input.Name)
	}
	if input.URI != "" {
		details = append(details, "uri="+input.URI)
	}
	if input.Protocol != "" {
		details = append(details, "protocol="+string(input.Protocol))
	}
	if input.MIMEType != "" {
		details = append(details, "mime="+input.MIMEType)
	}
	return details
}

func outputFormatDetails(output format.Output) []string {
	var details []string
	if output.Name != "" {
		details = append(details, "name="+output.Name)
	}
	if output.URI != "" {
		details = append(details, "uri="+output.URI)
	}
	if output.Protocol != "" {
		details = append(details, "protocol="+string(output.Protocol))
	}
	if output.MIMEType != "" {
		details = append(details, "mime="+output.MIMEType)
	}
	return details
}

func quoteFormat(id av.FormatID) string {
	return strconv.Quote(string(id))
}
