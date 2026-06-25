package goav

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
)

func TestRuntimeFormatErrorContracts(t *testing.T) {
	cause := format.ErrNotFound
	input := format.Input{
		Name:     "camera",
		URI:      "file:///tmp/camera.weird",
		Protocol: "file",
		MIMEType: "application/x-weird",
	}
	output := format.Output{
		Name:     "recording",
		URI:      "file:///tmp/recording.ogg",
		Protocol: "file",
		MIMEType: "audio/ogg",
	}

	tests := []struct {
		name          string
		err           error
		code          errcode.Code
		operation     string
		node          string
		reason        string
		details       []string
		suggestionAny []string
	}{
		{
			name:      "input probe",
			err:       inputFormatProbeError(input, cause),
			code:      errcode.InputFormatUnknown,
			operation: "open input",
			node:      "camera",
			reason:    "input format could not be detected",
			details:   []string{"name=camera", "uri=file:///tmp/camera.weird", "protocol=file", "mime=application/x-weird"},
			suggestionAny: []string{
				"registered prober",
				"register a format adapter",
				"goav.Input(provider)",
			},
		},
		{
			name:      "input demuxer",
			err:       inputDemuxerMissingError(input, av.FormatID("weird"), cause),
			code:      errcode.InputDemuxerMissing,
			operation: "open input",
			node:      "camera",
			reason:    `format "weird" was detected but no demuxer is registered`,
			details:   []string{"name=camera", "format=weird"},
			suggestionAny: []string{
				"weird demuxer",
				"supported by the runtime",
				"WithFormatAdapter",
			},
		},
		{
			name:      "output probe",
			err:       outputFormatProbeError(format.Output{Protocol: "file", MIMEType: "application/x-weird"}, 2, cause),
			code:      errcode.OutputFormatUnknown,
			operation: "open output",
			node:      "output-2",
			reason:    "output format could not be detected",
			details:   []string{"protocol=file", "mime=application/x-weird"},
			suggestionAny: []string{
				"registered prober",
				"goav.Format",
				"register a format adapter",
			},
		},
		{
			name:      "output muxer",
			err:       outputMuxerMissingError(output, 1, av.FormatID("ogg"), cause),
			code:      errcode.OutputMuxerMissing,
			operation: "open output",
			node:      "recording",
			reason:    `format "ogg" was selected but no muxer is registered`,
			details:   []string{"name=recording", "uri=file:///tmp/recording.ogg", "format=ogg"},
			suggestionAny: []string{
				"ogg muxer",
				"output container",
				"WithFormatAdapter",
			},
		},
		{
			name:      "destination probe",
			err:       destinationFormatProbeError("archive", output, cause),
			code:      errcode.DestinationFormatUnknown,
			operation: "open destination",
			node:      "archive",
			reason:    "destination format could not be detected",
			details:   []string{"name=recording", "mime=audio/ogg"},
			suggestionAny: []string{
				"goav.Format",
				"registered prober",
				"register a format adapter",
			},
		},
		{
			name:      "destination muxer",
			err:       destinationMuxerMissingError("archive", output, av.FormatID("ogg"), cause),
			code:      errcode.DestinationMuxerMissing,
			operation: "open destination",
			node:      "archive",
			reason:    `format "ogg" was selected for destination but no muxer is registered`,
			details:   []string{"uri=file:///tmp/recording.ogg", "format=ogg"},
			suggestionAny: []string{
				"ogg muxer",
				"destination container",
				"WithFormatAdapter",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buildErr := requireRuntimeFormatBuildError(t, tt.err, tt.code)
			if buildErr.Operation != tt.operation || buildErr.Node != tt.node || buildErr.Reason != tt.reason {
				t.Fatalf("BuildError = %+v, want operation=%q node=%q reason=%q", buildErr, tt.operation, tt.node, tt.reason)
			}
			if !errors.Is(buildErr, cause) {
				t.Fatalf("BuildError does not unwrap %v: %+v", cause, buildErr)
			}
			for _, detail := range tt.details {
				if !runtimeFormatSliceContains(buildErr.DetailLines(), detail) {
					t.Fatalf("details = %v, want fragment %q", buildErr.DetailLines(), detail)
				}
			}
			for _, suggestion := range tt.suggestionAny {
				if !runtimeFormatSliceContains(buildErr.FixLines(), suggestion) {
					t.Fatalf("suggestions = %v, want fragment %q", buildErr.FixLines(), suggestion)
				}
			}
		})
	}
}

func TestRuntimeFormatErrorsPassThroughUnexpectedCauses(t *testing.T) {
	cause := errors.New("adapter failed")
	input := format.Input{Name: "camera"}
	output := format.Output{Name: "recording"}
	tests := []struct {
		name string
		err  error
	}{
		{name: "input probe", err: inputFormatProbeError(input, cause)},
		{name: "input demuxer", err: inputDemuxerMissingError(input, "ogg", cause)},
		{name: "output probe", err: outputFormatProbeError(output, 0, cause)},
		{name: "output muxer", err: outputMuxerMissingError(output, 0, "ogg", cause)},
		{name: "destination probe", err: destinationFormatProbeError("archive", output, cause)},
		{name: "destination muxer", err: destinationMuxerMissingError("archive", output, "ogg", cause)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err != cause {
				t.Fatalf("err = %v, want original cause", tt.err)
			}
		})
	}
}

func requireRuntimeFormatBuildError(t *testing.T, err error, code errcode.Code) *BuildError {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	if buildErr.Code != code {
		t.Fatalf("code = %s, want %s", buildErr.Code, code)
	}
	return buildErr
}

func runtimeFormatSliceContains(values []string, fragment string) bool {
	for _, value := range values {
		if strings.Contains(value, fragment) {
			return true
		}
	}
	return false
}
