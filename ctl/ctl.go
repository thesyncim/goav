// Package ctl exposes the safe host-side control-plane surface for applications
// that run goav tasks and want the goav command line to inspect or control
// them over a Unix socket.
//
// The package is deliberately allowlist-based. Reflection is used by the
// underlying command binder only to fill known command structs and generate
// help; callers expose additional behavior by passing CommandSpec,
// BranchPipelineStepSpec, and EncoderSpec values to one Server or ServeUnix
// instance.
package ctl

import (
	"context"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/launchctl"
)

// Request is the JSON request shape spoken over a goav ctl control socket.
type Request = launchctl.Request

// Response is the JSON response envelope returned by a goav ctl control socket.
type Response = launchctl.Response

// Error is the structured control-plane refusal shape.
type Error = launchctl.Error

// ControlResponse is the structured result returned by a command handler.
type ControlResponse = launchctl.ControlResponse

// CommandSpec is one explicit, allowlisted control command.
type CommandSpec = launchctl.CommandSpec

// StepArgs are key/value settings parsed for one branch-pipeline step.
type StepArgs = launchctl.StepArgs

// BranchPipelineStepSpec is one allowlisted custom branch-pipeline step.
type BranchPipelineStepSpec = launchctl.BranchPipelineStepSpec

// EncoderSpec is one allowlisted custom encoder spelling for branch pipelines.
type EncoderSpec = launchctl.EncoderSpec

// PipelineRegistry contains the custom steps and encoders one server exposes.
type PipelineRegistry = launchctl.PipelineRegistry

// BranchPipeline is the safe builder handle custom branch steps receive.
type BranchPipeline = launchctl.BranchPipeline

// Server handles decoded control-plane requests against one task.
type Server = launchctl.Server

// ServerOption configures one control server.
type ServerOption = launchctl.ServerOption

// SuccessResponse wraps a successful result in the standard envelope.
func SuccessResponse(result any) Response {
	return launchctl.SuccessResponse(result)
}

// ErrorResponse wraps an error in the standard envelope.
func ErrorResponse(operation string, err error) Response {
	return launchctl.ErrorResponse(operation, err)
}

// ControlManifest returns the built-in control command allowlist.
func ControlManifest() []CommandSpec {
	return launchctl.ControlManifest()
}

// LookupControlCommand finds one built-in command by name or alias.
func LookupControlCommand(name string) (CommandSpec, bool) {
	return launchctl.LookupControlCommand(name)
}

// LookupCommand finds one command in a caller-supplied manifest.
func LookupCommand(manifest []CommandSpec, name string) (CommandSpec, bool) {
	return launchctl.LookupCommand(manifest, name)
}

// BindArgs fills a known command args struct from key=value command-line
// arguments.
func BindArgs(spec CommandSpec, args []string) (any, error) {
	return launchctl.BindArgs(spec, args)
}

// BindJSON fills a known command args struct from a JSON object.
func BindJSON(spec CommandSpec, data []byte) (any, error) {
	return launchctl.BindJSON(spec, data)
}

// Invoke binds and applies one allowlisted command.
func Invoke(ctx context.Context, task goav.Task, spec CommandSpec, args []string) (ControlResponse, error) {
	return launchctl.Invoke(ctx, task, spec, args)
}

// DecodeRawControl decodes raw JSON into the real goav.Control shape.
func DecodeRawControl(data []byte) (goav.Control, error) {
	return launchctl.DecodeRawControl(data)
}

// DecodeRawEvent decodes raw JSON into an av.Event.
func DecodeRawEvent(data []byte) (av.Event, error) {
	return launchctl.DecodeRawEvent(data)
}

// Help renders generated help for the built-in command set.
func Help(args []string) (string, error) {
	return launchctl.Help(args)
}

// HelpWithCommands renders generated help for a caller-supplied command set.
func HelpWithCommands(args []string, manifest []CommandSpec) (string, error) {
	return launchctl.HelpWithCommands(args, manifest)
}

// HelpWithRegistry renders generated help for caller-supplied commands and
// branch-pipeline extension points.
func HelpWithRegistry(args []string, manifest []CommandSpec, registry PipelineRegistry) (string, error) {
	return launchctl.HelpWithRegistry(args, manifest, registry)
}

// CommandUsage renders the canonical usage line for one command.
func CommandUsage(spec CommandSpec) string {
	return launchctl.CommandUsage(spec)
}

// CommandHelp renders generated help for one command.
func CommandHelp(spec CommandSpec) string {
	return launchctl.CommandHelp(spec)
}

// RequestFromCLI converts canonical goav ctl arguments into a protocol
// request.
func RequestFromCLI(argv []string) (Request, error) {
	return launchctl.RequestFromCLI(argv)
}

// ExecuteRequest applies one decoded request to a task.
func ExecuteRequest(ctx context.Context, task goav.Task, request Request) Response {
	return launchctl.ExecuteRequest(ctx, task, request)
}

// Execute applies one in-process ctl command directly to a task.
func Execute(ctx context.Context, task goav.Task, argv []string) (ControlResponse, error) {
	return launchctl.Execute(ctx, task, argv)
}

// WithCommands appends application-specific controls to a server's built-in
// allowlist. Command names and aliases must be unique within the server.
func WithCommands(commands ...CommandSpec) ServerOption {
	return launchctl.WithCommands(commands...)
}

// WithPipelineRegistry installs application-specific branch-pipeline steps and
// encoders for attach/rebranch parsing. Step and encoder names and aliases
// share one namespace and cannot shadow built-in branch-pipeline spellings.
func WithPipelineRegistry(registry PipelineRegistry) ServerOption {
	return launchctl.WithPipelineRegistry(registry)
}

// ServeUnix listens on unix://PATH or PATH and serves one JSON request per
// connection until ctx is cancelled.
func ServeUnix(ctx context.Context, task goav.Task, address string) error {
	return launchctl.ServeUnix(ctx, task, address)
}

// ServeUnixWithOptions listens on unix://PATH or PATH with custom command and
// pipeline allowlists.
func ServeUnixWithOptions(ctx context.Context, task goav.Task, address string, options ...ServerOption) error {
	return launchctl.ServeUnixWithOptions(ctx, task, address, options...)
}
