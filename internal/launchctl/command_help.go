package launchctl

import (
	"fmt"
	"strings"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
)

// Help renders generated help from the same manifest and tags used by parsing.
func Help(args []string) (string, error) {
	return HelpWithCommands(args, ControlManifest())
}

// HelpWithCommands renders help using the provided allowlisted command
// manifest. Servers use this to include application-specific controls.
func HelpWithCommands(args []string, manifest []CommandSpec) (string, error) {
	return HelpWithRegistry(args, manifest, PipelineRegistry{})
}

// HelpWithRegistry renders help using the provided allowlisted commands and
// branch-pipeline registry. Servers use this to include application-specific
// controls, pipeline steps, and encoder spellings.
func HelpWithRegistry(args []string, manifest []CommandSpec, registry PipelineRegistry) (string, error) {
	return helpWithRegistry(args, manifest, registry, runtimeBranchCapabilities{})
}

func helpWithRuntime(args []string, manifest []CommandSpec, registry PipelineRegistry, task goav.Task) (string, error) {
	return helpWithRegistry(args, manifest, registry, runtimeCapabilities(task))
}

func helpWithRegistry(args []string, manifest []CommandSpec, registry PipelineRegistry, caps runtimeBranchCapabilities) (string, error) {
	if err := validateCommandManifest(manifest); err != nil {
		return "", err
	}
	if err := validatePipelineRegistry(registry); err != nil {
		return "", err
	}
	if len(args) == 0 {
		return rootHelp(), nil
	}
	switch args[0] {
	case "control":
		if len(args) == 1 {
			return controlHelp(manifest), nil
		}
		spec, ok := LookupCommand(manifest, args[1])
		if !ok {
			return "", commandError("unknown_command", "help control", args[1], fmt.Sprintf("unknown control command %q", args[1]), nil, []string{"use one of: " + strings.Join(commandNames(manifest), ", ")}, nil)
		}
		return CommandHelp(spec), nil
	case "attach":
		return branchPipelineHelp("attach", "goav ctl --control unix://PATH attach <tap-name> as <branch-name> '<branch-pipeline>'", "Builds an allowlisted branch pipeline from a named tap.", registry, caps), nil
	case "rebranch":
		return branchPipelineHelp("rebranch", "goav ctl --control unix://PATH rebranch <branch-name> [--switch next_frame|next_keyframe] [--keep-old-on-failure] '<branch-pipeline>'", "Replaces an attachment created through this control server, using the same allowlisted branch-pipeline grammar as attach.", registry, caps), nil
	case "detach":
		return staticHelp("detach", "goav ctl --control unix://PATH detach <branch-name>", "Detaches an attachment created through this control server."), nil
	case "capabilities":
		return staticHelp("capabilities", "goav ctl --control unix://PATH capabilities", "Prints the server-aware command, branch-step, encoder, runtime encoder, and runtime muxer manifest as JSON."), nil
	case "graph", "flowchart":
		return staticHelp("graph", "goav ctl --control unix://PATH graph [format=mermaid|dot|text]", "Renders the running task snapshot as Mermaid, Graphviz DOT, or text. The default format is Mermaid and runtime branch-owned nodes are annotated by branch name and lifecycle state."), nil
	default:
		return "", commandError("unknown_command", "help", args[0], fmt.Sprintf("unknown help topic %q", args[0]), nil, []string{"use `goav ctl help control`"}, nil)
	}
}

func rootHelp() string {
	var out strings.Builder
	out.WriteString("goav ctl\n\n")
	out.WriteString("Usage:\n")
	out.WriteString("  goav ctl --control unix://PATH <command>\n\n")
	out.WriteString("Commands:\n")
	out.WriteString("  inspect\n")
	out.WriteString("  snapshot\n")
	out.WriteString("  stats\n")
	out.WriteString("  taps\n")
	out.WriteString("  streams\n")
	out.WriteString("  branches\n")
	out.WriteString("  destinations\n")
	out.WriteString("  capabilities\n")
	out.WriteString("  graph [format=mermaid|dot|text]\n")
	out.WriteString("  events --follow\n")
	out.WriteString("  watch [type=<event-type>] [stream=<stream-id>] --follow\n")
	out.WriteString("  stop\n")
	out.WriteString("  control <verb> ...\n")
	out.WriteString("  attach <tap-name> as <branch-name> '<branch-pipeline>'\n")
	out.WriteString("  rebranch <branch-name> [--switch next_frame|next_keyframe] [--keep-old-on-failure] '<branch-pipeline>'\n")
	out.WriteString("  detach <branch-name>\n")
	return out.String()
}

func controlHelp(manifest []CommandSpec) string {
	var out strings.Builder
	out.WriteString("control\n\n")
	out.WriteString("Usage:\n")
	out.WriteString("  goav ctl --control unix://PATH control <verb> [field=value...]\n")
	out.WriteString("  goav ctl --control unix://PATH control --json '<json-goav-control>'\n\n")
	out.WriteString("Verbs:\n")
	for _, spec := range manifest {
		out.WriteString("  ")
		out.WriteString(spec.Name)
		if spec.Summary != "" {
			out.WriteString("  ")
			out.WriteString(spec.Summary)
		}
		out.WriteString("\n")
	}
	return out.String()
}

// CommandUsage renders the canonical usage line for one control verb.
func CommandUsage(spec CommandSpec) string {
	return strings.TrimSpace("goav ctl --control unix://PATH control " + spec.Name + " " + ArgsUsage(spec.ArgsType))
}

// CommandHelp renders help for one manifest command.
func CommandHelp(spec CommandSpec) string {
	var out strings.Builder
	out.WriteString("control ")
	out.WriteString(spec.Name)
	out.WriteString("\n\n")
	if spec.Summary != "" {
		out.WriteString(spec.Summary)
		out.WriteString("\n\n")
	}
	out.WriteString("Usage:\n")
	out.WriteString("  ")
	out.WriteString(CommandUsage(spec))
	out.WriteString("\n\n")
	out.WriteString("Fields:\n")
	for _, field := range orderedFields(commandFields(spec.ArgsType)) {
		out.WriteString("  ")
		out.WriteString(padRight(field.name, 10))
		if field.required {
			out.WriteString("required  ")
		} else {
			out.WriteString("optional  ")
		}
		out.WriteString(field.help)
		out.WriteString("\n")
	}
	return out.String()
}

func staticHelp(name string, usage string, note string) string {
	var out strings.Builder
	out.WriteString(name)
	out.WriteString("\n\nUsage:\n  ")
	out.WriteString(usage)
	out.WriteString("\n\n")
	out.WriteString(note)
	out.WriteString("\n")
	return out.String()
}

func branchPipelineHelp(name string, usage string, note string, registry PipelineRegistry, caps runtimeBranchCapabilities) string {
	var out strings.Builder
	out.WriteString(name)
	out.WriteString("\n\nUsage:\n  ")
	out.WriteString(usage)
	out.WriteString("\n\n")
	out.WriteString(note)
	out.WriteString("\n\n")
	out.WriteString("Built-in steps:\n")
	for _, step := range builtinPipelineHelpRows() {
		writePipelineHelpRow(&out, step.name, step.usage, nil, step.summary)
	}
	if len(registry.Steps) != 0 {
		out.WriteString("\nCustom steps:\n")
		for _, step := range registry.Steps {
			writePipelineHelpRow(&out, step.Name, firstNonEmpty(step.Usage, ArgsUsage(step.ArgsType)), step.Aliases, step.Summary)
		}
	}
	if len(registry.Encoders) != 0 {
		out.WriteString("\nCustom encoders:\n")
		for _, encoder := range registry.Encoders {
			writePipelineHelpRow(&out, encoder.Name, firstNonEmpty(encoder.Usage, ArgsUsage(encoder.ArgsType)), encoder.Aliases, encoder.Summary)
		}
	}
	if len(caps.encoders) != 0 {
		out.WriteString("\nRuntime encoders:\n")
		for _, encoder := range caps.encoders {
			writeRuntimeEncoderHelpRow(&out, encoder)
		}
	}
	if len(caps.muxers) != 0 {
		out.WriteString("\nRuntime muxers:\n")
		for _, muxer := range caps.muxers {
			writeRuntimeMuxerHelpRow(&out, muxer)
		}
	}
	out.WriteString("\nBranch pipelines are written as `step key=value ! step key=value`. Custom steps and encoders receive their key=value settings through StepArgs.\n")
	out.WriteString("Any encoder registered on the task runtime is callable with `encode codec=<id> media=<kind> ...` and the common codec settings. Use a custom EncoderSpec only for native adapter knobs that need host code.\n")
	out.WriteString("Any muxer registered on the task runtime is callable from `filesink location=<path> format=<id>`; custom destinations such as uploaders remain host-owned branch steps.\n")
	return out.String()
}

type runtimeBranchCapabilities struct {
	encoders []codec.Descriptor
	muxers   []format.Descriptor
}

type runtimeEncoderDescriptorProvider interface {
	EncoderDescriptors() []codec.Descriptor
}

type runtimeMuxerDescriptorProvider interface {
	MuxerDescriptors() []format.Descriptor
}

func runtimeCapabilities(task goav.Task) runtimeBranchCapabilities {
	var caps runtimeBranchCapabilities
	if provider, ok := task.(runtimeEncoderDescriptorProvider); ok && provider != nil {
		caps.encoders = provider.EncoderDescriptors()
	}
	if provider, ok := task.(runtimeMuxerDescriptorProvider); ok && provider != nil {
		caps.muxers = provider.MuxerDescriptors()
	}
	return caps
}

type pipelineHelpRow struct {
	name    string
	usage   string
	summary string
}

func builtinPipelineHelpRows() []pipelineHelpRow {
	return []pipelineHelpRow{
		{name: "copy", summary: "copy packets or frames without transforming them"},
		{name: "decode", summary: "decode packets to frames"},
		{name: "resize", usage: "854x480 | width=854 height=480", summary: "resize video frames"},
		{name: "resample", usage: "48000 2 | rate=48000 channels=2", summary: "resample audio frames"},
		{name: "encode", usage: "codec=<id> media=<audio|video|subtitle> [bitrate=<rate>] [profile=<name>] [level=<name>] [sample_rate=<hz>] [channels=<n>] [clock_rate=<hz>] [keyframe_interval=<n>] [fps=<n|n/d>]", summary: "encode frames with a built-in or runtime-registered codec"},
		{name: "vp8enc", usage: "[bitrate=<rate>] [fps=<n|n/d>]", summary: "encode VP8 video"},
		{name: "vp9enc", usage: "[bitrate=<rate>] [fps=<n|n/d>]", summary: "encode VP9 video"},
		{name: "h264enc", usage: "[bitrate=<rate>] [profile=<name>] [level=<name>] [fps=<n|n/d>]", summary: "encode H.264 video"},
		{name: "av1enc", usage: "[bitrate=<rate>] [fps=<n|n/d>] [qindex=<n>] [min_qindex=<n>] [max_qindex=<n>] [temporal_layers=<n>]", summary: "encode AV1 video"},
		{name: "opusenc", usage: "[bitrate=<rate>] [sample_rate=<hz>] [channels=<n>]", summary: "encode Opus audio"},
		{name: "filesink", usage: "location=<path> [format=<container>]", summary: "write the branch into a file destination"},
	}
}

func writePipelineHelpRow(out *strings.Builder, name string, usage string, aliases []string, summary string) {
	if name == "" {
		return
	}
	label := name
	if usage != "" {
		label += " " + usage
	}
	out.WriteString("  ")
	out.WriteString(padRight(label, 18))
	if len(aliases) != 0 {
		out.WriteString("(aliases: ")
		out.WriteString(strings.Join(aliases, ", "))
		out.WriteString(") ")
	}
	out.WriteString(summary)
	out.WriteString("\n")
}

func writeRuntimeEncoderHelpRow(out *strings.Builder, desc codec.Descriptor) {
	if desc.ID == "" {
		return
	}
	media := desc.Type
	if media == "" {
		media = "<kind>"
	}
	label := "encode codec=" + string(desc.ID) + " media=" + string(media) + " [bitrate=<rate>] [profile=<name>] [level=<name>] [sample_rate=<hz>] [channels=<n>] [clock_rate=<hz>] [keyframe_interval=<n>] [fps=<n|n/d>]"
	summary := desc.Name
	if summary == "" {
		summary = "runtime-registered encoder"
	}
	writePipelineHelpRow(out, label, "", nil, summary)
}

func writeRuntimeMuxerHelpRow(out *strings.Builder, desc format.Descriptor) {
	if desc.Format == "" {
		return
	}
	label := "filesink location=<path> format=" + string(desc.Format)
	summary := "runtime-registered muxer"
	if len(desc.Codecs) != 0 {
		summary += " for codecs " + codecIDsLabel(desc.Codecs)
	}
	writePipelineHelpRow(out, label, "", nil, summary)
}

func codecIDsLabel(ids []av.CodecID) string {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" {
			values = append(values, string(id))
		}
	}
	return strings.Join(values, ",")
}

func padRight(value string, width int) string {
	if len(value) >= width {
		return value + "  "
	}
	return value + strings.Repeat(" ", width-len(value))
}
