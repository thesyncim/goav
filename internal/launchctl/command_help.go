package launchctl

import (
	"fmt"
	"strings"
)

// Help renders generated help from the same manifest and tags used by parsing.
func Help(args []string) (string, error) {
	if len(args) == 0 {
		return rootHelp(), nil
	}
	switch args[0] {
	case "control":
		if len(args) == 1 {
			return controlHelp(), nil
		}
		spec, ok := LookupControlCommand(args[1])
		if !ok {
			return "", commandError("unknown_command", "help control", args[1], fmt.Sprintf("unknown control command %q", args[1]), nil, []string{"use one of: " + strings.Join(controlCommandNames(), ", ")}, nil)
		}
		return CommandHelp(spec), nil
	case "attach":
		return staticHelp("attach", "goav ctl --control unix://PATH attach <tap-name> as <branch-name> '<branch-pipeline>'", "The current library exposes typed Task.Attach(ctx, goav.Branch(...)); launch-pipeline string parsing is not present in this checkout."), nil
	case "rebranch":
		return staticHelp("rebranch", "goav ctl --control unix://PATH rebranch <branch-name> [--switch next_frame|next_keyframe] [--keep-old-on-failure] '<branch-pipeline>'", "The current library exposes typed Attachment.Rebranch(ctx, goav.Branch(...), goav.SwitchAt(...)); resolving an attachment by branch name requires a control server handle table."), nil
	case "detach":
		return staticHelp("detach", "goav ctl --control unix://PATH detach <branch-name>", "The current library exposes typed Task.Detach(ctx, attachment); resolving an attachment by branch name requires a control server handle table."), nil
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
	out.WriteString("  events --follow\n")
	out.WriteString("  watch [type=<event-type>] [stream=<stream-id>] --follow\n")
	out.WriteString("  stop\n")
	out.WriteString("  control <verb> ...\n")
	out.WriteString("  attach <tap-name> as <branch-name> '<branch-pipeline>'\n")
	out.WriteString("  rebranch <branch-name> [--switch next_frame|next_keyframe] [--keep-old-on-failure] '<branch-pipeline>'\n")
	out.WriteString("  detach <branch-name>\n")
	return out.String()
}

func controlHelp() string {
	var out strings.Builder
	out.WriteString("control\n\n")
	out.WriteString("Usage:\n")
	out.WriteString("  goav ctl --control unix://PATH control <verb> [field=value...]\n")
	out.WriteString("  goav ctl --control unix://PATH control --json '<json-goav-control>'\n\n")
	out.WriteString("Verbs:\n")
	for _, spec := range ControlManifest() {
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
	fields := orderedFields(commandFields(spec.ArgsType))
	var parts []string
	for _, field := range fields {
		if field.usage != "" {
			parts = append(parts, field.usage)
			continue
		}
		text := field.name + "=<value>"
		if !field.required {
			text = "[" + text + "]"
		}
		parts = append(parts, text)
	}
	return strings.TrimSpace("goav ctl --control unix://PATH control " + spec.Name + " " + strings.Join(parts, " "))
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

func padRight(value string, width int) string {
	if len(value) >= width {
		return value + "  "
	}
	return value + strings.Repeat(" ", width-len(value))
}
