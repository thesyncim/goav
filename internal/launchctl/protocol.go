package launchctl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/pipeline"
)

// Request is the JSON request shape spoken over a goav ctl control socket.
type Request struct {
	Op       string            `json:"op"`
	Verb     string            `json:"verb,omitempty"`
	Args     map[string]string `json:"args,omitempty"`
	Control  json.RawMessage   `json:"control,omitempty"`
	Event    json.RawMessage   `json:"event,omitempty"`
	Tap      string            `json:"tap,omitempty"`
	Branch   string            `json:"branch,omitempty"`
	Pipeline string            `json:"pipeline,omitempty"`
	Switch   string            `json:"switch,omitempty"`
}

// Response is the standard envelope for a goav ctl control socket response.
type Response struct {
	OK     bool   `json:"ok"`
	Result any    `json:"result,omitempty"`
	Error  *Error `json:"error,omitempty"`
}

func SuccessResponse(result any) Response {
	return Response{OK: true, Result: result}
}

func ErrorResponse(operation string, err error) Response {
	return Response{OK: false, Error: structuredError(operation, err)}
}

// ExecuteRequest applies a decoded socket request to a Task and wraps the
// result in the standard response envelope.
func ExecuteRequest(ctx context.Context, task goav.LiveTask, request Request) Response {
	response, err := executeRequest(ctx, task, request)
	if err != nil {
		return ErrorResponse(request.Op, err)
	}
	return SuccessResponse(response.Result)
}

func executeRequest(ctx context.Context, task goav.LiveTask, request Request) (ControlResponse, error) {
	return executeRequestWithRegistry(ctx, task, request, ControlManifest(), PipelineRegistry{})
}

func executeRequestWithRegistry(ctx context.Context, task goav.LiveTask, request Request, manifest []CommandSpec, registry PipelineRegistry) (ControlResponse, error) {
	switch request.Op {
	case "help":
		text, err := helpWithRuntime(helpArgsFromRequest(request), manifest, registry, task)
		if err != nil {
			return ControlResponse{}, err
		}
		return ControlResponse{Operation: "help", Result: text}, nil
	case "capabilities":
		report, err := capabilityReport(manifest, registry, task)
		if err != nil {
			return ControlResponse{}, err
		}
		return ControlResponse{Operation: "capabilities", Result: report}, nil
	case "control_raw":
		return executeRawControl(ctx, task, request.Control)
	case "control":
		if request.Verb == "deliver" && len(request.Event) != 0 {
			return executeRawEvent(ctx, task, request.Event, argsFromMap(request.Args))
		}
		if request.Verb == "" {
			return ControlResponse{}, commandError("missing_command", "control", "", "missing control verb", nil, []string{"use verb=bitrate"}, nil)
		}
		spec, ok := LookupCommand(manifest, request.Verb)
		if !ok {
			return ControlResponse{}, commandError("unknown_command", "control", request.Verb, "unknown control command "+strconvQuote(request.Verb), nil, []string{"use one of: " + strings.Join(commandNames(manifest), ", ")}, nil)
		}
		return Invoke(ctx, task, spec, argsFromMap(request.Args))
	case "inspect", "snapshot", "stats", "taps", "streams", "branches", "destinations", "events", "watch", "stop":
		return Execute(ctx, task, append([]string{request.Op}, argsFromMap(request.Args)...))
	case "graph", "flowchart":
		return executeGraph(task, request.Args)
	case "attach":
		return executeAttachRequest(ctx, task, request, registry)
	case "rebranch":
		argv := []string{"rebranch"}
		if request.Branch != "" {
			argv = append(argv, request.Branch)
		}
		if request.Switch != "" {
			argv = append(argv, "--switch", request.Switch)
		}
		if request.Pipeline != "" {
			argv = append(argv, request.Pipeline)
		}
		return Execute(ctx, task, argv)
	case "detach":
		return Execute(ctx, task, []string{"detach", request.Branch})
	default:
		return ControlResponse{}, commandError("unknown_command", "ctl", request.Op, fmt.Sprintf("unknown request op %q", request.Op), nil, []string{"use op=control"}, nil)
	}
}

func executeAttachRequest(ctx context.Context, task goav.LiveTask, request Request, registry PipelineRegistry) (ControlResponse, error) {
	response, _, err := attachRequest(ctx, task, request, registry)
	return response, err
}

func attachRequest(ctx context.Context, task goav.LiveTask, request Request, registry PipelineRegistry) (ControlResponse, goav.Attachment, error) {
	spec, err := parseBranchPipelineWithRegistry(task, request.Tap, request.Branch, request.Pipeline, registry)
	if err != nil {
		return ControlResponse{}, nil, err
	}
	attachment, err := task.Attach(ctx, spec)
	if err != nil {
		return ControlResponse{}, nil, structuredError("attach", err)
	}
	return ControlResponse{Operation: "attach", Result: attachment.Snapshot()}, attachment, nil
}

func helpArgsFromRequest(request Request) []string {
	var args []string
	if topic := request.Args["topic"]; topic != "" {
		args = append(args, topic)
	}
	if command := request.Args["command"]; command != "" {
		args = append(args, command)
	}
	return args
}

// RequestFromCLI converts canonical goav ctl arguments into the socket
// protocol request. It does not execute anything locally.
func RequestFromCLI(argv []string) (Request, error) {
	if len(argv) == 0 {
		return Request{}, commandError("missing_command", "ctl", "", "missing ctl command", nil, []string{"use `goav ctl help`"}, nil)
	}
	switch argv[0] {
	case "help":
		return helpRequestFromCLI(argv[1:]), nil
	case "capabilities":
		if len(argv) != 1 {
			return Request{}, commandError("invalid_argument", "capabilities", "", "capabilities does not accept arguments", nil, []string{"use `goav ctl capabilities`"}, nil)
		}
		return Request{Op: "capabilities"}, nil
	case "control":
		return controlRequestFromCLI(argv[1:])
	case "attach":
		return attachRequestFromCLI(argv[1:])
	case "rebranch":
		return rebranchRequestFromCLI(argv[1:])
	case "detach":
		if len(argv) != 2 {
			return Request{}, commandError("invalid_argument", "detach", "", "detach needs exactly one branch name", nil, []string{"use `goav ctl detach <branch-name>`"}, nil)
		}
		return Request{Op: "detach", Branch: argv[1]}, nil
	case "graph", "flowchart":
		return graphRequestFromCLI(argv[0], argv[1:])
	case "inspect", "snapshot", "stats", "taps", "streams", "branches", "destinations", "events", "watch", "stop":
		args, err := argsMap(argv[0], argv[1:])
		if err != nil {
			return Request{}, err
		}
		return Request{Op: argv[0], Args: args}, nil
	default:
		return Request{}, commandError("unknown_command", "ctl", argv[0], fmt.Sprintf("unknown ctl command %q", argv[0]), nil, []string{"use `goav ctl help`"}, nil)
	}
}

func graphRequestFromCLI(op string, argv []string) (Request, error) {
	if len(argv) == 1 && !strings.Contains(argv[0], "=") && !strings.HasPrefix(argv[0], "--") {
		return Request{Op: op, Args: map[string]string{"format": argv[0]}}, nil
	}
	if len(argv) > 1 {
		return Request{}, commandError("invalid_argument", op, "", "graph accepts at most one format argument", nil, []string{"use `goav ctl graph`", "use `goav ctl graph format=dot`"}, nil)
	}
	args, err := argsMap(op, argv)
	if err != nil {
		return Request{}, err
	}
	return Request{Op: op, Args: args}, nil
}

func helpRequestFromCLI(argv []string) Request {
	args := make(map[string]string)
	if len(argv) > 0 {
		args["topic"] = argv[0]
	}
	if len(argv) > 1 {
		args["command"] = argv[1]
	}
	return Request{Op: "help", Args: args}
}

func controlRequestFromCLI(argv []string) (Request, error) {
	if len(argv) == 0 {
		return Request{}, commandError("missing_command", "control", "", "missing control verb", nil, []string{"use `goav ctl help control`"}, nil)
	}
	if argv[0] == "--json" {
		if len(argv) != 2 {
			return Request{}, commandError("invalid_argument", "control --json", "", "control --json needs exactly one JSON object", nil, []string{`use goav ctl control --json '{"type":"bitrate","stream_id":"video","bitrate":1200000}'`}, nil)
		}
		return Request{Op: "control_raw", Control: json.RawMessage(argv[1])}, nil
	}
	verb := argv[0]
	if verb == "deliver" && len(argv) >= 3 && argv[1] == "--json" {
		args, err := argsMap("control deliver --json", argv[3:])
		if err != nil {
			return Request{}, err
		}
		return Request{Op: "control", Verb: "deliver", Event: json.RawMessage(argv[2]), Args: args}, nil
	}
	args, err := argsMap("control "+verb, argv[1:])
	if err != nil {
		return Request{}, err
	}
	return Request{Op: "control", Verb: verb, Args: args}, nil
}

func attachRequestFromCLI(argv []string) (Request, error) {
	if len(argv) != 4 || argv[1] != "as" {
		return Request{}, commandError("invalid_argument", "attach", "", "attach needs: <tap-name> as <branch-name> '<branch-pipeline>'", nil, []string{"use `goav ctl help attach`"}, nil)
	}
	return Request{Op: "attach", Tap: argv[0], Branch: argv[2], Pipeline: argv[3]}, nil
}

func rebranchRequestFromCLI(argv []string) (Request, error) {
	if len(argv) < 2 {
		return Request{}, commandError("invalid_argument", "rebranch", "", "rebranch needs a branch name and pipeline", nil, []string{"use `goav ctl help rebranch`"}, nil)
	}
	req := Request{Op: "rebranch", Branch: argv[0]}
	args := argv[1:]
	for len(args) > 0 {
		switch args[0] {
		case "--switch":
			if len(args) < 2 {
				return Request{}, commandError("invalid_argument", "rebranch", "--switch", "--switch needs next_frame or next_keyframe", nil, nil, nil)
			}
			req.Switch = args[1]
			args = args[2:]
		default:
			if req.Pipeline != "" {
				return Request{}, commandError("invalid_argument", "rebranch", args[0], "rebranch accepts exactly one branch pipeline", nil, nil, nil)
			}
			req.Pipeline = args[0]
			args = args[1:]
		}
	}
	if req.Pipeline == "" {
		return Request{}, commandError("invalid_argument", "rebranch", "", "rebranch needs a branch pipeline", nil, []string{"use `goav ctl help rebranch`"}, nil)
	}
	return req, nil
}

func argsMap(operation string, argv []string) (map[string]string, error) {
	if len(argv) == 0 {
		return nil, nil
	}
	out := make(map[string]string)
	for _, arg := range argv {
		var key, value string
		if strings.HasPrefix(arg, "--") {
			key = strings.TrimPrefix(arg, "--")
			value = "true"
		} else {
			var ok bool
			key, value, ok = strings.Cut(arg, "=")
			if !ok {
				key = arg
				value = ""
			}
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, commandError("invalid_argument", operation, "argument", "argument name cannot be empty", []string{"value=" + arg}, []string{"use key=value"}, nil)
		}
		if _, exists := out[key]; exists {
			return nil, commandError("invalid_argument", operation, key, "argument "+key+" was provided more than once", nil, []string{"keep only one " + key + "=... value"}, nil)
		}
		out[key] = value
	}
	return out, nil
}

func argsFromMap(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]string, 0, len(keys))
	for _, key := range keys {
		if flagArg(key, values[key]) {
			args = append(args, "--"+key)
			continue
		}
		args = append(args, key+"="+values[key])
	}
	return args
}

func flagArg(key string, value string) bool {
	switch key {
	case "follow":
		return value == "true"
	default:
		return false
	}
}

// Execute applies one ctl command directly to a Task. It is the in-process
// equivalent of what a control server does after decoding Request.
func Execute(ctx context.Context, task goav.LiveTask, argv []string) (ControlResponse, error) {
	if len(argv) == 0 {
		return ControlResponse{}, commandError("missing_command", "ctl", "", "missing ctl command", nil, []string{"use `goav ctl help`"}, nil)
	}
	switch argv[0] {
	case "help":
		text, err := Help(argv[1:])
		if err != nil {
			return ControlResponse{}, err
		}
		return ControlResponse{Operation: "help", Result: text}, nil
	case "capabilities":
		report, err := capabilityReport(ControlManifest(), PipelineRegistry{}, task)
		if err != nil {
			return ControlResponse{}, err
		}
		return ControlResponse{Operation: "capabilities", Result: report}, nil
	case "control":
		return executeControl(ctx, task, argv[1:])
	case "inspect":
		return ControlResponse{Operation: "inspect", Result: task.Describe()}, nil
	case "snapshot":
		return ControlResponse{Operation: "snapshot", Result: task.Snapshot()}, nil
	case "stats":
		return ControlResponse{Operation: "stats", Result: task.Stats()}, nil
	case "taps":
		return ControlResponse{Operation: "taps", Result: task.Taps()}, nil
	case "streams":
		return ControlResponse{Operation: "streams", Result: streamsFromTask(task)}, nil
	case "branches":
		return ControlResponse{Operation: "branches", Result: task.Snapshot().Branches}, nil
	case "destinations":
		return ControlResponse{Operation: "destinations", Result: task.Snapshot().Destinations}, nil
	case "graph", "flowchart":
		request, err := graphRequestFromCLI(argv[0], argv[1:])
		if err != nil {
			return ControlResponse{}, err
		}
		return executeGraph(task, request.Args)
	case "events", "watch":
		return executeWatch(task, argv[0], argv[1:])
	case "stop":
		if err := task.Close(); err != nil {
			return ControlResponse{}, structuredError("stop", err)
		}
		return ControlResponse{Operation: "stop", Result: "closed"}, nil
	case "attach":
		request, err := attachRequestFromCLI(argv[1:])
		if err != nil {
			return ControlResponse{}, err
		}
		return executeAttachRequest(ctx, task, request, PipelineRegistry{})
	case "rebranch":
		return executeUnsupportedRebranch(task, argv[1:])
	case "detach":
		return executeUnsupportedDetach(task, argv[1:])
	default:
		return ControlResponse{}, commandError("unknown_command", "ctl", argv[0], fmt.Sprintf("unknown ctl command %q", argv[0]), nil, []string{"use `goav ctl help`"}, nil)
	}
}

func executeControl(ctx context.Context, task goav.LiveTask, argv []string) (ControlResponse, error) {
	if len(argv) == 0 {
		return ControlResponse{}, commandError("missing_command", "control", "", "missing control verb", nil, []string{"use `goav ctl help control`"}, nil)
	}
	if argv[0] == "--json" {
		if len(argv) != 2 {
			return ControlResponse{}, commandError("invalid_argument", "control --json", "", "control --json needs exactly one JSON object", nil, nil, nil)
		}
		return executeRawControl(ctx, task, []byte(argv[1]))
	}
	verb := argv[0]
	if verb == "deliver" && len(argv) >= 3 && argv[1] == "--json" {
		return executeRawEvent(ctx, task, []byte(argv[2]), argv[3:])
	}
	spec, ok := LookupControlCommand(verb)
	if !ok {
		return ControlResponse{}, commandError("unknown_command", "control", verb, fmt.Sprintf("unknown control command %q", verb), nil, []string{"use one of: " + strings.Join(controlCommandNames(), ", ")}, nil)
	}
	return Invoke(ctx, task, spec, argv[1:])
}

func executeGraph(task goav.LiveTask, args map[string]string) (ControlResponse, error) {
	for key := range args {
		if key != "format" {
			return ControlResponse{}, commandError("unknown_field", "graph", key, fmt.Sprintf("unknown graph field %q", key), nil, []string{"use format=mermaid", "use format=dot", "use format=text"}, nil)
		}
	}
	target, err := graphRenderTarget(args["format"])
	if err != nil {
		return ControlResponse{}, err
	}
	out, err := graphrender.RenderTaskURI(task, target)
	if err != nil {
		return ControlResponse{}, structuredError("graph", err)
	}
	return ControlResponse{Operation: "graph", Result: out}, nil
}

func graphRenderTarget(format string) (string, error) {
	switch strings.ToLower(format) {
	case "", "mermaid", "flowchart":
		return "goav:graph?format=mermaid", nil
	case "dot":
		return "goav://graph/dot", nil
	case "text":
		return "goav:graph", nil
	default:
		return "", commandError("invalid_value", "graph", "format", "format must be mermaid, dot, or text", []string{"value=" + format}, []string{"use format=mermaid", "use format=dot", "use format=text"}, nil)
	}
}

func executeWatch(task goav.LiveTask, operation string, args []string) (ControlResponse, error) {
	argValues, err := argsMap(operation, args)
	if err != nil {
		return ControlResponse{}, err
	}
	if argValues["follow"] == "true" {
		return ControlResponse{}, commandError(
			"unsupported_streaming_response",
			operation,
			"",
			"watch/event streaming requires a control server streaming response",
			nil,
			[]string{"use task.Watch(...).Events() in-process", "use a goav control server that supports streaming event responses"},
			nil,
		)
	}
	var filters []inspect.EventFilter
	if typ := argValues["type"]; typ != "" {
		filters = append(filters, inspect.WatchTypes(av.EventType(typ)))
	}
	if stream := argValues["stream"]; stream != "" {
		filters = append(filters, inspect.WatchStream(av.StreamID(stream)))
	}
	sub := task.Watch(filters...)
	defer sub.Close()
	ch := sub.Events()
	var events []av.Event
	for {
		select {
		case event, ok := <-ch:
			if !ok {
				return ControlResponse{Operation: operation, Result: events}, nil
			}
			events = append(events, event)
		default:
			return ControlResponse{Operation: operation, Result: events}, nil
		}
	}
}

func executeUnsupportedDetach(task goav.LiveTask, args []string) (ControlResponse, error) {
	if len(args) != 1 {
		return ControlResponse{}, commandError("invalid_argument", "detach", "", "detach needs exactly one branch name", nil, []string{"use `goav ctl detach <branch-name>`"}, nil)
	}
	name := args[0]
	branches := task.Snapshot().Branches
	for _, branch := range branches {
		if branch.Name == name {
			return ControlResponse{}, commandError(
				"unsupported",
				"detach",
				name,
				"detaching by branch name requires a control server handle table for goav.Attachment",
				nil,
				[]string{"keep the goav.Attachment returned by Mutable.Attach and call task.Detach(ctx, attachment)", "run `goav ctl branches` to inspect branch state"},
				nil,
			)
		}
	}
	available := branchNames(branches)
	suggestions := []string{"run `goav ctl branches`"}
	if nearest := closest(name, available); nearest != "" {
		suggestions = append([]string{"use " + nearest}, suggestions...)
	}
	return ControlResponse{}, commandError("unknown_branch", "detach", name, fmt.Sprintf("unknown branch %q", name), []string{"available_branches=" + strings.Join(available, ",")}, suggestions, nil)
}

func executeUnsupportedRebranch(task goav.LiveTask, args []string) (ControlResponse, error) {
	if len(args) == 0 {
		return ControlResponse{}, commandError("invalid_argument", "rebranch", "", "rebranch needs a branch name", nil, []string{"use `goav ctl help rebranch`"}, nil)
	}
	name := args[0]
	branches := task.Snapshot().Branches
	for _, branch := range branches {
		if branch.Name == name {
			return ControlResponse{}, unsupportedGraphMutation("rebranch")
		}
	}
	available := branchNames(branches)
	suggestions := []string{"run `goav ctl branches`"}
	if nearest := closest(name, available); nearest != "" {
		suggestions = append([]string{"use " + nearest}, suggestions...)
	}
	return ControlResponse{}, commandError("unknown_branch", "rebranch", name, fmt.Sprintf("unknown branch %q", name), []string{"available_branches=" + strings.Join(available, ",")}, suggestions, nil)
}

func unsupportedGraphMutation(operation string) error {
	switch operation {
	case "rebranch":
		return commandError(
			"unsupported",
			"rebranch",
			"",
			"rebranch by name and branch-pipeline string parsing require a control server attachment table",
			nil,
			[]string{"use typed attachment.Rebranch(ctx, goav.Branch(...), lifecycle.SwitchAt(...))", "add a launch pipeline parser and attachment handle table before enabling `goav ctl rebranch`"},
			nil,
		)
	default:
		return commandError("unsupported", operation, "", "unsupported graph mutation", nil, nil, nil)
	}
}

type StreamInfo struct {
	ID       av.StreamID       `json:"id,omitempty"`
	Media    av.MediaType      `json:"media,omitempty"`
	Domain   string            `json:"domain,omitempty"`
	Source   string            `json:"source,omitempty"`
	Tap      string            `json:"tap,omitempty"`
	Node     pipeline.NodeRef  `json:"node,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func streamsFromTask(task goav.LiveTask) []StreamInfo {
	var out []StreamInfo
	seen := make(map[av.StreamID]struct{})
	for _, tap := range task.Taps() {
		if tap.Shape.StreamID == "" {
			continue
		}
		if _, ok := seen[tap.Shape.StreamID]; ok {
			continue
		}
		seen[tap.Shape.StreamID] = struct{}{}
		out = append(out, StreamInfo{
			ID:     tap.Shape.StreamID,
			Media:  tap.MediaKind,
			Domain: string(tap.Domain),
			Tap:    tap.Name,
			Node:   tap.Node,
		})
	}
	return out
}
