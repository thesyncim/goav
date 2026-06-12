package launchctl

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sort"
	"strings"
	"sync"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
)

// Server handles one decoded control-plane request against a task.
type Server struct {
	Task        goav.Task
	Commands    []CommandSpec
	Pipeline    PipelineRegistry
	mu          sync.Mutex
	attachments map[string]goav.Attachment
}

// ServerOption configures one control server. Custom command and pipeline
// extension points stay explicit and scoped to the server instance.
type ServerOption func(*Server)

// WithCommands appends application-specific controls to the built-in allowlist.
func WithCommands(commands ...CommandSpec) ServerOption {
	return func(server *Server) {
		server.Commands = append(server.Commands, commands...)
	}
}

// WithPipelineRegistry installs application-specific branch-pipeline steps and
// encoders for attach/rebranch parsing.
func WithPipelineRegistry(registry PipelineRegistry) ServerOption {
	return func(server *Server) {
		server.Pipeline = registry
	}
}

// WithCapabilities installs a whole host-owned capability set on one server.
// Existing WithCommands and WithPipelineRegistry callers remain supported.
func WithCapabilities(caps CapabilitySet) ServerOption {
	return func(server *Server) {
		server.Commands = append(server.Commands, caps.Commands...)
		server.Pipeline.Steps = append(server.Pipeline.Steps, caps.Pipeline.Steps...)
		server.Pipeline.Encoders = append(server.Pipeline.Encoders, caps.Pipeline.Encoders...)
	}
}

func (s *Server) Handle(ctx context.Context, request Request) Response {
	if s.Task == nil {
		return ErrorResponse(request.Op, commandError("task_missing", request.Op, "", "control server has no task", nil, nil, nil))
	}
	if err := s.validateConfig(); err != nil {
		return ErrorResponse(request.Op, err)
	}
	response, err := s.execute(ctx, request)
	if err != nil {
		return ErrorResponse(request.Op, err)
	}
	return SuccessResponse(response.Result)
}

func (s *Server) execute(ctx context.Context, request Request) (ControlResponse, error) {
	switch request.Op {
	case "help":
		return s.help(request)
	case "capabilities":
		return s.capabilities()
	case "control":
		return s.control(ctx, request)
	case "control_raw":
		return executeRawControl(ctx, s.Task, request.Control)
	case "attach":
		return s.attach(ctx, request)
	case "rebranch":
		return s.rebranch(ctx, request)
	case "detach":
		return s.detach(ctx, request.Branch)
	default:
		return executeRequest(ctx, s.Task, request)
	}
}

func (s *Server) capabilities() (ControlResponse, error) {
	report, err := capabilityReport(s.commandManifest(), s.Pipeline, s.Task)
	if err != nil {
		return ControlResponse{}, err
	}
	return ControlResponse{Operation: "capabilities", Result: report}, nil
}

func (s *Server) help(request Request) (ControlResponse, error) {
	text, err := helpWithRuntime(helpArgsFromRequest(request), s.commandManifest(), s.Pipeline, s.Task)
	if err != nil {
		return ControlResponse{}, err
	}
	return ControlResponse{Operation: "help", Result: text}, nil
}

func (s *Server) control(ctx context.Context, request Request) (ControlResponse, error) {
	if request.Verb == "deliver" && len(request.Event) != 0 {
		return executeRawEvent(ctx, s.Task, request.Event, argsFromMap(request.Args))
	}
	if request.Verb == "" {
		return ControlResponse{}, commandError("missing_command", "control", "", "missing control verb", nil, []string{"use verb=bitrate"}, nil)
	}
	spec, ok := LookupCommand(s.commandManifest(), request.Verb)
	if !ok {
		return ControlResponse{}, commandError("unknown_command", "control", request.Verb, "unknown control command "+strconvQuote(request.Verb), nil, []string{"use one of: " + strings.Join(commandNames(s.commandManifest()), ", ")}, nil)
	}
	return Invoke(ctx, s.Task, spec, argsFromMap(request.Args))
}

func (s *Server) commandManifest() []CommandSpec {
	manifest := ControlManifest()
	if len(s.Commands) != 0 {
		manifest = append(manifest, s.Commands...)
	}
	return manifest
}

func (s *Server) validateConfig() error {
	if err := validateCommandManifest(s.commandManifest()); err != nil {
		return err
	}
	return validatePipelineRegistry(s.Pipeline)
}

func (s *Server) attach(ctx context.Context, request Request) (ControlResponse, error) {
	response, attachment, err := attachRequest(ctx, s.Task, request, s.Pipeline)
	if err != nil {
		return ControlResponse{}, err
	}
	s.storeAttachment(attachment)
	return response, nil
}

func (s *Server) rebranch(ctx context.Context, request Request) (ControlResponse, error) {
	if request.Branch == "" {
		return ControlResponse{}, commandError("missing_required", "rebranch", "branch", "rebranch needs a branch name", nil, []string{"use rebranch <branch-name> '<branch-pipeline>'"}, nil)
	}
	old, ok := s.attachment(request.Branch)
	if !ok {
		return ControlResponse{}, s.unknownBranchError("rebranch", request.Branch)
	}
	tap := firstNonEmpty(request.Tap, firstAnchorTap(old))
	if tap == "" {
		return ControlResponse{}, commandError(
			"unsupported_target",
			"rebranch",
			request.Branch,
			"cannot infer a replacement branch tap for this attachment",
			nil,
			[]string{"use the typed Attachment.Rebranch API for graph-node anchored branches"},
			nil,
		)
	}
	spec, err := parseBranchPipelineWithRegistry(s.Task, tap, request.Branch, request.Pipeline, s.Pipeline)
	if err != nil {
		return ControlResponse{}, err
	}
	options := []goav.RebranchOption{spec}
	switch request.Switch {
	case "":
	case "next_frame":
		options = append(options, goav.SwitchAt(goav.NextFrame()))
	case "next_keyframe":
		options = append(options, goav.SwitchAt(goav.NextKeyframe()))
	default:
		return ControlResponse{}, commandError("invalid_value", "rebranch", "switch", "switch must be next_frame or next_keyframe", []string{"value=" + request.Switch}, []string{"use --switch next_frame", "use --switch next_keyframe"}, nil)
	}
	if request.KeepOldOnFailure {
		options = append(options, goav.KeepOldOnFailure())
	}
	next, err := old.Rebranch(ctx, options...)
	if err != nil {
		return ControlResponse{}, structuredError("rebranch", err)
	}
	s.replaceAttachment(request.Branch, next)
	return ControlResponse{Operation: "rebranch", Result: next.Snapshot()}, nil
}

func (s *Server) detach(ctx context.Context, branch string) (ControlResponse, error) {
	if branch == "" {
		return ControlResponse{}, commandError("missing_required", "detach", "branch", "detach needs a branch name", nil, []string{"use detach <branch-name>"}, nil)
	}
	attachment, ok := s.attachment(branch)
	if !ok {
		return ControlResponse{}, s.unknownBranchError("detach", branch)
	}
	snapshot := attachment.Snapshot()
	if err := s.Task.Detach(ctx, attachment); err != nil {
		return ControlResponse{}, structuredError("detach", err)
	}
	s.deleteAttachment(branch)
	return ControlResponse{Operation: "detach", Result: snapshot}, nil
}

func (s *Server) storeAttachment(attachment goav.Attachment) {
	if attachment == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attachments == nil {
		s.attachments = make(map[string]goav.Attachment)
	}
	s.attachments[attachment.Name()] = attachment
}

func (s *Server) replaceAttachment(oldName string, next goav.Attachment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attachments == nil {
		s.attachments = make(map[string]goav.Attachment)
	}
	delete(s.attachments, oldName)
	if next != nil {
		s.attachments[next.Name()] = next
	}
}

func (s *Server) deleteAttachment(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.attachments, name)
}

func (s *Server) attachment(name string) (goav.Attachment, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	attachment, ok := s.attachments[name]
	return attachment, ok
}

func (s *Server) branchNames() []string {
	s.mu.Lock()
	names := make([]string, 0, len(s.attachments))
	seen := make(map[string]struct{}, len(s.attachments))
	for name := range s.attachments {
		if name == "" {
			continue
		}
		names = append(names, name)
		seen[name] = struct{}{}
	}
	s.mu.Unlock()
	for _, branch := range s.Task.Snapshot().Branches {
		if branch.Name == "" {
			continue
		}
		if _, ok := seen[branch.Name]; ok {
			continue
		}
		names = append(names, branch.Name)
	}
	sort.Strings(names)
	return names
}

func (s *Server) unknownBranchError(operation string, name string) error {
	available := s.branchNames()
	suggestions := []string{"run `goav ctl branches`"}
	if nearest := closest(name, available); nearest != "" {
		suggestions = append([]string{"use " + nearest}, suggestions...)
	}
	return commandError("unknown_branch", operation, name, "unknown branch "+strconvQuote(name), []string{"available_branches=" + strings.Join(available, ",")}, suggestions, nil)
}

// ServeUnix listens on unix://PATH or PATH and serves one JSON request per
// connection until ctx is cancelled.
func ServeUnix(ctx context.Context, task goav.Task, address string) error {
	return ServeUnixWithOptions(ctx, task, address)
}

// ServeUnixWithOptions listens on unix://PATH or PATH and serves one JSON
// request per connection until ctx is cancelled.
func ServeUnixWithOptions(ctx context.Context, task goav.Task, address string, options ...ServerOption) error {
	path := strings.TrimPrefix(address, "unix://")
	if path == "" {
		return commandError("invalid_address", "serve control", address, "unix control address needs a path", nil, []string{"use unix:///tmp/goav-live.sock"}, nil)
	}
	server := &Server{Task: task}
	for _, option := range options {
		if option != nil {
			option(server)
		}
	}
	if err := server.validateConfig(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(path)
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go server.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	var request Request
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(ErrorResponse("decode", commandError("invalid_json", "decode request", "", err.Error(), nil, nil, err)))
		return
	}
	if requestFollows(request) {
		s.handleFollow(ctx, conn, request)
		return
	}
	_ = json.NewEncoder(conn).Encode(s.Handle(ctx, request))
}

func (s *Server) handleFollow(ctx context.Context, conn net.Conn, request Request) {
	if s.Task == nil {
		_ = json.NewEncoder(conn).Encode(ErrorResponse(request.Op, commandError("task_missing", request.Op, "", "control server has no task", nil, nil, nil)))
		return
	}
	encoder := json.NewEncoder(conn)
	for event := range s.watch(request) {
		if err := encoder.Encode(SuccessResponse(event)); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (s *Server) watch(request Request) <-chan av.Event {
	if request.Op == "events" {
		return s.Task.Watch()
	}
	var filters []goav.EventFilter
	if typ := request.Args["type"]; typ != "" {
		filters = append(filters, goav.WatchTypes(av.EventType(typ)))
	}
	if stream := request.Args["stream"]; stream != "" {
		filters = append(filters, goav.WatchStream(av.StreamID(stream)))
	}
	return s.Task.Watch(filters...)
}

func requestFollows(request Request) bool {
	switch request.Op {
	case "events", "watch":
		return request.Args["follow"] == "true"
	default:
		return false
	}
}

func firstAnchorTap(attachment goav.Attachment) string {
	if attachment == nil {
		return ""
	}
	snapshot := attachment.Snapshot()
	if len(snapshot.AnchorTaps) != 0 {
		return snapshot.AnchorTaps[0]
	}
	return ""
}

func strconvQuote(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}
