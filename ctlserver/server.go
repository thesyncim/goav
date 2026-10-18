package ctlserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/inspect"
	"github.com/thesyncim/goav/internal/spelling"
	"github.com/thesyncim/goav/launch"
)

// Server serves one running task's control methods to clients. It does no
// work on the task's data plane: a call runs on its connection's goroutine,
// and a watch is an inspect subscription that sheds events rather than
// block media when its client falls behind.
type Server struct {
	task     goav.LiveTask
	files    launch.FileSystem
	elements []launch.Element
	methods  map[string]*method

	mu          sync.Mutex
	attachments map[string]goav.Attachment
	nextWatch   atomic.Uint64
}

type method struct {
	name    string
	summary string
	schema  paramSchema
	call    func(ctx context.Context, s *Server, sess *session, params reflect.Value) (any, error)
}

// Option configures a Server.
type Option func(*Server) error

// New builds a server for a running task.
func New(task goav.LiveTask, options ...Option) (*Server, error) {
	if task == nil {
		return nil, errors.New("ctlserver: no task to serve")
	}
	s := &Server{task: task, methods: map[string]*method{}, attachments: map[string]goav.Attachment{}}
	for _, m := range builtinMethods() {
		s.methods[m.name] = m
	}
	for _, opt := range options {
		if opt == nil {
			continue
		}
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Files lets programs attached through the server read and write files
// through fs. Without it, attached programs touch no files; a host serving
// untrusted clients grants a launch.Dir.
func Files(fs launch.FileSystem) Option {
	return func(s *Server) error { s.files = fs; return nil }
}

// Elements adds a host's own elements to the programs clients attach.
func Elements(elements ...launch.Element) Option {
	return func(s *Server) error { s.elements = append(s.elements, elements...); return nil }
}

// Method adds a host method. Its params are T's fields, named by their json
// tags; `ctl:"required"` marks a required one and `help:"..."` describes it.
// A client's call runs fn and returns its result as JSON.
func Method[T any](name, summary string, fn func(context.Context, goav.LiveTask, T) (any, error)) Option {
	return func(s *Server) error {
		if name == "" || strings.ContainsAny(name, " \t\n") {
			return fmt.Errorf("ctlserver: method name %q is not one word", name)
		}
		if _, taken := s.methods[name]; taken {
			return fmt.Errorf("ctlserver: method %q is already defined", name)
		}
		m, err := newMethod(name, summary, func(ctx context.Context, s *Server, _ *session, p T) (any, error) {
			return fn(ctx, s.task, p)
		})
		if err != nil {
			return err
		}
		s.methods[name] = m
		return nil
	}
}

// ListenAndServe serves clients at address — unix:///path/to.sock or
// tcp://host:port — until ctx ends. A unix socket is owner-only from the
// moment it exists; ListenAndServe replaces a stale one, and refuses a path
// another server is serving or that is not a socket.
func (s *Server) ListenAndServe(ctx context.Context, address string) error {
	network, addr, err := splitAddress(address)
	if err != nil {
		return err
	}
	if network != "unix" {
		l, err := (&net.ListenConfig{}).Listen(ctx, network, addr)
		if err != nil {
			return err
		}
		return s.Serve(ctx, l)
	}
	l, err := listenUnix(ctx, addr)
	if err != nil {
		return err
	}
	defer os.Remove(addr)
	return s.Serve(ctx, l)
}

// listenUnix creates the socket in a private directory, makes it
// owner-only, and only then moves it to path, so no other user can connect
// in between.
func listenUnix(ctx context.Context, path string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("ctlserver: %s exists and is not a socket", path)
		}
		if c, err := net.DialTimeout("unix", path, time.Second); err == nil {
			c.Close()
			return nil, fmt.Errorf("ctlserver: %s is already being served", path)
		}
		_ = os.Remove(path) // stale: its server is gone
	}
	dir, err := os.MkdirTemp(filepath.Dir(path), ".goav-ctl-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	tmp := filepath.Join(dir, "s")
	l, err := (&net.ListenConfig{}).Listen(ctx, "unix", tmp)
	if err != nil {
		return nil, err
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := os.Chmod(tmp, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

// Serve serves clients on l until ctx ends, then closes l and waits for
// each connection's calls in flight to answer.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	stop := context.AfterFunc(ctx, func() { l.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
}

func splitAddress(address string) (string, string, error) {
	switch {
	case strings.HasPrefix(address, "unix://"):
		if path := strings.TrimPrefix(address, "unix://"); path != "" {
			return "unix", path, nil
		}
	case strings.HasPrefix(address, "tcp://"):
		if hostport := strings.TrimPrefix(address, "tcp://"); hostport != "" {
			return "tcp", hostport, nil
		}
	}
	return "", "", fmt.Errorf("ctlserver: address %q is not unix:///path or tcp://host:port", address)
}

// session is one client connection: its writes, its watches, and its calls
// in flight, by id, for $/cancelRequest.
type session struct {
	mu       sync.Mutex
	enc      *json.Encoder
	watches  map[string]inspect.Subscription
	inflight map[string]context.CancelFunc
}

// cancelMethod is the notification a client sends to cancel a call in
// flight: {"method":"$/cancelRequest","params":{"id":7}}.
const cancelMethod = "$/cancelRequest"

// begin tracks a call so a cancel can reach it; end forgets it.
func (sess *session) begin(id string, cancel context.CancelFunc) {
	sess.mu.Lock()
	sess.inflight[id] = cancel
	sess.mu.Unlock()
}

func (sess *session) end(id string) {
	sess.mu.Lock()
	delete(sess.inflight, id)
	sess.mu.Unlock()
}

func (sess *session) cancel(params json.RawMessage) {
	var p struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(params, &p) != nil {
		return
	}
	sess.mu.Lock()
	cancel := sess.inflight[idKey(p.ID)]
	sess.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// idKey spells a request id one way, so a cancel finds its call.
func idKey(id json.RawMessage) string {
	var b bytes.Buffer
	if json.Compact(&b, id) != nil {
		return string(id)
	}
	return b.String()
}

func (sess *session) send(v any) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	_ = sess.enc.Encode(v)
}

func (sess *session) close() {
	sess.mu.Lock()
	watches := sess.watches
	sess.watches = nil
	sess.mu.Unlock()
	for _, w := range watches {
		w.Close()
	}
}

// maxInFlight bounds one connection's concurrent calls. Past it the server
// stops reading the connection, so a flooding client is backpressured, not
// buffered.
const maxInFlight = 32

// nullID answers a request whose id could not be read.
var nullID = json.RawMessage("null")

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	// On shutdown, stop reading and give the last answers a second to go out.
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetReadDeadline(time.Now())
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	})
	defer stop()
	sess := &session{enc: json.NewEncoder(conn), watches: map[string]inspect.Subscription{}, inflight: map[string]context.CancelFunc{}}
	defer sess.close()
	var calls sync.WaitGroup
	defer calls.Wait()
	// A client that hangs up cancels its calls in flight.
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	slots := make(chan struct{}, maxInFlight)
	dec := json.NewDecoder(conn)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() == nil {
				sess.send(response{JSONRPC: "2.0", ID: nullID, Error: &Error{Code: CodeParse, Message: "malformed JSON"}})
			}
			return
		}
		slots <- struct{}{}
		calls.Add(1)
		go func() {
			defer func() { <-slots; calls.Done() }()
			if reply, ok := s.handleMessage(callCtx, sess, raw); ok {
				sess.send(reply)
			}
		}()
	}
}

// handleMessage answers one message: a request, or a batch of them run in
// order. It reports false when nothing is owed: a notification, or a batch
// of them.
func (s *Server) handleMessage(ctx context.Context, sess *session, raw json.RawMessage) (any, bool) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return s.handle(ctx, sess, raw)
	}
	var batch []json.RawMessage
	if err := json.Unmarshal(raw, &batch); err != nil || len(batch) == 0 {
		return response{JSONRPC: "2.0", ID: nullID, Error: &Error{Code: CodeInvalidRequest, Message: "a batch is a non-empty array of requests"}}, true
	}
	var replies []response
	for _, one := range batch {
		if reply, ok := s.handle(ctx, sess, one); ok {
			replies = append(replies, reply)
		}
	}
	return replies, len(replies) > 0
}

// handle runs one request. It reports false for a notification, which gets
// no response.
func (s *Server) handle(ctx context.Context, sess *session, raw json.RawMessage) (response, bool) {
	var req request
	if err := json.Unmarshal(raw, &req); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
		id := req.ID
		if len(id) == 0 {
			id = nullID
		}
		return response{JSONRPC: "2.0", ID: id, Error: &Error{Code: CodeInvalidRequest, Message: "a request is {\"jsonrpc\":\"2.0\",\"id\":...,\"method\":...,\"params\":{...}}"}}, true
	}
	if req.Method == cancelMethod {
		if sess != nil {
			sess.cancel(req.Params)
		}
		return response{}, false
	}
	if sess != nil && len(req.ID) != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		key := idKey(req.ID)
		sess.begin(key, cancel)
		defer sess.end(key)
	}
	result, err := s.call(ctx, sess, req.Method, req.Params)
	if len(req.ID) == 0 {
		return response{}, false
	}
	resp := response{JSONRPC: "2.0", ID: req.ID}
	if err != nil {
		resp.Error = refusal(err)
		return resp, true
	}
	data, merr := json.Marshal(result)
	if merr != nil {
		resp.Error = &Error{Code: CodeRefused, Message: "result is not JSON: " + merr.Error()}
		return resp, true
	}
	resp.Result = data
	return resp, true
}

func (s *Server) call(ctx context.Context, sess *session, name string, params json.RawMessage) (any, error) {
	m, ok := s.methods[name]
	if !ok {
		msg := fmt.Sprintf("unknown method %q", name)
		if hint := spelling.Nearest(name, s.methodNames()); hint != "" {
			msg += fmt.Sprintf("; did you mean %q?", hint)
		}
		return nil, &Error{Code: CodeMethodNotFound, Message: msg}
	}
	args, perr := m.schema.bind(params)
	if perr != nil {
		perr.Message = name + ": " + perr.Message
		return nil, perr
	}
	return m.call(ctx, s, sess, args)
}

func (s *Server) methodNames() []string {
	names := make([]string, 0, len(s.methods))
	for name := range s.methods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Event is a task event as the wire carries it. Track describes the stream
// a stream_added or codec_changed event announces.
type Event struct {
	Type     av.EventType `json:"type"`
	Stream   av.StreamID  `json:"stream,omitempty"`
	Epoch    av.Epoch     `json:"epoch,omitempty"`
	At       time.Time    `json:"at,omitzero"`
	Position string       `json:"position,omitempty"`
	Track    *Track       `json:"track,omitempty"`
	Reason   string       `json:"reason,omitempty"`
	Cause    string       `json:"cause,omitempty"`
	Metadata av.Metadata  `json:"metadata,omitempty"`
}

// Track is a stream's identity and shape, as an event announces it.
type Track struct {
	Index      int          `json:"index"`
	Type       av.MediaType `json:"type"`
	Codec      av.CodecID   `json:"codec"`
	Profile    string       `json:"profile,omitempty"`
	Width      int          `json:"width,omitempty"`
	Height     int          `json:"height,omitempty"`
	SampleRate int          `json:"sampleRate,omitempty"`
	Channels   int          `json:"channels,omitempty"`
	Language   string       `json:"language,omitempty"`
	Name       string       `json:"name,omitempty"`
}

func wireEvent(e av.Event) Event {
	out := Event{Type: e.Type, Stream: e.StreamID, Epoch: e.Epoch, At: e.At, Reason: e.Reason, Metadata: e.Metadata}
	if d, ok := e.Timestamp.ToDuration(); ok && e.Timestamp.Base.Valid() {
		out.Position = d.String()
	}
	if e.Cause != nil {
		out.Cause = e.Cause.Error()
	}
	codec := e.Codec
	if e.Stream != nil {
		out.Track = &Track{Index: e.Stream.Index, Type: e.Stream.Type, Language: e.Stream.Language, Name: e.Stream.Name}
		if codec == nil {
			codec = &e.Stream.Codec
		}
	}
	if codec != nil {
		if out.Track == nil {
			out.Track = &Track{Index: -1, Type: codec.Type}
		}
		out.Track.Codec, out.Track.Profile = codec.ID, codec.Profile
		out.Track.Width, out.Track.Height = codec.Width, codec.Height
		out.Track.SampleRate, out.Track.Channels = codec.SampleRate, codec.Channels
	}
	return out
}
