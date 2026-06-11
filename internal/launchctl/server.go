package launchctl

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"

	goav "github.com/thesyncim/goav"
)

// Server handles one decoded control-plane request against a task.
type Server struct {
	Task goav.Task
}

func (s Server) Handle(ctx context.Context, request Request) Response {
	if s.Task == nil {
		return ErrorResponse(request.Op, commandError("task_missing", request.Op, "", "control server has no task", nil, nil, nil))
	}
	return ExecuteRequest(ctx, s.Task, request)
}

// ServeUnix listens on unix://PATH or PATH and serves one JSON request per
// connection until ctx is cancelled.
func ServeUnix(ctx context.Context, task goav.Task, address string) error {
	path := strings.TrimPrefix(address, "unix://")
	if path == "" {
		return commandError("invalid_address", "serve control", address, "unix control address needs a path", nil, []string{"use unix:///tmp/goav-live.sock"}, nil)
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
	server := Server{Task: task}
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

func (s Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	var request Request
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(ErrorResponse("decode", commandError("invalid_json", "decode request", "", err.Error(), nil, nil, err)))
		return
	}
	_ = json.NewEncoder(conn).Encode(s.Handle(ctx, request))
}
