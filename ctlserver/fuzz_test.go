package ctlserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/launch"
)

// fuzzServer runs a live task behind a Server on a loopback listener.
func fuzzServer(f *testing.F) (*ctlserver.Server, string) {
	f.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.Cleanup(cancel)
	outs, err := launch.Compile(liveProgram)
	if err != nil {
		f.Fatal(err)
	}
	task, err := bundle.Build(ctx, outs...)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = task.Close() })
	go func() { _ = task.Run(ctx) }()
	server, err := ctlserver.New(task)
	if err != nil {
		f.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { os.RemoveAll(dir) })
	l, err := net.Listen("unix", filepath.Join(dir, "s.sock"))
	if err != nil {
		f.Fatal(err)
	}
	go func() { _ = server.Serve(ctx, l) }()
	return server, l.Addr().String()
}

// checkReply reports why raw is not a valid JSON-RPC 2.0 message from a
// server: a response carrying an id and exactly one of result or error, a
// batch of them, or a pushed notification.
func checkReply(raw []byte) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(raw, &batch); err != nil || len(batch) == 0 {
			return "batch is not a non-empty JSON array"
		}
		for _, one := range batch {
			if why := checkReply(one); why != "" {
				return why
			}
		}
		return ""
	}
	var msg struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return "not JSON: " + err.Error()
	}
	switch {
	case msg.JSONRPC != "2.0":
		return "jsonrpc is not 2.0"
	case msg.Method == "event" && len(msg.ID) == 0:
		return ""
	case len(msg.ID) == 0:
		return "response without an id"
	case (len(msg.Result) == 0) == (msg.Error == nil):
		return "response needs exactly one of result and error"
	case msg.Error != nil && (msg.Error.Code == 0 || msg.Error.Message == ""):
		return "error without a code and message"
	}
	return ""
}

var fuzzSeeds = []string{
	`{"jsonrpc":"2.0","id":1,"method":"taps"}`,
	`{"jsonrpc":"2.0","id":"a","method":"describe","params":{"format":"text"}}`,
	`{"jsonrpc":"2.0","id":2,"method":"keyframe","params":{"stream":"cam"}}`,
	`{"jsonrpc":"2.0","id":3,"method":"attach","params":{"program":"tap live ! resize 16x16 ! null"}}`,
	`{"jsonrpc":"2.0","method":"stats"}`,
	`{"jsonrpc":"2.0","id":4,"method":"nope"}`,
	`{"jsonrpc":"2.0","id":5,"method":"seek","params":{"to":"x"}}`,
	`[{"jsonrpc":"2.0","id":1,"method":"snapshot"},{"jsonrpc":"2.0","id":2,"method":"capabilities"}]`,
	`[]`,
	`{"jsonrpc":"1.0","id":1,"method":"taps"}`,
	`{"id":1`,
	``,
}

// FuzzHandler posts arbitrary bytes to the http.Handler: it must not panic,
// and any 200 answer must be valid JSON-RPC.
func FuzzHandler(f *testing.F) {
	server, _ := fuzzServer(f)
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
		switch rec.Code {
		case http.StatusOK:
			if why := checkReply(rec.Body.Bytes()); why != "" {
				t.Fatalf("%s: %q", why, rec.Body.Bytes())
			}
		case http.StatusNoContent, http.StatusBadRequest:
		default:
			t.Fatalf("status %d", rec.Code)
		}
	})
}

// FuzzConnection writes arbitrary bytes down a client connection and reads
// everything the server answers: each value must be valid JSON-RPC.
func FuzzConnection(f *testing.F) {
	_, address := fuzzServer(f)
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
		f.Add([]byte(s + "\n" + s))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		conn, err := net.Dial("unix", address)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(input); err != nil {
			return
		}
		_ = conn.(*net.UnixConn).CloseWrite()
		dec := json.NewDecoder(conn)
		for {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				if err != io.EOF && !isClosed(err) {
					t.Fatalf("reply is not JSON: %v", err)
				}
				return
			}
			if why := checkReply(raw); why != "" {
				t.Fatalf("%s: %q", why, raw)
			}
		}
	})
}

// isClosed reports a connection the server ended: it hangs up after a parse
// error, and unread input makes that a reset rather than an EOF.
func isClosed(err error) bool {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}
