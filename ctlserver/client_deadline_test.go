package ctlserver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

func pipeClient(t *testing.T) (*Client, net.Conn) {
	return pipeClientWithConn(t, func(c net.Conn) net.Conn { return c })
}

func pipeClientWithConn(t *testing.T, wrap func(net.Conn) net.Conn) (*Client, net.Conn) {
	t.Helper()
	conn, peer := net.Pipe()
	conn = wrap(conn)
	c := &Client{conn: conn, enc: json.NewEncoder(conn), pending: map[string]chan response{},
		watches: map[string]chan Event{}, early: map[string][]Event{}, done: make(chan struct{})}
	go c.read()
	t.Cleanup(func() { c.Close(); peer.Close() })
	return c, peer
}

func TestClientDeadlineBoundsWrites(t *testing.T) {
	for _, readRequest := range []bool{false, true} {
		name := "initial"
		if readRequest {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			c, peer := pipeClient(t)
			if readRequest {
				go func() { var req request; _ = json.NewDecoder(peer).Decode(&req) }()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := c.call(ctx, "blocked", nil); done <- err }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("call = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("deadline did not bound write")
			}
		})
	}
}

func TestClientCanceledQueuedWriteDoesNotInterruptActiveWriter(t *testing.T) {
	started := make(chan struct{})
	c, peer := pipeClientWithConn(t, func(conn net.Conn) net.Conn {
		return &writeStartedConn{Conn: conn, started: started}
	})
	first := make(chan error, 1)
	go func() { _, err := c.call(t.Context(), "first", nil); first <- err }()
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.call(ctx, "queued", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued = %v", err)
	}
	var req request
	if err := json.NewDecoder(peer).Decode(&req); err != nil {
		t.Fatal(err)
	}
	if req.Method != "first" {
		t.Fatalf("request = %s", req.Method)
	}
	if err := json.NewEncoder(peer).Encode(response{ID: req.ID, Result: json.RawMessage(`true`)}); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatalf("active writer interrupted: %v", err)
	}
}

type writeStartedConn struct {
	net.Conn
	started chan struct{}
}

func (c *writeStartedConn) Write(p []byte) (int, error) {
	select {
	case <-c.started:
	default:
		close(c.started)
	}
	return c.Conn.Write(p)
}

func TestClientWatchClosesLocallyBeforeBlockedUnwatch(t *testing.T) {
	c, peer := pipeClient(t)
	go func() {
		var req request
		if json.NewDecoder(peer).Decode(&req) == nil {
			_ = json.NewEncoder(peer).Encode(response{ID: req.ID, Result: json.RawMessage(`{"subscription":"one"}`)})
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	events, err := c.Watch(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(time.Second):
		t.Fatal("local watch cleanup waited for unwatch")
	}
}
