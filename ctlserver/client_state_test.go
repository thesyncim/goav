package ctlserver

import (
	"context"
	"net"
	"testing"
)

func TestClientCloseImmediatelyRejectsCallsAndWatches(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()

	// Do not start c.read() so c.err stays nil and c.done stays open,
	// exercising the window between Client.Close() and read() exit.
	client := &Client{
		conn:    c1,
		pending: map[string]chan response{},
		watches: map[string]chan Event{},
		early:   map[string][]Event{},
		done:    make(chan struct{}),
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if _, err := client.call(context.Background(), "stats", nil); err == nil {
		t.Fatal("call() after Close() = nil, want connection-closed error")
	}
	if _, err := client.Watch(context.Background(), nil); err == nil {
		t.Fatal("Watch() after Close() = nil, want connection-closed error")
	}
}
