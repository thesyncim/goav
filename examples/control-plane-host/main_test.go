package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/ctlserver"
)

// TestHost drives the host the way goav ctl does: a ctlserver.Client calls
// the built-in methods and the host's own, and attaches programs that use
// the host's elements.
func TestHost(t *testing.T) {
	// A short directory: a unix socket's path must fit in about 100 bytes.
	dir, err := os.MkdirTemp("", "host")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	address := "unix://" + filepath.Join(dir, "ctl.sock")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- newHost().serve(ctx, address, dir) }()
	client := dial(t, ctx, address)

	caps := call[ctlserver.Capabilities](t, client, "capabilities", nil)
	var names []string
	for _, m := range caps.Methods {
		names = append(names, m.Name)
	}
	for _, e := range caps.Elements {
		names = append(names, e.Name)
	}
	for _, want := range []string{"cue", "controls", "collected", "meter", "thumbnail", "memory", "acme"} {
		if !slices.Contains(names, want) {
			t.Fatalf("capabilities lack %q: %v", want, names)
		}
	}

	// Built-in controls and the host's cue reach the player; controls
	// reports what it received.
	call[struct{}](t, client, "seek", map[string]string{"position": "2s", "source": "player"})
	call[struct{}](t, client, "segment", map[string]string{"start": "1s", "end": "3s", "source": "player"})
	if cued := call[map[string]string](t, client, "cue", map[string]string{"name": "chorus", "offset": "500ms"}); cued["position"] != "42.5s" {
		t.Fatalf("cue = %v, want position 42.5s", cued)
	}
	seeks := call[[]received](t, client, "controls", map[string]string{"type": "seek"})
	if len(seeks) != 2 || seeks[0].Position != "2s" || seeks[1].Position != "42.5s" {
		t.Fatalf("seeks = %+v, want 2s then 42.5s", seeks)
	}

	var attachments []string
	for _, program := range []string{
		"tap live ! meter name=cam ! thumbnail every=5 name=thumbs",
		"tap live ! resize 160x90 ! encode vp8 ! as preview ! file preview.webm",
		"tap live ! acme bitrate=250k lookahead=deep ! as acme ! memory name=acme",
	} {
		attachments = append(attachments, call[ctlserver.Attached](t, client, "attach", map[string]string{"program": program}).Attachment)
	}
	eventually(t, "the attached programs to measure, keep, and encode", func() bool {
		r := call[report](t, client, "collected", nil)
		_, metered := r.Levels["cam"]
		return metered && r.Kept["thumbs"] > 0 && r.Kept["acme"] > 0 &&
			slices.Contains(r.Opened, "acme bitrate=250000 lookahead=deep")
	})

	// A bad setting is refused with the program position in the error's Data.
	_, err = client.Call[ctlserver.Attached](ctx, "attach", map[string]string{"program": "tap live ! thumbnail every=0"})
	var refused *ctlserver.Error
	var detail struct {
		Kind    string `json:"kind"`
		Column  int    `json:"column"`
		Message string `json:"message"`
	}
	if !errors.As(err, &refused) || json.Unmarshal(refused.Data, &detail) != nil ||
		detail.Kind != "program" || detail.Column != 12 || !strings.Contains(detail.Message, "every=0 keeps no frames") {
		t.Fatalf("bad thumbnail: err = %v, detail = %+v", err, detail)
	}

	next := call[ctlserver.Attached](t, client, "rebranch", map[string]string{
		"attachment": attachments[0], "program": "tap live ! thumbnail every=10 name=slow", "switch": "next-frame",
	})
	eventually(t, "the rebranched thumbnails", func() bool { return call[report](t, client, "collected", nil).Kept["slow"] > 0 })
	for _, name := range []string{next.Attachment, attachments[1], attachments[2]} {
		call[struct{}](t, client, "detach", map[string]string{"attachment": name})
	}
	if info, err := os.Stat(filepath.Join(dir, "preview.webm")); err != nil || info.Size() == 0 {
		t.Fatalf("preview.webm after detach: %v, %v", info, err)
	}

	call[struct{}](t, client, "stop", nil)
	if err := <-served; err != nil {
		t.Fatalf("serve after stop: %v", err)
	}
}

// call makes a call that must succeed.
func call[T any](t *testing.T, client *ctlserver.Client, method string, params any) T {
	t.Helper()
	out, err := client.Call[T](context.Background(), method, params)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return out
}

func dial(t *testing.T, ctx context.Context, address string) *ctlserver.Client {
	t.Helper()
	var client *ctlserver.Client
	eventually(t, "the host to listen", func() bool {
		client, _ = ctlserver.Dial(ctx, address)
		return client != nil
	})
	t.Cleanup(func() { client.Close() })
	return client
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !ok(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}
