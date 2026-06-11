package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/ctl"
)

func TestRunHostServesCustomHelpAndAttach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := filepath.Join(os.TempDir(), "goav-control-plane-host-test-"+time.Now().Format("150405.000000000")+".sock")
	t.Cleanup(func() { _ = os.Remove(socket) })

	errC := make(chan error, 1)
	go func() {
		errC <- runHost(ctx, "unix://"+socket, io.Discard)
	}()
	if err := waitForHostSocket(ctx, "unix://"+socket, errC); err != nil {
		t.Fatal(err)
	}

	help := sendDemoRequest(t, socket, ctl.Request{
		Op:   "help",
		Args: map[string]string{"topic": "attach"},
	})
	text, ok := help.Result.(string)
	if !help.OK || help.Error != nil || !ok {
		t.Fatalf("help = %+v", help)
	}
	for _, fragment := range []string{"meter [label=<text>]", "acmeenc bitrate=<bps>", "aliases: levelmeter", "aliases: acme"} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, text)
		}
	}

	out := filepath.Join(t.TempDir(), "archive copy.ogg")
	attach := sendDemoRequest(t, socket, ctl.Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "archive",
		Pipeline: `meter label="left ! right" ! acmeenc bitrate=64k quality=voice lookahead=deep ! filesink location="` +
			out + `" format=ogg`,
	})
	if !attach.OK || attach.Error != nil {
		t.Fatalf("attach = %+v", attach)
	}

	cancel()
	select {
	case err := <-errC:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("host did not stop")
	}
}

func sendDemoRequest(t *testing.T, socket string, request ctl.Request) ctl.Response {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response ctl.Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}
