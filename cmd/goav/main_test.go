package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/goavtest"
)

func TestCLIInvokesCustomControlCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type customCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom CLI value"`
	}
	var applied string
	command := ctl.CommandSpec{
		Name:     "vendor.cli",
		Summary:  "custom CLI command",
		ArgsType: reflect.TypeOf(customCommand{}),
		Apply: func(_ context.Context, _ goav.Task, args any) (ctl.ControlResponse, error) {
			applied = args.(customCommand).Value
			return ctl.ControlResponse{Operation: "control vendor.cli", Result: applied}, nil
		},
	}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket, ctl.WithCommands(command))
	}()
	waitForCLISocket(t, socket, errC)

	cmd := exec.Command("go", "run", ".", "ctl", "--control", "unix://"+socket, "control", "vendor.cli", "value=via-cli")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goav ctl failed: %v\n%s", err, output)
	}
	if applied != "via-cli" || !strings.Contains(string(output), `"via-cli"`) {
		t.Fatalf("applied=%q output=%s", applied, output)
	}
}

func TestCLIPrintsGraphAsRawText(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-cli-graph-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket)
	}()
	waitForCLISocket(t, socket, errC)

	cmd := exec.Command("go", "run", ".", "ctl", "--control", "unix://"+socket, "graph")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("goav ctl graph failed: %v\n%s", err, output)
	}
	text := string(output)
	if !strings.HasPrefix(text, "flowchart LR\n") || strings.HasPrefix(text, `"`) {
		t.Fatalf("graph output = %q", text)
	}
}

func TestParseCtlArgs(t *testing.T) {
	control, args, err := parseCtlArgs([]string{"--control", "unix:///tmp/live.sock", "graph", "format=dot"})
	if err != nil {
		t.Fatal(err)
	}
	if control != "unix:///tmp/live.sock" || strings.Join(args, " ") != "graph format=dot" {
		t.Fatalf("control=%q args=%v", control, args)
	}

	control, args, err = parseCtlArgs([]string{"--control=unix:///tmp/live.sock", "help", "graph"})
	if err != nil {
		t.Fatal(err)
	}
	if control != "unix:///tmp/live.sock" || strings.Join(args, " ") != "help graph" {
		t.Fatalf("control=%q args=%v", control, args)
	}

	if _, _, err := parseCtlArgs([]string{"--control"}); err == nil {
		t.Fatal("expected missing control value error")
	}
}

func TestRawTextRequests(t *testing.T) {
	for _, op := range []string{"help", "graph", "flowchart"} {
		if !rawText(ctl.Request{Op: op}) {
			t.Fatalf("%s should be raw text", op)
		}
	}
	if rawText(ctl.Request{Op: "control"}) {
		t.Fatal("control response should stay JSON encoded")
	}
}

func TestSendRejectsUnsupportedAddress(t *testing.T) {
	err := send("tcp://127.0.0.1:9", ctl.Request{Op: "graph"})
	if err == nil || !strings.Contains(err.Error(), "expected unix://PATH") {
		t.Fatalf("send error = %v", err)
	}
}

func TestSendReturnsStructuredError(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-error-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := serveOneShot(t, socket, func(conn net.Conn) error {
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			return err
		}
		return json.NewEncoder(conn).Encode(ctl.ErrorResponse(request.Op, fmt.Errorf("boom")))
	})

	err := send("unix://"+socket, ctl.Request{Op: "graph"})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("send error = %v", err)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestSendRawTextRejectsNonStringResult(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-raw-type-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := serveOneShot(t, socket, func(conn net.Conn) error {
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			return err
		}
		return json.NewEncoder(conn).Encode(ctl.SuccessResponse(12))
	})

	err := send("unix://"+socket, ctl.Request{Op: "graph"})
	if err == nil || !strings.Contains(err.Error(), "want text") {
		t.Fatalf("send error = %v", err)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestSendFollowReturnsDecodeError(t *testing.T) {
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-follow-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	errC := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errC <- err
			return
		}
		defer conn.Close()
		var request ctl.Request
		if err := json.NewDecoder(conn).Decode(&request); err != nil {
			errC <- err
			return
		}
		if request.Op != "watch" || request.Args["follow"] != "true" {
			errC <- fmt.Errorf("request = %+v", request)
			return
		}
		if _, err := fmt.Fprintln(conn, `{"ok":true,"result":{"type":"stats"}}`); err != nil {
			errC <- err
			return
		}
		if _, err := fmt.Fprint(conn, `{"ok":`); err != nil {
			errC <- err
			return
		}
		errC <- nil
	}()

	err = send("unix://"+socket, ctl.Request{Op: "watch", Args: map[string]string{"follow": "true"}})
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("send error = %v, want unexpected EOF", err)
	}
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func serveOneShot(t *testing.T, socket string, handle func(net.Conn) error) <-chan error {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	errC := make(chan error, 1)
	go func() {
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			errC <- err
			return
		}
		defer conn.Close()
		errC <- handle(conn)
	}()
	return errC
}

func waitForCLISocket(t *testing.T, socket string, errC <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case err := <-errC:
			t.Fatalf("server stopped before creating socket: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not ready", socket)
		}
		time.Sleep(time.Millisecond)
	}
}
