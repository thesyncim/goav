package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
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

func TestDemoHostHelpers(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
	}{
		{value: "64000", want: 64000},
		{value: "64k", want: 64000},
		{value: "2M", want: 2_000_000},
	} {
		got, err := parseDemoRate(tc.value)
		if err != nil {
			t.Fatalf("parseDemoRate(%q): %v", tc.value, err)
		}
		if got != tc.want {
			t.Fatalf("parseDemoRate(%q) = %d, want %d", tc.value, got, tc.want)
		}
	}
	for _, value := range []string{"", "0", "nope"} {
		if got, err := parseDemoRate(value); err == nil || got != 0 {
			t.Fatalf("parseDemoRate(%q) = %d, %v; want error", value, got, err)
		}
	}
	if got := firstNonEmpty("", "fallback"); got != "fallback" {
		t.Fatalf("firstNonEmpty = %q", got)
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("firstNonEmpty empty = %q", got)
	}

	factory := &demoEncoderFactory{
		descriptor: codec.Descriptor{ID: demoCodec, Type: av.MediaAudio},
	}
	var nativeSeen bool
	settings := codec.CodecSettings{}
	codec.Control(func(native any) error {
		_, nativeSeen = native.(*demoNativeOptions)
		return nil
	})(&settings)
	encoder, err := factory.NewEncoder(context.Background(), codec.EncodeConfig{Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	if !nativeSeen {
		t.Fatal("native options callback was not called")
	}
	if encoder.Descriptor().ID != demoCodec {
		t.Fatalf("descriptor = %+v", encoder.Descriptor())
	}
	if err := encoder.Open(context.Background(), codec.EncodeConfig{}); err != nil {
		t.Fatal(err)
	}
	result := codec.EncodeResult{Packets: make([]av.Packet, 0, 1)}
	err = encoder.EncodeInto(context.Background(), &av.Frame{StreamID: "audio", Type: av.MediaAudio}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 || result.Packets[0].StreamID != "audio" || result.Packets[0].Type != av.MediaAudio {
		t.Fatalf("packets = %+v", result.Packets)
	}
	full := codec.EncodeResult{Packets: make([]av.Packet, 1, 1)}
	if err := encoder.EncodeInto(context.Background(), nil, &full); err != nil {
		t.Fatal(err)
	}
	if err := encoder.EncodeInto(context.Background(), &av.Frame{StreamID: "audio", Type: av.MediaAudio}, &full); err != nil {
		t.Fatal(err)
	}
	if err := encoder.FlushInto(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	if err := encoder.HandleEvent(context.Background(), &av.Event{Type: av.EventStats}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDemoHostShutdownHelpers(t *testing.T) {
	boom := fmt.Errorf("boom")
	errC := make(chan error, 1)
	errC <- boom
	if err := waitForHostSocket(context.Background(), "unix:///tmp/goav-missing.sock", errC); err != boom {
		t.Fatalf("waitForHostSocket error = %v, want boom", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForHostSocket(canceled, "unix:///tmp/goav-missing.sock", make(chan error)); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForHostSocket canceled = %v", err)
	}

	drainC := make(chan error, 2)
	drainC <- nil
	drainC <- boom
	if err := drainHost(context.Background(), drainC); err != boom {
		t.Fatalf("drainHost error = %v, want boom", err)
	}

	timeout, stop := context.WithTimeout(context.Background(), time.Nanosecond)
	defer stop()
	if err := drainHost(timeout, make(chan error)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drainHost timeout = %v", err)
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
