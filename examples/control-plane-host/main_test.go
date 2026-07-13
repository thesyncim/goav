package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/pipeline"
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

	help := sendDemoRequest(t, socket, ctlserver.Request{
		Op:   "help",
		Args: map[string]string{"topic": "attach"},
	})
	text, ok := help.Result.(string)
	if !help.OK || help.Error != nil || !ok {
		t.Fatalf("help = %+v", help)
	}
	for _, fragment := range []string{
		"meter [label=<text>]",
		"thumbnail every=<positive-int> [label=<text>]",
		"memorysink [name=<text>]",
		"acmeenc bitrate=<rate>",
		"aliases: levelmeter",
		"aliases: thumbs, sampleframes",
		"aliases: memsink",
		"aliases: acme",
		"Runtime encoders:",
		"encode codec=x_acme_video media=video",
		"ACME demo video",
		"Any encoder registered on the task runtime is callable",
		"Runtime muxers:",
		"filesink location=<path> [format=webm]",
		"Any muxer registered on the task runtime is callable",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, text)
		}
	}

	for _, request := range []ctlserver.Request{
		{Op: "control", Verb: "rate", Args: map[string]string{"value": "0.5", "source": "fixture"}},
		{Op: "control", Verb: "seek", Args: map[string]string{"position": "2s", "source": "fixture"}},
		{Op: "control", Verb: "segment", Args: map[string]string{"start": "1s", "end": "3s", "source": "fixture"}},
	} {
		response := sendDemoRequest(t, socket, request)
		if !response.OK || response.Error != nil {
			t.Fatalf("source control %s = %+v", request.Verb, response)
		}
	}
	controls := sendDemoRequest(t, socket, ctlserver.Request{Op: "control", Verb: "fixture.controls"})
	controlMap := responseMap(t, controls)
	if got := int(controlMap["count"].(float64)); got != 3 {
		t.Fatalf("fixture.controls count = %d, want 3; response=%+v", got, controls)
	}
	rateControls := sendDemoRequest(t, socket, ctlserver.Request{
		Op:   "control",
		Verb: "fixture.controls",
		Args: map[string]string{"type": string(av.EventRate)},
	})
	rateMap := responseMap(t, rateControls)
	if got := int(rateMap["count"].(float64)); got != 1 {
		t.Fatalf("fixture.controls type=rate count = %d, want 1; response=%+v", got, rateControls)
	}

	out := filepath.Join(t.TempDir(), "archive copy.webm")
	attach := sendDemoRequest(t, socket, ctlserver.Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "archive",
		Pipeline: `meter label="left ! right" ! resize width=640 height=360 ! encode codec=vp8 media=video bitrate=900k fps=30 keyframe_interval=30 ! filesink location="` +
			out + `" format=webm`,
	})
	if !attach.OK || attach.Error != nil {
		t.Fatalf("attach = %+v", attach)
	}

	thumbOut := filepath.Join(t.TempDir(), "thumbnails.ivf")
	thumbs := sendDemoRequest(t, socket, ctlserver.Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "thumbnails",
		Pipeline: `thumbnail every=5 label=sample ! resize width=160 height=90 ! encode codec=vp8 media=video bitrate=160k fps=1 keyframe_interval=1 ! filesink location="` +
			thumbOut + `"`,
	})
	if !thumbs.OK || thumbs.Error != nil {
		t.Fatalf("thumbnail attach = %+v", thumbs)
	}

	memory := sendDemoRequest(t, socket, ctlserver.Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "memory",
		Pipeline: `thumbnail every=3 label=preview ! memorysink name=preview`,
	})
	if !memory.OK || memory.Error != nil {
		t.Fatalf("memory attach = %+v", memory)
	}

	generic := sendDemoRequest(t, socket, ctlserver.Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "acme-generic",
		Pipeline: `thumbnail every=4 label=generic ! encode codec=x_acme_video media=video bitrate=220k profile=preview fps=2 keyframe_interval=1 lookahead=deep ! memorysink name=acme-generic`,
	})
	if !generic.OK || generic.Error != nil {
		t.Fatalf("generic custom encoder attach = %+v", generic)
	}

	genericFileOut := filepath.Join(t.TempDir(), "acme generic.webm")
	genericFile := sendDemoRequest(t, socket, ctlserver.Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "acme-file",
		Pipeline: `thumbnail every=6 label=file ! encode codec=x_acme_video media=video bitrate=320k profile=file fps=2 keyframe_interval=1 lookahead=file ! filesink location="` +
			genericFileOut + `"`,
	})
	if !genericFile.OK || genericFile.Error != nil {
		t.Fatalf("generic custom encoder file attach = %+v", genericFile)
	}

	custom := sendDemoRequest(t, socket, ctlserver.Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "acme-preview",
		Pipeline: `thumbnail every=2 label=acme ! acmeenc bitrate=250k quality=preview lookahead=shallow ! memorysink name=acme-preview`,
	})
	if !custom.OK || custom.Error != nil {
		t.Fatalf("custom encoder attach = %+v", custom)
	}

	badThumb := sendDemoRequest(t, socket, ctlserver.Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "bad-thumb",
		Pipeline: `thumbnail every=0 ! memorysink name=bad`,
	})
	if badThumb.OK || badThumb.Error == nil || badThumb.Error.Code != "invalid_value" || badThumb.Error.Node != "every" {
		t.Fatalf("bad thumbnail response = %+v", badThumb)
	}

	graph := sendDemoRequest(t, socket, ctlserver.Request{Op: "graph"})
	flowchart, ok := graph.Result.(string)
	if !graph.OK || graph.Error != nil || !ok {
		t.Fatalf("graph = %+v", graph)
	}
	for _, fragment := range []string{
		"flowchart LR",
		"branch=archive (attached)",
		"branch=thumbnails (attached)",
		"branch=memory (attached)",
		"branch=acme-generic (attached)",
		"branch=acme-file (attached)",
		"branch=acme-preview (attached)",
		"demo-left ! right",
		"demo-thumbnail-preview",
		"demo-thumbnail-generic",
		"demo-thumbnail-file",
	} {
		if !strings.Contains(flowchart, fragment) {
			t.Fatalf("flowchart missing %q:\n%s", fragment, flowchart)
		}
	}

	rebranch := sendDemoRequest(t, socket, ctlserver.Request{
		Op:       "rebranch",
		Branch:   "memory",
		Pipeline: `thumbnail every=10 label=slow ! memorysink name=slow-preview`,
	})
	if !rebranch.OK || rebranch.Error != nil {
		t.Fatalf("rebranch = %+v", rebranch)
	}

	for _, branch := range []string{"archive", "thumbnails", "memory", "acme-generic", "acme-file", "acme-preview"} {
		detach := sendDemoRequest(t, socket, ctlserver.Request{Op: "detach", Branch: branch})
		if !detach.OK || detach.Error != nil {
			t.Fatalf("detach %s = %+v", branch, detach)
		}
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

func TestRunHostAcceptsDocumentedCLICommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	socket := filepath.Join(os.TempDir(), "goav-control-plane-host-cli-"+time.Now().Format("150405.000000000")+".sock")
	t.Cleanup(func() { _ = os.Remove(socket) })

	errC := make(chan error, 1)
	go func() {
		errC <- runHost(ctx, "unix://"+socket, io.Discard)
	}()
	if err := waitForHostSocket(ctx, "unix://"+socket, errC); err != nil {
		t.Fatal(err)
	}

	attachHelp := runDemoCLI(t, socket, "help", "attach")
	for _, fragment := range []string{
		"thumbnail every=<positive-int> [label=<text>]",
		"memorysink [name=<text>]",
		"acmeenc bitrate=<rate>",
		"encode codec=x_acme_video media=video",
		"filesink location=<path> [format=webm]",
	} {
		if !strings.Contains(attachHelp, fragment) {
			t.Fatalf("attach help missing %q:\n%s", fragment, attachHelp)
		}
	}
	controlHelp := runDemoCLI(t, socket, "help", "control", "fixture.controls")
	if !strings.Contains(controlHelp, "controls recorded by the fixture test source") ||
		!strings.Contains(controlHelp, "[type=rate|seek|segment]") {
		t.Fatalf("fixture.controls help:\n%s", controlHelp)
	}

	runDemoCLI(t, socket, "control", "rate", "value=0.5", "source=fixture")
	runDemoCLI(t, socket, "control", "--json", `{"type":"rate","rate":0.75,"node":"fixture"}`)
	runDemoCLI(t, socket, "control", "deliver", "--json", `{"type":"vendor.force_idr","stream_id":"video","reason":"manual","metadata":{"source":"cli","attempt":1,"ok":true}}`, "at=frames")
	controls := runDemoCLI(t, socket, "control", "fixture.controls", "type=rate")
	if !strings.Contains(controls, `"count":2`) ||
		!strings.Contains(controls, `"rate":0.5`) ||
		!strings.Contains(controls, `"rate":0.75`) {
		t.Fatalf("fixture.controls CLI output:\n%s", controls)
	}
	runDemoCLI(t, socket, "attach", "frames", "as", "memory", `thumbnail every=3 label=preview ! memorysink name=preview`)
	runDemoCLI(t, socket, "attach", "frames", "as", "acme-generic", `thumbnail every=4 label=generic ! encode codec=x_acme_video media=video bitrate=220k profile=preview fps=2 keyframe_interval=1 lookahead=deep ! memorysink name=acme-generic`)
	acmeFileOut := filepath.Join(t.TempDir(), "acme cli.webm")
	runDemoCLI(t, socket, "attach", "frames", "as", "acme-file", `thumbnail every=6 label=file ! encode codec=x_acme_video media=video bitrate=320k profile=file fps=2 keyframe_interval=1 lookahead=file ! filesink location="`+acmeFileOut+`"`)
	graph := runDemoCLI(t, socket, "graph")
	if !strings.Contains(graph, "flowchart LR") ||
		!strings.Contains(graph, "branch=memory (attached)") ||
		!strings.Contains(graph, "branch=acme-generic (attached)") ||
		!strings.Contains(graph, "branch=acme-file (attached)") {
		t.Fatalf("graph CLI output:\n%s", graph)
	}
	runDemoCLI(t, socket, "detach", "memory")
	runDemoCLI(t, socket, "detach", "acme-generic")
	runDemoCLI(t, socket, "detach", "acme-file")

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

func TestPrintUsageIncludesCompleteBootstrapLoop(t *testing.T) {
	var out strings.Builder
	printUsage(&out, "unix:///tmp/live.sock")
	text := out.String()
	for _, fragment := range []string{
		"help attach",
		"help control vendor.rate",
		"help control fixture.controls",
		"capabilities",
		"taps",
		"fake source: live VP8 camera",
		"control rate value=0.5 source=fixture",
		"control seek position=2s source=fixture",
		"control segment start=1s end=3s source=fixture",
		"control fixture.controls type=rate",
		"control vendor.rate value=0.5 source=fixture",
		"control --json '{\"type\":\"rate\",\"rate\":0.75,\"node\":\"fixture\"}'",
		"control deliver --json '{\"type\":\"vendor.force_idr\"",
		"resize width=640 height=360",
		"filesink location=\"/tmp/goav archive.webm\"",
		"thumbnail every=5",
		"memorysink name=preview",
		"encode codec=x_acme_video media=video",
		"filesink location=\"/tmp/goav acme.webm\"",
		"detach acme-generic",
		"detach acme-file",
		"acmeenc bitrate=250k",
		"graph format=text",
		"rebranch memory",
		"detach archive",
		"detach thumbnails",
		"detach memory",
		"detach acme-preview",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("usage missing %q:\n%s", fragment, text)
		}
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
	if got, err := parsePositiveSetting("5", "every"); err != nil || got != 5 {
		t.Fatalf("parsePositiveSetting = %d, %v; want 5 nil", got, err)
	}
	if got, err := parsePositiveSetting("0", "every"); err == nil || got != 0 {
		t.Fatalf("parsePositiveSetting invalid = %d, %v; want error", got, err)
	}
	if got := demoNodeName("demo-thumbnail", "left ! right"); got != "demo-thumbnail-left---right" {
		t.Fatalf("demoNodeName = %q", got)
	}

	factory := &demoEncoderFactory{
		descriptor: codec.Descriptor{ID: demoCodec, Type: av.MediaVideo},
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
	err = encoder.EncodeInto(context.Background(), &av.Frame{StreamID: "video", Type: av.MediaVideo}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Packets) != 1 || result.Packets[0].StreamID != "video" || result.Packets[0].Type != av.MediaVideo {
		t.Fatalf("packets = %+v", result.Packets)
	}
	full := codec.EncodeResult{Packets: make([]av.Packet, 1)}
	if err := encoder.EncodeInto(context.Background(), nil, &full); err != nil {
		t.Fatal(err)
	}
	if err := encoder.EncodeInto(context.Background(), &av.Frame{StreamID: "video", Type: av.MediaVideo}, &full); err != nil {
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
	readySocket := filepath.Join(t.TempDir(), "ready.sock")
	if err := os.WriteFile(readySocket, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForHostSocket(canceled, "unix://"+readySocket, make(chan error)); err != nil {
		t.Fatalf("waitForHostSocket canceled after socket exists = %v", err)
	}
	stoppedAfterReady := make(chan error, 1)
	stoppedAfterReady <- context.Canceled
	if err := waitForHostSocket(context.Background(), "unix://"+readySocket, stoppedAfterReady); err != nil {
		t.Fatalf("waitForHostSocket stopped after socket exists = %v", err)
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

	if !expectedHostShutdownError(errors.New(context.Canceled.Error())) {
		t.Fatal("context-canceled string should be accepted during shutdown")
	}
}

func TestStartReadySourcePreservesControl(t *testing.T) {
	ready := make(chan struct{})
	base := &demoReadyControlSource{name: "fixture"}
	wrapped := newStartReadySource(base, ready)
	controllable, ok := wrapped.(pipeline.ControllableSource)
	if !ok {
		t.Fatalf("wrapped source = %T, want ControllableSource", wrapped)
	}
	if wrapped.Name() != "fixture" {
		t.Fatalf("name = %q, want fixture", wrapped.Name())
	}
	if node := wrapped.(interface{ DescribeNode() pipeline.NodeSpec }).DescribeNode(); node.Detail != "ready source" {
		t.Fatalf("node = %+v", node)
	}
	if err := controllable.Control(context.Background(), &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats}}); err != nil {
		t.Fatal(err)
	}
	if !base.controlled {
		t.Fatal("control was not delegated")
	}
	if err := wrapped.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	default:
		t.Fatal("ready channel was not closed")
	}
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if !base.closed {
		t.Fatal("close was not delegated")
	}

	plainReady := make(chan struct{})
	plain := newStartReadySource(demoPlainSource{name: "plain"}, plainReady)
	if _, ok := plain.(pipeline.ControllableSource); ok {
		t.Fatalf("plain wrapped source = %T, should not be controllable", plain)
	}
	if node := plain.(interface{ DescribeNode() pipeline.NodeSpec }).DescribeNode(); node.Name != "plain" {
		t.Fatalf("plain node = %+v", node)
	}
}

func sendDemoRequest(t *testing.T, socket string, request ctlserver.Request) ctlserver.Response {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response ctlserver.Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func runDemoCLI(t *testing.T, socket string, args ...string) string {
	t.Helper()
	argv := append([]string{"run", "../../cmd/goav", "ctl", "--control", "unix://" + socket}, args...)
	cmd := exec.Command("go", argv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s failed: %v\n%s", strings.Join(argv, " "), err, output)
	}
	return string(output)
}

func responseMap(t *testing.T, response ctlserver.Response) map[string]any {
	t.Helper()
	result, ok := response.Result.(map[string]any)
	if !response.OK || response.Error != nil || !ok {
		t.Fatalf("response = %+v, want JSON object result", response)
	}
	return result
}

type demoReadyControlSource struct {
	name       string
	controlled bool
	closed     bool
}

func (s *demoReadyControlSource) Name() string { return s.name }

func (s *demoReadyControlSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: "ready source"}
}

func (s *demoReadyControlSource) Start(context.Context, pipeline.Emitter) error { return nil }

func (s *demoReadyControlSource) Control(context.Context, *pipeline.Message) error {
	s.controlled = true
	return nil
}

func (s *demoReadyControlSource) Close() error {
	s.closed = true
	return nil
}

type demoPlainSource struct {
	name string
}

func (s demoPlainSource) Name() string { return s.name }

func (s demoPlainSource) Start(context.Context, pipeline.Emitter) error { return nil }

func (s demoPlainSource) Close() error { return nil }
