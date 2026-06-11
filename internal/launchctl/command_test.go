package launchctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

func TestControlManifestContainsBuiltInVerbs(t *testing.T) {
	want := []string{"keyframe", "bitrate", "seek", "rate", "segment", "select", "deliver"}
	for _, name := range want {
		spec, ok := LookupControlCommand(name)
		if !ok {
			t.Fatalf("manifest missing %q", name)
		}
		if spec.ArgsType.Kind() != reflect.Struct {
			t.Fatalf("%s ArgsType = %v, want struct", name, spec.ArgsType)
		}
		if spec.Apply == nil {
			t.Fatalf("%s has nil Apply", name)
		}
	}
}

func TestBindArgsParsesControlFields(t *testing.T) {
	bitrateSpec, _ := LookupControlCommand("bitrate")
	bitrateArgs, err := BindArgs(bitrateSpec, []string{"stream=video", "value=1200k", "at=raw_video"})
	if err != nil {
		t.Fatal(err)
	}
	bitrate := bitrateArgs.(BitrateCommand)
	if bitrate.Stream != "video" || bitrate.Value != 1_200_000 || bitrate.At != "raw_video" {
		t.Fatalf("bitrate = %+v", bitrate)
	}

	seekSpec, _ := LookupControlCommand("seek")
	seekArgs, err := BindArgs(seekSpec, []string{"position=12.5s"})
	if err != nil {
		t.Fatal(err)
	}
	if got := seekArgs.(SeekCommand).Position; got != 12500*time.Millisecond {
		t.Fatalf("seek position = %v", got)
	}

	segmentSpec, _ := LookupControlCommand("segment")
	segmentArgs, err := BindArgs(segmentSpec, []string{"start=10s", "end=20s"})
	if err != nil {
		t.Fatal(err)
	}
	segment := segmentArgs.(SegmentCommand)
	if segment.Start != 10*time.Second || segment.End != 20*time.Second {
		t.Fatalf("segment = %+v", segment)
	}
	segmentArgs, err = BindArgs(segmentSpec, []string{"10s..20s"})
	if err != nil {
		t.Fatal(err)
	}
	segment = segmentArgs.(SegmentCommand)
	if segment.Start != 10*time.Second || segment.End != 20*time.Second {
		t.Fatalf("range segment = %+v", segment)
	}

	selectSpec, _ := LookupControlCommand("select")
	selectArgs, err := BindArgs(selectSpec, []string{"active=camera_b"})
	if err != nil {
		t.Fatal(err)
	}
	if got := selectArgs.(SelectCommand).Active; got != "camera_b" {
		t.Fatalf("active = %q", got)
	}

	deliverSpec, _ := LookupControlCommand("deliver")
	deliverArgs, err := BindArgs(deliverSpec, []string{"type=vendor.force_idr", "stream=video", "at=raw_video", "reason=manual", "metadata.foo=bar"})
	if err != nil {
		t.Fatal(err)
	}
	deliver := deliverArgs.(DeliverCommand)
	if deliver.Type != "vendor.force_idr" || deliver.Stream != "video" || deliver.At != "raw_video" || deliver.Reason != "manual" || deliver.Metadata["foo"] != "bar" {
		t.Fatalf("deliver = %+v", deliver)
	}
}

func TestBindArgsMissingRequiredIncludesUsage(t *testing.T) {
	spec, _ := LookupControlCommand("bitrate")
	_, err := BindArgs(spec, []string{"stream=video"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "missing_required" || ctlErr.Node != "value" {
		t.Fatalf("err = %+v, want missing value", ctlErr)
	}
	if !detailsContain(ctlErr.Details, "usage=goav ctl --control unix://PATH control bitrate") {
		t.Fatalf("details = %v, want generated usage", ctlErr.Details)
	}
}

func TestBindArgsUnknownFieldSuggestsKnownField(t *testing.T) {
	spec, _ := LookupControlCommand("keyframe")
	_, err := BindArgs(spec, []string{"stram=video"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "unknown_field" || !suggestionsContain(ctlErr.Suggestions, "stream=") {
		t.Fatalf("err = %+v, want stream suggestion", ctlErr)
	}
}

func TestExecuteRawControlCallsTaskControl(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"control", "--json", `{"type":"bitrate","stream_id":"video","bitrate":1200000,"tap":"main_encoded"}`})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.controls) != 1 {
		t.Fatalf("controls = %d, want 1", len(task.controls))
	}
	control := task.controls[0]
	if control.Type != goav.ControlBitrate || control.StreamID != "video" || control.Bitrate != 1_200_000 || control.Tap != "main_encoded" {
		t.Fatalf("control = %+v", control)
	}
}

func TestExecuteRawEventCallsTaskControlDeliver(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"control", "deliver", "--json", `{"type":"vendor.force_idr","stream_id":"video","reason":"manual","metadata":{"source":"cli","count":2,"ok":true}}`, "at=raw_video"})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.controls) != 1 {
		t.Fatalf("controls = %d, want 1", len(task.controls))
	}
	control := task.controls[0]
	if control.Type != goav.ControlEvent || control.Tap != "raw_video" {
		t.Fatalf("control = %+v", control)
	}
	event := control.Event
	if event.Type != "vendor.force_idr" || event.StreamID != "video" || event.Reason != "manual" ||
		event.Metadata["source"] != "cli" || event.Metadata["count"] != "2" || event.Metadata["ok"] != "true" {
		t.Fatalf("event = %+v", event)
	}
}

func TestRequestFromCLIUsesCanonicalControlProtocol(t *testing.T) {
	request, err := RequestFromCLI([]string{"control", "bitrate", "stream=video", "value=1200k", "at=main_encoded"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "control" || request.Verb != "bitrate" ||
		request.Args["stream"] != "video" || request.Args["value"] != "1200k" || request.Args["at"] != "main_encoded" {
		t.Fatalf("request = %+v", request)
	}

	raw, err := RequestFromCLI([]string{"control", "--json", `{"type":"keyframe","stream_id":"video"}`})
	if err != nil {
		t.Fatal(err)
	}
	if raw.Op != "control_raw" || string(raw.Control) != `{"type":"keyframe","stream_id":"video"}` {
		t.Fatalf("raw request = %+v", raw)
	}
}

func TestExecuteRequestAppliesControlRequest(t *testing.T) {
	task := newFakeTask()
	response := ExecuteRequest(context.Background(), task, Request{
		Op:   "control",
		Verb: "bitrate",
		Args: map[string]string{"stream": "video", "value": "1200k", "at": "main_encoded"},
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("response = %+v", response)
	}
	if len(task.controls) != 1 || task.controls[0].Type != goav.ControlBitrate || task.controls[0].Bitrate != 1_200_000 {
		t.Fatalf("controls = %+v", task.controls)
	}
}

func TestExecuteRequestAppliesHelpRequest(t *testing.T) {
	response := ExecuteRequest(context.Background(), newFakeTask(), Request{
		Op:   "help",
		Args: map[string]string{"topic": "control", "command": "bitrate"},
	})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok || !strings.Contains(text, "control bitrate") {
		t.Fatalf("help response = %+v", response)
	}
}

func TestRequestFromCLIParsesGraphCommand(t *testing.T) {
	request, err := RequestFromCLI([]string{"graph", "format=dot"})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "graph" || request.Args["format"] != "dot" {
		t.Fatalf("request = %+v", request)
	}

	shorthand, err := RequestFromCLI([]string{"flowchart", "mermaid"})
	if err != nil {
		t.Fatal(err)
	}
	if shorthand.Op != "flowchart" || shorthand.Args["format"] != "mermaid" {
		t.Fatalf("shorthand = %+v", shorthand)
	}
}

func TestRequestFromCLIParsesRebranchCommand(t *testing.T) {
	request, err := RequestFromCLI([]string{
		"rebranch",
		"preview",
		"--switch", "next_keyframe",
		"--keep-old-on-failure",
		"copy ! filesink location=preview.webm",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.Op != "rebranch" ||
		request.Branch != "preview" ||
		request.Switch != "next_keyframe" ||
		!request.KeepOldOnFailure ||
		request.Pipeline != "copy ! filesink location=preview.webm" {
		t.Fatalf("request = %+v", request)
	}

	for _, argv := range [][]string{
		{"rebranch", "preview"},
		{"rebranch", "preview", "--switch"},
		{"rebranch", "preview", "copy ! filesink location=a.webm", "copy ! filesink location=b.webm"},
	} {
		if _, err := RequestFromCLI(argv); err == nil {
			t.Fatalf("RequestFromCLI(%v) succeeded, want error", argv)
		}
	}
}

func TestExecuteRequestRendersTaskGraph(t *testing.T) {
	response := ExecuteRequest(context.Background(), newFakeTask(), Request{Op: "graph"})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok {
		t.Fatalf("graph response = %+v", response)
	}
	if !strings.Contains(text, "flowchart LR") || !strings.Contains(text, "source") {
		t.Fatalf("graph:\n%s", text)
	}

	response = ExecuteRequest(context.Background(), newFakeTask(), Request{
		Op:   "graph",
		Args: map[string]string{"format": "dot"},
	})
	text, ok = response.Result.(string)
	if !response.OK || response.Error != nil || !ok || !strings.Contains(text, "digraph") {
		t.Fatalf("dot graph response = %+v text=%q", response, text)
	}

	direct, err := Execute(context.Background(), newFakeTask(), []string{"graph", "text"})
	if err != nil {
		t.Fatal(err)
	}
	text, ok = direct.Result.(string)
	if direct.Operation != "graph" || !ok || !strings.Contains(text, "pipeline fake") {
		t.Fatalf("direct graph = %+v text=%q", direct, text)
	}
}

func TestExecuteGraphRejectsInvalidFormat(t *testing.T) {
	_, err := Execute(context.Background(), newFakeTask(), []string{"graph", "format=json"})
	if err == nil {
		t.Fatal("expected invalid graph format")
	}
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "invalid_value" {
		t.Fatalf("err = %v", err)
	}
}

func TestBranchPipelineExtensionHandleCallsConfiguredHooks(t *testing.T) {
	var calls []string
	handle := &BranchPipeline{
		copyFn:   func() { calls = append(calls, "copy") },
		decodeFn: func() { calls = append(calls, "decode") },
		resizeFn: func(width int, height int) { calls = append(calls, fmt.Sprintf("resize:%dx%d", width, height)) },
		resampleFn: func(sampleRate int, channels int) {
			calls = append(calls, fmt.Sprintf("resample:%d:%d", sampleRate, channels))
		},
		doFn: func(...pipeline.Stage) { calls = append(calls, "do") },
		encodeFn: func(spec codec.CodecSpec) {
			calls = append(calls, "encode:"+string(spec.ID))
		},
		destinationFn: func(goav.Destination) { calls = append(calls, "destination") },
		finishFn: func() goav.BranchSpec {
			calls = append(calls, "finish")
			return goav.Branch("custom").To()
		},
	}

	handle.Copy()
	handle.Decode()
	handle.Resize(320, 180)
	handle.Resample(16000, 1)
	handle.Do()
	handle.Encode(codec.Opus())
	handle.Destination(goav.Destination{})
	_ = handle.finish()

	want := []string{
		"copy",
		"decode",
		"resize:320x180",
		"resample:16000:1",
		"do",
		"encode:opus",
		"destination",
		"finish",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}

	var nilHandle *BranchPipeline
	nilHandle.Copy()
	nilHandle.Decode()
	nilHandle.Resize(1, 1)
	nilHandle.Resample(1, 1)
	nilHandle.Do()
	nilHandle.Encode(codec.CodecSpec{})
	nilHandle.Destination(goav.Destination{})
	_ = nilHandle.finish()
}

func TestPipelineParserHelpersCoverCommonFormsAndErrors(t *testing.T) {
	width, height, err := parseResizeArgs([]string{"854x480"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if width != 854 || height != 480 {
		t.Fatalf("resize = %dx%d", width, height)
	}

	width, height, err = parseResizeArgs(nil, map[string]string{"w": "320", "height": "180"})
	if err != nil {
		t.Fatal(err)
	}
	if width != 320 || height != 180 {
		t.Fatalf("resize args = %dx%d", width, height)
	}

	rate, channels, err := parseResampleArgs([]string{"48000", "2"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rate != 48000 || channels != 2 {
		t.Fatalf("resample = %d/%d", rate, channels)
	}

	rate, channels, err = parseResampleArgs(nil, map[string]string{"sample_rate": "16000", "ch": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if rate != 16000 || channels != 1 {
		t.Fatalf("resample args = %d/%d", rate, channels)
	}

	num, den, err := parseFPS("30000/1001")
	if err != nil {
		t.Fatal(err)
	}
	if num != 30000 || den != 1001 {
		t.Fatalf("fps = %d/%d", num, den)
	}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "resize", run: func() error {
			_, _, err := parseResizeArgs([]string{"0x480"}, nil)
			return err
		}},
		{name: "resample", run: func() error {
			_, _, err := parseResampleArgs([]string{"48000", "0"}, nil)
			return err
		}},
		{name: "fps numerator", run: func() error {
			_, _, err := parseFPS("0")
			return err
		}},
		{name: "fps denominator", run: func() error {
			_, _, err := parseFPS("30/0")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var structured *Error
			if !errors.As(err, &structured) || structured.Code != "invalid_value" {
				t.Fatalf("err = %v, want invalid_value", err)
			}
		})
	}
}

func TestBranchPipelineLexingSupportsQuotedValues(t *testing.T) {
	steps, err := splitPipeline(`meter label="left ! right" note='two words' raw="a=b" ! filesink location="/tmp/a b=1.ogg" title="say \"hi\"" format=ogg`)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("steps = %v", steps)
	}

	fields, err := pipelineFields(steps[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, []string{"meter", "label=left ! right", "note=two words", "raw=a=b"}) {
		t.Fatalf("fields[0] = %v", fields)
	}

	fields, err = pipelineFields(steps[1])
	if err != nil {
		t.Fatal(err)
	}
	args := stepArgs(fields[1:])
	if fields[0] != "filesink" ||
		args["location"] != "/tmp/a b=1.ogg" ||
		args["title"] != `say "hi"` ||
		args["format"] != "ogg" {
		t.Fatalf("fields[1]=%v args=%v", fields, args)
	}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "split quote", run: func() error {
			_, err := splitPipeline(`copy ! filesink location="unterminated`)
			return err
		}},
		{name: "field quote", run: func() error {
			_, err := pipelineFields(`meter label="unterminated`)
			return err
		}},
		{name: "field escape", run: func() error {
			_, err := pipelineFields(`meter label="dangling\`)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var structured *Error
			if !errors.As(err, &structured) ||
				structured.Code != "invalid_value" ||
				!detailsContain(structured.Details, "offset=") {
				t.Fatalf("err = %+v, want invalid_value with offset", err)
			}
		})
	}
}

func TestPipelineStepNamesIncludeCustomAliases(t *testing.T) {
	names := pipelineStepNames(PipelineRegistry{
		Steps: []BranchPipelineStepSpec{{
			Name:    "VendorFilter",
			Aliases: []string{"vf"},
		}},
		Encoders: []EncoderSpec{{
			Name:    "VendorEnc",
			Aliases: []string{"venc"},
		}},
	})
	for _, want := range []string{"copy", "encode", "vendorfilter", "vf", "vendorenc", "venc"} {
		if !stringSliceContains(names, want) {
			t.Fatalf("names = %v, want %q", names, want)
		}
	}
}

func TestExecuteRequestAppliesAttachRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	out := filepath.Join(t.TempDir(), "direct.ogg")
	response := ExecuteRequest(ctx, task, Request{
		Op:       "attach",
		Tap:      "pkts",
		Branch:   "direct",
		Pipeline: "copy ! filesink location=" + out + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	branch, ok := response.Result.(snapshot.Branch)
	if !ok || branch.Name != "direct" {
		t.Fatalf("attach result = %#v", response.Result)
	}
}

func TestExecuteAppliesAttachCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	out := filepath.Join(t.TempDir(), "direct.ogg")
	response, err := Execute(ctx, task, []string{"attach", "pkts", "as", "direct", "copy ! filesink location=" + out + " format=ogg"})
	if err != nil {
		t.Fatal(err)
	}
	branch, ok := response.Result.(snapshot.Branch)
	if response.Operation != "attach" || !ok || branch.Name != "direct" {
		t.Fatalf("attach response = %+v", response)
	}
}

func TestCanonicalControlCommandsApplySupportedVerbs(t *testing.T) {
	ctx := context.Background()
	task := newFakeTask()
	commands := [][]string{
		{"control", "keyframe", "stream=video"},
		{"control", "bitrate", "stream=video", "value=1200k"},
		{"control", "seek", "position=12.5s"},
		{"control", "rate", "value=0.5"},
		{"control", "segment", "start=10s", "end=20s"},
		{"control", "select", "active=camera_b", "selector=program"},
		{"control", "deliver", "type=vendor.force_idr", "stream=video", "at=raw_video", "reason=manual"},
	}
	for _, command := range commands {
		if _, err := Execute(ctx, task, command); err != nil {
			t.Fatalf("%v: %v", command, err)
		}
	}
	if got, want := len(task.controls), len(commands); got != want {
		t.Fatalf("controls = %d, want %d: %+v", got, want, task.controls)
	}
	control := task.controls[5]
	if control.Type != goav.ControlSelect || control.StreamID != "camera_b" || control.Node != "program" {
		t.Fatalf("select control = %+v", control)
	}
	control = task.controls[6]
	if control.Type != goav.ControlEvent || control.Event.Metadata != nil || control.Event.Reason != "manual" {
		t.Fatalf("deliver control = %+v", control)
	}
}

func TestArgsFromMapPreservesTrueValues(t *testing.T) {
	args := argsFromMap(map[string]string{
		"follow":      "true",
		"metadata.ok": "true",
	})
	if !reflect.DeepEqual(args, []string{"--follow", "metadata.ok=true"}) {
		t.Fatalf("args = %v", args)
	}
}

func TestServeUnixHandlesOneControlRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := newFakeTask()
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goavctl-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ServeUnix(ctx, task, "unix://"+socket)
	}()
	waitForSocket(t, socket, errC)

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		Op:    "control",
		Verb:  "deliver",
		Event: json.RawMessage(`{"type":"vendor.force_idr","stream_id":"video"}`),
		Args:  map[string]string{"at": "raw_video"},
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if !response.OK || response.Error != nil {
		t.Fatalf("response = %+v", response)
	}
	if len(task.controls) != 1 || task.controls[0].Type != goav.ControlEvent || task.controls[0].Tap != "raw_video" {
		t.Fatalf("controls = %+v", task.controls)
	}
	cancel()
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestServeUnixWithOptionsHandlesCustomCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type socketCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"socket value"`
	}
	var applied string
	spec := CommandSpec{
		Name:     "vendor.socket",
		Summary:  "socket custom command",
		ArgsType: reflect.TypeOf(socketCommand{}),
		Apply: func(_ context.Context, _ goav.Task, args any) (ControlResponse, error) {
			applied = args.(socketCommand).Value
			return ControlResponse{Operation: "control vendor.socket", Result: applied}, nil
		},
	}

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goavctl-custom-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ServeUnixWithOptions(ctx, newFakeTask(), "unix://"+socket, WithCommands(spec))
	}()
	waitForSocket(t, socket, errC)

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Op: "control", Verb: "vendor.socket", Args: map[string]string{"value": "socket-called"}}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if !response.OK || response.Error != nil || applied != "socket-called" {
		t.Fatalf("response=%+v applied=%q", response, applied)
	}
	cancel()
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
}

func TestServerStreamsWatchFollowResponses(t *testing.T) {
	task := newFakeTask()
	task.events = []av.Event{
		{Type: av.EventStats, StreamID: "video"},
		{Type: av.EventBitrateChanged, StreamID: "video"},
	}
	server := &Server{Task: task}
	client, serverConn := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.handleConn(context.Background(), serverConn)
	}()
	request := Request{Op: "watch", Args: map[string]string{"follow": "true", "type": string(av.EventStats)}}
	if err := json.NewEncoder(client).Encode(request); err != nil {
		t.Fatal(err)
	}
	var response struct {
		OK     bool     `json:"ok"`
		Result av.Event `json:"result"`
		Error  *Error   `json:"error,omitempty"`
	}
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Error != nil || response.Result.Type != av.EventStats {
		t.Fatalf("response = %+v", response)
	}
	<-done
}

func TestServerAttachRebranchDetachUsesAttachmentTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
	task, err := goav.From(goavtest.Packets(av.CodecOpus, packet)).
		Audio().Copy().Tap(goav.PacketTap("pkts")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	server := &Server{Task: task}
	first := filepath.Join(t.TempDir(), "first.ogg")
	response := server.Handle(ctx, Request{
		Op:       "attach",
		Tap:      "pkts",
		Branch:   "preview",
		Pipeline: "copy ! filesink location=" + first + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}

	second := filepath.Join(t.TempDir(), "second.ogg")
	response = server.Handle(ctx, Request{
		Op:       "rebranch",
		Branch:   "preview",
		Pipeline: "copy ! filesink location=" + second + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("rebranch response = %+v", response)
	}

	response = server.Handle(ctx, Request{Op: "detach", Branch: "preview"})
	if !response.OK || response.Error != nil {
		t.Fatalf("detach response = %+v", response)
	}
}

func TestServerSupportsCustomControlCommand(t *testing.T) {
	type vendorCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom value"`
	}
	var applied string
	server := &Server{
		Task: newFakeTask(),
		Commands: []CommandSpec{{
			Name:     "vendor.custom",
			Summary:  "custom vendor command",
			ArgsType: reflect.TypeOf(vendorCommand{}),
			Apply: func(_ context.Context, _ goav.Task, args any) (ControlResponse, error) {
				applied = args.(vendorCommand).Value
				return ControlResponse{Operation: "control vendor.custom", Result: applied}, nil
			},
		}},
	}
	response := server.Handle(context.Background(), Request{
		Op:   "control",
		Verb: "vendor.custom",
		Args: map[string]string{"value": "called"},
	})
	if !response.OK || response.Error != nil || applied != "called" {
		t.Fatalf("response=%+v applied=%q", response, applied)
	}
	help := server.Handle(context.Background(), Request{
		Op:   "help",
		Args: map[string]string{"topic": "control", "command": "vendor.custom"},
	})
	if !help.OK || !strings.Contains(help.Result.(string), "custom vendor command") {
		t.Fatalf("help = %+v", help)
	}
}

func TestServerSupportsCustomEncoderSettings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const customCodec = av.CodecID("vendor_audio")
	factory := &recordingEncoderFactory{descriptor: codec.Descriptor{ID: customCodec, Type: av.MediaAudio}}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var controlSeen bool
	server := &Server{
		Task: task,
		Pipeline: PipelineRegistry{
			Encoders: []EncoderSpec{{
				Name: "fancyenc",
				Apply: func(args StepArgs) (codec.CodecSpec, error) {
					bitrate, err := parseRate(args["bitrate"])
					if err != nil {
						return codec.CodecSpec{}, err
					}
					return codec.Codec(customCodec, av.MediaAudio,
						codec.Bitrate(bitrate),
						codec.Profile(args["quality"]),
						codec.Control(func(any) error {
							controlSeen = true
							return nil
						}),
					), nil
				},
			}},
		},
	}
	out := filepath.Join(t.TempDir(), "custom.ogg")
	response := server.Handle(ctx, Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "custom",
		Pipeline: "fancyenc bitrate=123k quality=cinema ! filesink location=" + out + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	if factory.config.Settings.Bitrate != 123000 || factory.config.Settings.Profile != "cinema" || factory.config.Settings.Control == nil {
		t.Fatalf("encoder config = %+v", factory.config.Settings)
	}
	if err := factory.config.Settings.Control(nil); err != nil || !controlSeen {
		t.Fatalf("custom control callback err=%v seen=%v", err, controlSeen)
	}
}

func TestServerHelpListsCustomPipelineRegistry(t *testing.T) {
	server := &Server{
		Task: newFakeTask(),
		Pipeline: PipelineRegistry{
			Steps: []BranchPipelineStepSpec{{
				Name:    "meter",
				Aliases: []string{"levelmeter"},
				Summary: "observe samples before encoding",
				Usage:   "[window=<duration>]",
			}},
			Encoders: []EncoderSpec{{
				Name:    "acmeenc",
				Aliases: []string{"acme"},
				Summary: "ACME native audio encoder",
				Usage:   "bitrate=<bps> quality=<name> lookahead=<mode>",
			}},
		},
	}

	for _, topic := range []string{"attach", "rebranch"} {
		response := server.Handle(context.Background(), Request{
			Op:   "help",
			Args: map[string]string{"topic": topic},
		})
		text, ok := response.Result.(string)
		if !response.OK || response.Error != nil || !ok {
			t.Fatalf("%s response = %+v", topic, response)
		}
		for _, fragment := range []string{
			"Built-in steps:",
			"encode codec=<id>",
			"Custom steps:",
			"meter [window=<duration>]",
			"(aliases: levelmeter)",
			"observe samples before encoding",
			"Custom encoders:",
			"acmeenc bitrate=<bps> quality=<name> lookahead=<mode>",
			"(aliases: acme)",
			"ACME native audio encoder",
			"StepArgs",
		} {
			if !strings.Contains(text, fragment) {
				t.Fatalf("%s help missing %q:\n%s", topic, fragment, text)
			}
		}
	}
}

func TestServerGenericEncodeStepCarriesCommonCodecOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	const customCodec = av.CodecID("vendor_generic_audio")
	factory := &recordingEncoderFactory{descriptor: codec.Descriptor{ID: customCodec, Type: av.MediaAudio}}
	task, err := goav.From(goavtest.Audio(48000, 1, []int16{1})).
		Audio().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	out := filepath.Join(t.TempDir(), "generic.ogg")
	response := (&Server{Task: task}).Handle(ctx, Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "generic",
		Pipeline: "encode codec=vendor_generic_audio media=audio bitrate=64k profile=voice level=1 sample_rate=16000 channels=1 clock_rate=48000 keyframe_interval=20 fps=30" +
			" ! filesink location=" + out + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	settings := factory.config.Settings
	if settings.Bitrate != 64000 ||
		settings.Profile != "voice" ||
		settings.Level != "1" ||
		settings.SampleRate != 16000 ||
		!settings.SampleRateSet ||
		settings.Channels != 1 ||
		!settings.ChannelsSet ||
		settings.ClockRate != 48000 ||
		settings.KeyframeInterval != 20 ||
		settings.Framerate.Value != 1 ||
		settings.Framerate.Base.Den != 30 {
		t.Fatalf("encoder settings = %+v", settings)
	}
}

func TestExecuteControlAgainstRealControllableTestSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	source := goavtest.NewTestSource("fixture",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 1, av.SampleFormatS16)),
	)
	task, err := goav.From(source.Input()).
		Audio().Copy().
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime()).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if _, err := Execute(ctx, task, []string{"control", "rate", "value=0.5", "source=fixture"}); err != nil {
		t.Fatal(err)
	}
	event, err := source.WaitControl(ctx, av.EventRate)
	if err != nil {
		t.Fatal(err)
	}
	if rate, ok := av.EventRateValue(&event); !ok || rate != 0.5 {
		t.Fatalf("rate event = %+v, parsed=%v ok=%v", event, rate, ok)
	}

	if _, err := Execute(ctx, task, []string{"control", "seek", "position=12.5s", "source=fixture"}); err != nil {
		t.Fatal(err)
	}
	event, err = source.WaitControl(ctx, av.EventSeek)
	if err != nil {
		t.Fatal(err)
	}
	if position, ok := event.Timestamp.ToDuration(); !ok || position != 12500*time.Millisecond {
		t.Fatalf("seek event = %+v, parsed=%v ok=%v", event, position, ok)
	}
}

func TestUnknownTapErrorListsAvailableTaps(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"control", "deliver", "type=vendor.force_idr", "at=raw_vdieo"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "unknown_tap" || !detailsContain(ctlErr.Details, "raw_video") || !suggestionsContain(ctlErr.Suggestions, "at=raw_video") {
		t.Fatalf("err = %+v, want available tap details", ctlErr)
	}
}

func TestUnknownBranchErrorListsAvailableBranches(t *testing.T) {
	task := newFakeTask()
	_, err := Execute(context.Background(), task, []string{"detach", "preveiw"})
	var ctlErr *Error
	if !errors.As(err, &ctlErr) {
		t.Fatalf("err = %v, want *Error", err)
	}
	if ctlErr.Code != "unknown_branch" || !detailsContain(ctlErr.Details, "preview") || !suggestionsContain(ctlErr.Suggestions, "preview") {
		t.Fatalf("err = %+v, want available branch details", ctlErr)
	}
}

func TestHelpGeneratedFromManifestMetadata(t *testing.T) {
	spec, _ := LookupControlCommand("bitrate")
	help := CommandHelp(spec)
	for _, fragment := range []string{
		"control bitrate",
		"goav ctl --control unix://PATH control bitrate stream=<stream-id> value=<rate> [at=<tap>]",
		"stream    required",
		"value     required",
		"bits per second, accepts 1200k, 2M, or integer",
	} {
		if !strings.Contains(help, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, help)
		}
	}
}

func TestHelpRendersRootStaticAndCustomControlTopics(t *testing.T) {
	root, err := Help(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"goav ctl", "graph [format=mermaid|dot|text]", "rebranch <branch-name>"} {
		if !strings.Contains(root, fragment) {
			t.Fatalf("root help missing %q:\n%s", fragment, root)
		}
	}

	attach, err := Help([]string{"attach"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(attach, "Built-in steps:") || !strings.Contains(attach, "encode codec=<id>") {
		t.Fatalf("attach help:\n%s", attach)
	}

	graph, err := Help([]string{"graph"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(graph, "Mermaid") || !strings.Contains(graph, "Graphviz DOT") {
		t.Fatalf("graph help:\n%s", graph)
	}

	type vendorCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom value"`
	}
	manifest := []CommandSpec{{
		Name:     "vendor.tune",
		Summary:  "custom tuning command",
		ArgsType: reflect.TypeOf(vendorCommand{}),
	}}
	control, err := HelpWithCommands([]string{"control"}, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(control, "vendor.tune") || !strings.Contains(control, "custom tuning command") {
		t.Fatalf("control help:\n%s", control)
	}

	specific, err := HelpWithCommands([]string{"control", "vendor.tune"}, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specific, "goav ctl --control unix://PATH control vendor.tune value=<text>") {
		t.Fatalf("specific help:\n%s", specific)
	}

	_, err = Help([]string{"missing"})
	var structured *Error
	if !errors.As(err, &structured) || structured.Code != "unknown_command" {
		t.Fatalf("err = %v, want unknown_command", err)
	}
}

func TestReflectionConfinedToLaunchctlProductionFiles(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	var offenders []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		slash := filepath.ToSlash(path)
		if strings.Contains(slash, "internal/launchctl/") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), `"reflect"`) {
			offenders = append(offenders, slash)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) != 0 {
		t.Fatalf("reflect imports outside internal/launchctl: %v", offenders)
	}
}

func TestTestOnlyCommandExtensionPath(t *testing.T) {
	type fakeCommand struct {
		Name string `goavctl:"name,required" usage:"name=<value>" help:"test value"`
	}
	var applied string
	spec := CommandSpec{
		Name:     "fake",
		Summary:  "fake extension",
		ArgsType: reflect.TypeOf(fakeCommand{}),
		Apply: func(_ context.Context, _ goav.Task, args any) (ControlResponse, error) {
			applied = args.(fakeCommand).Name
			return ControlResponse{Operation: "control fake", Result: applied}, nil
		},
	}
	response, err := Invoke(context.Background(), newFakeTask(), spec, []string{"name=added"})
	if err != nil {
		t.Fatal(err)
	}
	if applied != "added" || response.Result != "added" {
		t.Fatalf("applied=%q response=%+v", applied, response)
	}
	if !strings.Contains(CommandHelp(spec), "name=<value>") {
		t.Fatalf("fake help was not generated from tags")
	}
}

func suggestionsContain(suggestions []string, fragment string) bool {
	for _, suggestion := range suggestions {
		if strings.Contains(suggestion, fragment) {
			return true
		}
	}
	return false
}

func detailsContain(details []string, fragment string) bool {
	for _, detail := range details {
		if strings.Contains(detail, fragment) {
			return true
		}
	}
	return false
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func waitForSocket(t *testing.T, socket string, errC <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			return
		}
		select {
		case err := <-errC:
			t.Fatalf("server stopped before creating socket: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not created", socket)
		}
		time.Sleep(time.Millisecond)
	}
}

type fakeTask struct {
	controls []goav.Control
	taps     []snapshot.Tap
	spec     pipeline.Spec
	snapshot snapshot.Task
	events   []av.Event
	closed   bool
}

func newFakeTask() *fakeTask {
	taps := []snapshot.Tap{
		{Name: "raw_video", MediaKind: av.MediaVideo, Domain: shape.DomainFrame, Shape: shape.Frame(av.MediaVideo, shape.Stream("video")), Node: "raw-node"},
		{Name: "main_encoded", MediaKind: av.MediaVideo, Domain: shape.DomainPacket, Shape: shape.Packet(av.MediaVideo, av.CodecVP8, shape.Stream("video")), Node: "enc-node"},
	}
	spec := pipeline.Spec{
		Name: "fake",
		Nodes: []pipeline.NodeSpec{
			{Name: "source", Kind: pipeline.NodeSource},
			{Name: "raw-node", Kind: pipeline.NodeStage},
			{Name: "enc-node", Kind: pipeline.NodeStage},
			{Name: "program", Kind: pipeline.NodeStage},
			{Name: "sink", Kind: pipeline.NodeSink},
		},
		Edges: []pipeline.EdgeSpec{
			{From: "source", To: "raw-node", Policy: pipeline.RouteAll},
			{From: "raw-node", To: "enc-node", Policy: pipeline.RouteAll},
			{From: "enc-node", To: "sink", Policy: pipeline.RouteAll},
		},
	}
	snap := snapshot.Task{
		Spec: spec,
		Taps: taps,
		Branches: []snapshot.Branch{
			{Name: "preview"},
			{Name: "archive"},
		},
	}
	return &fakeTask{taps: taps, spec: spec, snapshot: snap}
}

func (t *fakeTask) Describe() pipeline.Spec { return t.spec }

func (t *fakeTask) Explain(context.Context) (plan.Report, error) {
	return plan.Report{Graph: t.spec}, nil
}

func (t *fakeTask) Attach(context.Context, ...goav.BranchSpec) (goav.Attachment, error) {
	return nil, errors.New("not implemented")
}

func (t *fakeTask) Detach(context.Context, goav.Attachment) error { return nil }

func (t *fakeTask) Taps() []snapshot.Tap { return append([]snapshot.Tap(nil), t.taps...) }

func (t *fakeTask) Snapshot() snapshot.Task { return t.snapshot }

func (t *fakeTask) Control(_ context.Context, control goav.Control) error {
	t.controls = append(t.controls, control)
	return nil
}

func (t *fakeTask) Run(context.Context) error { return nil }

func (t *fakeTask) Events() <-chan av.Event {
	ch := make(chan av.Event, len(t.events))
	for _, event := range t.events {
		ch <- event
	}
	close(ch)
	return ch
}

func (t *fakeTask) Watch(filters ...goav.EventFilter) <-chan av.Event {
	ch := make(chan av.Event, len(t.events))
	for _, event := range t.events {
		if eventMatches(event, filters) {
			ch <- event
		}
	}
	close(ch)
	return ch
}

func (t *fakeTask) Stats() pipeline.GraphStats { return pipeline.GraphStats{} }

func (t *fakeTask) Close() error {
	t.closed = true
	return nil
}

func eventMatches(event av.Event, filters []goav.EventFilter) bool {
	for _, filter := range filters {
		if filter != nil && !filter(event) {
			return false
		}
	}
	return true
}

type recordingEncoderFactory struct {
	descriptor codec.Descriptor
	config     codec.EncodeConfig
}

func (f *recordingEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	f.config = config
	return recordingEncoder{descriptor: f.descriptor}, nil
}

type recordingEncoder struct {
	descriptor codec.Descriptor
}

func (e recordingEncoder) Descriptor() codec.Descriptor { return e.descriptor }
func (e recordingEncoder) Open(context.Context, codec.EncodeConfig) error {
	return nil
}
func (e recordingEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil || len(out.Packets) == cap(out.Packets) {
		return nil
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	out.Packets[index].Reset()
	out.Packets[index].StreamID = frame.StreamID
	out.Packets[index].Type = frame.Type
	out.Packets[index].Payload = av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}
	return nil
}
func (e recordingEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (e recordingEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (e recordingEncoder) Close() error                                         { return nil }
