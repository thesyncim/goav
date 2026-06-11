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
