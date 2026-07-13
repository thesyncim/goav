package ctlserver_test

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
	"sync/atomic"
	"testing"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	runconfig "github.com/thesyncim/goav/runconfig"
)

func TestExternalHostCustomCommandOverSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type vendorCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"external value"`
	}
	var applied string
	command := ctlserver.CommandSpec{
		Name:     "vendor.external",
		Summary:  "external command",
		ArgsType: reflect.TypeOf(vendorCommand{}),
		Apply: func(_ context.Context, _ goav.LiveTask, args any) (ctlserver.ControlResponse, error) {
			applied = args.(vendorCommand).Value
			return ctlserver.ControlResponse{Operation: "control vendor.external", Result: applied}, nil
		},
	}
	task := newExternalTask(t, goavtest.Runtime())
	defer task.Close()

	socket := startUnixServer(t, ctx, task, ctlserver.WithCommands(command))
	response := sendRequest(t, socket, ctlserver.Request{
		Op:   "control",
		Verb: "vendor.external",
		Args: map[string]string{"value": "from-socket"},
	})
	if !response.OK || response.Error != nil || applied != "from-socket" {
		t.Fatalf("response=%+v applied=%q", response, applied)
	}

	help := sendRequest(t, socket, ctlserver.Request{
		Op:   "help",
		Args: map[string]string{"topic": "control", "command": "vendor.external"},
	})
	text, ok := help.Result.(string)
	if !help.OK || !ok || !strings.Contains(text, "external command") {
		t.Fatalf("help = %+v", help)
	}
}

func TestExternalHostCustomCodecOptionsAndStageOverSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const customCodec = av.CodecID("x_cli_pcm")
	factory := &recordingEncoderFactory{
		descriptor: codec.Descriptor{ID: customCodec, Name: "CLI PCM", Type: av.MediaAudio},
	}
	task := newExternalTask(t, goavtest.Runtime(runconfig.WithEncoder(factory.descriptor, factory)))
	defer task.Close()

	var metered atomic.Int64
	var captured atomic.Int64
	registry := ctlserver.PipelineRegistry{
		Steps: []ctlserver.BranchPipelineStepSpec{
			{
				Name:    "meter",
				Summary: "external frame meter",
				Apply: func(branch *ctlserver.BranchPipeline, _ ctlserver.StepArgs) error {
					branch.Do(component.FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit component.Emit) error {
						metered.Add(1)
						return emit.Frame(frame)
					}))
					return nil
				},
			},
			{
				Name:    "memorysink",
				Summary: "external in-process sink",
				Apply: func(branch *ctlserver.BranchPipeline, args ctlserver.StepArgs) error {
					name := args["name"]
					if name == "" {
						name = "memory"
					}
					branch.Destination(goav.Sink(component.SinkFunc(name, func(context.Context, component.Message) error {
						captured.Add(1)
						return nil
					})))
					return nil
				},
			},
		},
	}

	socket := startUnixServer(t, ctx, task, ctlserver.WithPipelineRegistry(registry))
	response := sendRequest(t, socket, ctlserver.Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "record",
		Pipeline: "meter ! encode codec=x_cli_pcm media=audio bitrate=123k profile=cinema level=1 " +
			"sample_rate=16000 channels=1 clock_rate=16000 keyframe_interval=7 fps=24 ! " +
			"memorysink name=archive",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if metered.Load() == 0 {
		t.Fatal("custom stage did not observe frames")
	}
	if captured.Load() == 0 {
		t.Fatal("custom destination did not receive packets")
	}
	settings := factory.config.Settings
	if settings.Bitrate != 123000 ||
		settings.Profile != "cinema" ||
		settings.Level != "1" ||
		settings.SampleRate != 16000 ||
		settings.Channels != codec.Mono ||
		settings.ClockRate != 16000 ||
		settings.KeyframeInterval != 7 ||
		settings.Framerate.Value != 1 ||
		settings.Framerate.Base.Den != 24 {
		t.Fatalf("settings = %+v", settings)
	}
	if factory.config.Parameters.ID != customCodec ||
		factory.config.Parameters.Type != av.MediaAudio ||
		factory.config.Parameters.SampleRate != 16000 ||
		factory.config.Parameters.Channels != codec.Mono {
		t.Fatalf("parameters = %+v", factory.config.Parameters)
	}
}

func TestExternalHostHelpListsRuntimeRegisteredComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const customCodec = av.CodecID("x_help_pcm")
	const customFormat = av.FormatID("x_helpmux")
	factory := &recordingEncoderFactory{
		descriptor: codec.Descriptor{ID: customCodec, Name: "Help PCM", Type: av.MediaAudio},
	}
	task := newExternalTask(t, goavtest.Runtime(
		runconfig.WithEncoder(factory.descriptor, factory),
		runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
			registry.RegisterMuxerDescriptor(format.Descriptor{
				Format: customFormat,
				Codecs: []av.CodecID{customCodec},
			}, recordingMuxerFactory{format: customFormat})
		}),
	))
	defer task.Close()

	socket := startUnixServer(t, ctx, task)
	help := sendRequest(t, socket, ctlserver.Request{
		Op:   "help",
		Args: map[string]string{"topic": "attach"},
	})
	text, ok := help.Result.(string)
	if !help.OK || help.Error != nil || !ok {
		t.Fatalf("help = %+v", help)
	}
	for _, fragment := range []string{
		"Runtime encoders:",
		"encode codec=x_help_pcm media=audio",
		"Help PCM",
		"Runtime muxers:",
		"filesink location=<path> [format=x_helpmux]",
		"runtime-registered muxer for codecs x_help_pcm",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("help missing %q:\n%s", fragment, text)
		}
	}
}

func TestExternalHostCustomEncoderSpecCanMapArbitrarySettings(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const customCodec = av.CodecID("x_fancy_audio")
	factory := &recordingEncoderFactory{
		descriptor: codec.Descriptor{ID: customCodec, Name: "Fancy Audio", Type: av.MediaAudio},
	}
	task := newExternalTask(t, goavtest.Runtime(runconfig.WithEncoder(factory.descriptor, factory)))
	defer task.Close()

	var controlCalled atomic.Bool
	registry := ctlserver.PipelineRegistry{
		Encoders: []ctlserver.EncoderSpec{{
			Name: "fancyenc",
			Apply: func(args ctlserver.StepArgs) (codec.CodecSpec, error) {
				if args["bitrate"] == "" {
					return codec.CodecSpec{}, ctlserver.NewError(
						"missing_required",
						"parse branch pipeline",
						"bitrate",
						"fancyenc needs bitrate=<bps>",
						nil,
						[]string{"use fancyenc bitrate=128k"},
						nil,
					)
				}
				bitrate, err := parseTestRate(args["bitrate"])
				if err != nil {
					return codec.CodecSpec{}, ctlserver.NewError(
						"invalid_value",
						"parse branch pipeline",
						"bitrate",
						"fancyenc bitrate must be a positive integer or k-suffixed rate",
						[]string{"value=" + args["bitrate"]},
						[]string{"use fancyenc bitrate=128k"},
						err,
					)
				}
				return codec.Codec(customCodec, av.MediaAudio,
					codec.Bitrate(bitrate),
					codec.Profile(args["quality"]),
					codec.Control(func(any) error {
						controlCalled.Store(true)
						return nil
					}),
				), nil
			},
		}},
	}

	socket := startUnixServer(t, ctx, task, ctlserver.WithPipelineRegistry(registry))
	missing := sendRequest(t, socket, ctlserver.Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "bad-fancy",
		Pipeline: "fancyenc quality=cinema ! filesink location=" + filepath.Join(t.TempDir(), "missing.ogg") + " format=ogg",
	})
	if missing.OK || missing.Error == nil ||
		missing.Error.Code != "missing_required" ||
		missing.Error.Node != "bitrate" ||
		!strings.Contains(strings.Join(missing.Error.Suggestions, "\n"), "fancyenc bitrate=128k") {
		t.Fatalf("missing bitrate response = %+v", missing)
	}

	out := filepath.Join(t.TempDir(), "fancy.ogg")
	response := sendRequest(t, socket, ctlserver.Request{
		Op:       "attach",
		Tap:      "frames",
		Branch:   "fancy",
		Pipeline: "fancyenc bitrate=321k quality=cinema ! filesink location=" + out + " format=ogg",
	})
	if !response.OK || response.Error != nil {
		t.Fatalf("attach response = %+v", response)
	}
	if factory.config.Settings.Bitrate != 321000 ||
		factory.config.Settings.Profile != "cinema" ||
		factory.config.Settings.Control == nil {
		t.Fatalf("settings = %+v", factory.config.Settings)
	}
	if err := factory.config.Settings.Control(nil); err != nil || !controlCalled.Load() {
		t.Fatalf("control callback err=%v called=%v", err, controlCalled.Load())
	}
}

func TestPublicValidateCapabilitiesPreflightsCustomMetadata(t *testing.T) {
	type commandSettings struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"custom value"`
	}
	command := ctlserver.NewCommand[commandSettings](
		"vendor.preflight",
		"preflight command",
		func(context.Context, goav.LiveTask, commandSettings) (ctlserver.ControlResponse, error) {
			return ctlserver.ControlResponse{Operation: "control vendor.preflight"}, nil
		},
	)

	type stepSettings struct {
		Window time.Duration `goavctl:"window,duration" usage:"[window=<duration>]" help:"observation window"`
	}
	step := ctlserver.NewBranchStep[stepSettings](
		"meter",
		"preflight meter",
		func(*ctlserver.BranchPipeline, stepSettings) error { return nil },
	)

	type encoderSettings struct {
		Bitrate int `goavctl:"bitrate,required,rate" usage:"bitrate=<rate>" help:"target bitrate"`
	}
	encoder := ctlserver.NewEncoderSpec[encoderSettings](
		"acmeenc",
		"preflight encoder",
		func(args encoderSettings) (codec.CodecSpec, error) {
			return codec.Codec("acme", av.MediaAudio, codec.Bitrate(args.Bitrate)), nil
		},
	)

	err := ctlserver.ValidateCapabilities(ctlserver.CapabilitySet{
		Commands: []ctlserver.CommandSpec{command},
		Pipeline: ctlserver.PipelineRegistry{
			Steps:    []ctlserver.BranchPipelineStepSpec{step},
			Encoders: []ctlserver.EncoderSpec{encoder},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ctlserver.ValidateCapabilities(ctlserver.CapabilitySet{
		Pipeline: ctlserver.PipelineRegistry{
			Steps: []ctlserver.BranchPipelineStepSpec{ctlserver.NewBranchStep[stepSettings](
				"custom-copy",
				"bad alias",
				func(*ctlserver.BranchPipeline, stepSettings) error { return nil },
				ctlserver.Aliases("copy"),
			)},
		},
	})
	var structured *ctlserver.Error
	if !errors.As(err, &structured) ||
		structured.Code != "invalid_registry" ||
		structured.Node != "copy" {
		t.Fatalf("alias collision err = %+v", structured)
	}

	err = ctlserver.ValidateCapabilities(ctlserver.CapabilitySet{
		Commands: []ctlserver.CommandSpec{{
			Name:     "vendor.pointer",
			ArgsType: reflect.TypeOf(&commandSettings{}),
		}},
	})
	if !errors.As(err, &structured) ||
		structured.Code != "invalid_registry" ||
		structured.Node != "vendor.pointer" ||
		!strings.Contains(structured.Message, "ArgsType must be a struct") {
		t.Fatalf("pointer args err = %+v", structured)
	}
}

func TestPublicWrapperHelpersDelegateToControlSurface(t *testing.T) {
	success := ctlserver.SuccessResponse("done")
	if !success.OK || success.Result != "done" || success.Error != nil {
		t.Fatalf("success = %+v", success)
	}
	failed := ctlserver.ErrorResponse("test", fmt.Errorf("boom"))
	if failed.OK || failed.Error == nil || !strings.Contains(failed.Error.Message, "boom") {
		t.Fatalf("error = %+v", failed)
	}

	manifest := ctlserver.ControlManifest()
	bitrate, ok := ctlserver.LookupControlCommand("bitrate")
	if !ok {
		t.Fatal("missing bitrate command")
	}
	if found, ok := ctlserver.LookupCommand(manifest, "bitrate"); !ok || found.Name != bitrate.Name {
		t.Fatalf("lookup = %+v ok=%v", found, ok)
	}
	if usage := ctlserver.CommandUsage(bitrate); !strings.Contains(usage, "value=<rate>") {
		t.Fatalf("usage = %s", usage)
	}
	if help := ctlserver.CommandHelp(bitrate); !strings.Contains(help, "bits per second") {
		t.Fatalf("help = %s", help)
	}

	args, err := ctlserver.BindArgs(bitrate, []string{"stream=video", "value=1200k"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprintf("%+v", args), "1200000") {
		t.Fatalf("args = %+v", args)
	}
	args, err = ctlserver.BindJSON(bitrate, []byte(`{"stream":"video","value":"900k"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprintf("%+v", args), "900000") {
		t.Fatalf("json args = %+v", args)
	}

	control, err := ctlserver.DecodeRawControl([]byte(`{"type":"bitrate","stream_id":"video","bitrate":1200000}`))
	if err != nil {
		t.Fatal(err)
	}
	if control.Bitrate() != 1_200_000 {
		t.Fatalf("control = %+v", control)
	}
	event, err := ctlserver.DecodeRawEvent([]byte(`{"type":"vendor.force_idr","stream_id":"video"}`))
	if err != nil {
		t.Fatal(err)
	}
	if event.Type != "vendor.force_idr" || event.StreamID != "video" {
		t.Fatalf("event = %+v", event)
	}

	rootHelp, err := ctlserver.Help([]string{"graph"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rootHelp, "Mermaid") {
		t.Fatalf("root help = %s", rootHelp)
	}

	type wrapperCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"wrapper value"`
	}
	var applied string
	custom := ctlserver.CommandSpec{
		Name:     "wrapper",
		Summary:  "wrapper command",
		ArgsType: reflect.TypeOf(wrapperCommand{}),
		Apply: func(_ context.Context, _ goav.LiveTask, args any) (ctlserver.ControlResponse, error) {
			applied = args.(wrapperCommand).Value
			return ctlserver.ControlResponse{Operation: "control wrapper", Result: applied}, nil
		},
	}
	customHelp, err := ctlserver.HelpWithCommands([]string{"control", "wrapper"}, []ctlserver.CommandSpec{custom})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(customHelp, "wrapper command") {
		t.Fatalf("custom help = %s", customHelp)
	}
	registryHelp, err := ctlserver.HelpWithRegistry([]string{"attach"}, nil, ctlserver.PipelineRegistry{
		Steps: []ctlserver.BranchPipelineStepSpec{{
			Name:    "meter",
			Summary: "wrapper meter",
			Usage:   "[label=<text>]",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(registryHelp, "meter [label=<text>]") {
		t.Fatalf("registry help = %s", registryHelp)
	}

	response, err := ctlserver.Invoke(context.Background(), nil, custom, []string{"value=called"})
	if err != nil {
		t.Fatal(err)
	}
	if applied != "called" || response.Result != "called" {
		t.Fatalf("applied=%q response=%+v", applied, response)
	}
}

func TestPublicExecuteAndServeUnixWrappers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task := newExternalTask(t, goavtest.Runtime())
	defer task.Close()

	response := ctlserver.ExecuteRequest(ctx, task, ctlserver.Request{
		Op:   "help",
		Args: map[string]string{"topic": "graph"},
	})
	text, ok := response.Result.(string)
	if !response.OK || response.Error != nil || !ok || !strings.Contains(text, "Mermaid") {
		t.Fatalf("execute request = %+v", response)
	}
	direct, err := ctlserver.Execute(ctx, task, []string{"help", "attach"})
	if err != nil {
		t.Fatal(err)
	}
	text, ok = direct.Result.(string)
	if direct.Operation != "help" || !ok || !strings.Contains(text, "Built-in steps") {
		t.Fatalf("direct = %+v", direct)
	}

	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-serve-unix-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctlserver.ServeUnix(ctx, task, "unix://"+socket)
	}()
	waitForSocket(t, socket, errC)

	served := sendRequest(t, socket, ctlserver.Request{
		Op:   "help",
		Args: map[string]string{"topic": "graph"},
	})
	text, ok = served.Result.(string)
	if !served.OK || served.Error != nil || !ok || !strings.Contains(text, "Mermaid") {
		t.Fatalf("served = %+v", served)
	}
	cancel()
	select {
	case err := <-errC:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeUnix did not stop")
	}
}

func newExternalTask(t *testing.T, runtime *goav.Runtime) goav.LiveTask {
	t.Helper()
	task, err := goav.From(goavtest.Audio(48000, 2, []int16{1, 2, 3, 4})).
		Audio().
		Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(runtime).
		BuildLive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func startUnixServer(t *testing.T, ctx context.Context, task goav.LiveTask, options ...ctlserver.ServerOption) string {
	t.Helper()
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-ctl-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctlserver.ServeUnixWithOptions(ctx, task, "unix://"+socket, options...)
	}()
	waitForSocket(t, socket, errC)
	t.Cleanup(func() {
		select {
		case err := <-errC:
			if err != nil {
				t.Errorf("server error: %v", err)
			}
		default:
		}
	})
	return socket
}

func sendRequest(t *testing.T, socket string, request ctlserver.Request) ctlserver.Response {
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

func waitForSocket(t *testing.T, socket string, errC <-chan error) {
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
			t.Fatalf("socket %s was not created", socket)
		}
		time.Sleep(time.Millisecond)
	}
}

func parseTestRate(value string) (int, error) {
	var multiplier int
	switch {
	case strings.HasSuffix(value, "k"):
		multiplier = 1000
		value = strings.TrimSuffix(value, "k")
	default:
		multiplier = 1
	}
	var n int
	if _, err := fmt.Sscanf(value, "%d", &n); err != nil {
		return 0, err
	}
	return n * multiplier, nil
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

type recordingMuxerFactory struct {
	format av.FormatID
}

func (f recordingMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return recordingMuxer(f), nil
}

type recordingMuxer struct {
	format av.FormatID
}

func (m recordingMuxer) Format() av.FormatID { return m.format }
func (m recordingMuxer) Open(context.Context, format.Output, []av.Stream, format.OpenOptions) error {
	return nil
}
func (m recordingMuxer) Write(context.Context, *av.Packet, *format.WriteResult) error {
	return nil
}
func (m recordingMuxer) Close() error { return nil }
