package ctl_test

import (
	"context"
	"encoding/json"
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
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/goavtest"
)

func TestExternalHostCustomCommandOverSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type vendorCommand struct {
		Value string `goavctl:"value,required" usage:"value=<text>" help:"external value"`
	}
	var applied string
	command := ctl.CommandSpec{
		Name:     "vendor.external",
		Summary:  "external command",
		ArgsType: reflect.TypeOf(vendorCommand{}),
		Apply: func(_ context.Context, _ goav.Task, args any) (ctl.ControlResponse, error) {
			applied = args.(vendorCommand).Value
			return ctl.ControlResponse{Operation: "control vendor.external", Result: applied}, nil
		},
	}
	task := newExternalTask(t, goavtest.Runtime())
	defer task.Close()

	socket := startUnixServer(t, ctx, task, ctl.WithCommands(command))
	response := sendRequest(t, socket, ctl.Request{
		Op:   "control",
		Verb: "vendor.external",
		Args: map[string]string{"value": "from-socket"},
	})
	if !response.OK || response.Error != nil || applied != "from-socket" {
		t.Fatalf("response=%+v applied=%q", response, applied)
	}

	help := sendRequest(t, socket, ctl.Request{
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
	task := newExternalTask(t, goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory)))
	defer task.Close()

	var metered atomic.Int64
	registry := ctl.PipelineRegistry{
		Steps: []ctl.BranchPipelineStepSpec{{
			Name:    "meter",
			Summary: "external frame meter",
			Apply: func(branch *ctl.BranchPipeline, _ ctl.StepArgs) error {
				branch.Do(goav.FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit goav.Emit) error {
					metered.Add(1)
					return emit.Frame(frame)
				}))
				return nil
			},
		}},
	}

	socket := startUnixServer(t, ctx, task, ctl.WithPipelineRegistry(registry))
	out := filepath.Join(t.TempDir(), "custom.ogg")
	response := sendRequest(t, socket, ctl.Request{
		Op:     "attach",
		Tap:    "frames",
		Branch: "record",
		Pipeline: "meter ! encode codec=x_cli_pcm media=audio bitrate=123k profile=cinema level=1 " +
			"sample_rate=16000 channels=1 clock_rate=16000 keyframe_interval=7 fps=24 ! " +
			"filesink location=" + out + " format=ogg",
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

func TestExternalHostCustomEncoderSpecCanMapArbitrarySettings(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const customCodec = av.CodecID("x_fancy_audio")
	factory := &recordingEncoderFactory{
		descriptor: codec.Descriptor{ID: customCodec, Name: "Fancy Audio", Type: av.MediaAudio},
	}
	task := newExternalTask(t, goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory)))
	defer task.Close()

	var controlCalled atomic.Bool
	registry := ctl.PipelineRegistry{
		Encoders: []ctl.EncoderSpec{{
			Name: "fancyenc",
			Apply: func(args ctl.StepArgs) (codec.CodecSpec, error) {
				bitrate, err := parseTestRate(args["bitrate"])
				if err != nil {
					return codec.CodecSpec{}, err
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

	socket := startUnixServer(t, ctx, task, ctl.WithPipelineRegistry(registry))
	out := filepath.Join(t.TempDir(), "fancy.ogg")
	response := sendRequest(t, socket, ctl.Request{
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

func newExternalTask(t *testing.T, runtime goav.Runtime) goav.Task {
	t.Helper()
	task, err := goav.From(goavtest.Audio(48000, 2, []int16{1, 2, 3, 4})).
		Audio().
		Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(runtime).
		Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func startUnixServer(t *testing.T, ctx context.Context, task goav.Task, options ...ctl.ServerOption) string {
	t.Helper()
	socket := filepath.Join(os.TempDir(), fmt.Sprintf("goav-ctl-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() { _ = os.Remove(socket) })
	errC := make(chan error, 1)
	go func() {
		errC <- ctl.ServeUnixWithOptions(ctx, task, "unix://"+socket, options...)
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

func sendRequest(t *testing.T, socket string, request ctl.Request) ctl.Response {
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
