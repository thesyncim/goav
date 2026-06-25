package ctl_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/control"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	goavruntime "github.com/thesyncim/goav/runtime"
	"github.com/thesyncim/goav/shape"
)

func Example_bootstrapControlPlaneHost() {
	ctx := context.Background()
	const customCodec = av.CodecID("x_acme_audio")
	encoderFactory := &exampleEncoderFactory{
		descriptor: codec.Descriptor{ID: customCodec, Name: "ACME audio", Type: av.MediaAudio},
	}

	source := goavtest.NewTestSource("fixture",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 1, av.SampleFormatS16)),
		goavtest.TestSourceLive(),
	)
	task, err := goav.From(source.Input()).
		Audio().Decode().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(goavruntime.WithEncoder(encoderFactory.descriptor, encoderFactory))).
		Build(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer task.Close()

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	errC := make(chan error, 1)
	go func() {
		errC <- task.Run(runCtx)
	}()
	defer func() {
		stop()
		_ = task.Close()
		<-errC
	}()
	if err := waitExampleTaskRunning(task); err != nil {
		fmt.Println(err)
		return
	}

	type SetRate struct {
		Value  float64 `goavctl:"value,required" usage:"value=<float>" help:"playback rate"`
		Source string  `goavctl:"source,required" usage:"source=<source-name>" help:"source node to retime"`
	}
	command := ctl.NewCommand[SetRate](
		"vendor.rate",
		"vendor playback-rate control",
		func(ctx context.Context, task goav.LiveTask, cmd SetRate) (ctl.ControlResponse, error) {
			if err := task.Control(ctx, control.Rate(cmd.Value).At(pipeline.NodeRef(cmd.Source))); err != nil {
				return ctl.ControlResponse{}, err
			}
			return ctl.ControlResponse{
				Operation: "control vendor.rate",
				Result:    map[string]any{"value": cmd.Value},
			}, nil
		},
	)

	type MeterSettings struct {
		Window time.Duration `goavctl:"window,duration" usage:"[window=<duration>]" help:"observation window"`
	}
	meter := ctl.NewBranchStep[MeterSettings](
		"meter",
		"observe frames before encoding",
		func(branch *ctl.BranchPipeline, _ MeterSettings) error {
			branch.Do(component.FrameFunc("meter", func(_ context.Context, frame *av.Frame, emit component.Emit) error {
				return emit.Frame(frame)
			}))
			return nil
		},
	)

	type ACMESettings struct {
		Bitrate   int    `goavctl:"bitrate,required,rate" usage:"bitrate=<rate>" help:"target bitrate"`
		Quality   string `goavctl:"quality" usage:"[quality=<profile>]" help:"native quality profile"`
		Lookahead string `goavctl:"lookahead" usage:"[lookahead=<mode>]" help:"native lookahead mode"`
	}
	acme := ctl.NewEncoderSpec[ACMESettings](
		"acmeenc",
		"ACME audio encoder with native settings",
		func(args ACMESettings) (codec.CodecSpec, error) {
			return codec.Codec(customCodec, av.MediaAudio,
				codec.Bitrate(args.Bitrate),
				codec.Profile(args.Quality),
				codec.Control(func(native any) error {
					options := native.(*exampleNativeOptions)
					options.Lookahead = args.Lookahead
					return nil
				}),
			), nil
		},
	)
	capabilities := ctl.CapabilitySet{
		Commands: []ctl.CommandSpec{command},
		Pipeline: ctl.PipelineRegistry{
			Steps:    []ctl.BranchPipelineStepSpec{meter},
			Encoders: []ctl.EncoderSpec{acme},
		},
	}
	if err := ctl.ValidateCapabilities(capabilities); err != nil {
		fmt.Println(err)
		return
	}
	server := ctl.Server{Task: task}
	ctl.WithCapabilities(capabilities)(&server)

	rateRequest, _ := ctl.RequestFromCLI([]string{"control", "vendor.rate", "value=0.5", "source=fixture"})
	rateResponse := server.Handle(ctx, rateRequest)

	out := filepath.Join(os.TempDir(), "goav-control-plane-bootstrap.ogg")
	defer os.Remove(out)
	attachRequest, _ := ctl.RequestFromCLI([]string{
		"attach", "frames", "as", "archive",
		"meter ! acmeenc bitrate=128k quality=voice lookahead=deep ! filesink location=" + out + " format=ogg",
	})
	attachResponse := server.Handle(ctx, attachRequest)

	flowchart, err := graphrender.RenderTaskFlowchart(task)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("custom control ok:", rateResponse.OK)
	fmt.Println("custom branch ok:", attachResponse.OK)
	fmt.Println("encoder bitrate:", encoderFactory.config.Settings.Bitrate)
	fmt.Println("encoder quality:", encoderFactory.config.Settings.Profile)
	fmt.Println("native lookahead:", encoderFactory.native.Lookahead)
	fmt.Println("flowchart includes branch:", strings.Contains(flowchart, "branch=archive (attached)"))

	// Output:
	// custom control ok: true
	// custom branch ok: true
	// encoder bitrate: 128000
	// encoder quality: voice
	// native lookahead: deep
	// flowchart includes branch: true
}

func ExampleNewEncoderSpec_customEncoder() {
	const customCodec = av.CodecID("x_acme_audio")
	type ACMESettings struct {
		Profile string `goavctl:"profile,required" usage:"profile=<name>" help:"native profile"`
	}
	acme := ctl.NewEncoderSpec[ACMESettings](
		"acmeenc",
		"ACME audio encoder with native settings",
		func(args ACMESettings) (codec.CodecSpec, error) {
			return codec.Codec(customCodec, av.MediaAudio,
				codec.Profile(args.Profile),
				codec.Control(func(native any) error {
					// Type-assert native to the concrete encoder/options type
					// documented by the adapter package and apply every knob
					// that is not part of the common codec settings.
					return nil
				}),
			), nil
		},
	)
	_ = ctl.WithCapabilities(ctl.CapabilitySet{
		Pipeline: ctl.PipelineRegistry{Encoders: []ctl.EncoderSpec{acme}},
	})

	request, _ := ctl.RequestFromCLI([]string{
		"attach", "frames", "as", "archive",
		"acmeenc profile=cinema ! filesink location=archive.ogg",
	})
	fmt.Println(request.Pipeline)

	// Output:
	// acmeenc profile=cinema ! filesink location=archive.ogg
}

type exampleEncoderFactory struct {
	descriptor codec.Descriptor
	config     codec.EncodeConfig
	native     exampleNativeOptions
}

func (f *exampleEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	f.config = config
	if config.Settings.Control != nil {
		if err := config.Settings.Control(&f.native); err != nil {
			return nil, err
		}
	}
	return exampleEncoder{descriptor: f.descriptor}, nil
}

type exampleNativeOptions struct {
	Lookahead string
}

type exampleEncoder struct {
	descriptor codec.Descriptor
}

func (e exampleEncoder) Descriptor() codec.Descriptor { return e.descriptor }
func (e exampleEncoder) Open(context.Context, codec.EncodeConfig) error {
	return nil
}
func (e exampleEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	if frame == nil || len(out.Packets) == cap(out.Packets) {
		return nil
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	out.Packets[index].Reset()
	out.Packets[index].StreamID = frame.StreamID
	out.Packets[index].Type = frame.Type
	out.Packets[index].Keyframe = true
	out.Packets[index].Payload = av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}
	return nil
}
func (e exampleEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (e exampleEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (e exampleEncoder) Close() error                                         { return nil }

func waitExampleTaskRunning(task goav.LiveTask) error {
	deadline := time.Now().Add(time.Second)
	for {
		if task.Snapshot().State == lifecycle.TaskRunning {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("task did not start")
		}
		time.Sleep(time.Millisecond)
	}
}
