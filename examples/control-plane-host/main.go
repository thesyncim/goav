// Command control-plane-host starts a runnable goav task and exposes a Unix
// socket for experimenting with custom goav ctl commands, branch steps, and
// encoders.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const demoCodec = av.CodecID("x_acme_audio")

func main() {
	address := flag.String("control", "unix:///tmp/goav-control-plane-host.sock", "control socket address")
	duration := flag.Duration("duration", 0, "optional run duration; zero runs until interrupted")
	flag.Parse()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx := signalCtx
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(signalCtx, *duration)
		defer cancel()
	}

	if err := runHost(ctx, *address, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runHost(ctx context.Context, address string, out io.Writer) error {
	host, err := newDemoHost(ctx)
	if err != nil {
		return fmt.Errorf("new host: %w", err)
	}
	defer host.task.Close()

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	errC := make(chan error, 2)
	go func() {
		errC <- host.task.Run(runCtx)
	}()
	if err := waitForHostSource(runCtx, host.ready, errC); err != nil {
		stop()
		_ = host.task.Close()
		return fmt.Errorf("wait source: %w", err)
	}
	go func() {
		errC <- ctl.ServeUnixWithOptions(runCtx, host.task, address,
			ctl.WithCommands(host.commands...),
			ctl.WithPipelineRegistry(host.registry),
		)
	}()
	printUsage(out, address)

	var earlyErr error
	var earlyResult bool
	select {
	case <-ctx.Done():
	case earlyErr = <-errC:
		earlyResult = true
	}

	stop()
	_ = host.task.Close()
	if earlyResult && earlyErr != nil {
		if !expectedHostShutdownError(earlyErr) && (ctx.Err() == nil || !errors.Is(earlyErr, pipeline.ErrClosed)) {
			return fmt.Errorf("early result: %w", earlyErr)
		}
	} else if earlyResult && ctx.Err() == nil {
		return fmt.Errorf("early nil: pipeline stopped before host shutdown")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := drainHost(shutdownCtx, errC); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}

type demoHost struct {
	task     goav.Task
	commands []ctl.CommandSpec
	registry ctl.PipelineRegistry
	ready    <-chan struct{}
}

func newDemoHost(ctx context.Context) (*demoHost, error) {
	factory := &demoEncoderFactory{
		descriptor: codec.Descriptor{ID: demoCodec, Name: "ACME demo audio", Type: av.MediaAudio},
	}
	source := goavtest.NewTestSource("fixture",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48000, 1, av.SampleFormatS16)),
		goavtest.TestSourceLive(),
	)
	ready := make(chan struct{})
	input := goav.WrapSource(source.Input(), func(source pipeline.Source) pipeline.Source {
		return newStartReadySource(source, ready)
	})
	task, err := goav.From(input).
		Audio().Decode().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(goav.WithEncoder(factory.descriptor, factory))).
		Build(ctx)
	if err != nil {
		return nil, err
	}

	type setRate struct {
		Value  float64 `goavctl:"value,required" usage:"value=<float>" help:"playback rate"`
		Source string  `goavctl:"source,required" usage:"source=<source-name>" help:"source node to retime"`
	}
	command := ctl.CommandSpec{
		Name:     "vendor.rate",
		Summary:  "demo playback-rate control",
		ArgsType: reflect.TypeOf(setRate{}),
		Apply: func(ctx context.Context, task goav.Task, args any) (ctl.ControlResponse, error) {
			cmd := args.(setRate)
			if err := task.Control(ctx, goav.Rate(cmd.Value).At(pipeline.NodeRef(cmd.Source))); err != nil {
				return ctl.ControlResponse{}, err
			}
			return ctl.ControlResponse{
				Operation: "control vendor.rate",
				Result:    map[string]any{"source": cmd.Source, "value": cmd.Value},
			}, nil
		},
	}

	registry := ctl.PipelineRegistry{
		Steps: []ctl.BranchPipelineStepSpec{
			{
				Name:    "meter",
				Aliases: []string{"levelmeter"},
				Summary: "pass frames through a demo metering stage",
				Usage:   `[label=<text>]`,
				Apply: func(branch *ctl.BranchPipeline, args ctl.StepArgs) error {
					label := firstNonEmpty(args["label"], "meter")
					branch.Do(goav.FrameFunc("demo-"+label, func(_ context.Context, frame *av.Frame, emit goav.Emit) error {
						return emit.Frame(frame)
					}))
					return nil
				},
			},
			{
				Name:    "memorysink",
				Aliases: []string{"memsink"},
				Summary: "send messages to a demo in-process sink",
				Usage:   `[name=<text>]`,
				Apply: func(branch *ctl.BranchPipeline, args ctl.StepArgs) error {
					name := firstNonEmpty(args["name"], "memory")
					branch.Destination(goav.Sink(goav.SinkFunc("demo-"+name, func(context.Context, goav.Message) error {
						return nil
					})))
					return nil
				},
			},
		},
		Encoders: []ctl.EncoderSpec{{
			Name:    "acmeenc",
			Aliases: []string{"acme"},
			Summary: "demo encoder that maps native ACME settings",
			Usage:   `bitrate=<bps> quality=<profile> lookahead=<mode>`,
			Apply: func(args ctl.StepArgs) (codec.CodecSpec, error) {
				bitrate, err := parseDemoRate(args["bitrate"])
				if err != nil {
					return codec.CodecSpec{}, err
				}
				return codec.Codec(demoCodec, av.MediaAudio,
					codec.Bitrate(bitrate),
					codec.Profile(args["quality"]),
					codec.Control(func(native any) error {
						options, ok := native.(*demoNativeOptions)
						if !ok {
							return nil
						}
						options.Lookahead = args["lookahead"]
						return nil
					}),
				), nil
			},
		}},
	}
	return &demoHost{task: task, commands: []ctl.CommandSpec{command}, registry: registry, ready: ready}, nil
}

func printUsage(out io.Writer, address string) {
	fmt.Fprintf(out, "control=%s\n", address)
	fmt.Fprintf(out, "goav ctl --control %s help attach\n", address)
	fmt.Fprintf(out, "goav ctl --control %s help control vendor.rate\n", address)
	fmt.Fprintf(out, "goav ctl --control %s taps\n", address)
	fmt.Fprintf(out, "goav ctl --control %s control vendor.rate value=0.5 source=fixture\n", address)
	fmt.Fprintf(out, "goav ctl --control %s attach frames as archive 'meter label=\"left ! right\" ! acmeenc bitrate=128000 quality=voice lookahead=deep ! filesink location=\"/tmp/goav archive.ogg\" format=ogg'\n", address)
	fmt.Fprintf(out, "goav ctl --control %s attach frames as memory 'meter ! acmeenc bitrate=64000 quality=preview lookahead=shallow ! memorysink name=preview'\n", address)
	fmt.Fprintf(out, "goav ctl --control %s graph\n", address)
	fmt.Fprintf(out, "goav ctl --control %s graph format=text\n", address)
	fmt.Fprintf(out, "goav ctl --control %s rebranch archive 'meter ! acmeenc bitrate=96000 quality=voice lookahead=shallow ! filesink location=\"/tmp/goav archive-low.ogg\" format=ogg'\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach archive\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach memory\n", address)
}

func waitForHostSocket(ctx context.Context, address string, errC <-chan error) error {
	path := strings.TrimPrefix(address, "unix://")
	if path == "" {
		return fmt.Errorf("control address needs a unix socket path")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if hostSocketExists(path) {
			return nil
		}
		select {
		case err := <-errC:
			if hostSocketExists(path) && (err == nil || expectedHostShutdownError(err)) {
				return nil
			}
			if err == nil {
				return fmt.Errorf("control server stopped before creating socket")
			}
			return err
		case <-ctx.Done():
			if hostSocketExists(path) {
				return nil
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func hostSocketExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func waitForHostSource(ctx context.Context, ready <-chan struct{}, errC <-chan error) error {
	for {
		select {
		case <-ready:
			return nil
		case err := <-errC:
			if err == nil {
				return fmt.Errorf("pipeline stopped before control server started")
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func drainHost(ctx context.Context, errC <-chan error) error {
	var first error
	for i := 0; i < 2; i++ {
		select {
		case err := <-errC:
			if first == nil && err != nil && !expectedHostShutdownError(err) && !errors.Is(err, pipeline.ErrClosed) {
				first = err
			}
		case <-ctx.Done():
			if first == nil && ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
				first = ctx.Err()
			}
			return first
		}
	}
	return first
}

func expectedHostShutdownError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) ||
		err.Error() == context.Canceled.Error() ||
		errors.Is(err, net.ErrClosed)
}

type startReadySource struct {
	source pipeline.Source
	ready  chan<- struct{}
	once   sync.Once
}

func newStartReadySource(source pipeline.Source, ready chan<- struct{}) pipeline.Source {
	wrapped := &startReadySource{source: source, ready: ready}
	if _, ok := source.(pipeline.ControllableSource); ok {
		return &startReadyControllableSource{startReadySource: wrapped}
	}
	return wrapped
}

func (s *startReadySource) Name() string {
	return s.source.Name()
}

func (s *startReadySource) DescribeNode() pipeline.NodeSpec {
	if described, ok := s.source.(interface{ DescribeNode() pipeline.NodeSpec }); ok {
		return described.DescribeNode()
	}
	return pipeline.NodeSpec{Name: s.Name(), Kind: pipeline.NodeSource}
}

func (s *startReadySource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	s.once.Do(func() { close(s.ready) })
	return s.source.Start(ctx, emitter)
}

func (s *startReadySource) Close() error {
	return s.source.Close()
}

type startReadyControllableSource struct {
	*startReadySource
}

func (s *startReadyControllableSource) Control(ctx context.Context, msg *pipeline.Message) error {
	return s.source.(pipeline.ControllableSource).Control(ctx, msg)
}

func parseDemoRate(value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("bitrate is required")
	}
	multiplier := 1
	switch {
	case strings.HasSuffix(value, "k"):
		multiplier = 1000
		value = strings.TrimSuffix(value, "k")
	case strings.HasSuffix(value, "M"):
		multiplier = 1000_000
		value = strings.TrimSuffix(value, "M")
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("bitrate must be a positive integer, 128k, or 2M")
	}
	return parsed * multiplier, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type demoNativeOptions struct {
	Lookahead string
}

type demoEncoderFactory struct {
	descriptor codec.Descriptor
}

func (f *demoEncoderFactory) NewEncoder(_ context.Context, config codec.EncodeConfig) (codec.Encoder, error) {
	if control := config.Settings.Control; control != nil {
		_ = control(&demoNativeOptions{})
	}
	return demoEncoder{descriptor: f.descriptor}, nil
}

type demoEncoder struct {
	descriptor codec.Descriptor
}

func (e demoEncoder) Descriptor() codec.Descriptor { return e.descriptor }
func (e demoEncoder) Open(context.Context, codec.EncodeConfig) error {
	return nil
}
func (e demoEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
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
func (e demoEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (e demoEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (e demoEncoder) Close() error                                         { return nil }
