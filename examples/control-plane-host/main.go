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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/control"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	runconfig "github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/shape"
)

const demoCodec = av.CodecID("x_acme_video")

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
		errC <- ctlserver.ServeUnixWithOptions(runCtx, host.task, address,
			ctlserver.WithCapabilities(host.capabilities),
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
	task         goav.LiveTask
	capabilities ctlserver.CapabilitySet
	ready        <-chan struct{}
}

func newDemoHost(ctx context.Context) (*demoHost, error) {
	factory := &demoEncoderFactory{
		descriptor: codec.Descriptor{ID: demoCodec, Name: "ACME demo video", Type: av.MediaVideo},
	}
	source := goavtest.NewTestSource("fixture",
		shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(1280, 720, av.PixelFormatI420), shape.Realtime(true)),
		goavtest.TestSourceLive(),
	)
	ready := make(chan struct{})
	input := goav.WrapSource(source.Input(), func(source pipeline.Source) pipeline.Source {
		return newStartReadySource(source, ready)
	})
	task, err := goav.From(input).
		Video().Decode().Tap(goav.FrameTap("frames")).
		To(goavtest.NewCollector().Sink()).
		UseRuntime(goavtest.Runtime(runconfig.WithEncoder(factory.descriptor, factory))).
		BuildLive(ctx)
	if err != nil {
		return nil, err
	}

	type setPosition struct {
		Position time.Duration `goavctl:"position,required,duration" usage:"position=<duration>" help:"media position"`
		Source   string        `goavctl:"source,required" usage:"source=<source-name>" help:"source node to reposition"`
	}
	command := ctlserver.NewCommand[setPosition](
		"vendor.seek",
		"demo reposition control",
		func(ctx context.Context, task goav.LiveTask, cmd setPosition) (ctlserver.ControlResponse, error) {
			ctrl := control.Seek(cmd.Position)
			if err := task.Control(ctx, ctrl.At(pipeline.NodeRef(cmd.Source))); err != nil {
				return ctlserver.ControlResponse{}, err
			}
			return ctlserver.ControlResponse{
				Operation: "control vendor.seek",
				Result:    map[string]any{"source": cmd.Source, "position": cmd.Position.String()},
			}, nil
		},
	)
	type controlHistory struct {
		Type av.EventType `goavctl:"type" usage:"[type=seek|segment]" help:"optional source-control event type filter"`
	}
	controlsCommand := ctlserver.NewCommand[controlHistory](
		"fixture.controls",
		"report controls recorded by the fixture test source",
		func(_ context.Context, _ goav.LiveTask, query controlHistory) (ctlserver.ControlResponse, error) {
			return ctlserver.ControlResponse{
				Operation: "control fixture.controls",
				Result:    summarizeSourceControls(source, query.Type),
			}, nil
		},
	)

	type meterSettings struct {
		Label string `goavctl:"label" usage:"[label=<text>]" help:"diagnostic label"`
	}
	meterStep := ctlserver.NewBranchStep[meterSettings](
		"meter",
		"pass video frames through a demo metering stage",
		func(branch *ctlserver.BranchPipeline, args meterSettings) error {
			label := firstNonEmpty(args.Label, "meter")
			branch.Do(component.FrameFunc("demo-"+label, func(_ context.Context, frame *av.Frame, emit component.Emit) error {
				return emit.Frame(frame)
			}))
			return nil
		},
		ctlserver.Aliases("levelmeter"),
	)

	type thumbnailSettings struct {
		Every int    `goavctl:"every,required" usage:"every=<positive-int>" help:"keep every Nth frame"`
		Label string `goavctl:"label" usage:"[label=<text>]" help:"diagnostic label"`
	}
	thumbnailStep := ctlserver.NewBranchStep[thumbnailSettings](
		"thumbnail",
		"keep every Nth frame for thumbnail or preview branches",
		func(branch *ctlserver.BranchPipeline, args thumbnailSettings) error {
			every := args.Every
			if _, err := parsePositiveSetting(strconv.Itoa(every), "every"); err != nil {
				return err
			}
			var seen int
			name := demoNodeName("demo-thumbnail", args.Label)
			branch.Do(component.FrameFunc(name, func(_ context.Context, frame *av.Frame, emit component.Emit) error {
				seen++
				if (seen-1)%every != 0 {
					return nil
				}
				return emit.Frame(frame)
			}))
			return nil
		},
		ctlserver.Aliases("thumbs", "sampleframes"),
	)

	type memorySinkSettings struct {
		Name string `goavctl:"name" usage:"[name=<text>]" help:"sink name"`
	}
	memorySinkStep := ctlserver.NewBranchStep[memorySinkSettings](
		"memorysink",
		"send messages to a demo in-process sink",
		func(branch *ctlserver.BranchPipeline, args memorySinkSettings) error {
			name := firstNonEmpty(args.Name, "memory")
			branch.Destination(goav.Sink(component.SinkFunc("demo-"+name, func(context.Context, component.Message) error {
				return nil
			})))
			return nil
		},
		ctlserver.Aliases("memsink"),
	)

	type acmeSettings struct {
		Bitrate   int    `goavctl:"bitrate,required,rate" usage:"bitrate=<rate>" help:"target bitrate"`
		Quality   string `goavctl:"quality" usage:"[quality=<profile>]" help:"native quality profile"`
		Lookahead string `goavctl:"lookahead" usage:"[lookahead=<mode>]" help:"native lookahead mode"`
	}
	acmeEncoder := ctlserver.NewEncoderSpec[acmeSettings](
		"acmeenc",
		"demo video encoder that maps native ACME settings",
		func(args acmeSettings) (codec.Spec, error) {
			return codec.Codec(demoCodec, av.MediaVideo,
				codec.Bitrate(args.Bitrate),
				codec.Profile(args.Quality),
				codec.Control(func(native any) error {
					options, ok := native.(*demoNativeOptions)
					if !ok {
						return nil
					}
					options.Lookahead = args.Lookahead
					return nil
				}),
			), nil
		},
		ctlserver.Aliases("acme"),
	)

	capabilities := ctlserver.CapabilitySet{
		Commands: []ctlserver.CommandSpec{command, controlsCommand},
		Pipeline: ctlserver.PipelineRegistry{
			Steps:    []ctlserver.BranchPipelineStepSpec{meterStep, thumbnailStep, memorySinkStep},
			Encoders: []ctlserver.EncoderSpec{acmeEncoder},
		},
	}
	if err := ctlserver.ValidateCapabilities(capabilities); err != nil {
		_ = task.Close()
		return nil, err
	}
	return &demoHost{task: task, capabilities: capabilities, ready: ready}, nil
}

func printUsage(out io.Writer, address string) {
	fmt.Fprintf(out, "control=%s\n", address)
	fmt.Fprintf(out, "# fake source: live VP8 camera -> decode -> frame tap named frames\n")
	fmt.Fprintf(out, "# inspect the server-aware grammar\n")
	fmt.Fprintf(out, "goav ctl --control %s help attach\n", address)
	fmt.Fprintf(out, "goav ctl --control %s help control vendor.seek\n", address)
	fmt.Fprintf(out, "goav ctl --control %s help control fixture.controls\n", address)
	fmt.Fprintf(out, "goav ctl --control %s capabilities\n", address)
	fmt.Fprintf(out, "goav ctl --control %s taps\n", address)
	fmt.Fprintf(out, "# drive the goavtest TestSource through normal source controls\n")
	fmt.Fprintf(out, "goav ctl --control %s control seek position=2s source=fixture\n", address)
	fmt.Fprintf(out, "goav ctl --control %s control segment start=1s end=3s source=fixture\n", address)
	fmt.Fprintf(out, "goav ctl --control %s control fixture.controls\n", address)
	fmt.Fprintf(out, "goav ctl --control %s control fixture.controls type=seek\n", address)
	fmt.Fprintf(out, "goav ctl --control %s control vendor.seek position=2s source=fixture\n", address)
	fmt.Fprintf(out, "# use the raw JSON fallback when automation already has a control protocol or av.Event payload\n")
	fmt.Fprintf(out, "goav ctl --control %s control --json '{\"type\":\"seek\",\"position\":\"1.5s\",\"node\":\"fixture\"}'\n", address)
	fmt.Fprintf(out, "goav ctl --control %s control deliver --json '{\"type\":\"vendor.force_idr\",\"stream_id\":\"video\",\"reason\":\"manual\",\"metadata\":{\"source\":\"cli\"}}' at=frames\n", address)
	fmt.Fprintf(out, "# attach a stock VP8/WebM transcode from the decoded frame tap\n")
	fmt.Fprintf(out, "goav ctl --control %s attach frames as archive 'meter label=\"left ! right\" ! resize width=640 height=360 ! encode codec=vp8 media=video bitrate=900k fps=30 keyframe_interval=30 ! filesink location=\"/tmp/goav archive.webm\"'\n", address)
	fmt.Fprintf(out, "# attach a low-rate thumbnail/preview recording\n")
	fmt.Fprintf(out, "goav ctl --control %s attach frames as thumbnails 'thumbnail every=5 label=sample ! resize width=160 height=90 ! encode codec=vp8 media=video bitrate=160k fps=1 keyframe_interval=1 ! filesink location=\"/tmp/goav thumbnails.ivf\"'\n", address)
	fmt.Fprintf(out, "# attach an in-process thumbnail sink for app-owned diagnostics\n")
	fmt.Fprintf(out, "goav ctl --control %s attach frames as memory 'thumbnail every=3 label=preview ! memorysink name=preview'\n", address)
	fmt.Fprintf(out, "# attach the runtime-registered custom encoder through the default generic encode step\n")
	fmt.Fprintf(out, "goav ctl --control %s attach frames as acme-generic 'thumbnail every=4 label=generic ! encode codec=x_acme_video media=video bitrate=220k profile=preview fps=2 keyframe_interval=1 lookahead=deep ! memorysink name=acme-generic'\n", address)
	fmt.Fprintf(out, "# write the generic custom encoder through a runtime-registered muxer\n")
	fmt.Fprintf(out, "goav ctl --control %s attach frames as acme-file 'thumbnail every=6 label=file ! encode codec=x_acme_video media=video bitrate=320k profile=file fps=2 keyframe_interval=1 lookahead=file ! filesink location=\"/tmp/goav acme.webm\"'\n", address)
	fmt.Fprintf(out, "# attach a custom encoder spelling only when native settings need host code\n")
	fmt.Fprintf(out, "goav ctl --control %s attach frames as acme-preview 'thumbnail every=2 label=acme ! acmeenc bitrate=250k quality=preview lookahead=shallow ! memorysink name=acme-preview'\n", address)
	fmt.Fprintf(out, "# render the running graph with branch lifecycle annotations\n")
	fmt.Fprintf(out, "goav ctl --control %s graph\n", address)
	fmt.Fprintf(out, "goav ctl --control %s graph format=text\n", address)
	fmt.Fprintf(out, "# retarget a live in-process branch without rebuilding the host task\n")
	fmt.Fprintf(out, "goav ctl --control %s rebranch memory 'thumbnail every=10 label=slow ! memorysink name=slow-preview'\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach archive\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach thumbnails\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach memory\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach acme-generic\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach acme-file\n", address)
	fmt.Fprintf(out, "goav ctl --control %s detach acme-preview\n", address)
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

func parsePositiveSetting(value string, name string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, ctlserver.NewError(
			"invalid_value",
			"parse branch pipeline",
			name,
			name+" must be a positive integer",
			[]string{"value=" + value},
			[]string{"use " + name + "=5"},
			err,
		)
	}
	return parsed, nil
}

type sourceControlSummary struct {
	Source   string               `json:"source"`
	Type     av.EventType         `json:"type,omitempty"`
	Count    int                  `json:"count"`
	Controls []sourceControlEvent `json:"controls"`
}

type sourceControlEvent struct {
	Type     av.EventType `json:"type"`
	Stream   av.StreamID  `json:"stream,omitempty"`
	Position string       `json:"position,omitempty"`
	End      string       `json:"end,omitempty"`
	Rate     float64      `json:"rate,omitempty"`
	Reason   string       `json:"reason,omitempty"`
}

func summarizeSourceControls(source *goavtest.TestSource, typ av.EventType) sourceControlSummary {
	summary := sourceControlSummary{Source: "fixture", Type: typ}
	if source == nil {
		return summary
	}
	for _, event := range source.Controls() {
		if typ != "" && event.Type != typ {
			continue
		}
		summary.Controls = append(summary.Controls, sourceControlEventFromEvent(event))
	}
	summary.Count = len(summary.Controls)
	return summary
}

func sourceControlEventFromEvent(event av.Event) sourceControlEvent {
	out := sourceControlEvent{
		Type:   event.Type,
		Stream: event.StreamID,
		Reason: event.Reason,
	}
	if position, ok := event.Timestamp.ToDuration(); ok {
		out.Position = position.String()
	}
	if rate, ok := av.EventRateValue(&event); ok {
		out.Rate = rate
	}
	if end, ok := av.EventSegmentEnd(&event); ok {
		out.End = end.String()
	}
	return out
}

func demoNodeName(prefix string, label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return prefix
	}
	var out strings.Builder
	for _, r := range label {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-',
			r == '_':
			out.WriteRune(r)
		default:
			out.WriteByte('-')
		}
	}
	if out.Len() == 0 {
		return prefix
	}
	return prefix + "-" + out.String()
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
