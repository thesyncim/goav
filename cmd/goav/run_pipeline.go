package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/internal/cliargs"
	"github.com/thesyncim/goav/internal/codecargs"
	"github.com/thesyncim/goav/internal/fileargs"
	"github.com/thesyncim/goav/internal/sourceargs"
	"github.com/thesyncim/goav/internal/transformargs"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/std"
)

const defaultRunPipeline = "testsrc video width=1280 height=720 fps=30 duration=3s realtime=true ! encode codec=av1 media=video bitrate=1200k fps=30 keyframe_interval=60 ! filesink location=/tmp/goav-av1.mkv format=matroska"

const generatedRawVideoCodec = av.CodecID("goav_raw_video")

type runPipelineConfig struct {
	runtime  string
	control  string
	pipeline string
}

type runPipelinePlan struct {
	source      generatedVideoSource
	ops         []runOperation
	destination fileDestination
}

type generatedVideoSource struct {
	name        string
	width       int
	height      int
	fps         fpsValue
	frames      int
	realtime    bool
	pixelFormat string
	pattern     string
}

type fpsValue struct {
	num int
	den int
}

type runOperation struct {
	kind   string
	name   string
	width  int
	height int
	codec  codec.CodecSpec
}

type fileDestination struct {
	location string
	format   av.FormatID
}

type runPipelineResult struct {
	Runtime     string `json:"runtime"`
	Source      string `json:"source"`
	Frames      int    `json:"frames"`
	Realtime    bool   `json:"realtime"`
	Video       string `json:"video"`
	Codec       string `json:"codec,omitempty"`
	Output      string `json:"output"`
	Format      string `json:"format,omitempty"`
	Control     string `json:"control,omitempty"`
	Description string `json:"description"`
}

func runPipelineCommand(argv []string, stdout io.Writer, stderr io.Writer) int {
	config, err := parseRunPipelineArgs(argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if config.pipeline == "" {
		fmt.Fprint(stdout, runPipelineHelp())
		return 0
	}
	plan, err := parseRunPipeline(config.pipeline)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	result, err := executeRunPipeline(context.Background(), config.runtime, config.control, plan)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		printErr(stderr, err)
		return 1
	}
	return 0
}

func parseRunPipelineArgs(argv []string) (runPipelineConfig, error) {
	config := runPipelineConfig{runtime: "demo"}
	var parts []string
	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		switch {
		case arg == "--help" || arg == "-h" || arg == "help":
			return runPipelineConfig{}, nil
		case arg == "--runtime":
			if i+1 >= len(argv) {
				return config, fmt.Errorf("--runtime needs demo, default, or test")
			}
			config.runtime = strings.ToLower(argv[i+1])
			i++
		case strings.HasPrefix(arg, "--runtime="):
			config.runtime = strings.ToLower(strings.TrimPrefix(arg, "--runtime="))
		case arg == "--control":
			if i+1 >= len(argv) {
				return config, fmt.Errorf("--control needs unix://PATH")
			}
			config.control = argv[i+1]
			i++
		case strings.HasPrefix(arg, "--control="):
			config.control = strings.TrimPrefix(arg, "--control=")
		default:
			parts = append(parts, arg)
		}
	}
	config.pipeline = strings.TrimSpace(strings.Join(parts, " "))
	return config, nil
}

func runPipelineHelp() string {
	return "usage: goav run [--runtime demo|default|test] [--control unix://PATH] '<pipeline>'\n\n" +
		"examples:\n" +
		"  goav run '" + defaultRunPipeline + "'\n" +
		"  goav run 'testsrc video width=1280 height=720 fps=30 duration=3s realtime=true pattern=bars ! encode codec=av1 media=video bitrate=1200k fps=30 keyframe_interval=60 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=/tmp/goav-av1.ivf'\n\n" +
		"pipeline steps:\n" +
		"  testsrc video [name=<id>] width=<px> height=<px> fps=<n[/d]|decimal> frames=<n>|duration=<d> realtime=<bool> [format=i420|yuv420p] [pattern=bars|gradient|solid]\n" +
		"  tap name=<tap-name>\n" +
		"  resize width=<px> height=<px>\n" +
		"  encode codec=<id> media=<video|audio> bitrate=<rate> fps=<n[/d]> keyframe_interval=<n> [native_key=value...]\n" +
		"  encoder settings use canonical names; duplicate aliases such as rate, framerate, keyint, gop, samplerate, ch, clockrate, and bitrate_bps are rejected\n" +
		"  filesink location=<path> [format=<id>] (known file extensions infer the format)\n\n" +
		"control example:\n" +
		"  goav run --control unix:///tmp/goav-live.sock 'testsrc video name=fixture width=1280 height=720 fps=30 duration=30s realtime=true pattern=bars ! tap name=frames ! encode codec=av1 media=video bitrate=1200k fps=30 keyframe_interval=60 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=/tmp/goav-av1.mkv format=matroska'\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock taps\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock graph\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock control rate value=0.5 source=fixture\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock control seek position=2s source=fixture\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock attach frames as preview 'resize width=320 height=180 ! encode codec=av1 media=video bitrate=300k fps=2 keyframe_interval=1 ! filesink location=/tmp/goav-preview.ivf'\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock stop\n"
}

func parseRunPipeline(text string) (runPipelinePlan, error) {
	steps, err := splitPipelineSteps(text)
	if err != nil {
		return runPipelinePlan{}, err
	}
	if len(steps) < 2 {
		return runPipelinePlan{}, fmt.Errorf("goav run: pipeline needs a source and a filesink")
	}
	sourceTokens, err := tokenizeStep(steps[0])
	if err != nil {
		return runPipelinePlan{}, err
	}
	source, err := parseGeneratedVideoSource(sourceTokens)
	if err != nil {
		return runPipelinePlan{}, err
	}
	destTokens, err := tokenizeStep(steps[len(steps)-1])
	if err != nil {
		return runPipelinePlan{}, err
	}
	destination, err := parseFileDestination(destTokens)
	if err != nil {
		return runPipelinePlan{}, err
	}
	plan := runPipelinePlan{source: source, destination: destination}
	encoded := false
	for _, step := range steps[1 : len(steps)-1] {
		tokens, err := tokenizeStep(step)
		if err != nil {
			return runPipelinePlan{}, err
		}
		op, err := parseRunOperation(tokens)
		if err != nil {
			return runPipelinePlan{}, err
		}
		if op.kind == "encode" {
			encoded = true
		}
		plan.ops = append(plan.ops, op)
	}
	if !encoded {
		return runPipelinePlan{}, fmt.Errorf("goav run: filesink needs an encode step for generated frame sources")
	}
	return plan, nil
}

func executeRunPipeline(ctx context.Context, runtimeName string, control string, plan runPipelinePlan) (runPipelineResult, error) {
	file, err := openOutputFile(plan.destination.location)
	if err != nil {
		return runPipelineResult{}, err
	}
	defer file.Close()

	destOpts := []goav.DestinationOption(nil)
	if plan.destination.format != "" {
		destOpts = append(destOpts, goav.Format(plan.destination.format))
	}
	dest := goav.File(plan.destination.location, file, destOpts...)
	runtime, runtimeLabel, err := runtimeForRun(runtimeName, plan)
	if err != nil {
		return runPipelineResult{}, err
	}
	task, encoded, err := buildRunPipelineTask(ctx, runtime, plan, dest)
	if err != nil {
		return runPipelineResult{}, err
	}
	defer task.Close()
	if control != "" {
		if err := runPipelineTaskWithControl(ctx, task, control); err != nil {
			return runPipelineResult{}, err
		}
	} else if err := task.Run(ctx); err != nil {
		return runPipelineResult{}, err
	}
	outputFormat := reportedFileDestinationFormat(ctx, runtime, plan.destination)
	return runPipelineResult{
		Runtime:     runtimeLabel,
		Source:      plan.source.name,
		Frames:      plan.source.frames,
		Realtime:    plan.source.realtime,
		Video:       fmt.Sprintf("%dx%d@%s", plan.source.width, plan.source.height, plan.source.fps.String()),
		Codec:       string(encoded.ID),
		Output:      plan.destination.location,
		Format:      string(outputFormat),
		Control:     control,
		Description: "generated video source encoded and written successfully",
	}, nil
}

func reportedFileDestinationFormat(ctx context.Context, runtime *goav.Runtime, dest fileDestination) av.FormatID {
	if dest.format != "" || runtime == nil {
		return dest.format
	}
	result, err := runtime.Probe(ctx, format.ProbeRequest{
		Name: dest.location,
		Input: format.Input{
			Name:     dest.location,
			Protocol: av.ProtocolFile,
		},
	})
	if err != nil {
		return ""
	}
	return result.Format
}

func buildRunPipelineTask(ctx context.Context, runtime *goav.Runtime, plan runPipelinePlan, dest goav.Destination) (goav.Task, codec.CodecSpec, error) {
	source := plan.source.input()
	job := goav.From(source)
	stream := job.Video(goav.InputName(plan.source.name))
	var encoded codec.CodecSpec
	for _, op := range plan.ops {
		switch op.kind {
		case "tap":
			stream = stream.Tap(goav.FrameTap(op.name))
		case "resize":
			stream = stream.Resize(op.width, op.height)
		case "encode":
			encoded = op.codec
			stream = stream.Encode(op.codec)
		default:
			return nil, codec.CodecSpec{}, fmt.Errorf("goav run: unsupported operation %q", op.kind)
		}
	}
	task, err := stream.To(dest).UseRuntime(runtime).Build(ctx)
	if err != nil {
		return nil, codec.CodecSpec{}, err
	}
	return task, encoded, nil
}

func runPipelineTaskWithControl(ctx context.Context, task goav.Task, control string) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errC := make(chan error, 2)
	go func() {
		errC <- task.Run(runCtx)
	}()
	go func() {
		errC <- ctl.ServeUnix(runCtx, task, control)
	}()
	var first error
	for i := 0; i < 2; i++ {
		err := <-errC
		if expectedRunShutdownError(err) {
			err = nil
		}
		if err != nil && first == nil {
			first = err
			cancel()
			_ = task.Close()
		}
		if i == 0 {
			cancel()
		}
	}
	return first
}

func expectedRunShutdownError(err error) bool {
	return err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, goav.ErrClosed) ||
		errors.Is(err, pipeline.ErrClosed)
}

func runtimeForRun(name string, plan runPipelinePlan) (*goav.Runtime, string, error) {
	if name == "" {
		name = "demo"
	}
	codecIDs := plan.encodeCodecIDs()
	switch name {
	case "demo":
		return std.MustNew(goav.WithClock(goavtest.NewClock())), "demo", nil
	case "default", "std", "standard":
		return std.MustNew(), "default", nil
	case "test", "fake", "deterministic":
		opts := make([]goav.Option, 0, len(codecIDs))
		for _, id := range codecIDs {
			if !wellKnownTestCodec(id) {
				opts = append(opts, goavtest.Codec(id))
			}
		}
		return goavtest.Runtime(opts...), "test", nil
	default:
		return nil, "", fmt.Errorf("goav run: unsupported runtime %q (want demo, default, or test)", name)
	}
}

func (p runPipelinePlan) encodeCodecIDs() []av.CodecID {
	var ids []av.CodecID
	seen := map[av.CodecID]bool{}
	for _, op := range p.ops {
		if op.kind != "encode" || op.codec.ID == "" || seen[op.codec.ID] {
			continue
		}
		seen[op.codec.ID] = true
		ids = append(ids, op.codec.ID)
	}
	return ids
}

func wellKnownTestCodec(id av.CodecID) bool {
	switch id {
	case av.CodecOpus, av.CodecVorbis, av.CodecFLAC, av.CodecAAC,
		av.CodecVP8, av.CodecVP9, av.CodecH264, av.CodecAV1,
		av.CodecPCM, av.CodecTextUTF8:
		return true
	default:
		return false
	}
}

func openOutputFile(location string) (*os.File, error) {
	if location == "" {
		return nil, fmt.Errorf("goav run: filesink location is required")
	}
	dir := filepath.Dir(location)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return os.Create(location)
}

func (s generatedVideoSource) input() goav.InputSpec {
	return goav.Input(generatedVideoProvider{source: s})
}

func (s generatedVideoSource) shape() shape.Spec {
	return shape.Frame(av.MediaVideo,
		shape.Video(s.width, s.height, s.pixelFormat),
		shape.Stream(av.StreamID(s.name)),
		shape.Codec(generatedRawVideoCodec),
		shape.Realtime(s.realtime),
	)
}

func (s generatedVideoSource) stream() av.Stream {
	base := av.TimeBase{Num: int64(s.fps.den), Den: int64(s.fps.num)}
	return av.Stream{
		ID:       av.StreamID(s.name),
		Type:     av.MediaVideo,
		TimeBase: base,
		Codec: av.CodecParameters{
			ID:          generatedRawVideoCodec,
			Type:        av.MediaVideo,
			Width:       s.width,
			Height:      s.height,
			PixelFormat: s.pixelFormat,
			ClockRate:   uint32(s.fps.num / max(s.fps.den, 1)),
		},
		Name: s.name,
	}
}

type generatedVideoProvider struct {
	source generatedVideoSource
}

func (p generatedVideoProvider) Name() string {
	return p.source.name
}

func (p generatedVideoProvider) Detail() string {
	return "generated test video source"
}

func (p generatedVideoProvider) SourceShape() shape.Spec {
	return p.source.shape()
}

func (p generatedVideoProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	return newGeneratedVideoPipelineSource(p.source), []av.Stream{p.source.stream()}, nil
}

type generatedVideoPipelineSource struct {
	source generatedVideoSource
	mu     sync.Mutex

	next          int
	segmentEnd    int
	rate          float64
	discontinuity bool
	closed        bool
}

var _ pipeline.ControllableSource = (*generatedVideoPipelineSource)(nil)

func newGeneratedVideoPipelineSource(source generatedVideoSource) *generatedVideoPipelineSource {
	return &generatedVideoPipelineSource{
		source:     source,
		segmentEnd: source.frames,
		rate:       1,
	}
}

func (s *generatedVideoPipelineSource) Name() string {
	if s == nil {
		return ""
	}
	return s.source.name
}

func (s *generatedVideoPipelineSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.Name(), Kind: pipeline.NodeSource, Detail: "generated test video source"}
}

func (s *generatedVideoPipelineSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if s == nil {
		return nil
	}
	for {
		index, discontinuity, rate, done := s.nextFrame()
		if done {
			return s.emitEOS(ctx, emitter)
		}
		if discontinuity {
			event := av.Event{Type: av.EventDiscontinuity, StreamID: av.StreamID(s.source.name), Reason: "source control repositioned generated test video"}
			if err := s.emit(ctx, emitter, pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}); err != nil {
				return err
			}
		}
		frame := s.source.frame(index)
		if err := s.emit(ctx, emitter, pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}); err != nil {
			return err
		}
		if s.source.realtime {
			if err := waitGeneratedFrame(ctx, s.source.frameDuration(rate)); err != nil {
				return nil
			}
		}
	}
}

func (s *generatedVideoPipelineSource) Control(_ context.Context, msg *pipeline.Message) error {
	if s == nil {
		return nil
	}
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return fmt.Errorf("goav run: generated source control expects an event message")
	}
	event := *msg.Event
	s.mu.Lock()
	defer s.mu.Unlock()
	switch event.Type {
	case av.EventRate:
		rate, ok := av.EventRateValue(&event)
		if !ok {
			return fmt.Errorf("goav run: malformed rate control")
		}
		s.rate = rate
	case av.EventSeek:
		position, ok := event.Timestamp.ToDuration()
		if !ok {
			return fmt.Errorf("goav run: malformed seek control")
		}
		s.next = s.source.frameIndex(position, false)
		s.segmentEnd = s.source.frames
		s.discontinuity = true
	case av.EventSegment:
		start, ok := event.Timestamp.ToDuration()
		if !ok {
			return fmt.Errorf("goav run: malformed segment start")
		}
		end, ok := av.EventSegmentEnd(&event)
		if !ok || end <= start {
			return fmt.Errorf("goav run: malformed segment end")
		}
		s.next = s.source.frameIndex(start, false)
		s.segmentEnd = s.source.frameIndex(end, true)
		if s.segmentEnd <= s.next {
			s.segmentEnd = min(s.next+1, s.source.frames)
		}
		s.discontinuity = true
	default:
		return fmt.Errorf("goav run: generated source cannot apply control %q", event.Type)
	}
	return nil
}

func (s *generatedVideoPipelineSource) Close() error {
	if s != nil {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
	return nil
}

func (s *generatedVideoPipelineSource) nextFrame() (int, bool, float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, false, 1, true
	}
	if s.rate <= 0 || math.IsInf(s.rate, 1) || math.IsNaN(s.rate) {
		s.rate = 1
	}
	end := s.segmentEnd
	if end <= 0 || end > s.source.frames {
		end = s.source.frames
		s.segmentEnd = end
	}
	if s.next >= end {
		return 0, false, s.rate, true
	}
	index := s.next
	s.next++
	discontinuity := s.discontinuity
	s.discontinuity = false
	return index, discontinuity, s.rate, false
}

func (s *generatedVideoPipelineSource) emitEOS(ctx context.Context, emitter pipeline.Emitter) error {
	event := av.Event{Type: av.EventEndOfStream, StreamID: av.StreamID(s.source.name)}
	return s.emit(ctx, emitter, pipeline.Message{Kind: pipeline.MessageEvent, Event: &event})
}

func (s *generatedVideoPipelineSource) emit(ctx context.Context, emitter pipeline.Emitter, msg pipeline.Message) error {
	for {
		err := emitter.Emit(ctx, &msg)
		if err == nil {
			return nil
		}
		if errors.Is(err, pipeline.ErrBackpressure) {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		if errors.Is(err, pipeline.ErrClosed) {
			return nil
		}
		return err
	}
}

func waitGeneratedFrame(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s generatedVideoSource) frameDuration(rate float64) time.Duration {
	if rate <= 0 || math.IsInf(rate, 1) || math.IsNaN(rate) || s.fps.num <= 0 {
		rate = 1
	}
	seconds := float64(s.fps.den) / float64(s.fps.num) / rate
	return time.Duration(seconds * float64(time.Second))
}

func (s generatedVideoSource) frameIndex(position time.Duration, ceil bool) int {
	if position <= 0 || s.fps.num <= 0 || s.fps.den <= 0 {
		return 0
	}
	exact := position.Seconds() * float64(s.fps.num) / float64(s.fps.den)
	var index int
	if ceil {
		index = int(math.Ceil(exact))
	} else {
		index = int(math.Floor(exact))
	}
	if index < 0 {
		return 0
	}
	if index > s.frames {
		return s.frames
	}
	return index
}

func (s generatedVideoSource) frame(n int) *av.Frame {
	chromaW := (s.width + 1) / 2
	chromaH := (s.height + 1) / 2
	luma := make([]byte, s.width*s.height)
	for y := 0; y < s.height; y++ {
		row := y * s.width
		for x := 0; x < s.width; x++ {
			luma[row+x] = s.luma(x, y, n)
		}
	}
	cb := make([]byte, chromaW*chromaH)
	cr := make([]byte, chromaW*chromaH)
	for i := range cb {
		cb[i] = 128
		cr[i] = 128
	}
	base := av.TimeBase{Num: int64(s.fps.den), Den: int64(s.fps.num)}
	return &av.Frame{
		StreamID: av.StreamID(s.name),
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n), Base: base},
		Duration: av.Duration{Value: 1, Base: base},
		Video:    &av.VideoFrame{Width: s.width, Height: s.height, PixelFormat: s.pixelFormat},
		Planes: []av.Plane{
			{Buffer: av.Buffer{Bytes: luma, Ownership: av.BufferImmutable}, Stride: s.width},
			{Buffer: av.Buffer{Bytes: cb, Ownership: av.BufferImmutable}, Stride: chromaW},
			{Buffer: av.Buffer{Bytes: cr, Ownership: av.BufferImmutable}, Stride: chromaW},
		},
	}
}

func (s generatedVideoSource) luma(x, y, n int) byte {
	switch s.pattern {
	case "bars":
		bars := []byte{32, 64, 96, 128, 160, 192, 224, 240}
		index := x * len(bars) / max(s.width, 1)
		return bars[index%len(bars)] + byte(n%16)
	case "solid":
		return byte(n)
	default:
		return byte((x + y + n*3) % 256)
	}
}

func parseGeneratedVideoSource(tokens []string) (generatedVideoSource, error) {
	if len(tokens) == 0 {
		return generatedVideoSource{}, fmt.Errorf("goav run: source step is empty")
	}
	positionals, options, err := parseKeyValuesOrdered(tokens[1:])
	if err != nil {
		return generatedVideoSource{}, err
	}
	source, err := sourceargs.ParseGeneratedVideo(tokens[0], runSourceArgs(positionals, options))
	if err != nil {
		return generatedVideoSource{}, fmt.Errorf("goav run: %w", err)
	}
	return generatedVideoSource{
		name:        source.Name,
		width:       source.Width,
		height:      source.Height,
		fps:         fpsValue{num: source.FPS.Num, den: source.FPS.Den},
		frames:      source.Frames,
		realtime:    source.Realtime,
		pixelFormat: source.PixelFormat,
		pattern:     source.Pattern,
	}, nil
}

func parseRunOperation(tokens []string) (runOperation, error) {
	if len(tokens) == 0 {
		return runOperation{}, fmt.Errorf("goav run: operation step is empty")
	}
	name := strings.ToLower(tokens[0])
	switch name {
	case "tap":
		return parseTapStep(tokens[1:])
	case "resize":
		return parseResizeStep(tokens[1:])
	case "scale":
		return runOperation{}, fmt.Errorf("goav run: scale duplicates resize; use resize width=<px> height=<px>")
	case "encode":
		return parseEncodeStep(tokens[1:])
	case "enc", "av1enc", "vp9enc", "vp8enc", "h264enc", "opusenc":
		return runOperation{}, fmt.Errorf("goav run: %s duplicates encode; use encode codec=<id> media=<video|audio>", name)
	default:
		return runOperation{}, fmt.Errorf("goav run: unsupported step %q", tokens[0])
	}
}

func parseTapStep(tokens []string) (runOperation, error) {
	positionals, options, err := parseKeyValuesOrdered(tokens)
	if err != nil {
		return runOperation{}, err
	}
	op := runOperation{kind: "tap"}
	seen := make(map[string]struct{}, len(options))
	for _, positional := range positionals {
		return runOperation{}, fmt.Errorf("goav run: tap name must be written as name=<tap-name>, got %q", positional)
	}
	for _, option := range options {
		switch option.key {
		case "name":
			if _, ok := seen["name"]; ok {
				return runOperation{}, fmt.Errorf("goav run: duplicate tap option name")
			}
			seen["name"] = struct{}{}
			op.name = option.value
		case "id":
			return runOperation{}, fmt.Errorf("goav run: id duplicates tap name; use name=<tap-name>")
		default:
			return runOperation{}, fmt.Errorf("goav run: unknown tap option %q", option.key)
		}
	}
	if strings.TrimSpace(op.name) == "" {
		return runOperation{}, fmt.Errorf("goav run: tap needs name=<tap-name>")
	}
	return op, nil
}

func parseResizeStep(tokens []string) (runOperation, error) {
	positionals, options, err := parseKeyValuesOrdered(tokens)
	if err != nil {
		return runOperation{}, err
	}
	resize, err := transformargs.ParseResize(runTransformArgs(positionals, options))
	if err != nil {
		return runOperation{}, fmt.Errorf("goav run: %w", err)
	}
	return runOperation{kind: "resize", width: resize.Width, height: resize.Height}, nil
}

func parseEncodeStep(tokens []string) (runOperation, error) {
	positionals, options, err := parseKeyValuesOrdered(tokens)
	if err != nil {
		return runOperation{}, err
	}
	for _, positional := range positionals {
		return runOperation{}, fmt.Errorf("goav run: unsupported encode argument %q", positional)
	}
	var codecID av.CodecID
	var media av.MediaType
	seen := make(map[string]struct{}, len(options))
	for _, option := range options {
		switch option.key {
		case "codec":
			if _, ok := seen["codec"]; ok {
				return runOperation{}, fmt.Errorf("goav run: duplicate encode option codec")
			}
			seen["codec"] = struct{}{}
			codecID = av.CodecID(strings.TrimSpace(option.value))
		case "media":
			if _, ok := seen["media"]; ok {
				return runOperation{}, fmt.Errorf("goav run: duplicate encode option media")
			}
			seen["media"] = struct{}{}
			media, err = parseMediaType(option.value)
		case "id":
			return runOperation{}, fmt.Errorf("goav run: id duplicates codec; use codec=<id>")
		case "type":
			return runOperation{}, fmt.Errorf("goav run: type duplicates media; use media=<video|audio>")
		}
		if err != nil {
			return runOperation{}, err
		}
	}
	if codecID == "" {
		return runOperation{}, fmt.Errorf("goav run: encode needs codec=<id>")
	}
	if media == "" {
		return runOperation{}, fmt.Errorf("goav run: encode needs media=<video|audio>")
	}
	codecOptions, err := codecargs.ParseOptions(runCodecArgs(options))
	if err != nil {
		return runOperation{}, fmt.Errorf("goav run: %w", err)
	}
	return runOperation{kind: "encode", codec: codecargs.BuildSpec(codecID, media, codecOptions...)}, nil
}

func runCodecArgs(options []runOption) []codecargs.Arg {
	args := make([]codecargs.Arg, 0, len(options))
	for _, option := range options {
		args = append(args, codecargs.Arg{Key: option.key, Value: option.value})
	}
	return args
}

func runSourceArgs(positionals []string, options []runOption) []sourceargs.Arg {
	args := make([]sourceargs.Arg, 0, len(positionals)+len(options))
	for _, positional := range positionals {
		args = append(args, sourceargs.Arg{Value: positional})
	}
	for _, option := range options {
		args = append(args, sourceargs.Arg{Key: option.key, Value: option.value})
	}
	return args
}

func runTransformArgs(positionals []string, options []runOption) []transformargs.Arg {
	args := make([]transformargs.Arg, 0, len(positionals)+len(options))
	for _, positional := range positionals {
		args = append(args, transformargs.Arg{Value: positional})
	}
	for _, option := range options {
		args = append(args, transformargs.Arg{Key: option.key, Value: option.value})
	}
	return args
}

func parseFileDestination(tokens []string) (fileDestination, error) {
	if len(tokens) == 0 {
		return fileDestination{}, fmt.Errorf("goav run: destination step is empty")
	}
	name := strings.ToLower(tokens[0])
	if name != "filesink" {
		return fileDestination{}, fmt.Errorf("goav run: unsupported destination %q (want filesink)", tokens[0])
	}
	positionals, options, err := parseKeyValuesOrdered(tokens[1:])
	if err != nil {
		return fileDestination{}, err
	}
	sinkArgs := make([]fileargs.Arg, 0, len(positionals)+len(options))
	for _, positional := range positionals {
		sinkArgs = append(sinkArgs, fileargs.Arg{Value: positional})
	}
	for _, option := range options {
		sinkArgs = append(sinkArgs, fileargs.Arg{Key: option.key, Value: option.value})
	}
	sink, err := fileargs.ParseFileSink(sinkArgs)
	if err != nil {
		return fileDestination{}, fmt.Errorf("goav run: %w", err)
	}
	return fileDestination{location: sink.Location, format: sink.Format}, nil
}

type runOption struct {
	key   string
	value string
}

func parseKeyValuesOrdered(tokens []string) ([]string, []runOption, error) {
	positionals := make([]string, 0, len(tokens))
	options := make([]runOption, 0, len(tokens))
	for _, token := range tokens {
		key, value, ok := strings.Cut(token, "=")
		if !ok {
			positionals = append(positionals, token)
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			return nil, nil, fmt.Errorf("goav run: empty option key in %q", token)
		}
		options = append(options, runOption{key: key, value: strings.TrimSpace(value)})
	}
	return positionals, options, nil
}

func splitPipelineSteps(text string) ([]string, error) {
	steps, err := cliargs.SplitPipeline(text)
	if err != nil {
		return nil, fmt.Errorf("goav run: %w", err)
	}
	return steps, nil
}

func tokenizeStep(text string) ([]string, error) {
	tokens, err := cliargs.PipelineFields(text)
	if err != nil {
		return nil, fmt.Errorf("goav run: %w", err)
	}
	return tokens, nil
}

func (f fpsValue) String() string {
	if f.den == 1 {
		return strconv.Itoa(f.num)
	}
	return fmt.Sprintf("%d/%d", f.num, f.den)
}

func parseMediaType(value string) (av.MediaType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(av.MediaVideo):
		return av.MediaVideo, nil
	case string(av.MediaAudio):
		return av.MediaAudio, nil
	case string(av.MediaSubtitle):
		return av.MediaSubtitle, nil
	case string(av.MediaData):
		return av.MediaData, nil
	default:
		return "", fmt.Errorf("goav run: media must be video, audio, subtitle, or data")
	}
}
