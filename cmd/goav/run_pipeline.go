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
	"unicode"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/ctl"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const defaultRunPipeline = "testsrc video width=1280 height=720 fps=30 duration=3s realtime=true ! av1enc bitrate=1200k fps=30 keyframe_interval=60 ! filesink location=/tmp/goav-av1.mkv format=matroska"

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
		"  goav run 'testsrc video width=1280 height=720 fps=30 duration=3s realtime=true pattern=bars ! av1enc bitrate=1200k fps=30 keyframe_interval=60 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=/tmp/goav-av1.ivf'\n\n" +
		"pipeline steps:\n" +
		"  testsrc video [name=<id>] width=<px> height=<px>|size=<w>x<h> fps=<n[/d]|decimal> frames=<n>|duration=<d> realtime=<bool> [format=i420|yuv420p] [pattern=bars|gradient|solid]\n" +
		"  tap name=<tap-name>\n" +
		"  resize width=<px> height=<px>|size=<w>x<h>\n" +
		"  av1enc|vp9enc|vp8enc|h264enc|encode codec=<id> media=<video|audio> bitrate=<rate> fps=<n[/d]> keyframe_interval=<n> [native_key=value...]\n" +
		"  filesink location=<path> [format=<container>] (known file extensions infer the format)\n\n" +
		"control example:\n" +
		"  goav run --control unix:///tmp/goav-live.sock 'testsrc video name=fixture width=1280 height=720 fps=30 duration=30s realtime=true pattern=bars ! tap name=frames ! av1enc bitrate=1200k fps=30 keyframe_interval=60 min_qindex=20 max_qindex=180 tune=zerolatency ! filesink location=/tmp/goav-av1.mkv format=matroska'\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock taps\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock graph\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock control rate value=0.5 source=fixture\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock control seek position=2s source=fixture\n" +
		"  goav ctl --control unix:///tmp/goav-live.sock attach frames as preview 'resize 320x180 ! av1enc bitrate=300k fps=2 keyframe_interval=1 ! filesink location=/tmp/goav-preview.ivf'\n" +
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
	return runPipelineResult{
		Runtime:     runtimeLabel,
		Source:      plan.source.name,
		Frames:      plan.source.frames,
		Realtime:    plan.source.realtime,
		Video:       fmt.Sprintf("%dx%d@%s", plan.source.width, plan.source.height, plan.source.fps.String()),
		Codec:       string(encoded.ID),
		Output:      plan.destination.location,
		Format:      string(plan.destination.format),
		Control:     control,
		Description: "generated video source encoded and written successfully",
	}, nil
}

func buildRunPipelineTask(ctx context.Context, runtime goav.Runtime, plan runPipelinePlan, dest goav.Destination) (goav.Task, codec.CodecSpec, error) {
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

func runtimeForRun(name string, plan runPipelinePlan) (goav.Runtime, string, error) {
	if name == "" {
		name = "demo"
	}
	codecIDs := plan.encodeCodecIDs()
	switch name {
	case "demo":
		return goav.Default(goav.WithClock(goavtest.NewClock())), "demo", nil
	case "default", "std", "standard":
		return goav.Default(), "default", nil
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
	name := strings.ToLower(tokens[0])
	if name != "testsrc" && name != "videosrc" && name != "testvideo" {
		return generatedVideoSource{}, fmt.Errorf("goav run: unsupported source %q (want testsrc video)", tokens[0])
	}
	positionals, options, err := parseKeyValuesOrdered(tokens[1:])
	if err != nil {
		return generatedVideoSource{}, err
	}
	for _, positional := range positionals {
		if strings.EqualFold(positional, "video") {
			continue
		}
		return generatedVideoSource{}, fmt.Errorf("goav run: unsupported source argument %q", positional)
	}
	source := generatedVideoSource{
		name:        "testsrc",
		width:       1280,
		height:      720,
		fps:         fpsValue{num: 30, den: 1},
		frames:      90,
		realtime:    true,
		pixelFormat: av.PixelFormatI420,
		pattern:     "gradient",
	}
	var duration time.Duration
	var durationSet bool
	for _, option := range options {
		switch option.key {
		case "name":
			if option.value == "" {
				return generatedVideoSource{}, fmt.Errorf("goav run: source name cannot be empty")
			}
			source.name = option.value
		case "width", "w":
			source.width, err = parsePositiveInt("width", option.value)
		case "height", "h":
			source.height, err = parsePositiveInt("height", option.value)
		case "size":
			source.width, source.height, err = parseSize(option.value)
		case "fps", "framerate":
			source.fps, err = parseFPS(option.value)
		case "frames":
			source.frames, err = parsePositiveInt("frames", option.value)
		case "duration":
			duration, err = time.ParseDuration(option.value)
			durationSet = err == nil
		case "realtime", "live":
			source.realtime, err = parseBool(option.value)
		case "format", "pixel_format", "pix_fmt":
			source.pixelFormat, err = parseGeneratedPixelFormat(option.value)
		case "pattern":
			source.pattern, err = parsePattern(option.value)
		default:
			return generatedVideoSource{}, fmt.Errorf("goav run: unknown testsrc option %q", option.key)
		}
		if err != nil {
			return generatedVideoSource{}, err
		}
	}
	if durationSet {
		source.frames = framesForDuration(duration, source.fps)
	}
	if source.frames <= 0 {
		return generatedVideoSource{}, fmt.Errorf("goav run: frames must be greater than zero")
	}
	return source, nil
}

func parseRunOperation(tokens []string) (runOperation, error) {
	if len(tokens) == 0 {
		return runOperation{}, fmt.Errorf("goav run: operation step is empty")
	}
	name := strings.ToLower(tokens[0])
	switch name {
	case "tap":
		return parseTapStep(tokens[1:])
	case "resize", "scale":
		return parseResizeStep(tokens[1:])
	case "encode", "enc", "av1enc", "vp9enc", "vp8enc", "h264enc", "opusenc":
		return parseEncodeStep(name, tokens[1:])
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
	for _, positional := range positionals {
		if op.name == "" {
			op.name = positional
			continue
		}
		return runOperation{}, fmt.Errorf("goav run: unsupported tap argument %q", positional)
	}
	for _, option := range options {
		switch option.key {
		case "name", "id":
			op.name = option.value
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
	op := runOperation{kind: "resize"}
	for _, positional := range positionals {
		if strings.Contains(positional, "x") {
			op.width, op.height, err = parseSize(positional)
			if err != nil {
				return runOperation{}, err
			}
			continue
		}
		return runOperation{}, fmt.Errorf("goav run: unsupported resize argument %q", positional)
	}
	for _, option := range options {
		switch option.key {
		case "width", "w":
			op.width, err = parsePositiveInt("width", option.value)
		case "height", "h":
			op.height, err = parsePositiveInt("height", option.value)
		case "size":
			op.width, op.height, err = parseSize(option.value)
		default:
			return runOperation{}, fmt.Errorf("goav run: unknown resize option %q", option.key)
		}
		if err != nil {
			return runOperation{}, err
		}
	}
	if op.width <= 0 || op.height <= 0 {
		return runOperation{}, fmt.Errorf("goav run: resize needs width and height")
	}
	return op, nil
}

func parseEncodeStep(name string, tokens []string) (runOperation, error) {
	positionals, options, err := parseKeyValuesOrdered(tokens)
	if err != nil {
		return runOperation{}, err
	}
	codecID := codecIDFromEncodeName(name)
	media := av.MediaVideo
	if name == "opusenc" {
		media = av.MediaAudio
	}
	for _, positional := range positionals {
		if codecID == "" {
			codecID = normalizeCodecID(positional)
			continue
		}
		return runOperation{}, fmt.Errorf("goav run: unsupported encode argument %q", positional)
	}
	var codecOptions []codec.Option
	for _, option := range options {
		switch option.key {
		case "codec", "id":
			codecID = normalizeCodecID(option.value)
		case "media", "type":
			media, err = parseMediaType(option.value)
		case "bitrate", "rate":
			var bitrate int
			bitrate, err = parseRate(option.value)
			if err == nil {
				codecOptions = append(codecOptions, codec.Bitrate(bitrate))
			}
		case "fps", "framerate":
			var fps fpsValue
			fps, err = parseFPS(option.value)
			if err == nil {
				codecOptions = append(codecOptions, codec.FPS(fps.num, fps.den))
			}
		case "keyframe_interval", "keyint", "gop":
			var interval int
			interval, err = parsePositiveInt("keyframe_interval", option.value)
			if err == nil {
				codecOptions = append(codecOptions, codec.KeyframeInterval(interval))
			}
		case "profile":
			codecOptions = append(codecOptions, codec.Profile(option.value))
		case "level":
			codecOptions = append(codecOptions, codec.Level(option.value))
		case "channels":
			var channels int
			channels, err = parsePositiveInt("channels", option.value)
			if err == nil {
				codecOptions = append(codecOptions, codec.Channels(channels))
			}
		case "sample_rate", "samplerate":
			var sampleRate int
			sampleRate, err = parsePositiveInt("sample_rate", option.value)
			if err == nil {
				codecOptions = append(codecOptions, codec.SampleRate(sampleRate))
			}
		case "clock_rate", "clockrate":
			var clockRate int
			clockRate, err = parsePositiveInt("clock_rate", option.value)
			if err == nil {
				codecOptions = append(codecOptions, codec.ClockRate(uint32(clockRate)))
			}
		default:
			codecOptions = append(codecOptions, codec.Setting(option.key, option.value))
		}
		if err != nil {
			return runOperation{}, err
		}
	}
	if codecID == "" {
		return runOperation{}, fmt.Errorf("goav run: encode needs codec=<id>")
	}
	return runOperation{kind: "encode", codec: buildCodecSpec(codecID, media, codecOptions...)}, nil
}

func parseFileDestination(tokens []string) (fileDestination, error) {
	if len(tokens) == 0 {
		return fileDestination{}, fmt.Errorf("goav run: destination step is empty")
	}
	name := strings.ToLower(tokens[0])
	if name != "filesink" && name != "file" && name != "sink" {
		return fileDestination{}, fmt.Errorf("goav run: unsupported destination %q (want filesink)", tokens[0])
	}
	positionals, options, err := parseKeyValuesOrdered(tokens[1:])
	if err != nil {
		return fileDestination{}, err
	}
	dest := fileDestination{}
	for _, positional := range positionals {
		if dest.location == "" {
			dest.location = positional
			continue
		}
		return fileDestination{}, fmt.Errorf("goav run: unsupported filesink argument %q", positional)
	}
	for _, option := range options {
		switch option.key {
		case "location", "path", "file":
			dest.location = option.value
		case "format", "container":
			dest.format = av.FormatID(strings.ToLower(option.value))
		default:
			return fileDestination{}, fmt.Errorf("goav run: unknown filesink option %q", option.key)
		}
	}
	if dest.location == "" {
		return fileDestination{}, fmt.Errorf("goav run: filesink needs location=<path>")
	}
	if dest.format == "" {
		dest.format = inferFileDestinationFormat(dest.location)
	}
	return dest, nil
}

func inferFileDestinationFormat(location string) av.FormatID {
	switch strings.ToLower(filepath.Ext(location)) {
	case ".ivf":
		return av.FormatIVF
	case ".mkv", ".mka", ".mks":
		return av.FormatMatroska
	case ".webm":
		return av.FormatWebM
	case ".h264", ".264", ".avc", ".h265", ".hevc":
		return av.FormatAnnexB
	default:
		return ""
	}
}

func codecIDFromEncodeName(name string) av.CodecID {
	switch name {
	case "av1enc":
		return av.CodecAV1
	case "vp9enc":
		return av.CodecVP9
	case "vp8enc":
		return av.CodecVP8
	case "h264enc":
		return av.CodecH264
	case "opusenc":
		return av.CodecOpus
	default:
		return ""
	}
}

func normalizeCodecID(value string) av.CodecID {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, ".", "")
	switch normalized {
	case "av01":
		return av.CodecAV1
	case "h265", "hevc":
		return av.CodecID("h265")
	default:
		return av.CodecID(normalized)
	}
}

func buildCodecSpec(id av.CodecID, media av.MediaType, options ...codec.Option) codec.CodecSpec {
	switch id {
	case av.CodecAV1:
		return codec.AV1(options...)
	case av.CodecVP9:
		return codec.VP9(options...)
	case av.CodecVP8:
		return codec.VP8(options...)
	case av.CodecH264:
		return codec.H264(options...)
	case av.CodecOpus:
		return codec.Opus(options...)
	default:
		return codec.Codec(id, media, options...)
	}
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
	var steps []string
	var current strings.Builder
	var quote rune
	escaped := false
	for _, r := range text {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			current.WriteRune(r)
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
			}
			current.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			current.WriteRune(r)
		case r == '!':
			step := strings.TrimSpace(current.String())
			if step == "" {
				return nil, fmt.Errorf("goav run: empty pipeline step")
			}
			steps = append(steps, step)
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("goav run: unterminated quote")
	}
	step := strings.TrimSpace(current.String())
	if step == "" {
		return nil, fmt.Errorf("goav run: empty pipeline step")
	}
	steps = append(steps, step)
	return steps, nil
}

func tokenizeStep(text string) ([]string, error) {
	var tokens []string
	var current strings.Builder
	var quote rune
	escaped := false
	flush := func() {
		if current.Len() != 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	for _, r := range text {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
		case unicode.IsSpace(r):
			flush()
		default:
			current.WriteRune(r)
		}
	}
	if escaped {
		current.WriteRune('\\')
	}
	if quote != 0 {
		return nil, fmt.Errorf("goav run: unterminated quote")
	}
	flush()
	return tokens, nil
}

func parsePositiveInt(name string, value string) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("goav run: %s must be a positive integer", name)
	}
	return parsed, nil
}

func parseSize(value string) (int, int, error) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(value)), "x")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("goav run: size must be WIDTHxHEIGHT")
	}
	width, err := parsePositiveInt("width", parts[0])
	if err != nil {
		return 0, 0, err
	}
	height, err := parsePositiveInt("height", parts[1])
	if err != nil {
		return 0, 0, err
	}
	return width, height, nil
}

func parseFPS(value string) (fpsValue, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return fpsValue{}, fmt.Errorf("goav run: fps is required")
	}
	if left, right, ok := strings.Cut(value, "/"); ok {
		num, err := parsePositiveInt("fps numerator", left)
		if err != nil {
			return fpsValue{}, err
		}
		den, err := parsePositiveInt("fps denominator", right)
		if err != nil {
			return fpsValue{}, err
		}
		return reduceFPS(fpsValue{num: num, den: den}), nil
	}
	if strings.Contains(value, ".") {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || parsed <= 0 {
			return fpsValue{}, fmt.Errorf("goav run: fps must be positive")
		}
		return reduceFPS(fpsValue{num: int(math.Round(parsed * 1000)), den: 1000}), nil
	}
	num, err := parsePositiveInt("fps", value)
	if err != nil {
		return fpsValue{}, err
	}
	return fpsValue{num: num, den: 1}, nil
}

func reduceFPS(fps fpsValue) fpsValue {
	divisor := gcd(fps.num, fps.den)
	if divisor <= 1 {
		return fps
	}
	return fpsValue{num: fps.num / divisor, den: fps.den / divisor}
}

func gcd(a int, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

func (f fpsValue) String() string {
	if f.den == 1 {
		return strconv.Itoa(f.num)
	}
	return fmt.Sprintf("%d/%d", f.num, f.den)
}

func framesForDuration(duration time.Duration, fps fpsValue) int {
	if duration <= 0 {
		return 0
	}
	frames := math.Ceil(duration.Seconds() * float64(fps.num) / float64(fps.den))
	return max(int(frames), 1)
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("goav run: boolean value must be true or false")
	}
}

func parseGeneratedPixelFormat(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case av.PixelFormatI420, av.PixelFormatYUV420P:
		return av.PixelFormatI420, nil
	default:
		return "", fmt.Errorf("goav run: testsrc currently generates i420/yuv420p")
	}
}

func parsePattern(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "gradient":
		return "gradient", nil
	case "bars", "solid":
		return strings.ToLower(strings.TrimSpace(value)), nil
	default:
		return "", fmt.Errorf("goav run: pattern must be gradient, bars, or solid")
	}
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

func parseRate(value string) (int, error) {
	raw := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(value, "_", "")))
	if raw == "" {
		return 0, fmt.Errorf("goav run: bitrate is required")
	}
	multiplier := float64(1)
	for _, suffix := range []struct {
		text string
		mult float64
	}{
		{text: "gbps", mult: 1_000_000_000},
		{text: "mbps", mult: 1_000_000},
		{text: "kbps", mult: 1_000},
		{text: "bps", mult: 1},
		{text: "g", mult: 1_000_000_000},
		{text: "m", mult: 1_000_000},
		{text: "k", mult: 1_000},
	} {
		if strings.HasSuffix(raw, suffix.text) {
			raw = strings.TrimSuffix(raw, suffix.text)
			multiplier = suffix.mult
			break
		}
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("goav run: bitrate must be positive")
	}
	return int(math.Round(parsed * multiplier)), nil
}
