package launchctl

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// StepArgs are the key/value settings passed to one branch-pipeline step.
type StepArgs map[string]string

// BranchPipelineStepSpec is one allowlisted custom branch-pipeline step. A
// server owner can register application-specific encoders, filters, sinks, or
// control components here without opening arbitrary reflection calls.
type BranchPipelineStepSpec struct {
	Name    string
	Aliases []string
	Summary string
	Usage   string
	Apply   func(*BranchPipeline, StepArgs) error
}

// EncoderSpec is one allowlisted encoder factory for branch pipelines. It is
// how custom encoder settings become callable from the CLI: the server owns the
// factory and maps key=value settings into real codec.CodecSpec values.
type EncoderSpec struct {
	Name    string
	Aliases []string
	Summary string
	Usage   string
	Apply   func(StepArgs) (codec.CodecSpec, error)
}

// PipelineRegistry is the explicit allowlist for branch-pipeline parsing.
type PipelineRegistry struct {
	Steps    []BranchPipelineStepSpec
	Encoders []EncoderSpec
}

// BranchPipeline is the safe builder handle custom pipeline steps receive.
// It exposes only goav branch grammar operations, not arbitrary task methods.
type BranchPipeline struct {
	copyFn        func()
	decodeFn      func()
	resizeFn      func(int, int)
	resampleFn    func(int, int)
	doFn          func(...pipeline.Stage)
	encodeFn      func(codec.CodecSpec)
	destinationFn func(goav.Destination)
	finishFn      func() goav.BranchSpec
}

func (p *BranchPipeline) Copy() {
	if p != nil && p.copyFn != nil {
		p.copyFn()
	}
}

func (p *BranchPipeline) Decode() {
	if p != nil && p.decodeFn != nil {
		p.decodeFn()
	}
}

func (p *BranchPipeline) Resize(width, height int) {
	if p != nil && p.resizeFn != nil {
		p.resizeFn(width, height)
	}
}

func (p *BranchPipeline) Resample(sampleRate, channels int) {
	if p != nil && p.resampleFn != nil {
		p.resampleFn(sampleRate, channels)
	}
}

// Do appends external stages to the branch pipeline.
func (p *BranchPipeline) Do(stages ...pipeline.Stage) {
	if p != nil && p.doFn != nil {
		p.doFn(stages...)
	}
}

func (p *BranchPipeline) Encode(spec codec.CodecSpec) {
	if p != nil && p.encodeFn != nil {
		p.encodeFn(spec)
	}
}

func (p *BranchPipeline) Destination(dest goav.Destination) {
	if p != nil && p.destinationFn != nil {
		p.destinationFn(dest)
	}
}

func (p *BranchPipeline) finish() goav.BranchSpec {
	if p == nil || p.finishFn == nil {
		return goav.Branch("").To()
	}
	return p.finishFn()
}

func parseBranchPipelineWithRegistry(task goav.Task, tapName string, branchName string, pipelineText string, registry PipelineRegistry) (goav.BranchSpec, error) {
	if err := validatePipelineRegistry(registry); err != nil {
		return goav.BranchSpec{}, err
	}
	if branchName == "" {
		return goav.BranchSpec{}, commandError("missing_required", "attach", "branch", "branch name is required", nil, []string{"use attach <tap-name> as <branch-name> '<branch-pipeline>'"}, nil)
	}
	tap, err := resolveBranchTap(task, "attach", tapName)
	if err != nil {
		return goav.BranchSpec{}, err
	}
	steps, err := splitPipeline(pipelineText)
	if err != nil {
		return goav.BranchSpec{}, err
	}
	builder := goav.Branch(branchName).From(tap)
	var destinations []goav.Destination
	branch := &BranchPipeline{
		copyFn:     func() { builder = builder.Copy() },
		decodeFn:   func() { builder = builder.Decode() },
		resizeFn:   func(width int, height int) { builder = builder.Resize(width, height) },
		resampleFn: func(sampleRate int, channels int) { builder = builder.Resample(sampleRate, channels) },
		doFn:       func(stages ...pipeline.Stage) { builder = builder.Do(stages...) },
		encodeFn:   func(spec codec.CodecSpec) { builder = builder.Encode(spec) },
		destinationFn: func(dest goav.Destination) {
			destinations = append(destinations, dest)
		},
		finishFn: func() goav.BranchSpec {
			return builder.To(destinations...)
		},
	}
	stepsByName := pipelineStepMap(registry.Steps)
	encodersByName := encoderMap(registry.Encoders)
	for _, step := range steps {
		words, err := pipelineFields(step)
		if err != nil {
			return goav.BranchSpec{}, err
		}
		if len(words) == 0 {
			continue
		}
		name := strings.ToLower(words[0])
		args := stepArgs(words[1:])
		switch name {
		case "copy":
			branch.Copy()
		case "decode":
			branch.Decode()
		case "resize":
			width, height, err := parseResizeArgs(words[1:], args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Resize(width, height)
		case "resample":
			rate, channels, err := parseResampleArgs(words[1:], args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Resample(rate, channels)
		case "vp8enc", "vp8":
			enc, err := parseEncoder(av.CodecVP8, av.MediaVideo, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "vp9enc", "vp9":
			enc, err := parseEncoder(av.CodecVP9, av.MediaVideo, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "h264enc", "h264":
			enc, err := parseEncoder(av.CodecH264, av.MediaVideo, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "av1enc", "av1":
			enc, err := parseEncoder(av.CodecAV1, av.MediaVideo, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "opusenc", "opus":
			enc, err := parseEncoder(av.CodecOpus, av.MediaAudio, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "encode", "encoder":
			id := av.CodecID(firstNonEmpty(args["codec"], args["id"]))
			media := av.MediaType(firstNonEmpty(args["media"], args["type"]))
			enc, err := parseEncoder(id, media, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "filesink", "file":
			destination, err := parseFileSink(args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Destination(destination)
		default:
			if encoder, ok := encodersByName[name]; ok {
				spec, err := encoder.Apply(StepArgs(args))
				if err != nil {
					return goav.BranchSpec{}, structuredError("parse branch pipeline", err)
				}
				branch.Encode(spec)
				continue
			}
			if custom, ok := stepsByName[name]; ok {
				if custom.Apply == nil {
					return goav.BranchSpec{}, commandError("invalid_pipeline_step", "parse branch pipeline", name, "custom pipeline step has no Apply function", nil, nil, nil)
				}
				if err := custom.Apply(branch, StepArgs(args)); err != nil {
					return goav.BranchSpec{}, structuredError("parse branch pipeline", err)
				}
				continue
			}
			return goav.BranchSpec{}, commandError(
				"unsupported_pipeline_step",
				"parse branch pipeline",
				name,
				fmt.Sprintf("unsupported branch pipeline step %q", name),
				[]string{"supported_steps=" + strings.Join(pipelineStepNames(registry), ",")},
				[]string{"use a supported branch-pipeline step or attach with typed Task.Attach in-process"},
				nil,
			)
		}
	}
	if len(destinations) == 0 {
		return goav.BranchSpec{}, commandError("missing_required", "parse branch pipeline", "filesink", "branch pipeline needs a destination", nil, []string{"append `! filesink location=out.webm`"}, nil)
	}
	return branch.finish(), nil
}

func pipelineStepMap(specs []BranchPipelineStepSpec) map[string]BranchPipelineStepSpec {
	out := make(map[string]BranchPipelineStepSpec)
	for _, spec := range specs {
		if spec.Name != "" {
			out[strings.ToLower(spec.Name)] = spec
		}
		for _, alias := range spec.Aliases {
			if alias != "" {
				out[strings.ToLower(alias)] = spec
			}
		}
	}
	return out
}

func encoderMap(specs []EncoderSpec) map[string]EncoderSpec {
	out := make(map[string]EncoderSpec)
	for _, spec := range specs {
		if spec.Name != "" {
			out[strings.ToLower(spec.Name)] = spec
		}
		for _, alias := range spec.Aliases {
			if alias != "" {
				out[strings.ToLower(alias)] = spec
			}
		}
	}
	return out
}

func pipelineStepNames(registry PipelineRegistry) []string {
	seen := make(map[string]struct{})
	for _, name := range builtinPipelineNames() {
		seen[name] = struct{}{}
	}
	for _, spec := range registry.Steps {
		if spec.Name != "" {
			seen[strings.ToLower(spec.Name)] = struct{}{}
		}
		for _, alias := range spec.Aliases {
			if alias != "" {
				seen[strings.ToLower(alias)] = struct{}{}
			}
		}
	}
	for _, spec := range registry.Encoders {
		if spec.Name != "" {
			seen[strings.ToLower(spec.Name)] = struct{}{}
		}
		for _, alias := range spec.Aliases {
			if alias != "" {
				seen[strings.ToLower(alias)] = struct{}{}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func resolveBranchTap(task goav.Task, operation string, tapName string) (goav.TapRef, error) {
	if tapName == "" {
		return goav.TapRef{}, commandError("missing_required", operation, "tap", "tap name is required", nil, []string{"use attach <tap-name> as <branch-name> ..."}, nil)
	}
	for _, tap := range task.Taps() {
		if tap.Name != tapName {
			continue
		}
		switch tap.Domain {
		case shape.DomainFrame:
			return goav.FrameTap(tapName), nil
		case shape.DomainPacket:
			return goav.PacketTap(tapName), nil
		default:
			return goav.Tap(tapName), nil
		}
	}
	return goav.TapRef{}, ensureTap(task, operation, tapName)
}

func splitPipeline(pipeline string) ([]string, error) {
	var steps []string
	start := 0
	var quote byte
	var escaped bool
	for i := 0; i < len(pipeline); i++ {
		ch := pipeline[i]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '!':
			step := strings.TrimSpace(pipeline[start:i])
			if step != "" {
				steps = append(steps, step)
			}
			start = i + 1
		}
	}
	if quote != 0 {
		return nil, pipelineSyntaxError("pipeline", len(pipeline), "unterminated quoted value in branch pipeline")
	}
	if escaped {
		return nil, pipelineSyntaxError("pipeline", len(pipeline), "unterminated escape sequence in branch pipeline")
	}
	step := strings.TrimSpace(pipeline[start:])
	if step != "" {
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, commandError("missing_required", "parse branch pipeline", "pipeline", "branch pipeline is empty", nil, []string{"use `copy ! filesink location=out.webm`"}, nil)
	}
	return steps, nil
}

func pipelineFields(step string) ([]string, error) {
	var fields []string
	var field strings.Builder
	var quote byte
	var escaped bool
	var started bool
	for i := 0; i < len(step); i++ {
		ch := step[i]
		if escaped {
			field.WriteByte(ch)
			escaped = false
			started = true
			continue
		}
		if quote != 0 {
			switch ch {
			case '\\':
				escaped = true
			case quote:
				quote = 0
			default:
				field.WriteByte(ch)
			}
			started = true
			continue
		}
		switch {
		case ch == '\'' || ch == '"':
			quote = ch
			started = true
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			if started {
				fields = append(fields, field.String())
				field.Reset()
				started = false
			}
		default:
			field.WriteByte(ch)
			started = true
		}
	}
	if quote != 0 {
		return nil, pipelineSyntaxError("pipeline", len(step), "unterminated quoted value in branch pipeline step")
	}
	if escaped {
		return nil, pipelineSyntaxError("pipeline", len(step), "unterminated escape sequence in branch pipeline step")
	}
	if started {
		fields = append(fields, field.String())
	}
	return fields, nil
}

func pipelineSyntaxError(node string, offset int, message string) error {
	return commandError(
		"invalid_value",
		"parse branch pipeline",
		node,
		message,
		[]string{fmt.Sprintf("offset=%d", offset)},
		[]string{`quote values with spaces, for example location="/tmp/a b.ogg"`},
		nil,
	)
}

func stepArgs(words []string) map[string]string {
	args := make(map[string]string)
	for _, word := range words {
		key, value, ok := strings.Cut(word, "=")
		if !ok {
			args[strings.ToLower(word)] = ""
			continue
		}
		args[strings.ToLower(key)] = value
	}
	return args
}

func parseResizeArgs(words []string, args map[string]string) (int, int, error) {
	if len(words) > 0 {
		if widthText, heightText, ok := strings.Cut(words[0], "x"); ok {
			width, errW := strconv.Atoi(widthText)
			height, errH := strconv.Atoi(heightText)
			if errW == nil && errH == nil && width > 0 && height > 0 {
				return width, height, nil
			}
		}
	}
	width, okW := parsePositiveIntArg(args, "width", "w")
	height, okH := parsePositiveIntArg(args, "height", "h")
	if !okW || !okH {
		return 0, 0, commandError("invalid_value", "parse branch pipeline", "resize", "resize needs dimensions like 854x480 or width=854 height=480", nil, []string{"use `resize 854x480`"}, nil)
	}
	return width, height, nil
}

func parseResampleArgs(words []string, args map[string]string) (int, int, error) {
	if len(words) >= 2 {
		rate, errR := strconv.Atoi(words[0])
		channels, errC := strconv.Atoi(words[1])
		if errR == nil && errC == nil && rate > 0 && channels > 0 {
			return rate, channels, nil
		}
	}
	rate, okR := parsePositiveIntArg(args, "rate", "sample_rate")
	channels, okC := parsePositiveIntArg(args, "channels", "ch")
	if !okR || !okC {
		return 0, 0, commandError("invalid_value", "parse branch pipeline", "resample", "resample needs rate and channels", nil, []string{"use `resample 48000 2`", "use `resample rate=48000 channels=2`"}, nil)
	}
	return rate, channels, nil
}

func parsePositiveIntArg(args map[string]string, keys ...string) (int, bool) {
	for _, key := range keys {
		value, ok := args[key]
		if !ok || value == "" {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err == nil && parsed > 0 {
			return parsed, true
		}
	}
	return 0, false
}

func parseEncoder(id av.CodecID, media av.MediaType, args map[string]string) (codec.CodecSpec, error) {
	if id == "" {
		return codec.CodecSpec{}, commandError("missing_required", "parse branch pipeline", "codec", "encode needs codec=<codec-id>", nil, []string{"use `encode codec=x_pcm_s16 media=audio`"}, nil)
	}
	options, err := parseCodecOptions(args)
	if err != nil {
		return codec.CodecSpec{}, err
	}
	switch id {
	case av.CodecOpus:
		return codec.Opus(options...), nil
	case av.CodecVP8:
		return codec.VP8(options...), nil
	case av.CodecVP9:
		return codec.VP9(options...), nil
	case av.CodecH264:
		return codec.H264(options...), nil
	case av.CodecAV1:
		return codec.AV1(options...), nil
	default:
		if media == "" {
			return codec.CodecSpec{}, commandError("missing_required", "parse branch pipeline", "media", "custom encode needs media=audio, media=video, or media=subtitle", []string{"codec=" + string(id)}, []string{"use `encode codec=" + string(id) + " media=audio`"}, nil)
		}
		return codec.Codec(id, media, options...), nil
	}
}

func parseCodecOptions(args map[string]string) ([]codec.Option, error) {
	var options []codec.Option
	if bitrateText := firstNonEmpty(args["bitrate"], args["bitrate_bps"]); bitrateText != "" {
		bitrate, err := parseRate(bitrateText)
		if err != nil {
			return nil, commandError("invalid_value", "parse branch pipeline", "bitrate", "encoder bitrate must be like 300k, 2M, or integer bits per second", []string{"value=" + bitrateText}, []string{"use bitrate=900k"}, err)
		}
		options = append(options, codec.Bitrate(bitrate))
	}
	if profile := args["profile"]; profile != "" {
		options = append(options, codec.Profile(profile))
	}
	if level := args["level"]; level != "" {
		options = append(options, codec.Level(level))
	}
	if rate, ok := parsePositiveIntArg(args, "rate", "sample_rate"); ok {
		options = append(options, codec.SampleRate(rate))
	}
	if channels, ok := parsePositiveIntArg(args, "channels", "ch"); ok {
		options = append(options, codec.Channels(channels))
	}
	if clockRate, ok := parsePositiveUint32Arg(args, "clock_rate"); ok {
		options = append(options, codec.ClockRate(clockRate))
	}
	if keyframes, ok := parsePositiveIntArg(args, "keyframe_interval", "keyint", "gop"); ok {
		options = append(options, codec.KeyframeInterval(keyframes))
	}
	if fps := args["fps"]; fps != "" {
		num, den, err := parseFPS(fps)
		if err != nil {
			return nil, err
		}
		options = append(options, codec.FPS(num, den))
	}
	return options, nil
}

func parsePositiveUint32Arg(args map[string]string, keys ...string) (uint32, bool) {
	for _, key := range keys {
		value, ok := args[key]
		if !ok || value == "" {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err == nil && parsed > 0 {
			return uint32(parsed), true
		}
	}
	return 0, false
}

func parseFPS(value string) (int, int, error) {
	numText, denText, hasDen := strings.Cut(value, "/")
	num, err := strconv.Atoi(numText)
	if err != nil || num <= 0 {
		return 0, 0, commandError("invalid_value", "parse branch pipeline", "fps", "fps must be a positive integer or fraction", []string{"value=" + value}, []string{"use fps=30", "use fps=30000/1001"}, err)
	}
	if !hasDen {
		return num, 1, nil
	}
	den, err := strconv.Atoi(denText)
	if err != nil || den <= 0 {
		return 0, 0, commandError("invalid_value", "parse branch pipeline", "fps", "fps denominator must be positive", []string{"value=" + value}, []string{"use fps=30000/1001"}, err)
	}
	return num, den, nil
}

func parseFileSink(args map[string]string) (goav.Destination, error) {
	location := firstNonEmpty(args["location"], args["path"], args["file"])
	if location == "" {
		return goav.Destination{}, commandError("missing_required", "parse branch pipeline", "filesink", "filesink needs location=<path>", nil, []string{"use `filesink location=out.webm`"}, nil)
	}
	writer, err := os.Create(location)
	if err != nil {
		return goav.Destination{}, commandError("open_destination", "parse branch pipeline", location, err.Error(), nil, []string{"choose a writable filesink location"}, err)
	}
	var options []goav.DestinationOption
	if format := args["format"]; format != "" {
		options = append(options, goav.Format(av.FormatID(format)))
	}
	return goav.File(location, closeOnceWriter{Writer: writer, closer: writer}, options...), nil
}

type closeOnceWriter struct {
	io.Writer
	closer io.Closer
}

func (w closeOnceWriter) Close() error {
	if w.closer == nil {
		return nil
	}
	return w.closer.Close()
}
