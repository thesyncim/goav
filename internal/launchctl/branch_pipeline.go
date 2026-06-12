package launchctl

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/internal/cliargs"
	"github.com/thesyncim/goav/internal/codecargs"
	"github.com/thesyncim/goav/internal/fileargs"
	"github.com/thesyncim/goav/internal/transformargs"
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
	// ArgsType optionally records the typed settings struct used by helper
	// constructors. It is cold-path metadata for help and capabilities output.
	ArgsType reflect.Type
	Apply    func(*BranchPipeline, StepArgs) error
}

// EncoderSpec is one allowlisted encoder factory for branch pipelines. It is
// how custom encoder settings become callable from the CLI: the server owns the
// factory and maps key=value settings into real codec.CodecSpec values.
type EncoderSpec struct {
	Name    string
	Aliases []string
	Summary string
	Usage   string
	// ArgsType optionally records the typed settings struct used by helper
	// constructors. It is cold-path metadata for help and capabilities output.
	ArgsType reflect.Type
	Apply    func(StepArgs) (codec.CodecSpec, error)
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
			width, height, err := parseResizeArgs(words[1:])
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Resize(width, height)
		case "resample":
			rate, channels, err := parseResampleArgs(words[1:])
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Resample(rate, channels)
		case "encode":
			id := av.CodecID(strings.TrimSpace(args["codec"]))
			media := av.MediaType(strings.TrimSpace(args["media"]))
			enc, err := parseEncoder(id, media, args)
			if err != nil {
				return goav.BranchSpec{}, err
			}
			branch.Encode(enc)
		case "filesink":
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
	if strings.TrimSpace(pipeline) == "" {
		return nil, commandError("missing_required", "parse branch pipeline", "pipeline", "branch pipeline is empty", nil, []string{"use `copy ! filesink location=out.webm`"}, nil)
	}
	steps, err := cliargs.SplitPipeline(pipeline)
	if err != nil {
		return nil, pipelineSyntaxError("pipeline", err)
	}
	return steps, nil
}

func pipelineFields(step string) ([]string, error) {
	fields, err := cliargs.PipelineFields(step)
	if err != nil {
		return nil, pipelineSyntaxError("pipeline", err)
	}
	return fields, nil
}

func pipelineSyntaxError(node string, err error) error {
	offset := 0
	message := err.Error()
	var syntax *cliargs.PipelineSyntaxError
	if errors.As(err, &syntax) {
		offset = syntax.Offset
		message = syntax.Message
	}
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

func parseResizeArgs(words []string) (int, int, error) {
	resize, err := transformargs.ParseResize(pipelineTransformArgs(words))
	if err != nil {
		return 0, 0, transformOptionError(err)
	}
	return resize.Width, resize.Height, nil
}

func parseResampleArgs(words []string) (int, int, error) {
	resample, err := transformargs.ParseResample(pipelineTransformArgs(words))
	if err != nil {
		return 0, 0, transformOptionError(err)
	}
	return resample.SampleRate, resample.Channels, nil
}

func pipelineTransformArgs(words []string) []transformargs.Arg {
	args := make([]transformargs.Arg, 0, len(words))
	for _, word := range words {
		key, value, ok := strings.Cut(word, "=")
		if !ok {
			args = append(args, transformargs.Arg{Value: word})
			continue
		}
		args = append(args, transformargs.Arg{Key: strings.ToLower(key), Value: value})
	}
	return args
}

func transformOptionError(err error) error {
	var optErr *transformargs.Error
	if errors.As(err, &optErr) {
		details := []string(nil)
		if optErr.Value != "" {
			details = []string{"value=" + optErr.Value}
		}
		return commandError("invalid_value", "parse branch pipeline", optErr.Field, optErr.Message, details, optErr.Suggestions, err)
	}
	return commandError("invalid_value", "parse branch pipeline", "transform", err.Error(), nil, nil, err)
}

func parseEncoder(id av.CodecID, media av.MediaType, args map[string]string) (codec.CodecSpec, error) {
	if _, ok := args["id"]; ok {
		return codec.CodecSpec{}, commandError("invalid_value", "parse branch pipeline", "id", "id duplicates codec", nil, []string{"use codec=<codec-id>"}, nil)
	}
	if _, ok := args["type"]; ok {
		return codec.CodecSpec{}, commandError("invalid_value", "parse branch pipeline", "type", "type duplicates media", nil, []string{"use media=<audio|video|subtitle>"}, nil)
	}
	if id == "" {
		return codec.CodecSpec{}, commandError("missing_required", "parse branch pipeline", "codec", "encode needs codec=<codec-id>", nil, []string{"use `encode codec=x_pcm_s16 media=audio`"}, nil)
	}
	if media == "" {
		return codec.CodecSpec{}, commandError("missing_required", "parse branch pipeline", "media", "encode needs media=audio, media=video, or media=subtitle", []string{"codec=" + string(id)}, []string{"use `encode codec=" + string(id) + " media=audio`", "use `encode codec=" + string(id) + " media=video`"}, nil)
	}
	switch media {
	case av.MediaAudio, av.MediaVideo, av.MediaSubtitle:
	default:
		return codec.CodecSpec{}, commandError("invalid_value", "parse branch pipeline", "media", "media must be audio, video, or subtitle", []string{"value=" + string(media)}, []string{"use media=audio", "use media=video"}, nil)
	}
	options, err := codecargs.ParseOptionsMap(args)
	if err != nil {
		return codec.CodecSpec{}, codecOptionError(err)
	}
	return codecargs.BuildSpec(id, media, options...), nil
}

func codecOptionError(err error) error {
	var optErr *codecargs.Error
	if errors.As(err, &optErr) {
		details := []string(nil)
		if optErr.Value != "" {
			details = []string{"value=" + optErr.Value}
		}
		return commandError("invalid_value", "parse branch pipeline", optErr.Field, optErr.Message, details, optErr.Suggestions, err)
	}
	return commandError("invalid_value", "parse branch pipeline", "encoder", err.Error(), nil, nil, err)
}

func parseFileSink(args map[string]string) (goav.Destination, error) {
	sink, err := fileargs.ParseFileSinkMap(args)
	if err != nil {
		return goav.Destination{}, fileSinkOptionError(err)
	}
	writer, err := os.Create(sink.Location)
	if err != nil {
		return goav.Destination{}, commandError("open_destination", "parse branch pipeline", sink.Location, err.Error(), nil, []string{"choose a writable filesink location"}, err)
	}
	var options []goav.DestinationOption
	if sink.Format != "" {
		options = append(options, goav.Format(sink.Format))
	}
	return goav.File(sink.Location, closeOnceWriter{Writer: writer, closer: writer}, options...), nil
}

func fileSinkOptionError(err error) error {
	var optErr *fileargs.Error
	if errors.As(err, &optErr) {
		details := []string(nil)
		if optErr.Value != "" {
			details = []string{"value=" + optErr.Value}
		}
		return commandError("invalid_value", "parse branch pipeline", optErr.Field, optErr.Message, details, optErr.Suggestions, err)
	}
	return commandError("invalid_value", "parse branch pipeline", "filesink", err.Error(), nil, nil, err)
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
