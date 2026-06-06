package goav

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	transcodepkg "github.com/thesyncim/goav/transcode"
)

const (
	Mono   = 1
	Stereo = 2
)

type Intent struct {
	Name     string
	Inputs   []InputIntent
	Streams  []StreamIntent
	Outputs  []OutputIntent
	Policies PolicyIntent
}

type InputIntent struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	Codec    CodecSpec
	Realtime bool
}

type StreamIntent struct {
	Name       string
	Select     StreamSelect
	Decode     bool
	Transforms []TransformSpec
	Encode     CodecSpec
	RouteTo    []string
}

type OutputIntent struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	Format   av.FormatID
}

type PolicyIntent struct {
	Realtime bool
}

type StreamSelect struct {
	ID    av.StreamID
	Index int
	Type  av.MediaType
	Codec av.CodecID
	Name  string
}

type BuildError struct {
	Code        string
	Operation   string
	Node        string
	Reason      string
	Suggestions []string
	Cause       error
}

func (e *BuildError) Error() string {
	if e == nil {
		return ""
	}
	var out strings.Builder
	out.WriteString("goav")
	if e.Operation != "" {
		out.WriteString(": cannot ")
		out.WriteString(e.Operation)
	} else {
		out.WriteString(": build failed")
	}
	if e.Node != "" {
		out.WriteString(" for ")
		out.WriteString(e.Node)
	}
	if e.Reason != "" {
		out.WriteString(": ")
		out.WriteString(e.Reason)
	}
	if len(e.Suggestions) != 0 {
		out.WriteString("\nSuggestions:")
		for i := range e.Suggestions {
			out.WriteString("\n  - ")
			out.WriteString(e.Suggestions[i])
		}
	}
	return out.String()
}

func (e *BuildError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type JobOption func(*jobConfig)

type jobConfig struct {
	runtime Runtime
}

func UseRuntime(runtime Runtime) JobOption {
	return func(config *jobConfig) {
		config.runtime = runtime
	}
}

func Default() Runtime {
	return New(WithDefaults())
}

type CodecOption func(*CodecSpec)

type CodecSpec struct {
	ID         av.CodecID
	Type       av.MediaType
	Parameters av.CodecParameters
	Bitrate    int
	Copy       bool
	Auto       bool
}

func Auto() CodecSpec {
	return CodecSpec{Auto: true}
}

func Copy() CodecSpec {
	return CodecSpec{Copy: true}
}

func Opus(options ...CodecOption) CodecSpec {
	return codecSpec(av.CodecOpus, av.MediaAudio, av.CodecParameters{
		ID:            av.CodecOpus,
		Type:          av.MediaAudio,
		ClockRate:     48000,
		SampleRate:    48000,
		Channels:      Stereo,
		ChannelLayout: "stereo",
	}, options...)
}

func OpusVoice() CodecSpec {
	return Opus(Bitrate(32_000), Channels(Mono))
}

func OpusMusic() CodecSpec {
	return Opus(Bitrate(128_000), Channels(Stereo))
}

func VP8(options ...CodecOption) CodecSpec {
	return codecSpec(av.CodecVP8, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecVP8,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func VP9(options ...CodecOption) CodecSpec {
	return codecSpec(av.CodecVP9, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecVP9,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func H264(options ...CodecOption) CodecSpec {
	return codecSpec(av.CodecH264, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecH264,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func AV1(options ...CodecOption) CodecSpec {
	return codecSpec(av.CodecAV1, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecAV1,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func Bitrate(bitsPerSecond int) CodecOption {
	return func(spec *CodecSpec) {
		spec.Bitrate = bitsPerSecond
	}
}

func Channels(channels int) CodecOption {
	return func(spec *CodecSpec) {
		spec.Parameters.Channels = channels
		if channels == Mono {
			spec.Parameters.ChannelLayout = "mono"
		}
		if channels == Stereo {
			spec.Parameters.ChannelLayout = "stereo"
		}
	}
}

func SampleRate(sampleRate int) CodecOption {
	return func(spec *CodecSpec) {
		spec.Parameters.SampleRate = sampleRate
	}
}

func codecSpec(id av.CodecID, media av.MediaType, params av.CodecParameters, options ...CodecOption) CodecSpec {
	spec := CodecSpec{ID: id, Type: media, Parameters: params}
	for i := range options {
		if options[i] != nil {
			options[i](&spec)
		}
	}
	if spec.Parameters.ID == "" {
		spec.Parameters.ID = id
	}
	if spec.Parameters.Type == "" {
		spec.Parameters.Type = media
	}
	return spec
}

type ResizeOption func(*filter.ResizeConfig)
type AudioOption func(*filter.ResampleConfig)

type TransformSpec struct {
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
}

func Resize(width int, height int, options ...ResizeOption) TransformSpec {
	config := filter.ResizeConfig{Width: width, Height: height, Mode: filter.ResizeExact}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return TransformSpec{Resize: &config}
}

func Resample(sampleRate int, channels int, options ...AudioOption) TransformSpec {
	config := filter.ResampleConfig{SampleRate: sampleRate, Channels: channels}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return TransformSpec{Resample: &config}
}

type InputSpec struct {
	input    Input
	rtp      *rtpInputSpec
	codec    CodecSpec
	name     string
	realtime bool
}

type rtpInputSpec struct {
	receiver      rtpav.PacketReader
	feedback      rtpav.FeedbackWriter
	jitter        rtpav.JitterBuffer
	depacketizers []rtpav.Depacketizer
	limits        RTPBufferLimits
	maxTSGap      av.Duration
}

type RTPInputOption func(*InputSpec)

func FileInput(name string, reader io.Reader) InputSpec {
	return InputSpec{
		input: Input{
			Name:     name,
			Protocol: av.ProtocolFile,
			Reader:   reader,
		},
		name: name,
	}
}

func URI(uri string) InputSpec {
	return InputSpec{
		input: Input{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

func RTP(receiver rtpav.PacketReader, options ...RTPInputOption) InputSpec {
	spec := InputSpec{
		input: Input{Protocol: av.ProtocolRTP, Realtime: true},
		rtp:   &rtpInputSpec{receiver: receiver},
	}
	for i := range options {
		if options[i] != nil {
			options[i](&spec)
		}
	}
	return spec
}

func (s InputSpec) Name(name string) InputSpec {
	s.name = name
	s.input.Name = name
	return s
}

func (s InputSpec) Codec(codec CodecSpec) InputSpec {
	s.codec = codec
	return s
}

func (s InputSpec) Depacketize(depacketizers ...rtpav.Depacketizer) InputSpec {
	if s.rtp == nil {
		s.rtp = &rtpInputSpec{}
	}
	for i := range depacketizers {
		if depacketizers[i] != nil {
			s.rtp.depacketizers = append(s.rtp.depacketizers, depacketizers[i])
		}
	}
	return s
}

func (s InputSpec) Jitter(jitter rtpav.JitterBuffer) InputSpec {
	if s.rtp == nil {
		s.rtp = &rtpInputSpec{}
	}
	s.rtp.jitter = jitter
	return s
}

func (s InputSpec) Feedback(feedback rtpav.FeedbackWriter) InputSpec {
	if s.rtp == nil {
		s.rtp = &rtpInputSpec{}
	}
	s.rtp.feedback = feedback
	return s
}

func (s InputSpec) RTPBuffer(limits RTPBufferLimits) InputSpec {
	if s.rtp == nil {
		s.rtp = &rtpInputSpec{}
	}
	s.rtp.limits = limits
	return s
}

func (s InputSpec) MaxTimestampGap(gap av.Duration) InputSpec {
	if s.rtp == nil {
		s.rtp = &rtpInputSpec{}
	}
	s.rtp.maxTSGap = gap
	return s
}

func (s InputSpec) apply(builder Builder) Builder {
	if s.rtp == nil {
		input := s.input
		input.Realtime = input.Realtime || s.realtime
		return builder.Input(input)
	}
	return builder.RTP(s.rtp.receiver, s.rtpOptions()...)
}

func (s InputSpec) rtpOptions() []RTPOption {
	options := make([]RTPOption, 0, 6)
	if s.name != "" {
		options = append(options, WithRTPName(s.name))
	}
	if s.rtp.feedback != nil {
		options = append(options, WithRTPFeedback(s.rtp.feedback))
	}
	if s.rtp.jitter != nil {
		options = append(options, WithRTPJitter(s.rtp.jitter))
	}
	depacketizers := append([]rtpav.Depacketizer(nil), s.rtp.depacketizers...)
	depacketizers = append(depacketizers, s.codecDepacketizers()...)
	if len(depacketizers) != 0 {
		options = append(options, WithRTPDepacketizers(depacketizers...))
	}
	if s.rtp.limits != (RTPBufferLimits{}) {
		options = append(options, WithRTPBufferLimits(s.rtp.limits))
	}
	if s.rtp.maxTSGap != (av.Duration{}) {
		options = append(options, WithRTPMaxTimestampGap(s.rtp.maxTSGap))
	}
	return options
}

func (s InputSpec) codecDepacketizers() []rtpav.Depacketizer {
	if s.codec.ID == "" {
		return nil
	}
	stream := av.Stream{
		ID:    av.StreamID(s.name),
		Type:  s.codec.Type,
		Codec: s.codec.Parameters,
	}
	switch s.codec.ID {
	case av.CodecOpus:
		return []rtpav.Depacketizer{rtpav.NewOpusDepacketizer(stream)}
	case av.CodecVP8:
		return []rtpav.Depacketizer{rtpav.NewVP8Depacketizer(stream)}
	case av.CodecVP9:
		return []rtpav.Depacketizer{rtpav.NewVP9Depacketizer(stream)}
	case av.CodecH264:
		return []rtpav.Depacketizer{rtpav.NewH264Depacketizer(stream)}
	case av.CodecAV1:
		return []rtpav.Depacketizer{rtpav.NewAV1Depacketizer(stream)}
	default:
		return nil
	}
}

func (s InputSpec) intent() InputIntent {
	return InputIntent{
		Name:     firstNonEmpty(s.name, s.input.Name),
		URI:      s.input.URI,
		Protocol: s.input.Protocol,
		Codec:    s.codec,
		Realtime: s.input.Realtime || s.rtp != nil,
	}
}

func (s InputSpec) selector(media av.MediaType) av.StreamSelector {
	selector := av.StreamSelector{Type: media}
	if selector.Type == "" {
		selector.Type = s.codec.Type
	}
	if s.codec.ID != "" {
		selector.Codec = s.codec.ID
	}
	return selector
}

type OutputSpec struct {
	output Output
	sink   pipeline.Sink
	format av.FormatID
	name   string
}

func FileOutput(name string, writer io.Writer) OutputSpec {
	return OutputSpec{
		output: Output{
			Name:     name,
			Protocol: av.ProtocolFile,
			Writer:   writer,
		},
		name: name,
	}
}

func File(name string, writer io.Writer) OutputSpec {
	return FileOutput(name, writer)
}

func URIOutput(uri string) OutputSpec {
	return OutputSpec{
		output: Output{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

func FrameSink(sink pipeline.Sink) OutputSpec {
	name := ""
	if sink != nil {
		name = sink.Name()
	}
	return OutputSpec{sink: sink, name: name}
}

func (s OutputSpec) Name(name string) OutputSpec {
	s.name = name
	s.output.Name = name
	return s
}

func (s OutputSpec) Format(format av.FormatID) OutputSpec {
	s.format = format
	return s
}

func (s OutputSpec) apply(builder Builder) Builder {
	if s.sink != nil {
		return builder.Sink(s.sink)
	}
	return builder.Output(s.output)
}

func (s OutputSpec) label(fallback string) string {
	return firstNonEmpty(s.name, s.output.Name, s.output.URI, fallback)
}

func (s OutputSpec) intent() OutputIntent {
	return OutputIntent{
		Name:     s.label("output"),
		URI:      s.output.URI,
		Protocol: s.output.Protocol,
		Format:   s.format,
	}
}

type Job struct {
	name     string
	runtime  Runtime
	inputs   []InputSpec
	outputs  []OutputSpec
	selector av.StreamSelector
	decode   bool
}

func Record(input InputSpec, output OutputSpec, options ...JobOption) *Job {
	return From(input, options...).named("record").To(output)
}

func From(input InputSpec, options ...JobOption) *Job {
	job := newJob("from", options...)
	job.inputs = append(job.inputs, input)
	return job
}

func Decode(input InputSpec, sink pipeline.Sink, options ...JobOption) *Job {
	job := newJob("decode", options...)
	job.inputs = append(job.inputs, input)
	job.outputs = append(job.outputs, FrameSink(sink))
	job.selector = input.selector("")
	job.decode = true
	return job
}

func newJob(name string, options ...JobOption) *Job {
	config := jobConfig{runtime: Default()}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return &Job{name: name, runtime: config.runtime}
}

func (j *Job) named(name string) *Job {
	j.name = name
	return j
}

func (j *Job) To(outputs ...OutputSpec) *Job {
	j.outputs = append(j.outputs, outputs...)
	return j
}

func (j *Job) Intent() Intent {
	intent := Intent{Name: j.name}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
	}
	for i := range j.inputs {
		intent.Inputs = append(intent.Inputs, j.inputs[i].intent())
	}
	if j.decode {
		intent.Streams = append(intent.Streams, StreamIntent{
			Select: StreamSelect{
				ID:    j.selector.ID,
				Index: j.selector.Index,
				Type:  j.selector.Type,
				Codec: j.selector.Codec,
				Name:  j.selector.Name,
			},
			Decode:  true,
			RouteTo: outputLabels(j.outputs),
		})
	}
	for i := range j.outputs {
		intent.Outputs = append(intent.Outputs, j.outputs[i].intent())
	}
	return intent
}

func (j *Job) Describe() (pipeline.Spec, error) {
	builder, err := j.builder()
	if err != nil {
		return pipeline.Spec{}, err
	}
	return builder.Describe()
}

func (j *Job) Explain(context.Context) (Explanation, error) {
	spec, err := j.Describe()
	if err != nil {
		return Explanation{}, err
	}
	return Explanation{Intent: j.Intent(), Spec: spec}, nil
}

func (j *Job) Build(ctx context.Context) (Task, error) {
	builder, err := j.builder()
	if err != nil {
		return nil, err
	}
	return builder.Build(ctx)
}

func (j *Job) Run(ctx context.Context) error {
	task, err := j.Build(ctx)
	if err != nil {
		return err
	}
	defer task.Close()
	return task.Run(ctx)
}

func (j *Job) builder() (Builder, error) {
	if j.runtime == nil {
		return nil, &BuildError{Code: "runtime_missing", Operation: "build job", Reason: "no runtime is configured"}
	}
	if len(j.inputs) == 0 {
		return nil, &BuildError{Code: "input_missing", Operation: "build job", Reason: "no input is configured"}
	}
	if len(j.outputs) == 0 {
		return nil, &BuildError{Code: "output_missing", Operation: "build job", Reason: "no output is configured"}
	}
	builder := j.runtime.New()
	for i := range j.inputs {
		builder = j.inputs[i].apply(builder)
	}
	if j.decode {
		builder = builder.Decode(j.selector)
	}
	for i := range j.outputs {
		builder = j.outputs[i].apply(builder)
	}
	return builder, nil
}

type Explanation struct {
	Intent Intent
	Spec   pipeline.Spec
}

func (e Explanation) Text() string {
	var out strings.Builder
	name := e.Intent.Name
	if name == "" {
		name = "media job"
	}
	out.WriteString(name)
	out.WriteByte('\n')
	for i := range e.Intent.Inputs {
		input := e.Intent.Inputs[i]
		out.WriteString("\nInput:\n  ")
		out.WriteString(firstNonEmpty(input.Name, input.URI, "input"))
		if input.Codec.ID != "" {
			out.WriteString(": ")
			out.WriteString(string(input.Codec.ID))
		}
	}
	if len(e.Intent.Streams) != 0 {
		out.WriteString("\n\nStreams:")
		for i := range e.Intent.Streams {
			stream := e.Intent.Streams[i]
			out.WriteString("\n  ")
			out.WriteString(firstNonEmpty(stream.Name, string(stream.Select.Type), "stream"))
			if stream.Encode.ID != "" {
				out.WriteString(" -> ")
				out.WriteString(string(stream.Encode.ID))
			}
		}
	}
	if len(e.Intent.Outputs) != 0 {
		out.WriteString("\n\nOutputs:")
		for i := range e.Intent.Outputs {
			out.WriteString("\n  ")
			out.WriteString(firstNonEmpty(e.Intent.Outputs[i].Name, e.Intent.Outputs[i].URI, "output"))
		}
	}
	out.WriteString("\n\nPlan:\n")
	out.WriteString(e.Spec.String())
	return out.String()
}

func (e Explanation) Mermaid() string {
	return e.Spec.Render("mermaid")
}

func (e Explanation) DOT() string {
	return e.Spec.Render("dot")
}

type StreamOption func(*streamBuild)

type streamBuild struct {
	name       string
	selector   av.StreamSelector
	decode     bool
	transforms []TransformSpec
	encode     CodecSpec
	labels     []string
}

func StreamID(id av.StreamID) StreamOption {
	return func(stream *streamBuild) {
		stream.selector.ID = id
	}
}

func StreamName(name string) StreamOption {
	return func(stream *streamBuild) {
		stream.selector.Name = name
	}
}

func StreamIndex(index int) StreamOption {
	return func(stream *streamBuild) {
		stream.selector.Index = index
	}
}

type TranscodeJob struct {
	runtime Runtime
	input   InputSpec
	streams []streamBuild
	outputs []namedOutputSpec
}

type namedOutputSpec struct {
	name   string
	output OutputSpec
}

func Transcode(input InputSpec, options ...JobOption) *TranscodeJob {
	config := jobConfig{runtime: Default()}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return &TranscodeJob{runtime: config.runtime, input: input}
}

func (j *TranscodeJob) Audio(name string, options ...StreamOption) *StreamBuilder {
	return j.stream(name, av.MediaAudio, options...)
}

func (j *TranscodeJob) Video(name string, options ...StreamOption) *StreamBuilder {
	return j.stream(name, av.MediaVideo, options...)
}

func (j *TranscodeJob) Output(name string, output OutputSpec) *TranscodeJob {
	j.outputs = append(j.outputs, namedOutputSpec{name: name, output: output.Name(firstNonEmpty(output.name, name))})
	return j
}

func (j *TranscodeJob) Intent() Intent {
	intent := Intent{
		Name:   "transcode",
		Inputs: []InputIntent{j.input.intent()},
	}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
	}
	for i := range j.streams {
		stream := j.streams[i]
		intent.Streams = append(intent.Streams, StreamIntent{
			Name: stream.name,
			Select: StreamSelect{
				ID:    stream.selector.ID,
				Index: stream.selector.Index,
				Type:  stream.selector.Type,
				Codec: stream.selector.Codec,
				Name:  stream.selector.Name,
			},
			Decode:     stream.decode,
			Transforms: append([]TransformSpec(nil), stream.transforms...),
			Encode:     stream.encode,
			RouteTo:    append([]string(nil), stream.labels...),
		})
	}
	for i := range j.outputs {
		intent.Outputs = append(intent.Outputs, j.outputs[i].output.intent())
	}
	return intent
}

func (j *TranscodeJob) Plan() (transcodepkg.Plan, error) {
	if j.input.rtp != nil {
		return transcodepkg.Plan{}, &BuildError{
			Code:      "unsupported_input",
			Operation: "plan transcode",
			Reason:    "RTP transcode recipes are not lowered to transcode.Plan yet",
			Suggestions: []string{
				"use Record(...) for packet recording",
				"use the advanced builder for RTP decode/filter/encode paths until intent lowering covers RTP transcode",
			},
		}
	}
	outputs := make(map[string]OutputSpec, len(j.outputs))
	outputOrder := make([]string, 0, len(j.outputs))
	for i := range j.outputs {
		name := j.outputs[i].name
		if name == "" {
			name = j.outputs[i].output.label(fmt.Sprintf("output-%d", i))
		}
		if _, ok := outputs[name]; !ok {
			outputOrder = append(outputOrder, name)
		}
		outputs[name] = j.outputs[i].output.Name(firstNonEmpty(j.outputs[i].output.name, name))
	}

	renditions := make([]transcodepkg.Rendition, 0, len(j.streams))
	outputRenditions := make(map[string][]string, len(outputs))
	for i := range j.streams {
		stream := j.streams[i]
		if stream.encode.ID == "" && !stream.encode.Copy {
			return transcodepkg.Plan{}, &BuildError{
				Code:      "encode_missing",
				Operation: "plan transcode",
				Node:      stream.name,
				Reason:    "stream has no codec target",
				Suggestions: []string{
					"call .Opus(...), .VP9(...), .H264(...), or another codec method before .To(...)",
				},
			}
		}
		for _, label := range stream.labels {
			if _, ok := outputs[label]; ok {
				continue
			}
			return transcodepkg.Plan{}, &BuildError{
				Code:      "output_missing",
				Operation: "plan transcode",
				Node:      stream.name,
				Reason:    "output " + label + " is referenced but not defined",
				Suggestions: []string{
					"call .Output(" + label + ", goav.FileOutput(...))",
					"pass goav.FileOutput(...) directly to .To(...)",
				},
			}
		}
		renditionName := firstNonEmpty(stream.name, fmt.Sprintf("rendition-%d", i))
		rendition := transcodepkg.Rendition{
			Name:     renditionName,
			Selector: stream.selector,
			Decode:   true,
			Encode: codec.EncodeConfig{
				Parameters: stream.encode.Parameters,
				Bitrate:    stream.encode.Bitrate,
			},
			Labels: append([]string(nil), stream.labels...),
		}
		for _, label := range stream.labels {
			outputRenditions[label] = append(outputRenditions[label], renditionName)
		}
		for j := range stream.transforms {
			if stream.transforms[j].Resize != nil {
				resize := *stream.transforms[j].Resize
				rendition.Resize = &resize
			}
			if stream.transforms[j].Resample != nil {
				resample := *stream.transforms[j].Resample
				rendition.Resample = &resample
			}
		}
		renditions = append(renditions, rendition)
	}

	planOutputs := make([]transcodepkg.Output, 0, len(outputOrder))
	for i := range outputOrder {
		name := outputOrder[i]
		output := outputs[name]
		planOutputs = append(planOutputs, transcodepkg.Output{
			Name:       name,
			Target:     output.output,
			Format:     output.format,
			Renditions: append([]string(nil), outputRenditions[name]...),
		})
	}
	return transcodepkg.Plan{
		Name:       "transcode",
		Input:      j.input.input,
		Renditions: renditions,
		Outputs:    planOutputs,
	}, nil
}

func (j *TranscodeJob) Describe() (pipeline.Spec, error) {
	builder, err := j.builder()
	if err != nil {
		return pipeline.Spec{}, err
	}
	return builder.Describe()
}

func (j *TranscodeJob) Explain(context.Context) (Explanation, error) {
	spec, err := j.Describe()
	if err != nil {
		return Explanation{}, err
	}
	return Explanation{Intent: j.Intent(), Spec: spec}, nil
}

func (j *TranscodeJob) Build(ctx context.Context) (Task, error) {
	builder, err := j.builder()
	if err != nil {
		return nil, err
	}
	return builder.Build(ctx)
}

func (j *TranscodeJob) Run(ctx context.Context) error {
	task, err := j.Build(ctx)
	if err != nil {
		return err
	}
	defer task.Close()
	return task.Run(ctx)
}

func (j *TranscodeJob) builder() (Builder, error) {
	if j.runtime == nil {
		return nil, &BuildError{Code: "runtime_missing", Operation: "build transcode", Reason: "no runtime is configured"}
	}
	plan, err := j.Plan()
	if err != nil {
		return nil, err
	}
	return j.runtime.New().Transcode(plan), nil
}

func (j *TranscodeJob) stream(name string, media av.MediaType, options ...StreamOption) *StreamBuilder {
	stream := streamBuild{
		name:     name,
		selector: av.StreamSelector{Type: media},
		decode:   true,
	}
	for i := range options {
		if options[i] != nil {
			options[i](&stream)
		}
	}
	j.streams = append(j.streams, stream)
	return &StreamBuilder{job: j, index: len(j.streams) - 1}
}

type StreamBuilder struct {
	job   *TranscodeJob
	index int
}

func (b *StreamBuilder) Decode() *StreamBuilder {
	b.current().decode = true
	return b
}

func (b *StreamBuilder) Resize(width int, height int, options ...ResizeOption) *StreamBuilder {
	stream := b.current()
	stream.transforms = append(stream.transforms, Resize(width, height, options...))
	return b
}

func (b *StreamBuilder) Resample(sampleRate int, channels int, options ...AudioOption) *StreamBuilder {
	stream := b.current()
	stream.transforms = append(stream.transforms, Resample(sampleRate, channels, options...))
	return b
}

func (b *StreamBuilder) Opus(bitrate int, options ...CodecOption) *StreamBuilder {
	return b.encode(Opus(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) VP8(bitrate int, options ...CodecOption) *StreamBuilder {
	return b.encode(VP8(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) VP9(bitrate int, options ...CodecOption) *StreamBuilder {
	return b.encode(VP9(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) H264(bitrate int, options ...CodecOption) *StreamBuilder {
	return b.encode(H264(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) AV1(bitrate int, options ...CodecOption) *StreamBuilder {
	return b.encode(AV1(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) To(targets ...any) *TranscodeJob {
	stream := b.current()
	for i := range targets {
		switch target := targets[i].(type) {
		case string:
			if target != "" {
				stream.labels = append(stream.labels, target)
			}
		case OutputSpec:
			name := target.label(fmt.Sprintf("%s-output-%d", stream.name, len(b.job.outputs)))
			b.job.Output(name, target)
			stream.labels = append(stream.labels, name)
		default:
			stream.labels = append(stream.labels, fmt.Sprint(target))
		}
	}
	return b.job
}

func (b *StreamBuilder) encode(codec CodecSpec) *StreamBuilder {
	b.current().encode = codec
	return b
}

func (b *StreamBuilder) current() *streamBuild {
	return &b.job.streams[b.index]
}

func outputLabels(outputs []OutputSpec) []string {
	labels := make([]string, 0, len(outputs))
	for i := range outputs {
		labels = append(labels, outputs[i].label(fmt.Sprintf("output-%d", i)))
	}
	return labels
}

func firstNonEmpty(values ...string) string {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}
