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
	MIMEType string
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
	MIMEType string
	Format   av.FormatID
}

type PolicyIntent struct {
	Realtime bool
}

type StreamSelect struct {
	ID       av.StreamID
	Index    int
	UseIndex bool
	Type     av.MediaType
	Codec    av.CodecID
	Name     string
}

type BuildError struct {
	Code        string
	Operation   string
	Node        string
	Reason      string
	Details     []string
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
	if len(e.Details) != 0 {
		out.WriteString("\nDetails:")
		for i := range e.Details {
			out.WriteString("\n  - ")
			out.WriteString(e.Details[i])
		}
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

type RecordOption interface {
	applyRecord(*recordConfig)
}

type recordConfig struct {
	job     jobConfig
	outputs []OutputSpec
}

func (option JobOption) applyRecord(config *recordConfig) {
	if option != nil {
		option(&config.job)
	}
}

func (s OutputSpec) applyRecord(config *recordConfig) {
	config.outputs = append(config.outputs, s)
}

func Default() Runtime {
	return New(WithDefaults())
}

type CodecOption func(*CodecSpec)

type CodecSpec struct {
	ID            av.CodecID
	Type          av.MediaType
	Parameters    av.CodecParameters
	Bitrate       int
	Copy          bool
	Auto          bool
	sampleRateSet bool
	channelsSet   bool
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
		spec.channelsSet = true
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
		spec.Parameters.ClockRate = uint32(sampleRate)
		spec.sampleRateSet = true
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
	err      error
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

func (s InputSpec) MIME(mimeType string) InputSpec {
	s.input.MIMEType = mimeType
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

func (s InputSpec) validate() error {
	if s.err != nil {
		return &BuildError{
			Code:      "input_invalid",
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, s.input.URI, "input"),
			Reason:    s.err.Error(),
			Suggestions: []string{
				"check the input constructor arguments",
				"use goav.RTP(reader) when you already have an RTP packet reader",
			},
			Cause: s.err,
		}
	}
	if err := s.validateRTPReceiver(); err != nil {
		return err
	}
	if err := s.validatePlainInput(); err != nil {
		return err
	}
	if err := s.validateRTPPolicy(); err != nil {
		return err
	}
	return s.validateRTPCodec()
}

func (s InputSpec) validatePlainInput() error {
	if s.rtp != nil {
		return nil
	}
	if s.input.Name != "" || s.input.URI != "" || s.input.Protocol != "" || s.input.MIMEType != "" || s.input.Reader != nil || s.input.ReaderAt != nil {
		return nil
	}
	return &BuildError{
		Code:      "input_invalid",
		Operation: "build input",
		Node:      "input",
		Reason:    "empty input spec",
		Suggestions: []string{
			"use goav.FileInput(name, reader) for file-like input",
			"use goav.URI(uri) for URI-backed input",
			"use goav.RTP(reader) or goav.WebRTCTrack(track) for realtime receive",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (s InputSpec) validateRTPReceiver() error {
	if s.rtp == nil || s.rtp.receiver != nil {
		return nil
	}
	return &BuildError{
		Code:      "rtp_reader_missing",
		Operation: "build input",
		Node:      firstNonEmpty(s.name, s.input.Name, "rtp"),
		Reason:    "RTP input has no packet reader",
		Suggestions: []string{
			"pass a non-nil rtpav.PacketReader to goav.RTP(reader)",
			"use goav.WebRTCTrack(track) for Pion WebRTC receive",
		},
		Cause: ErrNilSource,
	}
}

func (s InputSpec) validateRTPPolicy() error {
	if s.rtp == nil {
		return nil
	}
	limits := s.rtp.limits
	switch {
	case limits.MaxReady < 0:
		return s.invalidRTPBufferLimitError("MaxReady", limits.MaxReady)
	case limits.MaxEvents < 0:
		return s.invalidRTPBufferLimitError("MaxEvents", limits.MaxEvents)
	case limits.MaxFeedback < 0:
		return s.invalidRTPBufferLimitError("MaxFeedback", limits.MaxFeedback)
	case limits.MaxPackets < 0:
		return s.invalidRTPBufferLimitError("MaxPackets", limits.MaxPackets)
	}

	gap := s.rtp.maxTSGap
	if gap == (av.Duration{}) {
		return nil
	}
	if gap.Value < 0 {
		return s.invalidRTPTimestampGapError("negative timestamp gap", gap)
	}
	if gap.Value > 0 && !gap.Base.Valid() {
		return s.invalidRTPTimestampGapError("timestamp gap has an invalid timebase", gap)
	}
	return nil
}

func (s InputSpec) invalidRTPBufferLimitError(field string, value int) error {
	return &BuildError{
		Code:      "rtp_buffer_invalid",
		Operation: "build input",
		Node:      firstNonEmpty(s.name, s.input.Name, "rtp"),
		Reason:    "RTP buffer limits must be positive when set",
		Details: []string{
			fmt.Sprintf("%s=%d", field, value),
		},
		Suggestions: []string{
			"use positive RTP buffer limits or leave fields zero for defaults",
			"set MaxPackets, MaxEvents, MaxReady, or MaxFeedback only when tightening realtime buffering",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (s InputSpec) invalidRTPTimestampGapError(reason string, gap av.Duration) error {
	return &BuildError{
		Code:      "rtp_timestamp_gap_invalid",
		Operation: "build input",
		Node:      firstNonEmpty(s.name, s.input.Name, "rtp"),
		Reason:    reason,
		Details: []string{
			fmt.Sprintf("gap=%d base=%d/%d", gap.Value, gap.Base.Num, gap.Base.Den),
		},
		Suggestions: []string{
			"use goav.SamplesDuration(samples, clockRate) or a positive av.Duration with a valid timebase",
			"omit .MaxTimestampGap(...) when no gap threshold is needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (s InputSpec) validateRTPCodec() error {
	if s.rtp == nil {
		return nil
	}
	if s.codec.Auto {
		return &BuildError{
			Code:      "rtp_codec_auto_unresolved",
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, "rtp"),
			Reason:    "automatic RTP codec detection is not implemented for recipe inputs yet",
			Suggestions: []string{
				"call .Codec(goav.Opus()), .Codec(goav.VP8()), .Codec(goav.VP9()), .Codec(goav.H264()), or .Codec(goav.AV1())",
				"pass a custom depacketizer with .Depacketize(...)",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.codec.Copy {
		return &BuildError{
			Code:      "rtp_codec_copy_invalid",
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, "rtp"),
			Reason:    "RTP input codec intent describes depacketization, not output copying",
			Suggestions: []string{
				"use goav.Record(goav.RTP(reader).Codec(...), output) for packet-preserving receive",
				"omit .Codec(goav.Copy()) on RTP inputs",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.codec.ID == "" || len(s.rtp.depacketizers) != 0 {
		return nil
	}
	switch s.codec.ID {
	case av.CodecOpus, av.CodecVP8, av.CodecVP9, av.CodecH264, av.CodecAV1:
		return nil
	default:
		return &BuildError{
			Code:      "rtp_codec_unsupported",
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, string(s.codec.ID), "rtp"),
			Reason:    string(s.codec.ID) + " has no built-in RTP depacketizer",
			Suggestions: []string{
				"use a built-in receive codec: Opus, VP8, VP9, H264, or AV1",
				"pass a custom depacketizer with .Depacketize(...)",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
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
	if len(depacketizers) != 0 {
		options = append(options, WithRTPDepacketizers(depacketizers...))
	}
	if s.codec.ID != "" {
		options = append(options, withRTPCodec(s.codec))
	}
	if s.rtp.limits != (RTPBufferLimits{}) {
		options = append(options, WithRTPBufferLimits(s.rtp.limits))
	}
	if s.rtp.maxTSGap != (av.Duration{}) {
		options = append(options, WithRTPMaxTimestampGap(s.rtp.maxTSGap))
	}
	return options
}

func (s InputSpec) intent() InputIntent {
	return InputIntent{
		Name:     firstNonEmpty(s.name, s.input.Name),
		URI:      s.input.URI,
		Protocol: s.input.Protocol,
		MIMEType: s.input.MIMEType,
		Codec:    s.codec,
		Realtime: s.input.Realtime || s.rtp != nil,
	}
}

func (s InputSpec) inputName(fallback string) string {
	return firstNonEmpty(s.name, s.input.Name, s.input.URI, fallback)
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
	err    error
}

type formattedOutputBuilder interface {
	outputWithFormat(Output, av.FormatID) Builder
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

func URIOutput(uri string) OutputSpec {
	return OutputSpec{
		output: Output{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

func FrameSink(sink Sink) OutputSpec {
	name := ""
	if sink != nil {
		name = sink.Name()
	}
	if sink == nil {
		return OutputSpec{err: ErrNilSink}
	}
	return OutputSpec{sink: sink, name: name}
}

func (s OutputSpec) Name(name string) OutputSpec {
	s.name = name
	s.output.Name = name
	return s
}

func (s OutputSpec) MIME(mimeType string) OutputSpec {
	s.output.MIMEType = mimeType
	return s
}

func (s OutputSpec) Format(format av.FormatID) OutputSpec {
	s.format = format
	return s
}

func (s OutputSpec) apply(builder Builder) (Builder, error) {
	if s.sink != nil {
		return builder.Sink(s.sink), nil
	}
	if s.format == "" {
		return builder.Output(s.output), nil
	}
	formatted, ok := builder.(formattedOutputBuilder)
	if !ok {
		return nil, &BuildError{
			Code:      "output_format_unsupported",
			Operation: "build job",
			Node:      s.label("output"),
			Reason:    "runtime builder cannot receive an explicit output format",
			Suggestions: []string{
				"use goav.Default() or goav.New(...) for recipe output formats",
				"use a named output whose extension can be probed by the runtime",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return formatted.outputWithFormat(s.output, s.format), nil
}

func (s OutputSpec) validate(operation string, fallback string) error {
	node := s.label(fallback)
	if s.err != nil {
		return &BuildError{
			Code:      "output_invalid",
			Operation: operation,
			Node:      node,
			Reason:    s.err.Error(),
			Suggestions: []string{
				"pass a non-nil sink to goav.FrameSink(...)",
				"use goav.FileOutput(...) or goav.URIOutput(...) for muxed output",
			},
			Cause: s.err,
		}
	}
	if s.sink != nil {
		return nil
	}
	if s.output.Name == "" && s.output.URI == "" && s.output.Protocol == "" && s.output.MIMEType == "" && s.output.Writer == nil && s.format == "" {
		return &BuildError{
			Code:      "output_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "empty output spec",
			Suggestions: []string{
				"use goav.FileOutput(name, writer) for muxed output",
				"use goav.FrameSink(sink) for decoded frames",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.Protocol == av.ProtocolFile && s.output.Writer == nil {
		return &BuildError{
			Code:      "output_writer_missing",
			Operation: operation,
			Node:      node,
			Reason:    "file output has no writer",
			Suggestions: []string{
				"pass a non-nil io.Writer to goav.FileOutput(name, writer)",
				"use goav.URIOutput(uri) when the output is opened by an adapter",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.Protocol == av.ProtocolFile && s.output.Writer != nil && s.output.Name == "" && s.output.URI == "" && s.output.MIMEType == "" && s.format == "" {
		return &BuildError{
			Code:      "output_format_missing",
			Operation: operation,
			Node:      node,
			Reason:    "writer-backed file output has no name, URI, MIME type, or explicit format",
			Suggestions: []string{
				"give goav.FileOutput(name, writer) a name with a container extension",
				"call .Format(...) with a registered container when the writer has no filename",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.URI == "" && s.output.Protocol != av.ProtocolFile && s.output.Writer == nil {
		return &BuildError{
			Code:      "output_target_missing",
			Operation: operation,
			Node:      node,
			Reason:    "output has no URI, writer, or sink",
			Suggestions: []string{
				"use goav.FileOutput(name, writer) for writer-backed output",
				"use goav.URIOutput(uri) for URI-backed output",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return nil
}

func (s OutputSpec) label(fallback string) string {
	return firstNonEmpty(s.name, s.output.Name, s.output.URI, fallback)
}

func (s OutputSpec) intent() OutputIntent {
	return OutputIntent{
		Name:     s.label("output"),
		URI:      s.output.URI,
		Protocol: s.output.Protocol,
		MIMEType: s.output.MIMEType,
		Format:   s.format,
	}
}

type Job struct {
	name    string
	runtime Runtime
	inputs  []InputSpec
	outputs []OutputSpec
	stream  *jobStreamBuild
	err     error
}

type jobStreamBuild struct {
	name     string
	selector av.StreamSelector
	decode   bool
	steps    []jobStreamStep
	encode   CodecSpec
	outputs  []OutputSpec
}

type jobStreamStep struct {
	stage     pipeline.Stage
	transform TransformSpec
}

func Record(input InputSpec, output OutputSpec, options ...RecordOption) *Job {
	config := recordConfig{
		job:     jobConfig{runtime: Default()},
		outputs: []OutputSpec{output},
	}
	for i := range options {
		if options[i] != nil {
			options[i].applyRecord(&config)
		}
	}
	return (&Job{
		name:    "record",
		runtime: config.job.runtime,
		inputs:  []InputSpec{input},
	}).To(config.outputs...)
}

func From(input InputSpec, options ...JobOption) *Job {
	job := newJob("from", options...)
	job.inputs = append(job.inputs, input)
	return job
}

func Decode(input InputSpec, output OutputSpec, options ...JobOption) *Job {
	job := newJob("decode", options...)
	job.inputs = append(job.inputs, input)
	job.stream = &jobStreamBuild{
		selector: input.selector(""),
		decode:   true,
		outputs:  []OutputSpec{output},
	}
	if output.err == nil && output.sink == nil {
		job.setErr(&BuildError{
			Code:      "decode_output_invalid",
			Operation: "build decode",
			Node:      output.label("output"),
			Reason:    "decode recipes write decoded frames to a frame sink",
			Suggestions: []string{
				"use goav.Decode(input, goav.FrameSink(sink)) for the decode shortcut",
				"use goav.From(input).Audio().To(goav.FrameSink(sink)) when stream selection matters",
				"use goav.Record(input, output) for packet-preserving record or remux",
			},
			Cause: ErrUnsupportedBuild,
		})
	}
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

func (j *Job) setErr(err error) {
	if j.err == nil {
		j.err = err
	}
}

func (j *Job) To(outputs ...OutputSpec) *Job {
	j.outputs = append(j.outputs, outputs...)
	return j
}

func (j *Job) And(inputs ...InputSpec) *Job {
	j.inputs = append(j.inputs, inputs...)
	return j
}

func (j *Job) Audio(options ...StreamOption) *JobStreamBuilder {
	return j.streamBuilder("audio", av.MediaAudio, options...)
}

func (j *Job) Video(options ...StreamOption) *JobStreamBuilder {
	return j.streamBuilder("video", av.MediaVideo, options...)
}

func (j *Job) streamBuilder(name string, media av.MediaType, options ...StreamOption) *JobStreamBuilder {
	stream := &jobStreamBuild{
		name:     name,
		selector: newStreamSelector(media, options...),
	}
	if j.stream != nil {
		j.err = duplicateJobStreamError(j.stream, stream)
		return &JobStreamBuilder{job: j, stream: stream}
	}
	j.stream = stream
	return &JobStreamBuilder{job: j, stream: stream}
}

func (j *Job) Intent() Intent {
	intent := Intent{Name: j.name}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
	}
	for i := range j.inputs {
		intent.Inputs = append(intent.Inputs, j.inputs[i].intent())
	}
	if j.stream != nil {
		intent.Streams = append(intent.Streams, StreamIntent{
			Name: j.stream.name,
			Select: StreamSelect{
				ID:       j.stream.selector.ID,
				Index:    j.stream.selector.Index,
				UseIndex: j.stream.selector.UseIndex,
				Type:     j.stream.selector.Type,
				Codec:    j.stream.selector.Codec,
				Name:     j.stream.selector.Name,
			},
			Decode:     j.stream.decode,
			Transforms: j.stream.transformSpecs(),
			Encode:     j.stream.encode,
			RouteTo:    outputLabels(j.stream.outputs),
		})
	}
	outputs := j.allOutputs()
	for i := range outputs {
		intent.Outputs = append(intent.Outputs, outputs[i].intent())
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
	if j.err != nil {
		return nil, j.err
	}
	if len(j.inputs) == 0 {
		return nil, &BuildError{Code: "input_missing", Operation: "build job", Reason: "no input is configured"}
	}
	if err := j.validateInputs(); err != nil {
		return nil, err
	}
	if err := j.validateOutputScope(); err != nil {
		return nil, err
	}
	outputs := j.allOutputs()
	if len(outputs) == 0 {
		return nil, &BuildError{Code: "output_missing", Operation: "build job", Reason: "no output is configured"}
	}
	if err := validateOutputSpecs("build job", outputs); err != nil {
		return nil, err
	}
	builder := j.runtime.New()
	for i := range j.inputs {
		builder = j.inputs[i].apply(builder)
	}
	if j.stream != nil {
		var err error
		builder, err = j.applyStream(builder, j.stream)
		if err != nil {
			return nil, err
		}
	}
	for i := range outputs {
		var err error
		builder, err = outputs[i].apply(builder)
		if err != nil {
			return nil, err
		}
	}
	return builder, nil
}

func (j *Job) validateInputs() error {
	for i := range j.inputs {
		if err := j.inputs[i].validate(); err != nil {
			return err
		}
	}
	if len(j.inputs) <= 1 {
		return nil
	}
	for i := range j.inputs {
		if j.inputs[i].rtp != nil {
			continue
		}
		return &BuildError{
			Code:      "multi_input_unsupported",
			Operation: "build job",
			Node:      firstNonEmpty(j.inputs[i].name, j.inputs[i].input.Name, j.inputs[i].input.URI, fmt.Sprintf("input-%d", i)),
			Reason:    "multiple recipe inputs currently require realtime RTP/WebRTC packet readers",
			Suggestions: []string{
				"use goav.From(goav.RTP(...)).And(goav.RTP(...)) for repeated live inputs",
				"use goav.WebRTCTrack(...) or goav.WebRTCRemote(...) for Pion WebRTC tracks",
				"build an explicit graph when combining multiple file or protocol sources",
			},
		}
	}
	if err := validateRealtimeInputNames(j.inputs); err != nil {
		return err
	}
	return nil
}

func validateRealtimeInputNames(inputs []InputSpec) error {
	seen := make(map[string]int, len(inputs))
	for i := range inputs {
		name := inputs[i].inputName("")
		if name == "" {
			continue
		}
		if firstIndex, ok := seen[name]; ok {
			return duplicateInputNameError(name, firstIndex, i)
		}
		seen[name] = i
	}
	return nil
}

func duplicateInputNameError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "input_duplicate",
		Operation: "build job",
		Node:      name,
		Reason:    fmt.Sprintf("realtime input name %q is defined more than once", name),
		Details: []string{
			fmt.Sprintf("first input index: %d", firstIndex),
			fmt.Sprintf("second input index: %d", secondIndex),
		},
		Suggestions: []string{
			"give each repeated realtime input a distinct .Name(...)",
			"use stable names such as \"audio\" and \"video\" for separate RTP/WebRTC streams",
			"use goav.WebRTCTrack(..., goav.WithTrackStream(...)) when track metadata should provide the name",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (j *Job) validateOutputScope() error {
	if j.stream == nil || len(j.outputs) == 0 {
		return nil
	}
	return &BuildError{
		Code:      "output_scope_mixed",
		Operation: "build job",
		Node:      jobStreamName(j.stream),
		Reason:    "stream recipes use stream-local outputs",
		Suggestions: []string{
			"attach outputs to the selected stream chain with .Audio()...To(...) or .Video()...To(...)",
			"use goav.Record(input, output) or goav.From(input).To(output...) for packet-preserving record/remux",
			"use goav.Transcode(input) when one input needs separate record, preview, or ladder branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (j *Job) allOutputs() []OutputSpec {
	if j.stream == nil || len(j.stream.outputs) == 0 {
		return append([]OutputSpec(nil), j.outputs...)
	}
	outputs := make([]OutputSpec, 0, len(j.outputs)+len(j.stream.outputs))
	outputs = append(outputs, j.outputs...)
	outputs = append(outputs, j.stream.outputs...)
	return outputs
}

func (j *Job) applyStream(builder Builder, stream *jobStreamBuild) (Builder, error) {
	if stream == nil {
		return builder, nil
	}
	outputs := j.allOutputs()
	if !stream.hasOperation() {
		return nil, &BuildError{
			Code:      "stream_operation_missing",
			Operation: "build stream",
			Node:      stream.name,
			Reason:    "the stream was selected but no decode, processing stage, or encoder was requested",
			Suggestions: []string{
				"call .To(goav.FrameSink(...)) to receive decoded frames",
				"call .Opus(...), .VP8(...), or .VP9(...) before writing to a file output",
				"use goav.Record(input, output) for packet-preserving record or remux",
			},
		}
	}
	if err := validateRecipeStreamSelector("build stream", jobStreamName(stream), stream.selector); err != nil {
		return nil, err
	}
	if err := validateRecipeEncode(stream.encode, "build stream", stream.name); err != nil {
		return nil, err
	}
	if outputsContainFrameSink(outputs) && outputsContainMuxTarget(outputs) {
		return nil, mixedStreamOutputError(stream)
	}
	if stream.encode.ID == "" && outputsContainMuxTarget(outputs) {
		return nil, &BuildError{
			Code:      "encode_missing",
			Operation: "build stream",
			Node:      stream.name,
			Reason:    "decoded frames cannot be written to a muxed output without an encoder",
			Suggestions: []string{
				"call .Opus(...), .VP8(...), or .VP9(...) before .To(goav.FileOutput(...))",
				"send decoded frames to goav.FrameSink(...)",
				"use goav.Record(input, output) if you want to copy packets without decoding",
			},
		}
	}
	if stream.encode.ID != "" && outputsContainFrameSink(outputs) {
		return nil, &BuildError{
			Code:      "encoded_sink_unsupported",
			Operation: "build stream",
			Node:      stream.name,
			Reason:    "stream recipes currently send encoded packets to file or URI outputs, not frame sinks",
			Suggestions: []string{
				"use .To(goav.FrameSink(...)) for decoded frames",
				"send encoded output to goav.FileOutput(...) or goav.URIOutput(...)",
				"use the expert graph API for custom packet sink wiring",
			},
		}
	}
	if stream.decode || len(stream.steps) != 0 || stream.encode.ID != "" {
		builder = builder.Decode(stream.selector)
	}
	for i := range stream.steps {
		step := stream.steps[i]
		if step.stage != nil {
			builder = builder.Filter(stream.selector, step.stage)
			continue
		}
		if step.transform.Resize == nil && step.transform.Resample == nil {
			return nil, streamStageMissingError(stream)
		}
		transform, err := streamTransform(stream.name, stream.selector, step.transform, i)
		if err != nil {
			return nil, err
		}
		internal, ok := builder.(interface {
			transform(av.StreamSelector, transcodeTransform) Builder
		})
		if !ok {
			return nil, &BuildError{
				Code:      "transform_runtime_unsupported",
				Operation: "build stream",
				Node:      stream.name,
				Reason:    "stream transforms require the standard runtime builder",
				Suggestions: []string{
					"use goav.Default() or goav.New(...) for recipe transforms",
					"use .Do(stage) when a custom runtime must provide its own filter stage",
				},
			}
		}
		builder = internal.transform(stream.selector, transform)
	}
	if stream.encode.ID != "" {
		builder = builder.Encode(stream.selector, encodeConfigFromSpec(stream.encode))
	}
	return builder, nil
}

func streamStageMissingError(stream *jobStreamBuild) error {
	return &BuildError{
		Code:      "stage_missing",
		Operation: "build stream",
		Node:      jobStreamName(stream),
		Reason:    "custom stream stage is nil",
		Suggestions: []string{
			"pass a non-nil stage to .Do(stage)",
			"use goav.FrameFunc, goav.PacketFunc, or goav.EventFunc for small hooks",
			"remove .Do(...) when no custom processing is needed",
		},
		Cause: ErrNilStage,
	}
}

func mixedStreamOutputError(stream *jobStreamBuild) error {
	return &BuildError{
		Code:      "output_kind_mixed",
		Operation: "build stream",
		Node:      jobStreamName(stream),
		Reason:    "stream recipes cannot mix frame sinks and muxed outputs",
		Suggestions: []string{
			"use .To(goav.FrameSink(...)) for decoded frames",
			"call .Opus(...), .VP8(...), or .VP9(...) before .To(goav.FileOutput(...)) for encoded output",
			"use goav.Transcode(input) or the expert graph API when one stream needs separate decoded and encoded branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobStreamName(stream *jobStreamBuild) string {
	if stream == nil {
		return "stream"
	}
	return firstNonEmpty(stream.name, string(stream.selector.ID), string(stream.selector.Type), "stream")
}

func duplicateJobStreamError(existing *jobStreamBuild, next *jobStreamBuild) error {
	return &BuildError{
		Code:      "stream_duplicate",
		Operation: "build job",
		Node:      jobStreamName(next),
		Reason:    "ordinary stream recipes select one audio or video stream",
		Details: []string{
			"first stream: " + jobStreamName(existing),
			"second stream: " + jobStreamName(next),
		},
		Suggestions: []string{
			"keep one .Audio(...) or .Video(...) chain on goav.From(...)",
			"use goav.Transcode(input) for multiple audio or video branches",
			"use the expert graph API for custom multi-stream routing",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (s *jobStreamBuild) hasOperation() bool {
	return s.decode || len(s.steps) != 0 || s.encode.ID != "" || s.encode.Auto || s.encode.Copy
}

func (s *jobStreamBuild) transformSpecs() []TransformSpec {
	if s == nil {
		return nil
	}
	transforms := make([]TransformSpec, 0, len(s.steps))
	for i := range s.steps {
		if s.steps[i].transform.Resize != nil || s.steps[i].transform.Resample != nil {
			transforms = append(transforms, s.steps[i].transform)
		}
	}
	return transforms
}

func outputsContainMuxTarget(outputs []OutputSpec) bool {
	for i := range outputs {
		if outputs[i].sink == nil {
			return true
		}
	}
	return false
}

func outputsContainFrameSink(outputs []OutputSpec) bool {
	for i := range outputs {
		if outputs[i].sink != nil {
			return true
		}
	}
	return false
}

func validateOutputSpecs(operation string, outputs []OutputSpec) error {
	seen := make(map[string]struct{}, len(outputs))
	for i := range outputs {
		fallback := fmt.Sprintf("output-%d", i)
		if err := outputs[i].validate(operation, fallback); err != nil {
			return err
		}
		name := outputs[i].label(fallback)
		if _, ok := seen[name]; ok {
			return duplicateOutputError(operation, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func duplicateOutputError(operation string, name string) error {
	return &BuildError{
		Code:      "output_duplicate",
		Operation: operation,
		Node:      name,
		Reason:    fmt.Sprintf("output label %q is defined more than once", name),
		Suggestions: []string{
			"use a unique output name for each output in the recipe",
			"remove repeated outputs when one output should receive the stream once",
			"call .Name(...) on outputs or choose distinct sink names when labels should differ",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateRecipeStreamSelector(operation string, node string, selector av.StreamSelector) error {
	if selector.Index >= 0 {
		return nil
	}
	return &BuildError{
		Code:      "stream_selector_invalid",
		Operation: operation,
		Node:      node,
		Reason:    "stream index must be non-negative",
		Details: []string{
			fmt.Sprintf("index=%d", selector.Index),
		},
		Suggestions: []string{
			"use goav.StreamIndex(0) for the first matching stream",
			"use goav.StreamID(...) or goav.StreamName(...) when stream metadata is stable",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func codecIntentSet(spec CodecSpec) bool {
	return spec.ID != "" || spec.Auto || spec.Copy
}

func streamStepAfterEncodeError(operation string, node string, step string, encode CodecSpec) error {
	return &BuildError{
		Code:      "stream_step_after_encode",
		Operation: operation,
		Node:      node,
		Reason:    "stream processing steps must be declared before the encoder",
		Details: []string{
			"step: " + step,
			"encoder: " + codecIntentName(encode),
		},
		Suggestions: []string{
			"place .Do(...), .Resize(...), or .Resample(...) before .Opus(...), .VP8(...), or .VP9(...)",
			"call .To(...) after the encoder to attach outputs",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func duplicateStreamEncodeError(operation string, node string, first CodecSpec, second CodecSpec) error {
	return &BuildError{
		Code:      "encode_duplicate",
		Operation: operation,
		Node:      node,
		Reason:    "stream recipes allow one terminal encoder",
		Details: []string{
			"first encoder: " + codecIntentName(first),
			"second encoder: " + codecIntentName(second),
		},
		Suggestions: []string{
			"choose one output codec for the stream chain",
			"use goav.Transcode(input) when one input needs multiple encoded branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func codecIntentName(spec CodecSpec) string {
	switch {
	case spec.Auto:
		return "auto"
	case spec.Copy:
		return "copy"
	case spec.ID != "":
		return string(spec.ID)
	default:
		return "none"
	}
}

func encodeConfigFromSpec(spec CodecSpec) codec.EncodeConfig {
	parameters := spec.Parameters
	if spec.ID == av.CodecOpus {
		if !spec.sampleRateSet {
			parameters.SampleRate = 0
			parameters.ClockRate = 0
		}
		if !spec.channelsSet {
			parameters.Channels = 0
			parameters.ChannelLayout = ""
		}
	}
	return codec.EncodeConfig{
		Parameters: parameters,
		Bitrate:    spec.Bitrate,
	}
}

func validateRecipeEncode(spec CodecSpec, operation string, node string) error {
	if spec.Auto {
		return &BuildError{
			Code:      "encode_auto_unresolved",
			Operation: operation,
			Node:      node,
			Reason:    "automatic codec selection is not implemented for stream recipes yet",
			Suggestions: []string{
				"choose an explicit recipe encoder such as .Opus(...), .VP8(...), or .VP9(...)",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if spec.Copy {
		return &BuildError{
			Code:      "copy_unresolved",
			Operation: operation,
			Node:      node,
			Reason:    "packet copy is only available through record/remux recipes today",
			Suggestions: []string{
				"use goav.Record(input, output) for packet-preserving output",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if spec.ID == "" {
		return nil
	}
	switch spec.ID {
	case av.CodecOpus, av.CodecVP8, av.CodecVP9:
		return validateRecipeEncodeValues(spec, operation, node)
	case av.CodecH264, av.CodecAV1:
		return &BuildError{
			Code:      "encode_work_in_progress",
			Operation: operation,
			Node:      node,
			Reason:    string(spec.ID) + " recipe encoding is work in progress; recipe encode paths currently target opus, vp8, and vp9",
			Suggestions: []string{
				"decode the stream with .To(goav.FrameSink(...))",
				"use .Opus(...), .VP8(...), or .VP9(...) for recipe encode paths",
				"use the expert builder with an explicit codec.EncodeConfig when testing an experimental encoder",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return &BuildError{
			Code:      "encode_unsupported",
			Operation: operation,
			Node:      node,
			Reason:    string(spec.ID) + " is not a recipe encode target",
			Suggestions: []string{
				"use .Opus(...), .VP8(...), or .VP9(...) for recipe encode paths",
				"use goav.Record(input, output) for packet-preserving output",
				"use the expert builder with an explicit codec.EncodeConfig for custom encoders",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
}

func validateRecipeEncodeValues(spec CodecSpec, operation string, node string) error {
	switch {
	case spec.Bitrate < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode bitrate must be non-negative",
			Details: []string{
				fmt.Sprintf("bitrate=%d", spec.Bitrate),
			},
			Suggestions: []string{
				"pass a positive bitrate to .Opus(...), .VP8(...), or .VP9(...)",
				"omit goav.Bitrate(...) when the encoder should choose its default",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.sampleRateSet && spec.Parameters.SampleRate <= 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "explicit encode sample rate must be positive",
			Details: []string{
				fmt.Sprintf("sample_rate=%d", spec.Parameters.SampleRate),
			},
			Suggestions: []string{
				"use goav.SampleRate(rate) with a positive rate",
				"omit goav.SampleRate(...) to use the selected stream rate",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.channelsSet && spec.Parameters.Channels <= 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "explicit encode channel count must be positive",
			Details: []string{
				fmt.Sprintf("channels=%d", spec.Parameters.Channels),
			},
			Suggestions: []string{
				"use goav.Channels(goav.Mono), goav.Channels(goav.Stereo), or another positive channel count",
				"omit goav.Channels(...) to use the selected stream channel count",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return nil
	}
}

func streamTransform(streamName string, selector av.StreamSelector, spec TransformSpec, index int) (transcodeTransform, error) {
	base := firstNonEmpty(streamName, string(selector.ID), string(selector.Type), "stream")
	suffix := ""
	if index > 0 {
		suffix = "-" + fmt.Sprint(index+1)
	}
	if err := validateTransformSpec("build stream", base, spec); err != nil {
		return transcodeTransform{}, err
	}
	switch {
	case spec.Resize != nil && spec.Resample != nil:
		return transcodeTransform{}, &BuildError{
			Code:      "transform_invalid",
			Operation: "build stream",
			Node:      base,
			Reason:    "one stream transform cannot be both resize and resample",
		}
	case spec.Resize != nil:
		if selector.Type == av.MediaAudio {
			return transcodeTransform{}, transformMediaError(base, "resize", "video")
		}
		resize := *spec.Resize
		return transcodeTransform{
			name:    "resize-" + base + suffix,
			factory: filter.FactoryResize,
			video:   &resize,
		}, nil
	case spec.Resample != nil:
		if selector.Type == av.MediaVideo {
			return transcodeTransform{}, transformMediaError(base, "resample", "audio")
		}
		resample := *spec.Resample
		return transcodeTransform{
			name:    "resample-" + base + suffix,
			factory: filter.FactoryResample,
			audio:   &resample,
		}, nil
	default:
		return transcodeTransform{}, &BuildError{
			Code:      "transform_invalid",
			Operation: "build stream",
			Node:      base,
			Reason:    "empty stream transform",
			Suggestions: []string{
				"call .Resize(width, height) for video streams",
				"call .Resample(sampleRate, channels) for audio streams",
			},
		}
	}
}

func validateTransformSpec(operation string, node string, spec TransformSpec) error {
	switch {
	case spec.Resize != nil && spec.Resample != nil:
		return nil
	case spec.Resize != nil:
		if spec.Resize.Width > 0 && spec.Resize.Height > 0 {
			return nil
		}
		return &BuildError{
			Code:      "transform_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "resize requires positive width and height",
			Details: []string{
				fmt.Sprintf("width=%d", spec.Resize.Width),
				fmt.Sprintf("height=%d", spec.Resize.Height),
			},
			Suggestions: []string{
				"call .Resize(width, height) with positive dimensions",
				"remove .Resize(...) when no video scaling is needed",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Resample != nil:
		if spec.Resample.SampleRate > 0 && spec.Resample.Channels > 0 {
			return nil
		}
		return &BuildError{
			Code:      "transform_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "resample requires positive sample rate and channels",
			Details: []string{
				fmt.Sprintf("sample_rate=%d", spec.Resample.SampleRate),
				fmt.Sprintf("channels=%d", spec.Resample.Channels),
			},
			Suggestions: []string{
				"call .Resample(sampleRate, channels) with positive values",
				"remove .Resample(...) when no audio conversion is needed",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return nil
	}
}

func transformMediaError(stream string, transform string, media string) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: "build stream",
		Node:      stream,
		Reason:    transform + " applies to " + media + " streams",
		Suggestions: []string{
			"use .Video().Resize(...) for video scaling",
			"use .Audio().Resample(...) for audio sample-rate or channel conversion",
		},
	}
}

type StreamOption func(*streamSelectConfig)

type streamSelectConfig struct {
	selector av.StreamSelector
}

func newStreamSelector(media av.MediaType, options ...StreamOption) av.StreamSelector {
	config := streamSelectConfig{selector: av.StreamSelector{Type: media}}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return config.selector
}

type streamBuild struct {
	name       string
	selector   av.StreamSelector
	decode     bool
	transforms []TransformSpec
	encode     CodecSpec
	labels     []string
}

func StreamID(id av.StreamID) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.ID = id
	}
}

func StreamName(name string) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.Name = name
	}
}

func StreamIndex(index int) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.Index = index
		config.selector.UseIndex = true
	}
}

type JobStreamBuilder struct {
	job    *Job
	stream *jobStreamBuild
}

func (b *JobStreamBuilder) Decode() *JobStreamBuilder {
	b.current().decode = true
	return b
}

func (b *JobStreamBuilder) Do(stage Stage) *JobStreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("build stream", jobStreamName(stream), "custom stage", stream.encode))
		return b
	}
	stream.decode = true
	stream.steps = append(stream.steps, jobStreamStep{stage: stage})
	return b
}

func (b *JobStreamBuilder) Resize(width int, height int, options ...ResizeOption) *JobStreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("build stream", jobStreamName(stream), "resize", stream.encode))
		return b
	}
	stream.decode = true
	stream.steps = append(stream.steps, jobStreamStep{transform: Resize(width, height, options...)})
	return b
}

func (b *JobStreamBuilder) Resample(sampleRate int, channels int, options ...AudioOption) *JobStreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("build stream", jobStreamName(stream), "resample", stream.encode))
		return b
	}
	stream.decode = true
	stream.steps = append(stream.steps, jobStreamStep{transform: Resample(sampleRate, channels, options...)})
	return b
}

func (b *JobStreamBuilder) Encode(codec CodecSpec) *JobStreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(duplicateStreamEncodeError("build stream", jobStreamName(stream), stream.encode, codec))
		return b
	}
	stream.decode = true
	stream.encode = codec
	return b
}

func (b *JobStreamBuilder) Opus(bitrate int, options ...CodecOption) *JobStreamBuilder {
	return b.Encode(Opus(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *JobStreamBuilder) VP8(bitrate int, options ...CodecOption) *JobStreamBuilder {
	return b.Encode(VP8(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *JobStreamBuilder) VP9(bitrate int, options ...CodecOption) *JobStreamBuilder {
	return b.Encode(VP9(append([]CodecOption{Bitrate(bitrate)}, options...)...))
}

func (b *JobStreamBuilder) To(outputs ...OutputSpec) *Job {
	stream := b.current()
	stream.outputs = append(stream.outputs, outputs...)
	if outputsContainFrameSink(outputs) {
		stream.decode = true
	}
	return b.job
}

func (b *JobStreamBuilder) current() *jobStreamBuild {
	if b.stream != nil {
		return b.stream
	}
	b.stream = &jobStreamBuild{}
	if b.job.stream == nil {
		b.job.stream = b.stream
	}
	return b.stream
}

type TranscodeJob struct {
	runtime Runtime
	input   InputSpec
	streams []streamBuild
	outputs []namedOutputSpec
	err     error
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
	if name == "" {
		j.setErr(transcodeEmptyOutputDefinitionLabelError(output))
		return j
	}
	j.outputs = append(j.outputs, namedOutputSpec{name: name, output: output.Name(firstNonEmpty(output.name, name))})
	return j
}

func (j *TranscodeJob) setErr(err error) {
	if j.err == nil {
		j.err = err
	}
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
				ID:       stream.selector.ID,
				Index:    stream.selector.Index,
				UseIndex: stream.selector.UseIndex,
				Type:     stream.selector.Type,
				Codec:    stream.selector.Codec,
				Name:     stream.selector.Name,
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
	if j.err != nil {
		return transcodepkg.Plan{}, j.err
	}
	if err := j.input.validate(); err != nil {
		return transcodepkg.Plan{}, err
	}
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
	if len(j.streams) == 0 {
		return transcodepkg.Plan{}, &BuildError{
			Code:      "stream_missing",
			Operation: "plan transcode",
			Reason:    "no audio or video branches are configured",
			Suggestions: []string{
				"add a video branch such as .Video(\"720p\").Resize(...).VP9(...).To(...)",
				"add an audio branch such as .Audio(\"main\").Resample(...).Opus(...).To(...)",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	renditionNames := make(map[string]int, len(j.streams))
	for i := range j.streams {
		stream := j.streams[i]
		if stream.name == "" {
			return transcodepkg.Plan{}, transcodeBranchNameMissingError(i, stream)
		}
		if err := validateRecipeStreamSelector("plan transcode", transcodeBranchName(stream), stream.selector); err != nil {
			return transcodepkg.Plan{}, err
		}
		if stream.encode.ID == "" && !stream.encode.Copy {
			return transcodepkg.Plan{}, &BuildError{
				Code:      "encode_missing",
				Operation: "plan transcode",
				Node:      stream.name,
				Reason:    "stream has no codec target",
				Suggestions: []string{
					"call .Opus(...), .VP8(...), or .VP9(...) before .To(...)",
				},
			}
		}
		if err := validateRecipeEncode(stream.encode, "plan transcode", stream.name); err != nil {
			return transcodepkg.Plan{}, err
		}
		if len(stream.labels) == 0 {
			return transcodepkg.Plan{}, &BuildError{
				Code:      "output_missing",
				Operation: "plan transcode",
				Node:      firstNonEmpty(stream.name, string(stream.selector.Type), "stream"),
				Reason:    "stream has no output target",
				Suggestions: []string{
					"call .To(\"label\") and define it with .Output(label, goav.FileOutput(...))",
					"reuse the same output label from multiple branches when they should share an output",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
		if err := validateTranscodeBranchOutputLabels(stream); err != nil {
			return transcodepkg.Plan{}, err
		}
		renditionName := stream.name
		if firstIndex, ok := renditionNames[renditionName]; ok {
			return transcodepkg.Plan{}, transcodeDuplicateBranchError(renditionName, firstIndex, i)
		}
		renditionNames[renditionName] = i
	}

	outputs := make(map[string]OutputSpec, len(j.outputs))
	outputOrder := make([]string, 0, len(j.outputs))
	for i := range j.outputs {
		if err := j.outputs[i].output.validate("plan transcode", fmt.Sprintf("output-%d", i)); err != nil {
			return transcodepkg.Plan{}, err
		}
		name := j.outputs[i].name
		if _, ok := outputs[name]; ok {
			return transcodepkg.Plan{}, transcodeDuplicateOutputError(name)
		}
		outputOrder = append(outputOrder, name)
		outputs[name] = j.outputs[i].output.Name(firstNonEmpty(j.outputs[i].output.name, name))
	}

	renditions := make([]transcodepkg.Rendition, 0, len(j.streams))
	outputRenditions := make(map[string][]string, len(outputs))
	for i := range j.streams {
		stream := j.streams[i]
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
					"define shared outputs once and route branches by label",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
		renditionName := stream.name
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
		resize, resample, err := transcodeBranchTransformConfigs(stream)
		if err != nil {
			return transcodepkg.Plan{}, err
		}
		rendition.Resize = resize
		rendition.Resample = resample
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
		selector: newStreamSelector(media, options...),
		decode:   true,
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
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("plan transcode", transcodeBranchName(*stream), "resize", stream.encode))
		return b
	}
	stream.transforms = append(stream.transforms, Resize(width, height, options...))
	return b
}

func (b *StreamBuilder) Resample(sampleRate int, channels int, options ...AudioOption) *StreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("plan transcode", transcodeBranchName(*stream), "resample", stream.encode))
		return b
	}
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

func (b *StreamBuilder) To(labels ...string) *TranscodeJob {
	stream := b.current()
	for i := range labels {
		if labels[i] == "" {
			b.job.setErr(transcodeEmptyOutputLabelError(*stream, i))
			continue
		}
		stream.labels = append(stream.labels, labels[i])
	}
	return b.job
}

func transcodeEmptyOutputLabelError(stream streamBuild, index int) error {
	return &BuildError{
		Code:      "output_label_invalid",
		Operation: "plan transcode",
		Node:      firstNonEmpty(stream.name, string(stream.selector.Type), "stream"),
		Reason:    "transcode output labels must be non-empty",
		Details: []string{
			fmt.Sprintf("target index: %d", index),
		},
		Suggestions: []string{
			"call .To(\"label\") with a non-empty output label",
			"define the label once with .Output(label, goav.FileOutput(...))",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeEmptyOutputDefinitionLabelError(output OutputSpec) error {
	err := &BuildError{
		Code:      "output_label_invalid",
		Operation: "plan transcode",
		Node:      output.label("output"),
		Reason:    "transcode output labels must be non-empty",
		Suggestions: []string{
			"call .Output(\"label\", goav.FileOutput(...)) with a stable output label",
			"route branches with .To(\"label\") using that same label",
		},
		Cause: ErrUnsupportedBuild,
	}
	if output.name != "" {
		err.Details = append(err.Details, "output name: "+output.name)
	}
	return err
}

func transcodeDuplicateOutputError(name string) error {
	return &BuildError{
		Code:      "output_duplicate",
		Operation: "plan transcode",
		Node:      name,
		Reason:    fmt.Sprintf("output label %q is defined more than once", name),
		Suggestions: []string{
			"use a unique .Output(label, ...) label for each transcode output",
			"route multiple branches to one shared output by calling .To(label) on each branch",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeDuplicateBranchError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "stream_duplicate",
		Operation: "plan transcode",
		Node:      name,
		Reason:    fmt.Sprintf("transcode branch name %q is defined more than once", name),
		Details: []string{
			fmt.Sprintf("first branch index: %d", firstIndex),
			fmt.Sprintf("second branch index: %d", secondIndex),
		},
		Suggestions: []string{
			"use unique names such as .Video(\"720p\") and .Video(\"360p\")",
			"route one branch to multiple outputs by calling .To(label, otherLabel)",
			"route different branches to the same output by reusing the output label",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeBranchNameMissingError(index int, stream streamBuild) error {
	return &BuildError{
		Code:      "stream_name_missing",
		Operation: "plan transcode",
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "transcode branches need stable names",
		Details: []string{
			"media type: " + firstNonEmpty(string(stream.selector.Type), "unknown"),
		},
		Suggestions: []string{
			"call .Video(\"720p\") for video branches",
			"call .Audio(\"main\") for audio branches",
			"use branch names as handles for graph inspection and output routing",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateTranscodeBranchOutputLabels(stream streamBuild) error {
	seen := make(map[string]int, len(stream.labels))
	for i, label := range stream.labels {
		if firstIndex, ok := seen[label]; ok {
			return transcodeDuplicateBranchOutputError(stream, label, firstIndex, i)
		}
		seen[label] = i
	}
	return nil
}

func transcodeDuplicateBranchOutputError(stream streamBuild, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "output_duplicate",
		Operation: "plan transcode",
		Node:      transcodeBranchName(stream),
		Reason:    fmt.Sprintf("branch routes to output %q more than once", label),
		Details: []string{
			fmt.Sprintf("first target index: %d", firstIndex),
			fmt.Sprintf("second target index: %d", secondIndex),
		},
		Suggestions: []string{
			"list each output label once in .To(...)",
			"route one branch to multiple outputs with distinct labels such as .To(\"archive\", \"preview\")",
			"define shared outputs once with .Output(label, goav.FileOutput(...))",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeBranchTransformConfigs(stream streamBuild) (*filter.ResizeConfig, *filter.ResampleConfig, error) {
	var resize *filter.ResizeConfig
	var resample *filter.ResampleConfig
	for i := range stream.transforms {
		transform := stream.transforms[i]
		if err := validateTransformSpec("plan transcode", transcodeBranchName(stream), transform); err != nil {
			return nil, nil, err
		}
		switch {
		case transform.Resize != nil && transform.Resample != nil:
			return nil, nil, &BuildError{
				Code:      "transform_invalid",
				Operation: "plan transcode",
				Node:      transcodeBranchName(stream),
				Reason:    "one transform cannot be both resize and resample",
				Cause:     ErrUnsupportedBuild,
			}
		case transform.Resize != nil:
			if stream.selector.Type == av.MediaAudio {
				return nil, nil, transcodeTransformMediaError(stream, "resize", "video")
			}
			if resize != nil || resample != nil {
				return nil, nil, transcodeTransformChainError(stream)
			}
			config := *transform.Resize
			resize = &config
		case transform.Resample != nil:
			if stream.selector.Type == av.MediaVideo {
				return nil, nil, transcodeTransformMediaError(stream, "resample", "audio")
			}
			if resize != nil || resample != nil {
				return nil, nil, transcodeTransformChainError(stream)
			}
			config := *transform.Resample
			resample = &config
		default:
			return nil, nil, &BuildError{
				Code:      "transform_invalid",
				Operation: "plan transcode",
				Node:      transcodeBranchName(stream),
				Reason:    "empty stream transform",
				Suggestions: []string{
					"call .Resize(width, height) once on video branches",
					"call .Resample(sampleRate, channels) once on audio branches",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
	}
	return resize, resample, nil
}

func transcodeTransformMediaError(stream streamBuild, transform string, media string) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: "plan transcode",
		Node:      transcodeBranchName(stream),
		Reason:    transform + " applies to " + media + " branches",
		Suggestions: []string{
			"use .Video(...).Resize(...) for video ladder branches",
			"use .Audio(...).Resample(...) for audio branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeTransformChainError(stream streamBuild) error {
	return &BuildError{
		Code:      "transform_chain_unsupported",
		Operation: "plan transcode",
		Node:      transcodeBranchName(stream),
		Reason:    "transcode branches currently support one media transform",
		Suggestions: []string{
			"call one Resize or Resample per branch",
			"create another Video(...) or Audio(...) branch for another output shape",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeBranchName(stream streamBuild) string {
	return firstNonEmpty(stream.name, string(stream.selector.Type), "stream")
}

func (b *StreamBuilder) encode(codec CodecSpec) *StreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(duplicateStreamEncodeError("plan transcode", transcodeBranchName(*stream), stream.encode, codec))
		return b
	}
	stream.encode = codec
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
