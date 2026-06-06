package goav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
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
	Name        string
	Select      StreamSelect
	Decode      bool
	Transforms  []TransformSpec
	Encode      CodecSpec
	CodecChange CodecChangePolicy
	RouteTo     []string
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

type CodecChangePolicy struct {
	RebindCompatible     bool
	RequestKeyframe      bool
	DropUntilSync        bool
	FailOnDifferentCodec bool
}

func RealtimeCodecChangePolicy() CodecChangePolicy {
	return CodecChangePolicy{
		RebindCompatible:     true,
		RequestKeyframe:      true,
		DropUntilSync:        true,
		FailOnDifferentCodec: true,
	}
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

type builderProvider interface {
	New() builderAPI
}

func Default() Runtime {
	return New(WithDefaults())
}

type codecOption func(*CodecSpec)

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

func Opus(options ...codecOption) CodecSpec {
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

func VP8(options ...codecOption) CodecSpec {
	return codecSpec(av.CodecVP8, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecVP8,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func VP9(options ...codecOption) CodecSpec {
	return codecSpec(av.CodecVP9, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecVP9,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func H264(options ...codecOption) CodecSpec {
	return codecSpec(av.CodecH264, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecH264,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func AV1(options ...codecOption) CodecSpec {
	return codecSpec(av.CodecAV1, av.MediaVideo, av.CodecParameters{
		ID:        av.CodecAV1,
		Type:      av.MediaVideo,
		ClockRate: 90000,
	}, options...)
}

func Bitrate(bitsPerSecond int) codecOption {
	return func(spec *CodecSpec) {
		spec.Bitrate = bitsPerSecond
	}
}

func Channels(channels int) codecOption {
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

func SampleRate(sampleRate int) codecOption {
	return func(spec *CodecSpec) {
		spec.Parameters.SampleRate = sampleRate
		spec.Parameters.ClockRate = uint32(sampleRate)
		spec.sampleRateSet = true
	}
}

func codecSpec(id av.CodecID, media av.MediaType, params av.CodecParameters, options ...codecOption) CodecSpec {
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

type resizeOption func(*filter.ResizeConfig)
type audioOption func(*filter.ResampleConfig)

type TransformSpec struct {
	Resize   *filter.ResizeConfig
	Resample *filter.ResampleConfig
}

func Resize(width int, height int, options ...resizeOption) TransformSpec {
	config := filter.ResizeConfig{Width: width, Height: height, Mode: filter.ResizeExact}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return TransformSpec{Resize: &config}
}

func Resample(sampleRate int, channels int, options ...audioOption) TransformSpec {
	config := filter.ResampleConfig{SampleRate: sampleRate, Channels: channels}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return TransformSpec{Resample: &config}
}

type InputSpec struct {
	input    format.Input
	rtp      *rtpInputSpec
	codec    CodecSpec
	name     string
	realtime bool
	err      error
}

type rtpInputSpec struct {
	receiver rtpav.PacketReader
	feedback rtpav.FeedbackWriter
	jitter   rtpav.JitterBuffer
	limits   RTPBufferLimits
	maxTSGap av.Duration
}

func FileInput(name string, reader io.Reader) InputSpec {
	return InputSpec{
		input: format.Input{
			Name:     name,
			Protocol: av.ProtocolFile,
			Reader:   reader,
		},
		name: name,
	}
}

func URI(uri string) InputSpec {
	return InputSpec{
		input: format.Input{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

func RTP(receiver rtpav.PacketReader) InputSpec {
	return InputSpec{
		input: format.Input{Protocol: av.ProtocolRTP, Realtime: true},
		rtp:   &rtpInputSpec{receiver: receiver},
	}
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

func (s InputSpec) apply(builder builderAPI) builderAPI {
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
				"set RTP receive intent with .Codec(goav.Opus()), .Codec(goav.VP8()), .Codec(goav.VP9()), .Codec(goav.H264()), or .Codec(goav.AV1())",
				"for custom RTP payloads, add an advanced receive adapter before using the recipe",
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
	if s.codec.ID == "" {
		if s.input.Protocol == av.ProtocolWebRTC {
			return &BuildError{
				Code:      "webrtc_codec_unknown",
				Operation: "build input",
				Node:      firstNonEmpty(s.name, s.input.Name, "webrtc"),
				Reason:    "WebRTC track codec is unknown or unsupported by built-in receive recipes",
				Suggestions: []string{
					"verify the Pion TrackRemote codec is Opus, VP8, VP9, H264, or AV1",
					"use goav.RTP(reader).Codec(...) when adapting an RTP reader outside WebRTCTrack",
					"add an advanced RTP receive adapter for custom payloads before using recipes",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
		return &BuildError{
			Code:      "rtp_codec_missing",
			Operation: "build input",
			Node:      firstNonEmpty(s.name, s.input.Name, "rtp"),
			Reason:    "RTP input needs an explicit receive codec intent",
			Suggestions: []string{
				"call .Codec(goav.Opus()), .Codec(goav.VP8()), .Codec(goav.VP9()), .Codec(goav.H264()), or .Codec(goav.AV1()) on goav.RTP(reader)",
				"use goav.WebRTCTrack(track) when Pion track metadata should provide the codec intent",
			},
			Cause: ErrUnsupportedBuild,
		}
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
				"use a built-in receive codec: goav.Opus(), goav.VP8(), goav.VP9(), goav.H264(), or goav.AV1()",
				"for custom RTP payloads, add an advanced receive adapter before using the recipe",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
}

func (s InputSpec) rtpOptions() []rtpOption {
	options := make([]rtpOption, 0, 6)
	if s.name != "" {
		options = append(options, withRTPName(s.name))
	}
	if s.rtp.feedback != nil {
		options = append(options, withRTPFeedback(s.rtp.feedback))
	}
	if s.rtp.jitter != nil {
		options = append(options, withRTPJitter(s.rtp.jitter))
	}
	if s.codec.ID != "" {
		options = append(options, withRTPCodec(s.codec))
	}
	if s.rtp.limits != (RTPBufferLimits{}) {
		options = append(options, withRTPBufferLimits(s.rtp.limits))
	}
	if s.rtp.maxTSGap != (av.Duration{}) {
		options = append(options, withRTPMaxTimestampGap(s.rtp.maxTSGap))
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
	output format.Output
	sink   pipeline.Sink
	format av.FormatID
	name   string
	err    error
}

type formattedOutputBuilder interface {
	outputWithFormat(format.Output, av.FormatID) builderAPI
}

func FileOutput(name string, writer io.Writer) OutputSpec {
	return OutputSpec{
		output: format.Output{
			Name:     name,
			Protocol: av.ProtocolFile,
			Writer:   writer,
		},
		name: name,
	}
}

func URIOutput(uri string) OutputSpec {
	return OutputSpec{
		output: format.Output{
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

func (s OutputSpec) apply(builder builderAPI) (builderAPI, error) {
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
	name        string
	selector    av.StreamSelector
	decode      bool
	steps       []jobStreamStep
	encode      CodecSpec
	codecChange CodecChangePolicy
	outputs     []OutputSpec
}

type jobStreamStep struct {
	stage     pipeline.Stage
	transform TransformSpec
}

type jobStreamStepAttachment struct {
	stage          pipeline.Stage
	hasTransform   bool
	transformIndex int
	stepIndex      int
}

func Record(input InputSpec, outputs ...OutputSpec) *Job {
	job := newJob("record")
	job.inputs = append(job.inputs, input)
	return job.To(outputs...)
}

func From(input InputSpec) *Job {
	job := newJob("from")
	job.inputs = append(job.inputs, input)
	return job
}

func Decode(input InputSpec, output OutputSpec) *Job {
	job := newJob("decode")
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

func newJob(name string) *Job {
	return &Job{name: name, runtime: Default()}
}

func (j *Job) named(name string) *Job {
	j.name = name
	return j
}

func (j *Job) UseRuntime(runtime Runtime) *Job {
	if j != nil {
		j.runtime = runtime
	}
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

func (j *Job) Audio(options ...streamOption) *JobStreamBuilder {
	return j.streamBuilder("audio", av.MediaAudio, options...)
}

func (j *Job) Video(options ...streamOption) *JobStreamBuilder {
	return j.streamBuilder("video", av.MediaVideo, options...)
}

func (j *Job) streamBuilder(name string, media av.MediaType, options ...streamOption) *JobStreamBuilder {
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
		intent.Streams = append(intent.Streams, jobStreamIntent(j.stream))
	}
	outputs := j.allOutputs()
	for i := range outputs {
		intent.Outputs = append(intent.Outputs, outputs[i].intent())
	}
	return intent
}

func (j *Job) Describe() (pipeline.Spec, error) {
	resolved, err := compileJobRecipe(j)
	if err != nil {
		return pipeline.Spec{}, err
	}
	return resolved.Describe()
}

func (j *Job) Build(ctx context.Context) (Task, error) {
	resolved, err := compileJobRecipeForBuild(j)
	if err != nil {
		return nil, err
	}
	return resolved.Build(ctx)
}

func (j *Job) Run(ctx context.Context) error {
	task, err := j.Build(ctx)
	if err != nil {
		return err
	}
	defer task.Close()
	return task.Run(ctx)
}

func newRuntimeBuilder(runtime Runtime, operation string) (builderAPI, error) {
	provider, ok := runtime.(builderProvider)
	if !ok {
		return nil, &BuildError{
			Code:      "runtime_builder_missing",
			Operation: operation,
			Reason:    "runtime cannot compile recipe jobs",
			Suggestions: []string{
				"use goav.Default() for the standard recipe runtime",
				"use goav.New(...) when customizing adapters",
				"use runtime.Graph() for explicit graph wiring",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return provider.New(), nil
}

func (j *Job) validateInputs() error {
	return validateJobInputs(j.inputs)
}

func validateJobInputs(inputs []InputSpec) error {
	for i := range inputs {
		if err := inputs[i].validate(); err != nil {
			return err
		}
	}
	if len(inputs) <= 1 {
		return nil
	}
	for i := range inputs {
		if inputs[i].rtp != nil {
			continue
		}
		return &BuildError{
			Code:      "multi_input_unsupported",
			Operation: "build job",
			Node:      firstNonEmpty(inputs[i].name, inputs[i].input.Name, inputs[i].input.URI, fmt.Sprintf("input-%d", i)),
			Reason:    "multiple recipe inputs currently require realtime RTP/WebRTC packet readers",
			Suggestions: []string{
				"use goav.From(goav.RTP(...)).And(goav.RTP(...)) for repeated live inputs",
				"use goav.WebRTCTrack(...) for Pion WebRTC tracks",
				"build an explicit graph when combining multiple file or protocol sources",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if err := validateRealtimeInputNames(inputs); err != nil {
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
			"use goav.WebRTCTrack(track).Name(...) when track metadata is not enough",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (j *Job) validateOutputScope() error {
	stream, ok := jobStreamIntentIfPresent(j.stream)
	return validateJobOutputScope(len(j.outputs), stream, ok)
}

func validateJobOutputScope(outputCount int, stream StreamIntent, hasStream bool) error {
	if !hasStream || outputCount == 0 {
		return nil
	}
	return jobOutputScopeMixedError("build job", stream)
}

func validateJobOutputBindings(operation string, stream StreamIntent, outputs []OutputSpec) error {
	labels := jobOutputLabelSet(outputs)
	for _, label := range stream.RouteTo {
		if _, ok := labels[label]; ok {
			continue
		}
		return jobOutputReferenceMissingError(operation, stream, label)
	}
	return nil
}

func jobOutputLabelSet(outputs []OutputSpec) map[string]struct{} {
	labels := make(map[string]struct{}, len(outputs))
	for i := range outputs {
		labels[outputs[i].label(fmt.Sprintf("output-%d", i))] = struct{}{}
	}
	return labels
}

func (j *Job) allOutputs() []OutputSpec {
	return jobAllOutputs(j.outputs, jobStreamOutputs(j.stream))
}

func jobAllOutputs(outputs []OutputSpec, streamOutputs []OutputSpec) []OutputSpec {
	if len(streamOutputs) == 0 {
		return append([]OutputSpec(nil), outputs...)
	}
	all := make([]OutputSpec, 0, len(outputs)+len(streamOutputs))
	all = append(all, outputs...)
	all = append(all, streamOutputs...)
	return all
}

func (j *Job) applyStream(builder builderAPI, stream *jobStreamBuild) (builderAPI, error) {
	intent, ok := jobStreamIntentIfPresent(stream)
	if !ok {
		return builder, nil
	}
	return applyJobStream(builder, j.allOutputs(), intent, jobStreamStepAttachments(stream))
}

func applyJobStream(builder builderAPI, outputs []OutputSpec, stream StreamIntent, steps []jobStreamStepAttachment) (builderAPI, error) {
	selector := streamIntentSelector(stream)
	node := jobStreamIntentName(stream)
	if err := validateJobStreamIntentShape("build stream", stream, steps); err != nil {
		return nil, err
	}
	if err := validateJobStreamOutputKinds("build stream", stream, outputs); err != nil {
		return nil, err
	}
	if err := validateJobStreamRuntimeCapabilities("build stream", builder, stream); err != nil {
		return nil, err
	}
	if stream.Decode || len(steps) != 0 || stream.Encode.ID != "" {
		if codecChangePolicySet(stream.CodecChange) {
			internal, ok := builder.(interface {
				decodeWithPolicy(av.StreamSelector, CodecChangePolicy) builderAPI
			})
			if !ok {
				return nil, streamCodecChangeRuntimeUnsupportedError("build stream", node)
			}
			builder = internal.decodeWithPolicy(selector, stream.CodecChange)
		} else {
			builder = builder.Decode(selector)
		}
	}
	for i := range steps {
		step := steps[i]
		if step.stage != nil {
			builder = builder.Filter(selector, step.stage)
			continue
		}
		if !step.hasTransform || step.transformIndex < 0 || step.transformIndex >= len(stream.Transforms) {
			return nil, streamStageMissingError(stream)
		}
		transform, err := streamTransform(stream.Name, selector, stream.Transforms[step.transformIndex], step.stepIndex)
		if err != nil {
			return nil, err
		}
		internal, ok := builder.(interface {
			transform(av.StreamSelector, transcodeTransform) builderAPI
		})
		if !ok {
			return nil, streamTransformRuntimeUnsupportedError("build stream", node)
		}
		builder = internal.transform(selector, transform)
	}
	if stream.Encode.ID != "" {
		builder = builder.Encode(selector, encodeConfigFromSpec(stream.Encode))
	}
	return builder, nil
}

func jobStreamIntentIfPresent(stream *jobStreamBuild) (StreamIntent, bool) {
	if stream == nil {
		return StreamIntent{}, false
	}
	return jobStreamIntent(stream), true
}

func jobIntentHasStream(intent Intent) bool {
	return len(intent.Streams) != 0
}

func jobIntentStream(intent Intent) (StreamIntent, bool) {
	if len(intent.Streams) == 0 {
		return StreamIntent{}, false
	}
	return intent.Streams[0], true
}

func jobStreamIntent(stream *jobStreamBuild) StreamIntent {
	if stream == nil {
		return StreamIntent{}
	}
	return StreamIntent{
		Name: stream.name,
		Select: StreamSelect{
			ID:       stream.selector.ID,
			Index:    stream.selector.Index,
			UseIndex: stream.selector.UseIndex,
			Type:     stream.selector.Type,
			Codec:    stream.selector.Codec,
			Name:     stream.selector.Name,
		},
		Decode:      stream.decode,
		Transforms:  stream.transformSpecs(),
		Encode:      stream.encode,
		CodecChange: stream.codecChange,
		RouteTo:     outputLabels(stream.outputs),
	}
}

func jobStreamOutputs(stream *jobStreamBuild) []OutputSpec {
	if stream == nil || len(stream.outputs) == 0 {
		return nil
	}
	return append([]OutputSpec(nil), stream.outputs...)
}

func jobStreamStepAttachments(stream *jobStreamBuild) []jobStreamStepAttachment {
	if stream == nil || len(stream.steps) == 0 {
		return nil
	}
	attachments := make([]jobStreamStepAttachment, 0, len(stream.steps))
	transformIndex := 0
	for i := range stream.steps {
		step := stream.steps[i]
		if step.stage != nil {
			attachments = append(attachments, jobStreamStepAttachment{stage: step.stage, stepIndex: i})
			continue
		}
		if step.transform.Resize != nil || step.transform.Resample != nil {
			attachments = append(attachments, jobStreamStepAttachment{
				hasTransform:   true,
				transformIndex: transformIndex,
				stepIndex:      i,
			})
			transformIndex++
			continue
		}
		attachments = append(attachments, jobStreamStepAttachment{stepIndex: i})
	}
	return attachments
}

func streamStageMissingError(stream StreamIntent) error {
	return &BuildError{
		Code:      "stage_missing",
		Operation: "build stream",
		Node:      jobStreamIntentName(stream),
		Reason:    "custom stream stage is nil",
		Suggestions: []string{
			"pass a non-nil stage to .Do(stage)",
			"use goav.FrameFunc, goav.PacketFunc, or goav.EventFunc for small hooks",
			"remove .Do(...) when no custom processing is needed",
		},
		Cause: ErrNilStage,
	}
}

func validateJobStreamOutputKinds(operation string, stream StreamIntent, outputs []OutputSpec) error {
	if outputsContainFrameSink(outputs) && outputsContainMuxTarget(outputs) {
		return mixedStreamOutputError(operation, stream)
	}
	if stream.Encode.ID == "" && outputsContainMuxTarget(outputs) {
		return streamEncodeMissingError(operation, stream)
	}
	if stream.Encode.ID != "" && outputsContainFrameSink(outputs) {
		return encodedStreamFrameSinkError(operation, stream)
	}
	return nil
}

func mixedStreamOutputError(operation string, stream StreamIntent) error {
	return &BuildError{
		Code:      "output_kind_mixed",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes cannot mix frame sinks and muxed outputs",
		Suggestions: []string{
			"use .To(goav.FrameSink(...)) for decoded frames",
			"call .Opus(...), .VP8(...), or .VP9(...) before .To(goav.FileOutput(...)) for encoded output",
			"use goav.Transcode(input) or the expert graph API when one stream needs separate decoded and encoded branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamEncodeMissingError(operation string, stream StreamIntent) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "decoded frames cannot be written to a muxed output without an encoder",
		Suggestions: []string{
			"call .Opus(...), .VP8(...), or .VP9(...) before .To(goav.FileOutput(...))",
			"send decoded frames to goav.FrameSink(...)",
			"use goav.Record(input, output) if you want to copy packets without decoding",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func encodedStreamFrameSinkError(operation string, stream StreamIntent) error {
	return &BuildError{
		Code:      "encoded_sink_unsupported",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes currently send encoded packets to file or URI outputs, not frame sinks",
		Suggestions: []string{
			"use .To(goav.FrameSink(...)) for decoded frames",
			"send encoded output to goav.FileOutput(...) or goav.URIOutput(...)",
			"use the expert graph API for custom packet sink wiring",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateJobStreamRuntimeCapabilities(operation string, builder builderAPI, stream StreamIntent) error {
	node := jobStreamIntentName(stream)
	if builder == nil {
		return &BuildError{
			Code:      "runtime_builder_missing",
			Operation: operation,
			Node:      node,
			Reason:    "recipe compiler produced no runtime builder",
			Suggestions: []string{
				"use goav.Default() for the standard recipe runtime",
				"use goav.New(...) when customizing adapters",
				"use runtime.Graph() for explicit graph wiring",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if codecChangePolicySet(stream.CodecChange) {
		if _, ok := builder.(interface {
			decodeWithPolicy(av.StreamSelector, CodecChangePolicy) builderAPI
		}); !ok {
			return streamCodecChangeRuntimeUnsupportedError(operation, node)
		}
	}
	if len(stream.Transforms) != 0 {
		if _, ok := builder.(interface {
			transform(av.StreamSelector, transcodeTransform) builderAPI
		}); !ok {
			return streamTransformRuntimeUnsupportedError(operation, node)
		}
	}
	return nil
}

func streamCodecChangeRuntimeUnsupportedError(operation string, node string) error {
	return &BuildError{
		Code:      "codec_change_runtime_unsupported",
		Operation: operation,
		Node:      node,
		Reason:    "codec-change policy requires the standard runtime builder",
		Suggestions: []string{
			"use goav.Default() or goav.New(...) for live stream recipes",
			"remove .OnCodecChange(...) when using a custom recipe runtime",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamTransformRuntimeUnsupportedError(operation string, node string) error {
	return &BuildError{
		Code:      "transform_runtime_unsupported",
		Operation: operation,
		Node:      node,
		Reason:    "stream transforms require the standard runtime builder",
		Suggestions: []string{
			"use goav.Default() or goav.New(...) for recipe transforms",
			"use .Do(stage) when a custom runtime must provide its own filter stage",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobStreamIntentName(stream StreamIntent) string {
	return firstNonEmpty(stream.Name, string(stream.Select.ID), string(stream.Select.Type), "stream")
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

func streamIntentHasOperation(stream StreamIntent, steps []jobStreamStepAttachment) bool {
	return stream.Decode || len(steps) != 0 || stream.Encode.ID != "" || stream.Encode.Auto || stream.Encode.Copy
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

func validateOutputFormatAdapters(ctx context.Context, rt Runtime, outputs []OutputSpec) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range outputs {
		if outputs[i].sink != nil {
			continue
		}
		formatID := outputs[i].format
		if formatID == "" {
			result, err := standard.formats.Probe(ctx, outputProbeRequest(outputs[i].output))
			if err != nil {
				return outputFormatProbeError(outputs[i].output, i, err)
			}
			formatID = result.Format
		}
		if _, err := standard.formats.MuxerFactory(formatID); err != nil {
			return outputMuxerMissingError(outputs[i].output, i, formatID, err)
		}
	}
	return nil
}

func validateRecipeEncodeAdapters(operation string, rt Runtime, streams []StreamIntent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range streams {
		codecID := streams[i].Encode.ID
		if codecID == "" {
			continue
		}
		if _, err := standard.codecs.EncoderFactory(codecID); err != nil {
			return recipeEncodeAdapterError(operation, streams[i], standard.codecs, err)
		}
	}
	return nil
}

func recipeEncodeAdapterError(operation string, stream StreamIntent, registry *codec.SimpleRegistry, cause error) error {
	code := "encode_adapter_missing"
	reason := "no encoder adapter is registered for " + string(stream.Encode.ID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = "encode_adapter_unavailable"
		reason = string(stream.Encode.ID) + " encoder adapter is descriptor-only in this build"
	}
	details := []string{"codec=" + string(stream.Encode.ID)}
	if registry != nil {
		descriptors, err := registry.Find(stream.Encode.ID, codec.ModeEncode)
		if err == nil {
			details = append(details, encodeDescriptorDetails(descriptors)...)
		}
	}
	return &BuildError{
		Code:      code,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    reason,
		Details:   details,
		Suggestions: []string{
			"register a codec adapter that provides a " + string(stream.Encode.ID) + " encoder",
			"use .To(goav.FrameSink(...)) to receive decoded frames without encoding",
			"use goav.Record(input, output) for packet-preserving output when re-encoding is not needed",
		},
		Cause: cause,
	}
}

func encodeDescriptorDetails(descriptors []codec.Descriptor) []string {
	details := make([]string, 0, len(descriptors)*3)
	for i := range descriptors {
		if descriptors[i].Backend.Name != "" {
			details = append(details, "backend="+descriptors[i].Backend.Name)
		}
		if len(descriptors[i].Capabilities.BuildTags) != 0 {
			details = append(details, "build_tags="+strings.Join(descriptors[i].Capabilities.BuildTags, ","))
		}
		if descriptors[i].Backend.Status != "" {
			details = append(details, "status="+descriptors[i].Backend.Status)
		}
	}
	return details
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

func validateCodecChangePolicy(operation string, node string, policy CodecChangePolicy) error {
	if !codecChangePolicySet(policy) || policy == RealtimeCodecChangePolicy() {
		return nil
	}
	return &BuildError{
		Code:      "codec_change_policy_unsupported",
		Operation: operation,
		Node:      node,
		Reason:    "custom codec-change policies are not implemented yet",
		Details: []string{
			"supported: " + codecChangePolicyDetail(RealtimeCodecChangePolicy()),
			"requested: " + codecChangePolicyDetail(policy),
		},
		Suggestions: []string{
			"use goav.RealtimeCodecChangePolicy() for today's live receive behavior",
			"use packet-preserving goav.Record(...) when codec changes should stay encoded",
			"rebuild the job when a live stream switches to a different decoder codec",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func codecChangePolicySet(policy CodecChangePolicy) bool {
	return policy.RebindCompatible || policy.RequestKeyframe || policy.DropUntilSync || policy.FailOnDifferentCodec
}

func codecChangePolicyDetail(policy CodecChangePolicy) string {
	if !codecChangePolicySet(policy) {
		return "codec-change=default"
	}
	parts := make([]string, 0, 4)
	if policy.RebindCompatible {
		parts = append(parts, "rebind-compatible")
	}
	if policy.RequestKeyframe {
		parts = append(parts, "request-keyframe")
	}
	if policy.DropUntilSync {
		parts = append(parts, "drop-until-sync")
	}
	if policy.FailOnDifferentCodec {
		parts = append(parts, "fail-different-codec")
	}
	if len(parts) == 0 {
		return "codec-change=custom"
	}
	return "codec-change=" + strings.Join(parts, ",")
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

type streamOption func(*streamSelectConfig)

type streamSelectConfig struct {
	selector av.StreamSelector
}

func newStreamSelector(media av.MediaType, options ...streamOption) av.StreamSelector {
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

func StreamID(id av.StreamID) streamOption {
	return func(config *streamSelectConfig) {
		config.selector.ID = id
	}
}

func StreamName(name string) streamOption {
	return func(config *streamSelectConfig) {
		config.selector.Name = name
	}
}

func StreamIndex(index int) streamOption {
	return func(config *streamSelectConfig) {
		config.selector.Index = index
		config.selector.UseIndex = true
	}
}

type JobStreamBuilder struct {
	job    *Job
	stream *jobStreamBuild
}

func (b *JobStreamBuilder) Do(stage pipeline.Stage) *JobStreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("build stream", jobStreamName(stream), "custom stage", stream.encode))
		return b
	}
	stream.decode = true
	stream.steps = append(stream.steps, jobStreamStep{stage: stage})
	return b
}

func (b *JobStreamBuilder) Resize(width int, height int, options ...resizeOption) *JobStreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError("build stream", jobStreamName(stream), "resize", stream.encode))
		return b
	}
	stream.decode = true
	stream.steps = append(stream.steps, jobStreamStep{transform: Resize(width, height, options...)})
	return b
}

func (b *JobStreamBuilder) Resample(sampleRate int, channels int, options ...audioOption) *JobStreamBuilder {
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

func (b *JobStreamBuilder) OnCodecChange(policy CodecChangePolicy) *JobStreamBuilder {
	stream := b.current()
	stream.codecChange = policy
	return b
}

func (b *JobStreamBuilder) Opus(bitrate int, options ...codecOption) *JobStreamBuilder {
	return b.Encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *JobStreamBuilder) VP8(bitrate int, options ...codecOption) *JobStreamBuilder {
	return b.Encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *JobStreamBuilder) VP9(bitrate int, options ...codecOption) *JobStreamBuilder {
	return b.Encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
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

const transcodeRecipeOperation = "build transcode"

func Transcode(input InputSpec) *TranscodeJob {
	return &TranscodeJob{runtime: Default(), input: input}
}

func (j *TranscodeJob) UseRuntime(runtime Runtime) *TranscodeJob {
	if j != nil {
		j.runtime = runtime
	}
	return j
}

func (j *TranscodeJob) Audio(name string, options ...streamOption) *StreamBuilder {
	return j.stream(name, av.MediaAudio, options...)
}

func (j *TranscodeJob) Video(name string, options ...streamOption) *StreamBuilder {
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

func planTranscodeRecipe(intent Intent, input InputSpec, namedOutputs []namedOutputSpec) (transcodepkg.Plan, error) {
	streams := intent.Streams
	outputs, outputOrder := transcodeOutputAttachmentSet(namedOutputs)

	renditions := make([]transcodepkg.Rendition, 0, len(streams))
	outputRenditions := make(map[string][]string, len(outputs))
	if len(streams) == 0 {
		return transcodepkg.Plan{}, transcodeStreamMissingError()
	}
	for i := range streams {
		stream := streams[i]
		renditionName := stream.Name
		selector := streamIntentSelector(stream)
		rendition := transcodepkg.Rendition{
			Name:     renditionName,
			Selector: selector,
			Decode:   true,
			Encode: codec.EncodeConfig{
				Parameters: stream.Encode.Parameters,
				Bitrate:    stream.Encode.Bitrate,
			},
			Labels: append([]string(nil), stream.RouteTo...),
		}
		for _, label := range stream.RouteTo {
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
		Input:      input.input,
		Renditions: renditions,
		Outputs:    planOutputs,
	}, nil
}

func validateTranscodeIntentShape(operation string, intent Intent) error {
	if len(intent.Inputs) == 0 {
		return &BuildError{Code: "input_missing", Operation: operation, Reason: "no input is configured", Cause: ErrUnsupportedBuild}
	}
	if len(intent.Inputs) > 1 {
		return &BuildError{
			Code:      "input_count_unsupported",
			Operation: operation,
			Reason:    "transcode recipes currently take one input",
			Details: []string{
				fmt.Sprintf("inputs=%d", len(intent.Inputs)),
			},
			Suggestions: []string{
				"use one goav.Transcode(input) source per transcode job",
				"use the expert graph API when multiple sources must be composed manually",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	streams := intent.Streams
	if len(streams) == 0 {
		return transcodeStreamMissingError()
	}
	renditionNames := make(map[string]int, len(streams))
	for i := range streams {
		stream := streams[i]
		if err := validateTranscodeBranchIntentShape(stream, i); err != nil {
			return err
		}
		if _, _, err := transcodeBranchTransformConfigs(stream); err != nil {
			return err
		}
		renditionName := stream.Name
		if firstIndex, ok := renditionNames[renditionName]; ok {
			return transcodeDuplicateBranchError(renditionName, firstIndex, i)
		}
		renditionNames[renditionName] = i
	}
	return nil
}

func validateTranscodeBranchIntentShape(stream StreamIntent, index int) error {
	selector := streamIntentSelector(stream)
	if stream.Name == "" {
		return transcodeIntentBranchNameMissingError(index, stream)
	}
	if err := validateRecipeStreamSelector(transcodeRecipeOperation, transcodeIntentBranchName(stream), selector); err != nil {
		return err
	}
	if stream.Encode.ID == "" && !stream.Encode.Copy {
		return transcodeEncodeMissingError(stream)
	}
	if err := validateRecipeEncode(stream.Encode, transcodeRecipeOperation, stream.Name); err != nil {
		return err
	}
	if len(stream.RouteTo) == 0 {
		return transcodeBranchOutputMissingError(stream)
	}
	return validateTranscodeBranchOutputLabels(stream)
}

func validateTranscodeAttachments(input InputSpec, namedOutputs []namedOutputSpec) error {
	if err := input.validate(); err != nil {
		return err
	}
	if input.rtp != nil {
		return transcodeUnsupportedRTPInputError()
	}
	seen := make(map[string]struct{}, len(namedOutputs))
	for i := range namedOutputs {
		if err := namedOutputs[i].output.validate(transcodeRecipeOperation, fmt.Sprintf("output-%d", i)); err != nil {
			return err
		}
		if namedOutputs[i].output.sink != nil {
			return transcodeFrameSinkOutputError(namedOutputs[i].name, namedOutputs[i].output)
		}
		name := namedOutputs[i].name
		if _, ok := seen[name]; ok {
			return transcodeDuplicateOutputError(name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateTranscodeOutputBindings(intent Intent, namedOutputs []namedOutputSpec) error {
	outputs := transcodeOutputLabelSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		for _, label := range stream.RouteTo {
			if _, ok := outputs[label]; ok {
				continue
			}
			return transcodeOutputReferenceMissingError(stream, label)
		}
	}
	return nil
}

func transcodeOutputAttachmentSet(namedOutputs []namedOutputSpec) (map[string]OutputSpec, []string) {
	outputs := make(map[string]OutputSpec, len(namedOutputs))
	outputOrder := make([]string, 0, len(namedOutputs))
	for i := range namedOutputs {
		name := namedOutputs[i].name
		outputOrder = append(outputOrder, name)
		outputs[name] = namedOutputs[i].output.Name(firstNonEmpty(namedOutputs[i].output.name, name))
	}
	return outputs, outputOrder
}

func transcodeOutputLabelSet(namedOutputs []namedOutputSpec) map[string]struct{} {
	outputs := make(map[string]struct{}, len(namedOutputs))
	for i := range namedOutputs {
		outputs[namedOutputs[i].name] = struct{}{}
	}
	return outputs
}

func transcodeStreamMissingError() error {
	return &BuildError{
		Code:      "stream_missing",
		Operation: transcodeRecipeOperation,
		Reason:    "no audio or video branches are configured",
		Suggestions: []string{
			"add a video branch such as .Video(\"720p\").Resize(...).VP9(...).To(...)",
			"add an audio branch such as .Audio(\"main\").Resample(...).Opus(...).To(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeEncodeMissingError(stream StreamIntent) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: transcodeRecipeOperation,
		Node:      stream.Name,
		Reason:    "stream has no codec target",
		Suggestions: []string{
			"call .Opus(...), .VP8(...), or .VP9(...) before .To(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeBranchOutputMissingError(stream StreamIntent) error {
	selector := streamIntentSelector(stream)
	return &BuildError{
		Code:      "output_missing",
		Operation: transcodeRecipeOperation,
		Node:      firstNonEmpty(stream.Name, string(selector.Type), "stream"),
		Reason:    "stream has no output target",
		Suggestions: []string{
			"call .To(\"label\") and define it with .Output(label, goav.FileOutput(...))",
			"reuse the same output label from multiple branches when they should share an output",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeOutputReferenceMissingError(stream StreamIntent, label string) error {
	return &BuildError{
		Code:      "output_missing",
		Operation: transcodeRecipeOperation,
		Node:      stream.Name,
		Reason:    "output " + label + " is referenced but not defined",
		Suggestions: []string{
			"call .Output(" + label + ", goav.FileOutput(...))",
			"define shared outputs once and route branches by label",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeUnsupportedRTPInputError() error {
	return &BuildError{
		Code:      "unsupported_input",
		Operation: transcodeRecipeOperation,
		Reason:    "RTP transcode recipes are not supported by the transcode recipe compiler yet",
		Suggestions: []string{
			"use Record(...) for packet recording",
			"use From(...).Audio() or From(...).Video() for one selected RTP receive path",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (j *TranscodeJob) Describe() (pipeline.Spec, error) {
	resolved, err := compileTranscodeRecipe(j)
	if err != nil {
		return pipeline.Spec{}, err
	}
	return resolved.Describe()
}

func (j *TranscodeJob) Build(ctx context.Context) (Task, error) {
	resolved, err := compileTranscodeRecipeForBuild(j)
	if err != nil {
		return nil, err
	}
	return resolved.Build(ctx)
}

func (j *TranscodeJob) Run(ctx context.Context) error {
	task, err := j.Build(ctx)
	if err != nil {
		return err
	}
	defer task.Close()
	return task.Run(ctx)
}

func (j *TranscodeJob) stream(name string, media av.MediaType, options ...streamOption) *StreamBuilder {
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

func (b *StreamBuilder) Resize(width int, height int, options ...resizeOption) *StreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError(transcodeRecipeOperation, transcodeBranchName(*stream), "resize", stream.encode))
		return b
	}
	stream.transforms = append(stream.transforms, Resize(width, height, options...))
	return b
}

func (b *StreamBuilder) Resample(sampleRate int, channels int, options ...audioOption) *StreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(streamStepAfterEncodeError(transcodeRecipeOperation, transcodeBranchName(*stream), "resample", stream.encode))
		return b
	}
	stream.transforms = append(stream.transforms, Resample(sampleRate, channels, options...))
	return b
}

func (b *StreamBuilder) Opus(bitrate int, options ...codecOption) *StreamBuilder {
	return b.encode(Opus(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) VP8(bitrate int, options ...codecOption) *StreamBuilder {
	return b.encode(VP8(append([]codecOption{Bitrate(bitrate)}, options...)...))
}

func (b *StreamBuilder) VP9(bitrate int, options ...codecOption) *StreamBuilder {
	return b.encode(VP9(append([]codecOption{Bitrate(bitrate)}, options...)...))
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
		Operation: transcodeRecipeOperation,
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
		Operation: transcodeRecipeOperation,
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

func transcodeFrameSinkOutputError(label string, output OutputSpec) error {
	err := &BuildError{
		Code:      "output_kind_invalid",
		Operation: transcodeRecipeOperation,
		Node:      firstNonEmpty(label, output.label("output")),
		Reason:    "transcode outputs are muxed output groups, not frame sinks",
		Suggestions: []string{
			"use goav.FileOutput(...) or goav.URIOutput(...) in .Output(label, ...)",
			"use goav.Decode(input, goav.FrameSink(sink)) or goav.From(input).Audio()/Video().To(goav.FrameSink(sink)) for decoded frames",
			"use the expert graph API when one pipeline must feed both decoded frame sinks and muxed outputs",
		},
		Cause: ErrUnsupportedBuild,
	}
	if output.sink != nil && output.sink.Name() != "" {
		err.Details = append(err.Details, "sink: "+output.sink.Name())
	}
	return err
}

func transcodeDuplicateOutputError(name string) error {
	return &BuildError{
		Code:      "output_duplicate",
		Operation: transcodeRecipeOperation,
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
		Operation: transcodeRecipeOperation,
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

func transcodeIntentBranchNameMissingError(index int, stream StreamIntent) error {
	return &BuildError{
		Code:      "stream_name_missing",
		Operation: transcodeRecipeOperation,
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "transcode branches need stable names",
		Details: []string{
			"media type: " + firstNonEmpty(string(stream.Select.Type), "unknown"),
		},
		Suggestions: []string{
			"call .Video(\"720p\") for video branches",
			"call .Audio(\"main\") for audio branches",
			"use branch names as handles for graph inspection and output routing",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateTranscodeBranchOutputLabels(stream StreamIntent) error {
	seen := make(map[string]int, len(stream.RouteTo))
	for i, label := range stream.RouteTo {
		if firstIndex, ok := seen[label]; ok {
			return transcodeDuplicateBranchOutputError(stream, label, firstIndex, i)
		}
		seen[label] = i
	}
	return nil
}

func transcodeDuplicateBranchOutputError(stream StreamIntent, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "output_duplicate",
		Operation: transcodeRecipeOperation,
		Node:      transcodeIntentBranchName(stream),
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

func transcodeBranchTransformConfigs(stream StreamIntent) (*filter.ResizeConfig, *filter.ResampleConfig, error) {
	var resize *filter.ResizeConfig
	var resample *filter.ResampleConfig
	for i := range stream.Transforms {
		transform := stream.Transforms[i]
		if err := validateTransformSpec(transcodeRecipeOperation, transcodeIntentBranchName(stream), transform); err != nil {
			return nil, nil, err
		}
		switch {
		case transform.Resize != nil && transform.Resample != nil:
			return nil, nil, &BuildError{
				Code:      "transform_invalid",
				Operation: transcodeRecipeOperation,
				Node:      transcodeIntentBranchName(stream),
				Reason:    "one transform cannot be both resize and resample",
				Cause:     ErrUnsupportedBuild,
			}
		case transform.Resize != nil:
			if stream.Select.Type == av.MediaAudio {
				return nil, nil, transcodeTransformMediaError(stream, "resize", "video")
			}
			if resize != nil || resample != nil {
				return nil, nil, transcodeTransformChainError(stream)
			}
			config := *transform.Resize
			resize = &config
		case transform.Resample != nil:
			if stream.Select.Type == av.MediaVideo {
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
				Operation: transcodeRecipeOperation,
				Node:      transcodeIntentBranchName(stream),
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

func transcodeTransformMediaError(stream StreamIntent, transform string, media string) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: transcodeRecipeOperation,
		Node:      transcodeIntentBranchName(stream),
		Reason:    transform + " applies to " + media + " branches",
		Suggestions: []string{
			"use .Video(...).Resize(...) for video ladder branches",
			"use .Audio(...).Resample(...) for audio branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeTransformChainError(stream StreamIntent) error {
	return &BuildError{
		Code:      "transform_chain_unsupported",
		Operation: transcodeRecipeOperation,
		Node:      transcodeIntentBranchName(stream),
		Reason:    "transcode branches currently support one media transform",
		Suggestions: []string{
			"call one Resize or Resample per branch",
			"create another Video(...) or Audio(...) branch for another output shape",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamIntentSelector(stream StreamIntent) av.StreamSelector {
	return av.StreamSelector{
		ID:       stream.Select.ID,
		Index:    stream.Select.Index,
		UseIndex: stream.Select.UseIndex,
		Type:     stream.Select.Type,
		Codec:    stream.Select.Codec,
		Name:     stream.Select.Name,
	}
}

func transcodeIntentBranchName(stream StreamIntent) string {
	return firstNonEmpty(stream.Name, string(stream.Select.Type), "stream")
}

func transcodeBranchName(stream streamBuild) string {
	return firstNonEmpty(stream.name, string(stream.selector.Type), "stream")
}

func (b *StreamBuilder) encode(codec CodecSpec) *StreamBuilder {
	stream := b.current()
	if codecIntentSet(stream.encode) {
		b.job.setErr(duplicateStreamEncodeError(transcodeRecipeOperation, transcodeBranchName(*stream), stream.encode, codec))
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
