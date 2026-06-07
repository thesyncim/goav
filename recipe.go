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
)

const (
	Mono   = 1
	Stereo = 2
)

type Intent struct {
	Name     string
	Inputs   []InputIntent
	Streams  []StreamIntent
	Targets  []TargetIntent
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
	FromTap     string
	Decode      bool
	Operations  []StreamOperation
	Transforms  []TransformSpec
	Taps        []TapIntent
	Encode      CodecSpec
	CodecChange CodecChangePolicy
	Targets     []string
}

type StreamOperation struct {
	Kind      OperationKind
	Component string
	Stage     pipeline.Stage
	Transform TransformSpec
	Tap       TapIntent
	Encode    CodecSpec
}

type TargetIntent struct {
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

func Codec(id av.CodecID, media av.MediaType, options ...codecOption) CodecSpec {
	return codecSpec(id, media, av.CodecParameters{
		ID:   id,
		Type: media,
	}, options...)
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

func ClockRate(clockRate uint32) codecOption {
	return func(spec *CodecSpec) {
		spec.Parameters.ClockRate = clockRate
	}
}

func Parameters(parameters av.CodecParameters) codecOption {
	return func(spec *CodecSpec) {
		spec.Parameters = parameters
		if spec.Parameters.ID == "" {
			spec.Parameters.ID = spec.ID
		}
		if spec.Parameters.Type == "" {
			spec.Parameters.Type = spec.Type
		}
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

func (s InputSpec) formatInput() format.Input {
	input := s.input
	input.Realtime = input.Realtime || s.realtime
	return input
}

func (s InputSpec) rtpBuildInput() rtpInput {
	input := rtpInput{}
	if s.rtp != nil {
		input.receiver = s.rtp.receiver
	}
	options := s.rtpOptions()
	for i := range options {
		if options[i] != nil {
			options[i](&input)
		}
	}
	return input
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
				"use goav.From(goav.RTP(reader).Codec(...)).Copy().To(output) for packet-preserving receive",
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

// EndpointSpec describes a concrete destination endpoint such as a file writer,
// URI, or sink endpoint.
type EndpointSpec struct {
	output         format.Output
	sink           pipeline.Sink
	format         av.FormatID
	resolvedFormat av.FormatID
	name           string
	err            error
}

// FileOutput creates a writer-backed file endpoint.
func FileOutput(name string, writer io.Writer) EndpointSpec {
	return EndpointSpec{
		output: format.Output{
			Name:     name,
			Protocol: av.ProtocolFile,
			Writer:   writer,
		},
		name: name,
	}
}

// URIOutput creates a URI endpoint opened by a registered format adapter.
func URIOutput(uri string) EndpointSpec {
	return EndpointSpec{
		output: format.Output{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

// SinkEndpoint creates a decoded-frame or packet sink endpoint.
func SinkEndpoint(sink pipeline.Sink) EndpointSpec {
	name := ""
	if sink != nil {
		name = sink.Name()
	}
	if sink == nil {
		return EndpointSpec{err: ErrNilSink}
	}
	return EndpointSpec{sink: sink, name: name}
}

// Name overrides the endpoint name used for diagnostics and mux graph nodes.
// Sink graph nodes use the wrapped sink's Name.
func (s EndpointSpec) Name(name string) EndpointSpec {
	s.name = name
	if s.sink == nil {
		s.output.Name = name
	}
	return s
}

// MIME sets the endpoint MIME type used for format detection.
func (s EndpointSpec) MIME(mimeType string) EndpointSpec {
	s.output.MIMEType = mimeType
	return s
}

// Format sets the endpoint container format explicitly.
func (s EndpointSpec) Format(format av.FormatID) EndpointSpec {
	s.format = format
	return s
}

func (s EndpointSpec) withResolvedFormat(format av.FormatID) EndpointSpec {
	s.resolvedFormat = format
	return s
}

func (s EndpointSpec) validate(operation string, fallback string) error {
	node := s.label(fallback)
	if s.err != nil {
		return &BuildError{
			Code:      "output_invalid",
			Operation: operation,
			Node:      node,
			Reason:    s.err.Error(),
			Suggestions: []string{
				"pass a non-nil sink to goav.SinkEndpoint(...)",
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
			Reason:    "empty endpoint spec",
			Suggestions: []string{
				"use goav.FileOutput(name, writer) for muxed output",
				"use goav.SinkEndpoint(sink) for decoded frames",
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

func (s EndpointSpec) label(fallback string) string {
	return firstNonEmpty(s.name, s.output.Name, s.output.URI, fallback)
}

func (s EndpointSpec) intent() TargetIntent {
	return TargetIntent{
		Name:     s.label("output"),
		URI:      s.output.URI,
		Protocol: s.output.Protocol,
		MIMEType: s.output.MIMEType,
		Format:   s.format,
	}
}

func (s EndpointSpec) intentWithName(name string) TargetIntent {
	intent := s.intent()
	intent.Name = firstNonEmpty(name, intent.Name)
	return intent
}

type Job struct {
	name          string
	runtime       Runtime
	inputs        []InputSpec
	outputs       []EndpointSpec
	stream        *jobStreamBuild
	branchStreams []streamBuild
	branchTargets []namedTargetSpec
	err           error
}

type jobStreamBuild struct {
	name           string
	selector       av.StreamSelector
	decode         bool
	steps          []jobStreamStep
	taps           []string
	postEncodeTaps []string
	encode         CodecSpec
	codecChange    CodecChangePolicy
	outputs        []EndpointSpec
}

type jobStreamStep struct {
	stage     pipeline.Stage
	transform TransformSpec
	tap       string
}

type jobStreamStepAttachment struct {
	stage          pipeline.Stage
	hasTransform   bool
	transformIndex int
	tap            string
	stepIndex      int
}

func From(input InputSpec) *Job {
	job := newJob("from")
	job.inputs = append(job.inputs, input)
	return job
}

func (j *Job) Copy() *Job {
	return j
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

func (j *Job) To(outputs ...EndpointSpec) *Job {
	if len(j.branchStreams) != 0 {
		j.setErr(branchOutputScopeError("branches"))
		return j
	}
	j.outputs = append(j.outputs, outputs...)
	return j
}

func (j *Job) addBranchTargets(targets ...TargetSpec) error {
	seen := make(map[string]string, len(j.branchTargets)+len(targets))
	for i := range j.branchTargets {
		seen[j.branchTargets[i].name] = targetIdentity(j.branchTargets[i])
	}
	for i := range targets {
		target := cloneTargetSpec(targets[i])
		if target.err != nil {
			return target.err
		}
		if target.name == "" {
			return targetNameMissingError(target.endpoint)
		}
		target.endpoint = target.endpoint.Name(firstNonEmpty(target.endpoint.name, target.name))
		named := namedTargetSpec{name: target.name, output: target.endpoint}
		identity := targetIdentity(named)
		if existing, ok := seen[named.name]; ok {
			if existing != identity {
				return branchTargetDuplicateError(named.name)
			}
			continue
		}
		seen[named.name] = identity
		j.branchTargets = append(j.branchTargets, named)
	}
	return nil
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

func (j *Job) Stream(options ...streamOption) *JobStreamBuilder {
	return j.streamBuilder("stream", "", options...)
}

func (j *Job) streamBuilder(name string, media av.MediaType, options ...streamOption) *JobStreamBuilder {
	stream := &jobStreamBuild{
		name:     name,
		selector: newStreamSelector(media, options...),
	}
	if j.stream != nil {
		if len(j.branchStreams) != 0 {
			j.stream = stream
			return &JobStreamBuilder{job: j, stream: stream}
		}
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
	if len(j.branchStreams) != 0 {
		for i := range j.branchStreams {
			intent.Streams = append(intent.Streams, branchStreamIntent(j.branchStreams[i]))
		}
		for i := range j.branchTargets {
			intent.Targets = append(intent.Targets, j.branchTargets[i].output.intentWithName(j.branchTargets[i].name))
		}
		return intent
	} else if j.stream != nil {
		intent.Streams = append(intent.Streams, jobStreamIntent(j.stream))
	}
	outputs := j.allOutputs()
	for i := range outputs {
		intent.Targets = append(intent.Targets, outputs[i].intent())
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
	resolved, err := compileJobRecipeForBuildContext(ctx, j)
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

func validateJobOutputBindings(operation string, stream StreamIntent, outputs []EndpointSpec) error {
	labels := jobOutputLabelSet(outputs)
	for _, label := range stream.Targets {
		if _, ok := labels[label]; ok {
			continue
		}
		return jobTargetReferenceMissingError(operation, stream, label)
	}
	return nil
}

func jobOutputLabelSet(outputs []EndpointSpec) map[string]struct{} {
	labels := make(map[string]struct{}, len(outputs))
	for i := range outputs {
		labels[outputs[i].label(fmt.Sprintf("output-%d", i))] = struct{}{}
	}
	return labels
}

func (j *Job) allOutputs() []EndpointSpec {
	if len(j.branchTargets) != 0 {
		outputs := make([]EndpointSpec, 0, len(j.branchTargets))
		for i := range j.branchTargets {
			outputs = append(outputs, j.branchTargets[i].output)
		}
		return outputs
	}
	return jobAllOutputs(j.outputs, jobStreamOutputs(j.stream))
}

func jobAllOutputs(outputs []EndpointSpec, streamOutputs []EndpointSpec) []EndpointSpec {
	if len(streamOutputs) == 0 {
		return append([]EndpointSpec(nil), outputs...)
	}
	all := make([]EndpointSpec, 0, len(outputs)+len(streamOutputs))
	all = append(all, outputs...)
	all = append(all, streamOutputs...)
	return all
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
	afterPacketOperation := OpEncode
	if stream.encode.Copy {
		afterPacketOperation = OpCopy
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
		Operations:  jobStreamOperations(stream),
		Transforms:  stream.transformSpecs(),
		Taps:        append(streamStepTapIntents(stream.steps, stream.selector.Type), postPacketTapIntents(stream.postEncodeTaps, stream.selector.Type, afterPacketOperation)...),
		Encode:      stream.encode,
		CodecChange: stream.codecChange,
		Targets:     endpointTargetNames(stream.outputs),
	}
}

func branchStreamIntent(stream streamBuild) StreamIntent {
	afterPacketOperation := OpEncode
	if stream.encode.Copy {
		afterPacketOperation = OpCopy
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
		FromTap:    stream.fromTap,
		Decode:     stream.decode,
		Operations: streamBuildOperations(stream),
		Transforms: cloneTransformSpecs(stream.transforms),
		Taps:       append(streamStepTapIntents(streamBuildSteps(stream), stream.selector.Type), postPacketTapIntents(stream.postEncodeTaps, stream.selector.Type, afterPacketOperation)...),
		Encode:     stream.encode,
		Targets:    append([]string(nil), stream.labels...),
	}
}

func jobStreamOperations(stream *jobStreamBuild) []StreamOperation {
	if stream == nil {
		return nil
	}
	operations := make([]StreamOperation, 0, len(stream.steps)+1+len(stream.postEncodeTaps))
	if stream.decode {
		operations = append(operations, StreamOperation{Kind: OpDecode, Component: string(stream.selector.Codec)})
	}
	operations = append(operations, streamStepOperations(stream.steps, stream.selector.Type)...)
	if stream.encode.Copy {
		operations = append(operations, StreamOperation{Kind: OpCopy, Component: "packet-copy", Encode: stream.encode})
	} else if codecIntentSet(stream.encode) {
		operations = append(operations, StreamOperation{Kind: OpEncode, Component: string(stream.encode.ID), Encode: stream.encode})
	}
	for i := range stream.postEncodeTaps {
		after := OpEncode
		if stream.encode.Copy {
			after = OpCopy
		}
		tap := TapIntent{Name: stream.postEncodeTaps[i], MediaKind: stream.selector.Type, Domain: DomainPacket, After: after}
		operations = append(operations, StreamOperation{Kind: OpTap, Component: tap.Name, Tap: tap})
	}
	return operations
}

func streamBuildOperations(stream streamBuild) []StreamOperation {
	steps := streamBuildSteps(stream)
	operations := make([]StreamOperation, 0, len(steps)+2+len(stream.postEncodeTaps))
	if stream.decode {
		operations = append(operations, StreamOperation{Kind: OpDecode, Component: string(stream.selector.Codec)})
	}
	operations = append(operations, streamStepOperations(steps, stream.selector.Type)...)
	if stream.encode.Copy {
		operations = append(operations, StreamOperation{Kind: OpCopy, Component: "packet-copy", Encode: stream.encode})
	} else if codecIntentSet(stream.encode) {
		operations = append(operations, StreamOperation{Kind: OpEncode, Component: string(stream.encode.ID), Encode: stream.encode})
	}
	for i := range stream.postEncodeTaps {
		after := OpEncode
		if stream.encode.Copy {
			after = OpCopy
		}
		tap := TapIntent{Name: stream.postEncodeTaps[i], MediaKind: stream.selector.Type, Domain: DomainPacket, After: after}
		operations = append(operations, StreamOperation{Kind: OpTap, Component: tap.Name, Tap: tap})
	}
	return operations
}

func streamBuildSteps(stream streamBuild) []jobStreamStep {
	return appendBranchSteps(stream.sharedSteps, stream.steps)
}

func streamStepOperations(steps []jobStreamStep, media av.MediaType) []StreamOperation {
	if len(steps) == 0 {
		return nil
	}
	operations := make([]StreamOperation, 0, len(steps))
	for i := range steps {
		step := steps[i]
		switch {
		case step.stage != nil:
			operations = append(operations, StreamOperation{
				Kind:      OpStage,
				Component: step.stage.Name(),
				Stage:     step.stage,
			})
		case step.transform.Resize != nil || step.transform.Resample != nil:
			operations = append(operations, StreamOperation{
				Kind:      OpTransform,
				Component: transformFactoryName(step.transform),
				Transform: cloneTransformSpec(step.transform),
			})
		case step.tap != "":
			tap := TapIntent{Name: step.tap, MediaKind: media, Domain: DomainFrame}
			operations = append(operations, StreamOperation{
				Kind:      OpTap,
				Component: tap.Name,
				Tap:       tap,
			})
		}
	}
	return operations
}

func streamStepTapIntents(steps []jobStreamStep, media av.MediaType) []TapIntent {
	if len(steps) == 0 {
		return nil
	}
	taps := make([]TapIntent, 0)
	domain := DomainFrame
	for i := range steps {
		step := steps[i]
		if step.tap == "" {
			continue
		}
		taps = append(taps, TapIntent{
			Name:      step.tap,
			MediaKind: media,
			Domain:    domain,
		})
	}
	return taps
}

func postPacketTapIntents(names []string, media av.MediaType, after OperationKind) []TapIntent {
	if len(names) == 0 {
		return nil
	}
	taps := make([]TapIntent, 0, len(names))
	for i := range names {
		taps = append(taps, TapIntent{
			Name:      names[i],
			MediaKind: media,
			Domain:    DomainPacket,
			After:     after,
		})
	}
	return taps
}

func streamNeedsDecodeFromBuild(stream *jobStreamBuild) bool {
	if stream == nil {
		return false
	}
	return stream.decode || len(stream.steps) != 0 || stream.encode.ID != "" || stream.encode.Auto || stream.encode.Copy
}

func jobStreamOutputs(stream *jobStreamBuild) []EndpointSpec {
	if stream == nil || len(stream.outputs) == 0 {
		return nil
	}
	return append([]EndpointSpec(nil), stream.outputs...)
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
		if step.tap != "" {
			attachments = append(attachments, jobStreamStepAttachment{tap: step.tap, stepIndex: i})
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

func validateJobStreamOutputKinds(operation string, stream StreamIntent, outputs []EndpointSpec) error {
	if outputsContainSinkEndpoint(outputs) && outputsContainMuxTarget(outputs) {
		return mixedStreamOutputError(operation, stream)
	}
	if stream.Encode.ID == "" && !stream.Encode.Copy && outputsContainMuxTarget(outputs) {
		return streamEncodeMissingError(operation, stream)
	}
	return nil
}

func mixedStreamOutputError(operation string, stream StreamIntent) error {
	return &BuildError{
		Code:      "output_kind_mixed",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes cannot mix sink endpoints and muxed outputs",
		Suggestions: []string{
			"use .To(goav.SinkEndpoint(...)) for decoded frames",
			"call .Opus(...), .VP8(...), or .VP9(...) before .To(goav.FileOutput(...)) for encoded output",
			"use .Branches(...) when one stream needs separate decoded and encoded branches",
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
			"send decoded frames to goav.SinkEndpoint(...)",
			"use .Copy().To(output) if you want to copy packets without decoding",
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
			transform(av.StreamSelector, mediaTransform) builderAPI
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
			"use goav.From(input).Video().Decode().Branches(...) for multiple branches from one stream",
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

func streamStepsFromTransforms(transforms []TransformSpec) []jobStreamStep {
	if len(transforms) == 0 {
		return nil
	}
	steps := make([]jobStreamStep, 0, len(transforms))
	for i := range transforms {
		steps = append(steps, jobStreamStep{transform: cloneTransformSpec(transforms[i])})
	}
	return steps
}

func cloneJobStreamSteps(steps []jobStreamStep) []jobStreamStep {
	if len(steps) == 0 {
		return nil
	}
	out := make([]jobStreamStep, 0, len(steps))
	for i := range steps {
		step := steps[i]
		step.transform = cloneTransformSpec(step.transform)
		out = append(out, step)
	}
	return out
}

func appendBranchSteps(prefix []jobStreamStep, branch []jobStreamStep) []jobStreamStep {
	out := make([]jobStreamStep, 0, len(prefix)+len(branch))
	out = append(out, cloneJobStreamSteps(prefix)...)
	out = append(out, cloneJobStreamSteps(branch)...)
	return out
}

func appendTransformSpecs(prefix []TransformSpec, branch []TransformSpec) []TransformSpec {
	out := make([]TransformSpec, 0, len(prefix)+len(branch))
	out = append(out, cloneTransformSpecs(prefix)...)
	out = append(out, cloneTransformSpecs(branch)...)
	return out
}

func outputsContainMuxTarget(outputs []EndpointSpec) bool {
	for i := range outputs {
		if outputs[i].sink == nil {
			return true
		}
	}
	return false
}

func outputsContainSinkEndpoint(outputs []EndpointSpec) bool {
	for i := range outputs {
		if outputs[i].sink != nil {
			return true
		}
	}
	return false
}

func validateEndpointSpecs(operation string, outputs []EndpointSpec) error {
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

func validateInputFormatAdapters(ctx context.Context, rt Runtime, inputs []InputSpec) ([]format.ProbeResult, error) {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil, nil
	}
	probes := make([]format.ProbeResult, len(inputs))
	for i := range inputs {
		if inputs[i].rtp != nil {
			continue
		}
		input := inputs[i].input
		result, err := standard.formats.Probe(ctx, inputProbeRequest(input))
		if err != nil {
			return nil, inputFormatProbeError(input, err)
		}
		probes[i] = result
		if _, err := standard.formats.DemuxerFactory(result.Format); err != nil {
			return nil, inputDemuxerMissingError(input, result.Format, err)
		}
	}
	return probes, nil
}

func validateOutputFormatAdapters(ctx context.Context, rt Runtime, outputs []EndpointSpec, targetNames ...string) ([]EndpointSpec, error) {
	resolved := append([]EndpointSpec(nil), outputs...)
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return resolved, nil
	}
	for i := range resolved {
		if resolved[i].sink != nil {
			continue
		}
		formatID := resolved[i].format
		if formatID == "" {
			result, err := standard.formats.Probe(ctx, outputProbeRequest(resolved[i].output))
			if err != nil {
				return nil, targetFormatProbeError(targetNodeName(resolved[i].output, i, targetNames), resolved[i].output, err)
			}
			formatID = result.Format
			resolved[i] = resolved[i].withResolvedFormat(formatID)
		}
		if _, err := standard.formats.MuxerFactory(formatID); err != nil {
			return nil, targetMuxerMissingError(targetNodeName(resolved[i].output, i, targetNames), resolved[i].output, formatID, err)
		}
	}
	return resolved, nil
}

func targetNodeName(output format.Output, index int, targetNames []string) string {
	if index >= 0 && index < len(targetNames) && targetNames[index] != "" {
		return targetNames[index]
	}
	return muxNodeName(output, index)
}

func validateRecipeDecodeAdapters(operation string, rt Runtime, intent Intent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range intent.Streams {
		stream := intent.Streams[i]
		if !streamNeedsDecode(stream) {
			continue
		}
		codecID, ok := liveDecodeCodec(intent.Inputs, stream)
		if !ok || codecID == "" {
			continue
		}
		if _, err := standard.codecs.DecoderFactory(codecID); err != nil {
			return recipeDecodeAdapterError(operation, stream, codecID, standard.codecs, err)
		}
	}
	return nil
}

func validateKnownRecipeDecodeAdapters(operation string, rt Runtime, probes []format.ProbeResult, streams []StreamIntent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		if !streamNeedsDecode(stream) {
			continue
		}
		codecID, ok := knownProbeDecodeCodec(probes, stream)
		if !ok || codecID == "" {
			continue
		}
		if _, err := standard.codecs.DecoderFactory(codecID); err != nil {
			return recipeDecodeAdapterError(operation, stream, codecID, standard.codecs, err)
		}
	}
	return nil
}

func validateLiveStreamSelection(inputs []InputIntent, stream StreamIntent) error {
	streams := liveIntentStreams(inputs)
	if len(streams) == 0 {
		return nil
	}
	_, err := selectDecodeStream(streams, streamIntentSelector(stream))
	return err
}

func liveIntentStreams(inputs []InputIntent) []av.Stream {
	streams := make([]av.Stream, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		if !input.Realtime || input.Codec.ID == "" {
			continue
		}
		stream := av.Stream{
			Index: i,
			Type:  input.Codec.Type,
			Codec: input.Codec.Parameters,
		}
		if input.Name != "" {
			stream.ID = av.StreamID(input.Name)
			stream.Name = input.Name
		}
		if stream.Codec.ID == "" {
			stream.Codec.ID = input.Codec.ID
		}
		if stream.Codec.Type == "" {
			stream.Codec.Type = stream.Type
		}
		if stream.Type == "" {
			stream.Type = stream.Codec.Type
		}
		streams = append(streams, stream)
	}
	return streams
}

func knownProbeDecodeCodec(probes []format.ProbeResult, stream StreamIntent) (av.CodecID, bool) {
	candidates := make([]av.CodecID, 0, len(probes))
	selector := streamIntentSelector(stream)
	for i := range probes {
		if len(probes[i].Streams) == 0 {
			continue
		}
		selected, err := selectDecodeStream(probes[i].Streams, selector)
		if err != nil || selected.Codec.ID == "" {
			continue
		}
		candidates = append(candidates, selected.Codec.ID)
	}
	if len(candidates) != 1 {
		return "", false
	}
	return candidates[0], true
}

func streamNeedsDecode(stream StreamIntent) bool {
	return stream.Decode || len(stream.Transforms) != 0 || stream.Encode.ID != ""
}

func liveDecodeCodec(inputs []InputIntent, stream StreamIntent) (av.CodecID, bool) {
	selector := stream.Select
	candidates := make([]av.CodecID, 0, len(inputs))
	for i := range inputs {
		input := inputs[i]
		if !input.Realtime || input.Codec.ID == "" {
			continue
		}
		if selector.Codec != "" && selector.Codec != input.Codec.ID {
			continue
		}
		if selector.Type != "" && input.Codec.Type != "" && selector.Type != input.Codec.Type {
			continue
		}
		if selector.Name != "" && selector.Name != input.Name {
			continue
		}
		if selector.ID != "" && input.Name != string(selector.ID) {
			continue
		}
		candidates = append(candidates, input.Codec.ID)
	}
	if len(candidates) != 1 {
		return "", false
	}
	return candidates[0], true
}

func recipeDecodeAdapterError(operation string, stream StreamIntent, codecID av.CodecID, registry *codec.SimpleRegistry, cause error) error {
	code := "decode_adapter_missing"
	reason := "no decoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = "decode_adapter_unavailable"
		reason = string(codecID) + " decoder adapter is descriptor-only in this build"
	}
	details := []string{"codec=" + string(codecID)}
	if registry != nil {
		descriptors, err := registry.Find(codecID, codec.ModeDecode)
		if err == nil {
			details = append(details, codecDescriptorDetails(descriptors)...)
		}
	}
	return &BuildError{
		Code:      code,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    reason,
		Details:   details,
		Suggestions: []string{
			"register a codec adapter that provides a " + string(codecID) + " decoder",
			"enable the adapter build tag or choose a runtime with a concrete decoder",
			"use goav.From(input).Copy().To(output) for packet-preserving receive when decoding is not needed",
		},
		Cause: cause,
	}
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

func validateRecipeTransformAdapters(operation string, rt Runtime, streams []StreamIntent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		for j := range stream.Transforms {
			name := transformFactoryName(stream.Transforms[j])
			if name == "" {
				continue
			}
			if _, err := standard.filters.Factory(name); err != nil {
				return recipeTransformAdapterError(operation, stream, name, err)
			}
		}
	}
	return nil
}

func transformFactoryName(spec TransformSpec) string {
	switch {
	case spec.Resize != nil:
		return filter.FactoryResize
	case spec.Resample != nil:
		return filter.FactoryResample
	default:
		return ""
	}
}

func recipeTransformAdapterError(operation string, stream StreamIntent, name string, cause error) error {
	if !errors.Is(cause, filter.ErrNotFound) {
		return cause
	}
	return &BuildError{
		Code:      "transform_adapter_missing",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "no " + name + " filter adapter is registered",
		Details: []string{
			"transform=" + name,
		},
		Suggestions: []string{
			"register a filter adapter that provides " + name,
			"use goav.Default() or goav.New(goav.WithDefaults()) for standard resize and resample adapters",
			"remove ." + transformMethodName(name) + "(...) when that conversion is not needed",
		},
		Cause: cause,
	}
}

func transformMethodName(name string) string {
	switch name {
	case filter.FactoryResize:
		return "Resize"
	case filter.FactoryResample:
		return "Resample"
	default:
		return "Do"
	}
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
			details = append(details, codecDescriptorDetails(descriptors)...)
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
			"use .To(goav.SinkEndpoint(...)) to receive decoded frames without encoding",
			"use .Copy().To(output) for packet-preserving output when re-encoding is not needed",
		},
		Cause: cause,
	}
}

func codecDescriptorDetails(descriptors []codec.Descriptor) []string {
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
		Reason:    fmt.Sprintf("output name %q is defined more than once", name),
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
			"use .Branches(...) when one input needs multiple encoded branches",
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
		return nil
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
			Reason:    string(spec.ID) + " recipe encoding is work in progress; recipe encode branches currently target opus, vp8, and vp9",
			Suggestions: []string{
				"decode the stream with .To(goav.SinkEndpoint(...))",
				"use .Opus(...), .VP8(...), or .VP9(...) for recipe encode branches",
				"use the expert builder with an explicit codec.EncodeConfig when testing an experimental encoder",
			},
			Cause: ErrUnsupportedBuild,
		}
	default:
		return validateRecipeEncodeValues(spec, operation, node)
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
			"use packet-preserving goav.From(input).Copy().To(output) when codec changes should stay encoded",
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

func streamTransform(streamName string, selector av.StreamSelector, spec TransformSpec, index int) (mediaTransform, error) {
	base := firstNonEmpty(streamName, string(selector.ID), string(selector.Type), "stream")
	suffix := ""
	if index > 0 {
		suffix = "-" + fmt.Sprint(index+1)
	}
	if err := validateTransformSpec("build stream", base, spec); err != nil {
		return mediaTransform{}, err
	}
	switch {
	case spec.Resize != nil && spec.Resample != nil:
		return mediaTransform{}, &BuildError{
			Code:      "transform_invalid",
			Operation: "build stream",
			Node:      base,
			Reason:    "one stream transform cannot be both resize and resample",
		}
	case spec.Resize != nil:
		if selector.Type == av.MediaAudio {
			return mediaTransform{}, transformMediaError(base, "resize", "video")
		}
		resize := *spec.Resize
		return mediaTransform{
			name:    "resize-" + base + suffix,
			factory: filter.FactoryResize,
			video:   &resize,
		}, nil
	case spec.Resample != nil:
		if selector.Type == av.MediaVideo {
			return mediaTransform{}, transformMediaError(base, "resample", "audio")
		}
		resample := *spec.Resample
		return mediaTransform{
			name:    "resample-" + base + suffix,
			factory: filter.FactoryResample,
			audio:   &resample,
		}, nil
	default:
		return mediaTransform{}, &BuildError{
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
	name           string
	selector       av.StreamSelector
	fromTap        string
	decode         bool
	sharedSteps    []jobStreamStep
	steps          []jobStreamStep
	postEncodeTaps []string
	transforms     []TransformSpec
	encode         CodecSpec
	labels         []string
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

func (b *JobStreamBuilder) Apply(flow Flow) *JobStreamBuilder {
	spec, err := flowSpecFrom(flow)
	if err != nil {
		b.job.setErr(err)
		return b
	}
	stream := b.current()
	if err := validateFlowMedia("build stream", jobStreamName(stream), stream.selector.Type, spec); err != nil {
		b.job.setErr(err)
		return b
	}
	if codecIntentSet(stream.encode) && (len(spec.steps) != 0 || codecIntentSet(spec.encode)) {
		b.job.setErr(streamStepAfterEncodeError("build stream", jobStreamName(stream), "flow", stream.encode))
		return b
	}
	if len(spec.steps) != 0 {
		stream.decode = true
		stream.steps = append(stream.steps, cloneJobStreamSteps(spec.steps)...)
	}
	if codecIntentSet(spec.encode) {
		return b.Encode(spec.encode)
	}
	return b
}

func (b *JobStreamBuilder) Decode() *JobStreamBuilder {
	stream := b.current()
	stream.decode = true
	return b
}

func (b *JobStreamBuilder) Copy() *JobStreamBuilder {
	stream := b.current()
	stream.decode = false
	stream.encode = Copy()
	return b
}

func (b *JobStreamBuilder) Tap(name string) *JobStreamBuilder {
	stream := b.current()
	if name == "" {
		b.job.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build stream",
			Node:      jobStreamName(stream),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(\"video.decoded\") or another stable tap name",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return b
	}
	if codecIntentSet(stream.encode) {
		stream.postEncodeTaps = append(stream.postEncodeTaps, name)
		return b
	}
	stream.decode = true
	stream.steps = append(stream.steps, jobStreamStep{tap: name})
	return b
}

func streamSelectFromAV(selector av.StreamSelector) StreamSelect {
	return StreamSelect{
		ID:       selector.ID,
		Index:    selector.Index,
		UseIndex: selector.UseIndex,
		Type:     selector.Type,
		Codec:    selector.Codec,
		Name:     selector.Name,
	}
}

func lastStreamTap(stream *jobStreamBuild) string {
	if stream == nil {
		return ""
	}
	for i := len(stream.steps) - 1; i >= 0; i-- {
		if stream.steps[i].tap != "" {
			return stream.steps[i].tap
		}
	}
	if len(stream.steps) == 0 && stream.selector.Type != "" && stream.decode {
		return defaultDecodedTapName(stream.selector.Type)
	}
	return ""
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

func (b *JobStreamBuilder) To(outputs ...EndpointSpec) *Job {
	stream := b.current()
	stream.outputs = append(stream.outputs, outputs...)
	if outputsContainSinkEndpoint(outputs) && !codecIntentSet(stream.encode) {
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

type branchCompositionJob struct {
	runtime Runtime
	name    string
	input   InputSpec
	streams []streamBuild
	outputs []namedTargetSpec
	err     error

	fromBranchSplit bool
}

type namedTargetSpec struct {
	name   string
	output EndpointSpec
}

func targetIdentity(target namedTargetSpec) string {
	output := target.output
	sinkName := ""
	sinkAddr := ""
	if output.sink != nil {
		sinkName = output.sink.Name()
		sinkAddr = fmt.Sprintf("%p", output.sink)
	}
	return strings.Join([]string{
		target.name,
		output.label(""),
		sinkName,
		sinkAddr,
		output.output.Name,
		output.output.URI,
		string(output.output.Protocol),
		output.output.MIMEType,
		string(output.format),
		string(output.resolvedFormat),
	}, "\x00")
}

const branchCompositionOperation = "build branch composition"

func (j *branchCompositionJob) setErr(err error) {
	if j.err == nil {
		j.err = err
	}
}

func (j *branchCompositionJob) Intent() Intent {
	intent := Intent{
		Name:   firstNonEmpty(j.name, "branch-composition"),
		Inputs: []InputIntent{j.input.intent()},
	}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
	}
	for i := range j.streams {
		intent.Streams = append(intent.Streams, branchStreamIntent(j.streams[i]))
	}
	for i := range j.outputs {
		intent.Targets = append(intent.Targets, j.outputs[i].output.intentWithName(j.outputs[i].name))
	}
	return intent
}

func (j *branchCompositionJob) Plan() (branchComposePlan, error) {
	if j == nil {
		return branchComposePlan{}, nil
	}
	return planBranchCompositionRecipe(j.Intent(), j.input, j.outputs, j.streams)
}

func planBranchCompositionRecipe(intent Intent, input InputSpec, namedOutputs []namedTargetSpec, branchBuilds []streamBuild) (branchComposePlan, error) {
	streams := intent.Streams
	outputs, outputOrder := branchTargetAttachmentSet(namedOutputs)

	branches := make([]branchComposeBranch, 0, len(streams))
	outputBranches := make(map[string][]string, len(outputs))
	if len(streams) == 0 {
		return branchComposePlan{}, branchStreamMissingError()
	}
	for i := range streams {
		stream := streams[i]
		branchName := stream.Name
		selector := streamIntentSelector(stream)
		sharedSteps, branchSteps := branchComposeStepsForStream(stream)
		if i < len(branchBuilds) {
			sharedSteps, branchSteps = branchComposeStepsForStreamBuild(branchBuilds[i])
		}
		branch := branchComposeBranch{
			Name:        branchName,
			Selector:    selector,
			Decode:      true,
			SharedSteps: sharedSteps,
			Steps:       branchSteps,
			Encode: codec.EncodeConfig{
				Parameters: stream.Encode.Parameters,
				Bitrate:    stream.Encode.Bitrate,
			},
			Labels: append([]string(nil), stream.Targets...),
		}
		for _, label := range stream.Targets {
			outputBranches[label] = append(outputBranches[label], branchName)
		}
		if err := validateBranchTransforms(stream); err != nil {
			return branchComposePlan{}, err
		}
		branches = append(branches, branch)
	}

	planTargets := make([]branchComposeTarget, 0, len(outputOrder))
	for i := range outputOrder {
		name := outputOrder[i]
		output := outputs[name]
		planTarget := branchComposeTarget{
			Name:     name,
			Target:   output.output,
			Sink:     output.sink,
			Format:   output.format,
			Branches: append([]string(nil), outputBranches[name]...),
		}
		if output.resolvedFormat != "" {
			planTarget = resolveBranchComposeTargetFormat(planTarget, output.resolvedFormat)
		}
		planTargets = append(planTargets, planTarget)
	}
	return branchComposePlan{
		Name:     "branch-composition",
		Input:    input.input,
		Branches: branches,
		Targets:  planTargets,
	}, nil
}

func branchComposePlanReady(plan branchComposePlan) bool {
	return len(plan.Branches) != 0 || len(plan.Targets) != 0
}

func branchComposeStepsForStreamBuild(stream streamBuild) ([]branchComposeStep, []branchComposeStep) {
	return branchComposeStepsFromJobSteps(stream.sharedSteps), branchComposeStepsFromJobSteps(stream.steps)
}

func branchComposeStepsForStream(stream StreamIntent) ([]branchComposeStep, []branchComposeStep) {
	if len(stream.Operations) != 0 {
		return branchComposeStepsFromOperations(stream.Operations, stream.FromTap)
	}
	if len(stream.Transforms) == 0 {
		return nil, nil
	}
	return nil, branchComposeStepsFromJobSteps(streamStepsFromTransforms(stream.Transforms))
}

func branchComposeStepsFromOperations(operations []StreamOperation, fromTap string) ([]branchComposeStep, []branchComposeStep) {
	if len(operations) == 0 {
		return nil, nil
	}
	steps := make([]branchComposeStep, 0, len(operations))
	shared := make([]branchComposeStep, 0)
	branch := make([]branchComposeStep, 0)
	split := fromTap == ""
	foundSplit := fromTap == ""
	for i := range operations {
		operation := operations[i]
		if operation.Kind == OpTap && operation.Component == fromTap {
			split = true
			foundSplit = true
			continue
		}
		var step branchComposeStep
		hasStep := false
		switch operation.Kind {
		case OpStage:
			if operation.Stage != nil {
				step = branchComposeStep{Stage: operation.Stage}
				hasStep = true
			}
		case OpTransform:
			switch {
			case operation.Transform.Resize != nil:
				resize := *operation.Transform.Resize
				step = branchComposeStep{Resize: &resize}
				hasStep = true
			case operation.Transform.Resample != nil:
				resample := *operation.Transform.Resample
				step = branchComposeStep{Resample: &resample}
				hasStep = true
			}
		}
		if !hasStep {
			continue
		}
		steps = append(steps, step)
		if split {
			branch = append(branch, step)
		} else {
			shared = append(shared, step)
		}
	}
	if !foundSplit {
		return nil, steps
	}
	return shared, branch
}

func branchComposeStepsFromJobSteps(steps []jobStreamStep) []branchComposeStep {
	if len(steps) == 0 {
		return nil
	}
	out := make([]branchComposeStep, 0, len(steps))
	for i := range steps {
		step := steps[i]
		switch {
		case step.stage != nil:
			out = append(out, branchComposeStep{Stage: step.stage})
		case step.transform.Resize != nil:
			resize := *step.transform.Resize
			out = append(out, branchComposeStep{Resize: &resize})
		case step.transform.Resample != nil:
			resample := *step.transform.Resample
			out = append(out, branchComposeStep{Resample: &resample})
		}
	}
	return out
}

func validateBranchCompositionIntentShape(operation string, intent Intent) error {
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
				"use one goav.From(input) source per composed job",
				"use the expert graph API when multiple sources must be composed manually",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	streams := intent.Streams
	if len(streams) == 0 {
		return branchStreamMissingError()
	}
	branchNames := make(map[string]int, len(streams))
	for i := range streams {
		stream := streams[i]
		if err := validateBranchIntentShape(stream, i); err != nil {
			return err
		}
		if err := validateBranchTransforms(stream); err != nil {
			return err
		}
		branchName := stream.Name
		if firstIndex, ok := branchNames[branchName]; ok {
			return branchIntentDuplicateError(branchName, firstIndex, i)
		}
		branchNames[branchName] = i
	}
	return nil
}

func validateBranchIntentShape(stream StreamIntent, index int) error {
	selector := streamIntentSelector(stream)
	if stream.Name == "" {
		return branchIntentNameMissingError(index, stream)
	}
	if err := validateRecipeStreamSelector(branchCompositionOperation, branchIntentName(stream), selector); err != nil {
		return err
	}
	if codecIntentSet(stream.Encode) {
		if stream.Encode.Copy {
			return branchCopyUnsupportedError(stream)
		}
		if err := validateRecipeEncode(stream.Encode, branchCompositionOperation, stream.Name); err != nil {
			return err
		}
	}
	if len(stream.Targets) == 0 {
		return branchIntentTargetMissingError(stream)
	}
	return validateBranchTargets(stream)
}

func validateBranchCompositionAttachments(input InputSpec, namedOutputs []namedTargetSpec, fromBranchSplit bool) error {
	if err := input.validate(); err != nil {
		return err
	}
	if input.rtp != nil {
		if !fromBranchSplit {
			return transcodeUnsupportedRTPInputError()
		}
	}
	seen := make(map[string]struct{}, len(namedOutputs))
	for i := range namedOutputs {
		if err := namedOutputs[i].output.validate(branchCompositionOperation, fmt.Sprintf("output-%d", i)); err != nil {
			return err
		}
		name := namedOutputs[i].name
		if _, ok := seen[name]; ok {
			return branchTargetDuplicateError(name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateBranchTargetKinds(intent Intent, namedOutputs []namedTargetSpec) error {
	outputs := branchTargetEndpointSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		if stream.Encode.Copy {
			return branchCopyUnsupportedError(stream)
		}
		hasMuxTarget := false
		for _, label := range stream.Targets {
			output, ok := outputs[label]
			if !ok {
				continue
			}
			if output.sink == nil {
				hasMuxTarget = true
				break
			}
		}
		if hasMuxTarget && !codecIntentSet(stream.Encode) {
			return branchEncodeMissingError(stream)
		}
	}
	return nil
}

func validateBranchTargetBindings(intent Intent, namedOutputs []namedTargetSpec) error {
	outputs := branchTargetLabelSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		for _, label := range stream.Targets {
			if _, ok := outputs[label]; ok {
				continue
			}
			return branchTargetReferenceMissingError(stream, label)
		}
	}
	return nil
}

func branchTargetEndpointSet(namedOutputs []namedTargetSpec) map[string]EndpointSpec {
	outputs := make(map[string]EndpointSpec, len(namedOutputs))
	for i := range namedOutputs {
		outputs[namedOutputs[i].name] = namedOutputs[i].output
	}
	return outputs
}

func branchTargetAttachmentSet(namedOutputs []namedTargetSpec) (map[string]EndpointSpec, []string) {
	outputs := make(map[string]EndpointSpec, len(namedOutputs))
	outputOrder := make([]string, 0, len(namedOutputs))
	for i := range namedOutputs {
		name := namedOutputs[i].name
		outputOrder = append(outputOrder, name)
		outputs[name] = namedOutputs[i].output.Name(firstNonEmpty(namedOutputs[i].output.name, name))
	}
	return outputs, outputOrder
}

func branchTargetLabelSet(namedOutputs []namedTargetSpec) map[string]struct{} {
	outputs := make(map[string]struct{}, len(namedOutputs))
	for i := range namedOutputs {
		outputs[namedOutputs[i].name] = struct{}{}
	}
	return outputs
}

func branchStreamMissingError() error {
	return &BuildError{
		Code:      "stream_missing",
		Operation: branchCompositionOperation,
		Reason:    "no audio or video branches are configured",
		Suggestions: []string{
			"add a video branch such as .Video(\"720p\").Resize(...).VP9(...).To(...)",
			"add an audio branch such as .Audio(\"main\").Resample(...).Opus(...).To(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchEncodeMissingError(stream StreamIntent) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "branch needs an encoder before writing to a muxed target",
		Suggestions: []string{
			"call .Opus(...), .VP8(...), or .VP9(...) before .To(...)",
			"route raw frames to goav.SinkEndpoint(...) when the branch should stay decoded",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchCopyUnsupportedError(stream StreamIntent) error {
	return &BuildError{
		Code:      "copy_unsupported",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    "planned branches start from decoded frame domain and cannot copy packets",
		Suggestions: []string{
			"use goav.From(input).Copy().To(output) for packet-preserving output",
			"attach a runtime branch from a packet tap and call .Copy() when packet-domain fanout is needed",
			"omit .Copy() when the branch should deliver decoded frames to goav.SinkEndpoint(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentTargetMissingError(stream StreamIntent) error {
	selector := streamIntentSelector(stream)
	return &BuildError{
		Code:      "target_missing",
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.Name, string(selector.Type), "stream"),
		Reason:    "branch has no target",
		Suggestions: []string{
			"finish the branch with .To(goav.Target(\"web\", goav.FileOutput(...)))",
			"reuse the same target value from multiple branches when they should share one mux group",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchTargetReferenceMissingError(stream StreamIntent, label string) error {
	return &BuildError{
		Code:      "target_missing",
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "target " + label + " is referenced but not defined",
		Suggestions: []string{
			"pass a goav.Target(\"" + label + "\", endpoint) value to the branch .To(...) call",
			"reuse typed target values instead of repeating string target refs",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeUnsupportedRTPInputError() error {
	return &BuildError{
		Code:      "unsupported_input",
		Operation: branchCompositionOperation,
		Reason:    "RTP transcode recipes are not supported by the transcode recipe compiler yet",
		Suggestions: []string{
			"use From(...).Copy().To(...) for packet recording",
			"use From(...).Audio().Decode() or From(...).Video().Decode() for one selected RTP receive path",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeEmptyOutputLabelError(stream streamBuild, index int) error {
	return &BuildError{
		Code:      "target_invalid",
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.name, string(stream.selector.Type), "stream"),
		Reason:    "branch targets must be non-empty",
		Details: []string{
			fmt.Sprintf("target index: %d", index),
		},
		Suggestions: []string{
			"call .To(goav.Target(\"web\", goav.FileOutput(...))) with a non-empty target name",
			"pass an endpoint directly when a separate target name is not needed",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transcodeEmptyOutputDefinitionLabelError(output EndpointSpec) error {
	err := &BuildError{
		Code:      "target_invalid",
		Operation: branchCompositionOperation,
		Node:      output.label("output"),
		Reason:    "target name is empty",
		Suggestions: []string{
			"call goav.Target(\"web\", goav.FileOutput(...)) with a stable target name",
			"pass goav.FileOutput(...) directly to .To(...) when a separate target name is not needed",
		},
		Cause: ErrUnsupportedBuild,
	}
	if output.name != "" {
		err.Details = append(err.Details, "output name: "+output.name)
	}
	return err
}

func branchTargetDuplicateError(name string) error {
	return &BuildError{
		Code:      "target_duplicate",
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("target %q is defined more than once with different endpoints", name),
		Suggestions: []string{
			"reuse the same goav.Target value when multiple branches should share one mux group",
			"use distinct target names when branches should write to different endpoints",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentDuplicateError(name string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "stream_duplicate",
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("branch name %q is defined more than once", name),
		Details: []string{
			fmt.Sprintf("first branch index: %d", firstIndex),
			fmt.Sprintf("second branch index: %d", secondIndex),
		},
		Suggestions: []string{
			"use unique names such as .Video(\"720p\") and .Video(\"360p\")",
			"route one branch to multiple targets by calling .To(target, otherTarget)",
			"route different branches to the same target by reusing the target value",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentNameMissingError(index int, stream StreamIntent) error {
	return &BuildError{
		Code:      "stream_name_missing",
		Operation: branchCompositionOperation,
		Node:      fmt.Sprintf("branch-%d", index),
		Reason:    "branches need stable names",
		Details: []string{
			"media type: " + firstNonEmpty(string(stream.Select.Type), "unknown"),
		},
		Suggestions: []string{
			"call .Video(\"720p\") for video branches",
			"call .Audio(\"main\") for audio branches",
			"use branch names as handles for graph inspection and target planning",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateBranchTargets(stream StreamIntent) error {
	seen := make(map[string]int, len(stream.Targets))
	for i, label := range stream.Targets {
		if firstIndex, ok := seen[label]; ok {
			return duplicateBranchTargetRefError(stream, label, firstIndex, i)
		}
		seen[label] = i
	}
	return nil
}

func duplicateBranchTargetRefError(stream StreamIntent, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "target_duplicate",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    fmt.Sprintf("branch routes to target %q more than once", label),
		Details: []string{
			fmt.Sprintf("first target index: %d", firstIndex),
			fmt.Sprintf("second target index: %d", secondIndex),
		},
		Suggestions: []string{
			"list each target once in .To(...)",
			"route one branch to multiple targets with distinct values such as .To(archive, preview)",
			"reuse typed target values instead of repeating string labels",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateBranchTransforms(stream StreamIntent) error {
	for i := range stream.Transforms {
		transform := stream.Transforms[i]
		if err := validateTransformSpec(branchCompositionOperation, branchIntentName(stream), transform); err != nil {
			return err
		}
		switch {
		case transform.Resize != nil && transform.Resample != nil:
			return &BuildError{
				Code:      "transform_invalid",
				Operation: branchCompositionOperation,
				Node:      branchIntentName(stream),
				Reason:    "one transform cannot be both resize and resample",
				Cause:     ErrUnsupportedBuild,
			}
		case transform.Resize != nil:
			if stream.Select.Type == av.MediaAudio {
				return branchTransformMediaError(stream, "resize", "video")
			}
		case transform.Resample != nil:
			if stream.Select.Type == av.MediaVideo {
				return branchTransformMediaError(stream, "resample", "audio")
			}
		default:
			return &BuildError{
				Code:      "transform_invalid",
				Operation: branchCompositionOperation,
				Node:      branchIntentName(stream),
				Reason:    "empty stream transform",
				Suggestions: []string{
					"call .Resize(width, height) on video branches",
					"call .Resample(sampleRate, channels) on audio branches",
				},
				Cause: ErrUnsupportedBuild,
			}
		}
	}
	return nil
}

func branchTransformMediaError(stream StreamIntent, transform string, media string) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    transform + " applies to " + media + " branches",
		Suggestions: []string{
			"use .Video(...).Resize(...) for video ladder branches",
			"use .Audio(...).Resample(...) for audio branches",
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

func branchIntentName(stream StreamIntent) string {
	return firstNonEmpty(stream.Name, string(stream.Select.Type), "stream")
}

func branchStreamName(stream streamBuild) string {
	return firstNonEmpty(stream.name, string(stream.selector.Type), "stream")
}

func endpointTargetNames(outputs []EndpointSpec) []string {
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
