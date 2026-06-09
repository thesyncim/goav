package goav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
)

const (
	Mono   = codec.Mono
	Stereo = codec.Stereo
)

type Intent struct {
	Name         string
	Inputs       []inputIntent
	Streams      []streamIntent
	Destinations []destinationIntent
	Policies     policyIntent
}

type inputIntent struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Codec    CodecSpec
	Realtime bool
}

type streamIntent struct {
	Name         string
	Select       StreamSelect
	From         TapRef
	Decode       bool
	DecodeCodec  CodecSpec
	Operations   []OperationSpec
	Taps         []tapIntent
	Encode       CodecSpec
	CodecChange  CodecChangePolicy
	Destinations []string
}

type OperationSpec struct {
	Kind      OperationKind
	Component string
	Stage     pipeline.Stage
	Shape     shape.Spec
	Transform TransformSpec
	Tap       tapIntent
	Decode    CodecSpec
	Encode    CodecSpec
	Shared    bool
}

type destinationIntent struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Format   av.FormatID
}

type policyIntent struct {
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

// Default builds a runtime with the standard codecs, formats, and filters already
// registered, then applies opts on top. Because the defaults are applied first
// and registration is last-wins, opts can both ADD new implementations and
// OVERRIDE a default in the same call — solid batteries you can still change:
//
//	rt := goav.Default()                                   // stock everything
//	rt := goav.Default(goav.WithDemuxer("flv", myFLV))     // defaults + a new format
//	rt := goav.Default(goav.WithEncoder(vp9Desc, myVP9))   // defaults, but my VP9 wins
//
// Use New(opts...) instead to start bare and register only what you list.
func Default(opts ...Option) Runtime {
	return New(append([]Option{WithDefaults()}, opts...)...)
}

// CodecOption configures a codec spec's encoder/decoder settings. The options
// themselves live in the codec package (codec.Bitrate, codec.RateControl, …) so
// the goav grammar stays small; this alias lets the constructors accept them.
type CodecOption = codec.Option

// CodecSpec is re-exported from the codec package, which now owns the type and
// the constructors (codec.VP9, codec.Opus, codec.Codec, codec.Copy, …). The
// alias keeps goav's build layer and external callers using goav.CodecSpec.
type CodecSpec = codec.CodecSpec

// codecSpecFromOptions builds a spec carrying only the Settings configured by
// decode options (decode does not set output caps).
func codecSpecFromOptions(options ...CodecOption) CodecSpec {
	var spec CodecSpec
	for i := range options {
		if options[i] != nil {
			options[i](&spec.Settings)
		}
	}
	return spec
}

func cloneCodecSpec(spec CodecSpec) CodecSpec {
	spec.Parameters.Attributes = cloneMetadata(spec.Parameters.Attributes)
	spec.Parameters.ExtraData = cloneBuffer(spec.Parameters.ExtraData)
	spec.Settings = cloneCodecSettings(spec.Settings)
	return spec
}

func cloneCodecSettings(settings codec.CodecSettings) codec.CodecSettings {
	// Config (Tier 2) and Control (Tier 3) are reference values owned by the
	// caller; copy the reference, not the target.
	return settings
}

func mergeCodecSettings(base codec.CodecSettings, override codec.CodecSettings) codec.CodecSettings {
	if override.Bitrate != 0 {
		base.Bitrate = override.Bitrate
	}
	if override.Framerate != (av.Duration{}) {
		base.Framerate = override.Framerate
	}
	if override.KeyframeInterval != 0 {
		base.KeyframeInterval = override.KeyframeInterval
	}
	if override.Profile != "" {
		base.Profile = override.Profile
	}
	if override.Level != "" {
		base.Level = override.Level
	}
	if override.ChannelsSet {
		base.Channels = override.Channels
		base.ChannelLayout = override.ChannelLayout
		base.ChannelsSet = true
	}
	if override.SampleRateSet {
		base.SampleRate = override.SampleRate
		base.SampleRateSet = true
	}
	if override.ClockRate != 0 {
		base.ClockRate = override.ClockRate
	}
	if override.Control != nil {
		base.Control = override.Control
	}
	return base
}

func mergeDecodeCodecSpec(base CodecSpec, override CodecSpec) CodecSpec {
	if override.ID != "" {
		base.ID = override.ID
	}
	if override.Type != "" {
		base.Type = override.Type
	}
	base.Parameters = mergeCodecParameters(base.Parameters, override.Parameters)
	base.Settings = mergeCodecSettings(base.Settings, override.Settings)
	return base
}

func codecSpecHasParameters(spec CodecSpec) bool {
	parameters := spec.Parameters
	return parameters.ID != "" ||
		parameters.Type != "" ||
		parameters.Profile != "" ||
		parameters.Level != "" ||
		parameters.ClockRate != 0 ||
		parameters.SampleRate != 0 ||
		parameters.Channels != 0 ||
		parameters.ChannelLayout != "" ||
		parameters.Width != 0 ||
		parameters.Height != 0 ||
		parameters.PixelFormat != "" ||
		parameters.SampleFormat != "" ||
		len(parameters.ExtraData.Bytes) != 0 ||
		len(parameters.Attributes) != 0
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
	source   *sourceInputSpec
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
	s.codec = cloneCodecSpec(codec)
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
	if err := s.validateCustomSource(); err != nil {
		return err
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

func (s InputSpec) validateCustomSource() error {
	if s.source == nil {
		return nil
	}
	node := firstNonEmpty(s.name, s.input.Name, "source")
	if s.source.fn == nil {
		return &BuildError{
			Code:      "source_callback_missing",
			Operation: "build input",
			Node:      node,
			Reason:    "custom source has no push callback",
			Suggestions: []string{
				"pass a non-nil callback to goav.Source(name, shape, fn)",
				"use goav.FileInput, goav.RTP, or goav.WebRTCTrack for built-in source adapters",
			},
			Cause: ErrNilSource,
		}
	}
	spec := normalizeCustomSourceShape(node, s.source.shape)
	if spec.Domain != shape.DomainPacket && spec.Domain != shape.DomainFrame && spec.Domain != shape.DomainEvent {
		return &BuildError{
			Code:      "source_shape_unsupported",
			Operation: "build input",
			Node:      node,
			Reason:    "custom recipe sources currently produce packet-domain, frame-domain, or event-domain media",
			Details: []string{
				"actual_shape=" + spec.String(),
			},
			Suggestions: []string{
				"declare the source with shape.Packet(media, codec, ...)",
				"declare raw generated media with shape.Frame(media, ...)",
				"declare diagnostic or lifecycle sources with shape.Event(...)",
				"use goav.Sink(...) after decode or transform when observing frame-domain media",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if spec.Domain != shape.DomainEvent && spec.MediaKind == "" {
		return &BuildError{
			Code:      "source_shape_invalid",
			Operation: "build input",
			Node:      node,
			Reason:    "custom source shape needs a media kind",
			Suggestions: []string{
				"use shape.Packet(av.MediaAudio, codec) or shape.Packet(av.MediaVideo, codec)",
				"add shape.Media(...) when constructing a custom shape",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return nil
}

func (s InputSpec) validatePlainInput() error {
	if s.rtp != nil {
		return nil
	}
	if s.source != nil {
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
			"use goav.Source(name, shape, fn) for application-pushed packets",
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
				"set RTP receive intent with .Codec(codec.Opus()), .Codec(codec.VP8()), .Codec(codec.VP9()), .Codec(codec.H264()), or .Codec(codec.AV1())",
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
				"omit .Codec(codec.Copy()) on RTP inputs",
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
				"call .Codec(codec.Opus()), .Codec(codec.VP8()), .Codec(codec.VP9()), .Codec(codec.H264()), or .Codec(codec.AV1()) on goav.RTP(reader)",
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
				"use a built-in receive codec: codec.Opus(), codec.VP8(), codec.VP9(), codec.H264(), or codec.AV1()",
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

func (s InputSpec) intent() inputIntent {
	return inputIntent{
		Name:     firstNonEmpty(s.name, s.input.Name),
		URI:      s.input.URI,
		Protocol: s.input.Protocol,
		MIMEType: s.input.MIMEType,
		Codec:    cloneCodecSpec(s.codec),
		Realtime: s.input.Realtime || s.rtp != nil || (s.source != nil && s.source.shape.Realtime),
	}
}

func (s InputSpec) inputName(fallback string) string {
	return firstNonEmpty(s.name, s.input.Name, s.input.URI, fallback)
}

// destinationSpec describes a concrete file, URI, writer, or sink destination.
type destinationSpec struct {
	id             uint64
	output         format.Output
	sink           pipeline.Sink
	custom         DestinationProvider
	format         av.FormatID
	resolvedFormat av.FormatID
	name           string
	err            error
}

func destinationHandle(spec destinationSpec) Destination {
	return Destination{spec: spec}
}

// File creates a writer-backed destination.
func File(name string, writer io.Writer, opts ...DestinationOption) Destination {
	spec := fileDestination(name, writer)
	for i := range opts {
		if opts[i] != nil {
			opts[i](&spec)
		}
	}
	return Destination{spec: spec}
}

func fileDestination(name string, writer io.Writer) destinationSpec {
	return destinationSpec{
		id: destinationSpecSeq.Add(1),
		output: format.Output{
			Name:     name,
			Protocol: av.ProtocolFile,
			Writer:   writer,
		},
		name: name,
	}
}

// URIOut creates a URI destination opened by a registered format adapter.
func URIOut(uri string, opts ...DestinationOption) Destination {
	spec := uriDestination(uri)
	for i := range opts {
		if opts[i] != nil {
			opts[i](&spec)
		}
	}
	return Destination{spec: spec}
}

func uriDestination(uri string) destinationSpec {
	return destinationSpec{
		id: destinationSpecSeq.Add(1),
		output: format.Output{
			Name: uri,
			URI:  uri,
		},
		name: uri,
	}
}

// Sink creates a media-message destination for decoded frames or packets.
func Sink(sink pipeline.Sink) Destination {
	return Destination{spec: sinkDestination(sink)}
}

func sinkDestination(sink pipeline.Sink) destinationSpec {
	name := ""
	if sink != nil {
		name = sink.Name()
	}
	if sink == nil {
		return destinationSpec{id: destinationSpecSeq.Add(1), err: ErrNilSink}
	}
	return destinationSpec{id: destinationSpecSeq.Add(1), sink: sink, name: name}
}

func Custom(name string, provider DestinationProvider, opts ...DestinationOption) Destination {
	spec := customDestination(name, provider)
	for i := range opts {
		if opts[i] != nil {
			opts[i](&spec)
		}
	}
	return Destination{spec: spec}
}

func customDestination(name string, provider DestinationProvider) destinationSpec {
	if provider == nil {
		return destinationSpec{id: destinationSpecSeq.Add(1), err: ErrNilWriter}
	}
	contract := provider.Contract()
	spec := destinationSpec{
		id:     destinationSpecSeq.Add(1),
		custom: provider,
		output: format.Output{
			Name:     name,
			URI:      name,
			Protocol: contract.Protocol,
			Realtime: contract.Realtime,
		},
		name: firstNonEmpty(name, provider.Name()),
	}
	if spec.output.Name == "" {
		spec.output.Name = spec.name
		spec.output.URI = spec.name
	}
	if len(contract.Formats) != 0 {
		spec.format = contract.Formats[0]
	}
	if len(contract.MIMETypes) != 0 {
		spec.output.MIMEType = contract.MIMETypes[0]
	}
	return spec
}

func Writer(name string, open WriterOpenFunc, opts ...DestinationOption) Destination {
	spec := destinationSpec{
		id:     destinationSpecSeq.Add(1),
		custom: writerDestination{name: name, open: open},
		output: format.Output{
			Name: name,
			URI:  name,
		},
		name: name,
	}
	for i := range opts {
		if opts[i] != nil {
			opts[i](&spec)
		}
	}
	return Destination{spec: spec}
}

func WriteCloser(name string, writer io.WriteCloser, opts ...DestinationOption) Destination {
	return Writer(name, func(context.Context, DestinationInfo) (io.WriteCloser, error) {
		if writer == nil {
			return nil, ErrNilWriter
		}
		return writer, nil
	}, opts...)
}

func Object(name string, open ObjectOpenFunc, opts ...DestinationOption) Destination {
	return Writer(name, func(ctx context.Context, info DestinationInfo) (io.WriteCloser, error) {
		if open == nil {
			return nil, ErrNilWriter
		}
		return open(ctx, info)
	}, opts...)
}

type writerDestination struct {
	name     string
	open     WriterOpenFunc
	contract DestinationContract
}

func (d writerDestination) Name() string {
	return d.name
}

func (d writerDestination) Contract() DestinationContract {
	contract := d.contract
	contract.ByteStream = true
	return contract
}

func (d writerDestination) Open(ctx context.Context, info DestinationInfo) (DestinationWriter, error) {
	if d.open == nil {
		return nil, ErrNilWriter
	}
	writer, err := d.open(ctx, info)
	if err != nil {
		return nil, err
	}
	if writer == nil {
		return nil, ErrNilWriter
	}
	return writer, nil
}

type nopDestinationWriter struct {
	io.Writer
}

func (w nopDestinationWriter) Close() error {
	return nil
}

func MIME(mimeType string) DestinationOption {
	return func(spec *destinationSpec) {
		if spec != nil {
			*spec = spec.withMIME(mimeType)
		}
	}
}

func Format(format av.FormatID) DestinationOption {
	return func(spec *destinationSpec) {
		if spec != nil {
			*spec = spec.withFormat(format)
		}
	}
}

func Metadata(metadata av.Metadata) DestinationOption {
	return func(spec *destinationSpec) {
		if spec != nil {
			spec.output.Metadata = cloneMetadata(metadata)
		}
	}
}

func (s destinationSpec) withName(name string) destinationSpec {
	s.name = name
	if s.sink == nil {
		s.output.Name = name
	}
	return s
}

func (s destinationSpec) withMIME(mimeType string) destinationSpec {
	s.output.MIMEType = mimeType
	return s
}

func (s destinationSpec) withFormat(format av.FormatID) destinationSpec {
	s.format = format
	return s
}

func (s destinationSpec) withResolvedFormat(format av.FormatID) destinationSpec {
	s.resolvedFormat = format
	return s
}

func (s destinationSpec) Name() string {
	return s.name
}

func (s destinationSpec) Contract() DestinationContract {
	contract := DestinationContract{
		ByteStream: s.sink == nil,
		Protocol:   s.output.Protocol,
		Realtime:   s.output.Realtime,
	}
	if s.format != "" {
		contract.Formats = append(contract.Formats, s.format)
	}
	if s.resolvedFormat != "" && s.resolvedFormat != s.format {
		contract.Formats = append(contract.Formats, s.resolvedFormat)
	}
	if s.output.MIMEType != "" {
		contract.MIMETypes = append(contract.MIMETypes, s.output.MIMEType)
	}
	if s.custom != nil {
		customContract := s.custom.Contract()
		if contract.Protocol == "" {
			contract.Protocol = customContract.Protocol
		}
		contract.Seekable = customContract.Seekable
		contract.Realtime = contract.Realtime || customContract.Realtime
		if len(contract.Formats) == 0 {
			contract.Formats = append(contract.Formats, customContract.Formats...)
		}
		if len(contract.MIMETypes) == 0 {
			contract.MIMETypes = append(contract.MIMETypes, customContract.MIMETypes...)
		}
	}
	return contract
}

func (s destinationSpec) Open(ctx context.Context, info DestinationInfo) (DestinationWriter, error) {
	if s.custom != nil {
		return s.custom.Open(ctx, info)
	}
	if s.output.Writer != nil {
		return nopDestinationWriter{Writer: s.output.Writer}, nil
	}
	return nil, destinationInvalidError("open destination", firstNonEmpty(info.Name, s.name, "destination"), "destination does not provide a writer")
}

func (s destinationSpec) validate(operation string, fallback string) error {
	node := s.label(fallback)
	if s.err != nil {
		return &BuildError{
			Code:      "output_invalid",
			Operation: operation,
			Node:      node,
			Reason:    s.err.Error(),
			Suggestions: []string{
				"pass a non-nil sink to goav.Sink(...)",
				"use goav.File(...) or goav.URIOut(...) for muxed output",
			},
			Cause: s.err,
		}
	}
	if s.sink != nil {
		return nil
	}
	if s.output.Name == "" && s.output.URI == "" && s.output.Protocol == "" && s.output.MIMEType == "" && s.output.Writer == nil && s.custom == nil && s.format == "" {
		return &BuildError{
			Code:      "output_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "empty destination",
			Suggestions: []string{
				"use goav.File(name, writer) for muxed output",
				"use goav.Sink(sink) for decoded frames or packets",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.Protocol == av.ProtocolFile && s.output.Writer == nil && s.custom == nil {
		return &BuildError{
			Code:      "output_writer_missing",
			Operation: operation,
			Node:      node,
			Reason:    "file output has no writer",
			Suggestions: []string{
				"pass a non-nil io.Writer to goav.File(name, writer)",
				"use goav.URIOut(uri) when the output is opened by an adapter",
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
				"give goav.File(name, writer) a name with a container extension",
				"pass goav.Format(...) to goav.File(...) when the writer has no filename",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	if s.output.URI == "" && s.output.Protocol != av.ProtocolFile && s.output.Writer == nil && s.custom == nil {
		return &BuildError{
			Code:      "output_destination_missing",
			Operation: operation,
			Node:      node,
			Reason:    "output has no URI, writer, or sink",
			Suggestions: []string{
				"use goav.File(name, writer) for writer-backed output",
				"use goav.URIOut(uri) for URI-backed output",
			},
			Cause: ErrUnsupportedBuild,
		}
	}
	return nil
}

func (s destinationSpec) label(fallback string) string {
	return firstNonEmpty(s.name, s.output.Name, s.output.URI, fallback)
}

func (s destinationSpec) intent() destinationIntent {
	return destinationIntent{
		Name:     s.label("output"),
		URI:      s.output.URI,
		Protocol: s.output.Protocol,
		MIMEType: s.output.MIMEType,
		Format:   s.format,
	}
}

func (s destinationSpec) intentWithName(name string) destinationIntent {
	intent := s.intent()
	intent.Name = firstNonEmpty(name, intent.Name)
	return intent
}

type Job struct {
	name               string
	runtime            Runtime
	inputs             []InputSpec
	outputs            []destinationSpec
	outputNames        []string
	stream             *jobStreamBuild
	branchStreams      []streamBuild
	branchDestinations []namedDestinationSpec
	join               *joinSpec
	err                error
}

type jobStreamBuild struct {
	name        string
	selector    av.StreamSelector
	operations  []OperationSpec
	codecChange CodecChangePolicy
	outputs     []destinationSpec
	outputNames []string
}

type chainStep struct {
	stage     pipeline.Stage
	shape     shape.Spec
	transform TransformSpec
	tap       string
	tapDomain shape.MediaDomain
}

func operationSpecForDecode(codec CodecSpec, component string) OperationSpec {
	return OperationSpec{Kind: OpDecode, Component: component, Decode: cloneCodecSpec(codec)}
}

func operationSpecForCopy(codec CodecSpec) OperationSpec {
	return OperationSpec{Kind: OpCopy, Component: "packet-copy", Encode: cloneCodecSpec(codec)}
}

func operationSpecForEncode(codec CodecSpec) OperationSpec {
	if codec.Copy {
		return operationSpecForCopy(codec)
	}
	return OperationSpec{Kind: OpEncode, Component: string(codec.ID), Encode: cloneCodecSpec(codec)}
}

func operationSpecForStage(stage pipeline.Stage) OperationSpec {
	name := ""
	if stage != nil {
		name = stage.Name()
	}
	return OperationSpec{Kind: OpStage, Component: name, Stage: stage}
}

func operationSpecForShape(shape shape.Spec) OperationSpec {
	return OperationSpec{Kind: OpShape, Component: "shape", Shape: shape}
}

func operationSpecForTransform(transform TransformSpec) OperationSpec {
	return OperationSpec{
		Kind:      OpTransform,
		Component: transformFactoryName(transform),
		Transform: cloneTransformSpec(transform),
	}
}

func operationSpecForTap(tap TapRef, media av.MediaType, after OperationKind) OperationSpec {
	domain := tap.domain
	if domain == "" {
		domain = tapDomainForAfter(after)
	}
	intent := tapIntent{Name: tap.name, MediaKind: media, Domain: domain, After: after}
	return OperationSpec{Kind: OpTap, Component: tap.name, Tap: intent}
}

// tapDomainForAfter infers a domain-less tap's media domain from the operation it
// follows: packets after select/copy/encode, frames otherwise.
func tapDomainForAfter(after OperationKind) shape.MediaDomain {
	switch after {
	case OpSelect, OpCopy, OpEncode:
		return shape.DomainPacket
	default:
		return shape.DomainFrame
	}
}

func operationSpecAfter(operations []OperationSpec, fallback OperationKind) OperationKind {
	after := fallback
	for i := range operations {
		switch operations[i].Kind {
		case OpTap:
			continue
		default:
			after = operations[i].Kind
		}
	}
	return after
}

func operationSpecsContainKind(operations []OperationSpec, kind OperationKind) bool {
	for i := range operations {
		if operations[i].Kind == kind {
			return true
		}
	}
	return false
}

func operationSpecsContainChainStep(operations []OperationSpec) bool {
	for i := range operations {
		switch operations[i].Kind {
		case OpStage, OpShape, OpTransform:
			return true
		case OpTap:
			if operations[i].Tap.Domain != shape.DomainPacket {
				return true
			}
		}
	}
	return false
}

func jobStreamHasDecodeOperation(stream *jobStreamBuild) bool {
	if stream == nil {
		return false
	}
	return operationSpecsContainKind(stream.operations, OpDecode)
}

func ensureJobStreamDecodeOperation(stream *jobStreamBuild) {
	if stream == nil {
		return
	}
	if jobStreamHasDecodeOperation(stream) {
		return
	}
	// Reached only when no decode op exists yet (so no codec options were given);
	// an explicit Decode(opts) always appends its own OpDecode.
	operation := operationSpecForDecode(CodecSpec{}, string(stream.selector.Codec))
	stream.operations = append([]OperationSpec{operation}, stream.operations...)
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

func (j *Job) To(destinations ...Destination) *Job {
	if len(j.branchStreams) != 0 || (j.join != nil && len(j.join.branches) != 0) {
		j.setErr(branchOutputScopeError("branches"))
		return j
	}
	for i := range destinations {
		destination := destinations[i]
		binding, err := destinationBindingFromDestination(destination)
		if err != nil {
			j.setErr(jobDestinationInvalidError("job", err.Error()))
			return j
		}
		output, name, err := destinationFromBinding("build job", "job", binding, i)
		if err != nil {
			j.setErr(err)
			return j
		}
		j.outputs = append(j.outputs, output)
		j.outputNames = append(j.outputNames, name)
	}
	return j
}

func (j *Job) addBranchDestinations(destinations ...destinationRef) error {
	seen := make(map[string]string, len(j.branchDestinations)+len(destinations))
	for i := range j.branchDestinations {
		seen[j.branchDestinations[i].name] = destinationIdentity(j.branchDestinations[i])
	}
	for i := range destinations {
		destination := cloneDestinationRef(destinations[i])
		if destination.err != nil {
			return destination.err
		}
		if destination.name == "" {
			return destinationNameMissingError(destination.dest)
		}
		destination.dest = destination.dest.withName(firstNonEmpty(destination.dest.name, destination.name))
		named := namedDestinationSpec{name: destination.name, output: destination.dest}
		identity := destinationIdentity(named)
		if existing, ok := seen[named.name]; ok {
			if existing != identity {
				return branchDestinationDuplicateError(named.name)
			}
			continue
		}
		seen[named.name] = identity
		j.branchDestinations = append(j.branchDestinations, named)
	}
	return nil
}

func (j *Job) And(inputs ...InputSpec) *Job {
	j.inputs = append(j.inputs, inputs...)
	return j
}

func (j *Job) Audio(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("audio", av.MediaAudio, options...)
}

func (j *Job) Video(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("video", av.MediaVideo, options...)
}

func (j *Job) Stream(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("stream", "", options...)
}

func (j *Job) streamBuilder(name string, media av.MediaType, options ...streamOption) *jobStreamBuilder {
	stream := &jobStreamBuild{
		name:     name,
		selector: newStreamSelector(media, options...),
	}
	if j.stream != nil {
		if len(j.branchStreams) != 0 {
			j.stream = stream
			return &jobStreamBuilder{job: j, stream: stream}
		}
		j.err = duplicateJobStreamError(j.stream, stream)
		return &jobStreamBuilder{job: j, stream: stream}
	}
	j.stream = stream
	return &jobStreamBuilder{job: j, stream: stream}
}

func (j *Job) Plan() Intent {
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
		for i := range j.branchDestinations {
			intent.Destinations = append(intent.Destinations, j.branchDestinations[i].output.intentWithName(j.branchDestinations[i].name))
		}
		return intent
	} else if j.stream != nil {
		intent.Streams = append(intent.Streams, jobStreamIntent(j.stream))
		for i := range j.outputs {
			name := ""
			if i < len(j.outputNames) {
				name = j.outputNames[i]
			}
			intent.Destinations = append(intent.Destinations, j.outputs[i].intentWithName(name))
		}
		for i := range j.stream.outputs {
			name := ""
			if i < len(j.stream.outputNames) {
				name = j.stream.outputNames[i]
			}
			intent.Destinations = append(intent.Destinations, j.stream.outputs[i].intentWithName(name))
		}
		return intent
	}
	outputs := j.allOutputs()
	outputNames := j.allOutputNames()
	for i := range outputs {
		name := ""
		if i < len(outputNames) {
			name = outputNames[i]
		}
		intent.Destinations = append(intent.Destinations, outputs[i].intentWithName(name))
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
	if j.join != nil {
		return j.buildJoin(ctx)
	}
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
		if inputs[i].rtp != nil || inputs[i].source != nil {
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

func validateJobOutputBindings(operation string, stream streamIntent, outputs []destinationSpec, destinationNames []string) error {
	destinations := jobOutputDestinationSet(outputs, destinationNames)
	for _, destinationName := range stream.Destinations {
		if _, ok := destinations[destinationName]; ok {
			continue
		}
		return jobDestinationReferenceMissingError(operation, stream, destinationName)
	}
	return nil
}

func jobOutputDestinationSet(outputs []destinationSpec, destinationNames []string) map[string]struct{} {
	destinations := make(map[string]struct{}, len(outputs))
	for i := range outputs {
		destinations[jobOutputDestinationName(outputs, destinationNames, i)] = struct{}{}
	}
	return destinations
}

func (j *Job) allOutputs() []destinationSpec {
	if len(j.branchDestinations) != 0 {
		outputs := make([]destinationSpec, 0, len(j.branchDestinations))
		for i := range j.branchDestinations {
			outputs = append(outputs, j.branchDestinations[i].output)
		}
		return outputs
	}
	return jobAllOutputs(j.outputs, jobStreamOutputs(j.stream))
}

func (j *Job) allOutputNames() []string {
	if len(j.branchDestinations) != 0 {
		names := make([]string, 0, len(j.branchDestinations))
		for i := range j.branchDestinations {
			names = append(names, j.branchDestinations[i].name)
		}
		return names
	}
	return jobAllOutputNames(j.outputNames, jobStreamOutputNames(j.stream))
}

func jobAllOutputs(outputs []destinationSpec, streamOutputs []destinationSpec) []destinationSpec {
	if len(streamOutputs) == 0 {
		return append([]destinationSpec(nil), outputs...)
	}
	all := make([]destinationSpec, 0, len(outputs)+len(streamOutputs))
	all = append(all, outputs...)
	all = append(all, streamOutputs...)
	return all
}

func jobAllOutputNames(outputNames []string, streamOutputNames []string) []string {
	if len(streamOutputNames) == 0 {
		return append([]string(nil), outputNames...)
	}
	all := make([]string, 0, len(outputNames)+len(streamOutputNames))
	all = append(all, outputNames...)
	all = append(all, streamOutputNames...)
	return all
}

func jobIntentStream(intent Intent) (streamIntent, bool) {
	if len(intent.Streams) == 0 {
		return streamIntent{}, false
	}
	return intent.Streams[0], true
}

func jobStreamIntent(stream *jobStreamBuild) streamIntent {
	if stream == nil {
		return streamIntent{}
	}
	operations := jobOperationSpecs(stream)
	return streamIntent{
		Name:         stream.name,
		Select:       streamSelectFromAV(stream.selector),
		Decode:       chainHasDecode(operations),
		DecodeCodec:  cloneCodecSpec(chainDecodeCodec(operations)),
		Operations:   operations,
		Taps:         operationSpecTaps(operations, stream.selector.Type),
		Encode:       cloneCodecSpec(chainEncodeSpec(operations)),
		CodecChange:  stream.codecChange,
		Destinations: destinationNamesWithOverrides(stream.outputs, stream.outputNames),
	}
}

func branchStreamIntent(stream streamBuild) streamIntent {
	operations := streamBuildOperationSpecs(stream)
	return streamIntent{
		Name:         stream.name,
		Select:       streamSelectFromAV(stream.selector),
		From:         stream.from,
		Decode:       stream.decode,
		DecodeCodec:  cloneCodecSpec(stream.decodeCodec),
		Operations:   operations,
		Taps:         operationSpecTaps(operations, stream.selector.Type),
		Encode:       cloneCodecSpec(chainEncodeSpec(operations)),
		Destinations: append([]string(nil), stream.destinationNames...),
	}
}

func jobOperationSpecs(stream *jobStreamBuild) []OperationSpec {
	if stream == nil {
		return nil
	}
	// The operation list is authoritative: every builder method (Decode, Copy,
	// Encode, transforms, taps, and the implicit decode-for-sink in To) appends
	// its operation, so there is nothing to reconstruct from decode/encode flags.
	return cloneOperationSpecs(stream.operations)
}

func streamBuildOperationSpecs(stream streamBuild) []OperationSpec {
	if len(stream.sharedOps) != 0 || len(stream.privateOps) != 0 {
		return streamBuildSplitOperationSpecs(stream)
	}
	if len(stream.operations) != 0 {
		return cloneOperationSpecs(stream.operations)
	}
	operations := make([]OperationSpec, 0, 2)
	if stream.decode {
		operations = append(operations, OperationSpec{
			Kind:      OpDecode,
			Component: string(stream.selector.Codec),
			Decode:    cloneCodecSpec(stream.decodeCodec),
			Shared:    stream.from.Domain() == shape.DomainFrame,
		})
	}
	// The encode op is carried by stream.operations (or sharedOps/privateOps in
	// the split path) — plannedBranchPrivateOperationSpecs injects the copy for
	// the parentPacket passthrough — so there is no encode to reconstruct here.
	return operations
}

func streamBuildSplitOperationSpecs(stream streamBuild) []OperationSpec {
	operations := make([]OperationSpec, 0, len(stream.sharedOps)+len(stream.privateOps)+2)
	operations = append(operations, cloneOperationSpecs(stream.sharedOps)...)
	operations = append(operations, cloneOperationSpecs(stream.privateOps)...)
	if stream.decode && !operationSpecsContainKind(operations, OpDecode) {
		operation := operationSpecForDecode(stream.decodeCodec, string(stream.selector.Codec))
		operation.Shared = stream.from.Domain() == shape.DomainFrame && len(stream.sharedOps) != 0
		operations = append([]OperationSpec{operation}, operations...)
	}
	// The encode op (OpEncode/OpCopy) is always already in sharedOps/privateOps,
	// so there is nothing to re-add.
	return operations
}

func plannedBranchSharedOperationSpecs(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) []OperationSpec {
	if stream == nil || parentPacket {
		return nil
	}
	parentOperations := jobOperationSpecs(stream)
	if len(parentOperations) == 0 {
		return nil
	}
	if spec.source.tap == "" {
		return sharedOperationSpecs(parentOperations)
	}
	if prefix, ok := operationSpecsThroughTap(parentOperations, spec.source.tap); ok {
		return sharedOperationSpecs(prefix)
	}
	if chainHasDecode(parentOperations) && spec.source.tap == defaultDecodedTapName(stream.selector.Type) {
		if prefix, ok := operationSpecsThroughKind(parentOperations, OpDecode); ok {
			return sharedOperationSpecs(prefix)
		}
		return sharedOperationSpecs([]OperationSpec{operationSpecForDecode(chainDecodeCodec(parentOperations), string(stream.selector.Codec))})
	}
	return nil
}

func plannedBranchPrivateOperationSpecs(stream *jobStreamBuild, spec BranchSpec, parentPacket bool) []OperationSpec {
	if !parentPacket || chainHasDecode(spec.operations) || codecIntentSet(chainEncodeSpec(spec.operations)) {
		return cloneOperationSpecs(spec.operations)
	}
	if stream == nil {
		return cloneOperationSpecs(spec.operations)
	}
	if len(spec.operations) != 0 {
		if operationSpecsContainKind(spec.operations, OpCopy) {
			return cloneOperationSpecs(spec.operations)
		}
		if prefix, ok := operationSpecsThroughKind(stream.operations, OpCopy); ok {
			out := cloneOperationSpecs(prefix)
			out = append(out, cloneOperationSpecs(spec.operations)...)
			return out
		}
		out := []OperationSpec{operationSpecForCopy(codec.Copy())}
		out = append(out, cloneOperationSpecs(spec.operations)...)
		return out
	}
	if spec.source.tap != "" {
		if prefix, ok := operationSpecsThroughTap(stream.operations, spec.source.tap); ok {
			return prefix
		}
	}
	return cloneOperationSpecs(stream.operations)
}

func sharedOperationSpecs(operations []OperationSpec) []OperationSpec {
	if len(operations) == 0 {
		return nil
	}
	out := cloneOperationSpecs(operations)
	for i := range out {
		out[i].Shared = true
	}
	return out
}

func operationSpecsThroughKind(operations []OperationSpec, kind OperationKind) ([]OperationSpec, bool) {
	for i := range operations {
		if operations[i].Kind == kind {
			return cloneOperationSpecs(operations[:i+1]), true
		}
	}
	return nil, false
}

func operationSpecsThroughTap(operations []OperationSpec, tap string) ([]OperationSpec, bool) {
	if tap == "" {
		return nil, false
	}
	for i := range operations {
		operation := operations[i]
		if operation.Kind != OpTap {
			continue
		}
		if operation.Component == tap || operation.Tap.Name == tap {
			return cloneOperationSpecs(operations[:i+1]), true
		}
	}
	return nil, false
}

// operationSpecTaps derives a stream's exported taps from its OpTap operations —
// the single source of truth — instead of the parallel chain-step-tap and
// post-encode-tap projections. Each OpTap already carries the resolved tap
// (name/domain/media/after) from operationSpecForTap.
func operationSpecTaps(operations []OperationSpec, media av.MediaType) []tapIntent {
	taps := make([]tapIntent, 0, len(operations))
	for i := range operations {
		if operations[i].Kind != OpTap {
			continue
		}
		tap := operations[i].Tap
		if tap.MediaKind == "" {
			tap.MediaKind = media // backfill from the resolved stream selector
		}
		taps = append(taps, tap)
	}
	return taps
}

func initialStepAfter(decode bool) OperationKind {
	if decode {
		return OpDecode
	}
	return ""
}

func jobStreamOutputs(stream *jobStreamBuild) []destinationSpec {
	if stream == nil || len(stream.outputs) == 0 {
		return nil
	}
	return append([]destinationSpec(nil), stream.outputs...)
}

func jobStreamOutputNames(stream *jobStreamBuild) []string {
	if stream == nil || len(stream.outputNames) == 0 {
		return nil
	}
	return append([]string(nil), stream.outputNames...)
}

func streamStageMissingError(stream streamIntent) error {
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

func validateJobStreamOutputKinds(operation string, stream streamIntent, outputs []destinationSpec) error {
	if outputsContainSinkDestination(outputs) && outputsContainMuxDestination(outputs) && !codecIntentSet(stream.Encode) {
		return mixedStreamOutputError(operation, stream)
	}
	if stream.Encode.ID == "" && !stream.Encode.Copy && outputsContainMuxDestination(outputs) {
		return streamEncodeMissingError(operation, stream)
	}
	return nil
}

func mixedStreamOutputError(operation string, stream streamIntent) error {
	return &BuildError{
		Code:      "output_kind_mixed",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "stream recipes cannot mix sinks and muxed outputs",
		Suggestions: []string{
			"use .To(goav.Sink(...)) for decoded frames",
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(goav.File(...)) for encoded output",
			"use .Branches(...) when one stream needs separate decoded and encoded branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamEncodeMissingError(operation string, stream streamIntent) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    "decoded frames cannot be written to a muxed output without an encoder",
		Details: []string{
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(stream.Select.Type)).String(),
			"actual_shape=" + shape.Frame(stream.Select.Type).String(),
		},
		Suggestions: []string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(goav.File(...))",
			"send decoded frames to goav.Sink(...)",
			"use .Copy().To(output) if you want to copy packets without decoding",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func recipeRuntimeUnsupportedError(operation string) error {
	return &BuildError{
		Code:      "runtime_unsupported",
		Operation: operation,
		Reason:    "recipe compilation requires a goav runtime",
		Suggestions: []string{
			"use goav.Default() for the standard recipe runtime",
			"use goav.New(...) when customizing adapters",
			"use goav.Expert(runtime).Graph() for explicit graph wiring with a goav runtime",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func jobStreamIntentName(stream streamIntent) string {
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

func streamIntentHasOperation(stream streamIntent) bool {
	return stream.Decode || len(stream.Operations) != 0 || stream.Encode.ID != "" || stream.Encode.Auto || stream.Encode.Copy
}

func jobStreamChainSteps(stream *jobStreamBuild) []chainStep {
	if stream == nil {
		return nil
	}
	return chainStepsFromChainOperations(stream.operations)
}

func cloneChainSteps(steps []chainStep) []chainStep {
	if len(steps) == 0 {
		return nil
	}
	out := make([]chainStep, 0, len(steps))
	for i := range steps {
		step := steps[i]
		step.transform = cloneTransformSpec(step.transform)
		out = append(out, step)
	}
	return out
}

func outputsContainMuxDestination(outputs []destinationSpec) bool {
	for i := range outputs {
		if outputs[i].sink == nil {
			return true
		}
	}
	return false
}

func outputsContainSinkDestination(outputs []destinationSpec) bool {
	for i := range outputs {
		if outputs[i].sink != nil {
			return true
		}
	}
	return false
}

func validateDestinationSpecs(operation string, outputs []destinationSpec, destinationNames ...string) error {
	seen := make(map[string]bool, len(outputs))
	for i := range outputs {
		fallback := fmt.Sprintf("output-%d", i)
		if err := outputs[i].validate(operation, fallback); err != nil {
			return err
		}
		name := jobOutputDestinationName(outputs, destinationNames, i)
		destinationNamed := i < len(destinationNames) && destinationNames[i] != ""
		if previousTargetNamed, ok := seen[name]; ok {
			if destinationNamed || previousTargetNamed {
				return duplicateDestinationHandleError(operation, name)
			}
			return duplicateOutputError(operation, name)
		}
		seen[name] = destinationNamed
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
		if inputs[i].source != nil {
			probes[i] = customSourceProbeResult(inputs[i])
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

func validateOutputFormatAdapters(ctx context.Context, rt Runtime, outputs []destinationSpec, destinationNames ...string) ([]destinationSpec, error) {
	resolved := append([]destinationSpec(nil), outputs...)
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
				return nil, destinationFormatProbeError(destinationNodeName(resolved[i].output, i, destinationNames), resolved[i].output, err)
			}
			formatID = result.Format
			resolved[i] = resolved[i].withResolvedFormat(formatID)
		}
		if _, err := standard.formats.MuxerFactory(formatID); err != nil {
			return nil, destinationMuxerMissingError(destinationNodeName(resolved[i].output, i, destinationNames), resolved[i].output, formatID, err)
		}
	}
	return resolved, nil
}

func destinationNodeName(output format.Output, index int, destinationNames []string) string {
	if index >= 0 && index < len(destinationNames) && destinationNames[index] != "" {
		return destinationNames[index]
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
		request, ok := liveDecodeAdapterRequest(intent.Inputs, stream)
		if !ok || request.Codec == "" {
			continue
		}
		if _, err := standard.codecs.DecoderFactory(request.Codec); err != nil {
			return recipeDecodeAdapterError(operation, stream, request.Codec, standard.codecs, err)
		}
		if err := validateDecodeAdapterDescriptors(operation, stream, standard.codecs, request); err != nil {
			return err
		}
	}
	return nil
}

func validateKnownRecipeDecodeAdapters(operation string, rt Runtime, probes []format.ProbeResult, streams []streamIntent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		if !streamNeedsDecode(stream) {
			continue
		}
		selected, ok := knownProbeDecodeStream(probes, stream)
		if !ok || selected.Codec.ID == "" {
			continue
		}
		if _, err := standard.codecs.DecoderFactory(selected.Codec.ID); err != nil {
			return recipeDecodeAdapterError(operation, stream, selected.Codec.ID, standard.codecs, err)
		}
		request := decodeAdapterRequestFromStream(selected, stream)
		if err := validateDecodeAdapterDescriptors(operation, stream, standard.codecs, request); err != nil {
			return err
		}
	}
	return nil
}

func validateLiveStreamSelection(inputs []inputIntent, stream streamIntent) error {
	streams := liveIntentStreams(inputs)
	if len(streams) == 0 {
		return nil
	}
	_, err := selectDecodeStream(streams, streamIntentSelector(stream))
	return err
}

func liveIntentStreams(inputs []inputIntent) []av.Stream {
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

func knownProbeDecodeStream(probes []format.ProbeResult, stream streamIntent) (av.Stream, bool) {
	candidates := make([]av.Stream, 0, len(probes))
	selector := streamIntentSelector(stream)
	for i := range probes {
		if len(probes[i].Streams) == 0 {
			continue
		}
		selected, err := selectDecodeStream(probes[i].Streams, selector)
		if err != nil || selected.Codec.ID == "" {
			continue
		}
		candidates = append(candidates, selected)
	}
	if len(candidates) != 1 {
		return av.Stream{}, false
	}
	return candidates[0], true
}

func knownProbeDecodeCodec(probes []format.ProbeResult, stream streamIntent) (av.CodecID, bool) {
	selected, ok := knownProbeDecodeStream(probes, stream)
	if !ok {
		return "", false
	}
	return selected.Codec.ID, true
}

func streamNeedsDecode(stream streamIntent) bool {
	return stream.Decode || len(streamIntentTransformSpecs(stream)) != 0 || stream.Encode.ID != ""
}

func liveDecodeAdapterRequest(inputs []inputIntent, stream streamIntent) (codecAdapterRequest, bool) {
	selected, ok := liveDecodeStream(inputs, stream)
	if !ok {
		return codecAdapterRequest{}, false
	}
	return decodeAdapterRequestFromStream(selected, stream), true
}

func liveDecodeStream(inputs []inputIntent, stream streamIntent) (av.Stream, bool) {
	streams := liveIntentStreams(inputs)
	if len(streams) == 0 {
		return av.Stream{}, false
	}
	selected, err := selectDecodeStream(streams, streamIntentSelector(stream))
	if err != nil || selected.Codec.ID == "" {
		return av.Stream{}, false
	}
	return selected, true
}

func liveDecodeCodec(inputs []inputIntent, stream streamIntent) (av.CodecID, bool) {
	selected, ok := liveDecodeStream(inputs, stream)
	if !ok {
		return "", false
	}
	return selected.Codec.ID, true
}

func recipeDecodeAdapterError(operation string, stream streamIntent, codecID av.CodecID, registry *codec.SimpleRegistry, cause error) error {
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

func validateDecodeAdapterDescriptors(operation string, stream streamIntent, registry *codec.SimpleRegistry, request codecAdapterRequest) error {
	if registry == nil || request.Codec == "" {
		return nil
	}
	descriptors, err := registry.Find(request.Codec, codec.ModeDecode)
	if err != nil {
		return nil
	}
	for i := range descriptors {
		if codecDescriptorSupports(descriptors[i], request) {
			return nil
		}
	}
	return decodeAdapterIncompatibleError(operation, stream, request, descriptors)
}

func decodeAdapterRequestFromStream(stream av.Stream, intent streamIntent) codecAdapterRequest {
	return codecAdapterRequest{
		Codec:        stream.Codec.ID,
		Media:        firstNonEmptyMedia(stream.Codec.Type, stream.Type, intent.Select.Type, codecMedia(stream.Codec.ID)),
		SampleFormat: stream.Codec.SampleFormat,
		PixelFormat:  stream.Codec.PixelFormat,
	}
}

func decodeAdapterIncompatibleError(operation string, stream streamIntent, request codecAdapterRequest, descriptors []codec.Descriptor) error {
	field, requested, supported := codecAdapterIncompatibilityField(request, descriptors)
	label := strings.ReplaceAll(field, "_", " ")
	details := []string{
		"codec=" + string(request.Codec),
		"field=" + field,
		"requested=" + requested,
		"supported=" + supported,
	}
	if request.Media != "" {
		details = append(details, "requested_media="+string(request.Media))
	}
	if media := descriptorSupportedMedia(descriptors); len(media) != 0 {
		details = append(details, "supported_media="+joinMediaTypes(media))
	}
	if sampleFormats := descriptorSupportedSampleFormats(descriptors); len(sampleFormats) != 0 {
		details = append(details, "supported_sample_formats="+strings.Join(sampleFormats, ","))
	}
	if pixelFormats := descriptorSupportedPixelFormats(descriptors); len(pixelFormats) != 0 {
		details = append(details, "supported_pixel_formats="+strings.Join(pixelFormats, ","))
	}
	return &BuildError{
		Code:      "decode_adapter_incompatible",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    string(request.Codec) + " decoder adapter does not support the requested " + label,
		Details:   details,
		Suggestions: []string{
			"choose a decoder adapter that supports this " + label,
			"fix the input stream metadata if it describes the wrong media or frame format",
			"fix the codec descriptor if the implementation already supports this config",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateRecipeEncodeAdapters(operation string, rt Runtime, streams []streamIntent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		codecID := stream.Encode.ID
		if codecID == "" {
			continue
		}
		if _, err := standard.codecs.EncoderFactory(codecID); err != nil {
			return recipeEncodeAdapterError(operation, stream, standard.codecs, err)
		}
		request := encodeAdapterRequestFromStreamIntent(stream)
		if err := validateEncodeAdapterDescriptors(operation, stream, standard.codecs, request); err != nil {
			return err
		}
	}
	return nil
}

type codecAdapterRequest struct {
	Codec        av.CodecID
	Media        av.MediaType
	SampleFormat string
	PixelFormat  string
}

func encodeAdapterRequestFromStreamIntent(stream streamIntent) codecAdapterRequest {
	return codecAdapterRequest{
		Codec:        stream.Encode.ID,
		Media:        firstNonEmptyMedia(stream.Encode.Type, stream.Encode.Parameters.Type, stream.Select.Type, codecMedia(stream.Encode.ID)),
		SampleFormat: firstNonEmpty(stream.Encode.Parameters.SampleFormat, streamIntentSampleFormat(stream)),
		PixelFormat:  firstNonEmpty(stream.Encode.Parameters.PixelFormat, streamIntentPixelFormat(stream)),
	}
}

func encodeAdapterRequestFromPreparedStream(spec CodecSpec, stream av.Stream) codecAdapterRequest {
	return codecAdapterRequest{
		Codec:        spec.ID,
		Media:        firstNonEmptyMedia(spec.Type, spec.Parameters.Type, stream.Type, stream.Codec.Type, codecMedia(spec.ID)),
		SampleFormat: firstNonEmpty(spec.Parameters.SampleFormat, stream.Codec.SampleFormat),
		PixelFormat:  firstNonEmpty(spec.Parameters.PixelFormat, stream.Codec.PixelFormat),
	}
}

func streamIntentSampleFormat(stream streamIntent) string {
	transforms := streamIntentTransformSpecs(stream)
	for i := len(transforms) - 1; i >= 0; i-- {
		if transforms[i].Resample != nil && transforms[i].Resample.SampleFormat != "" {
			return transforms[i].Resample.SampleFormat
		}
	}
	return ""
}

func streamIntentPixelFormat(stream streamIntent) string {
	transforms := streamIntentTransformSpecs(stream)
	for i := len(transforms) - 1; i >= 0; i-- {
		if transforms[i].Resize != nil && transforms[i].Resize.PixelFormat != "" {
			return transforms[i].Resize.PixelFormat
		}
	}
	return ""
}

func validateEncodeAdapterDescriptors(operation string, stream streamIntent, registry *codec.SimpleRegistry, request codecAdapterRequest) error {
	if registry == nil || request.Codec == "" {
		return nil
	}
	descriptors, err := registry.Find(request.Codec, codec.ModeEncode)
	if err != nil {
		return nil
	}
	for i := range descriptors {
		if codecDescriptorSupports(descriptors[i], request) {
			return nil
		}
	}
	return encodeAdapterIncompatibleError(operation, stream, request, descriptors)
}

func codecDescriptorSupports(desc codec.Descriptor, request codecAdapterRequest) bool {
	if request.Media != "" && desc.Type != "" && desc.Type != request.Media {
		return false
	}
	if request.SampleFormat != "" && len(desc.Capabilities.SampleFormats) != 0 && !stringAllowed(desc.Capabilities.SampleFormats, request.SampleFormat) {
		return false
	}
	if request.PixelFormat != "" && len(desc.Capabilities.PixelFormats) != 0 && !stringAllowed(desc.Capabilities.PixelFormats, request.PixelFormat) {
		return false
	}
	return true
}

func encodeAdapterIncompatibleError(operation string, stream streamIntent, request codecAdapterRequest, descriptors []codec.Descriptor) error {
	field, requested, supported := codecAdapterIncompatibilityField(request, descriptors)
	label := strings.ReplaceAll(field, "_", " ")
	details := []string{
		"codec=" + string(request.Codec),
		"field=" + field,
		"requested=" + requested,
		"supported=" + supported,
	}
	if request.Media != "" {
		details = append(details, "requested_media="+string(request.Media))
	}
	if media := descriptorSupportedMedia(descriptors); len(media) != 0 {
		details = append(details, "supported_media="+joinMediaTypes(media))
	}
	if sampleFormats := descriptorSupportedSampleFormats(descriptors); len(sampleFormats) != 0 {
		details = append(details, "supported_sample_formats="+strings.Join(sampleFormats, ","))
	}
	if pixelFormats := descriptorSupportedPixelFormats(descriptors); len(pixelFormats) != 0 {
		details = append(details, "supported_pixel_formats="+strings.Join(pixelFormats, ","))
	}
	return &BuildError{
		Code:      "encode_adapter_incompatible",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    string(request.Codec) + " encoder adapter does not support the requested " + label,
		Details:   details,
		Suggestions: []string{
			"choose an encoder adapter that supports this " + label,
			"change the operation spec chain so the encoder receives one of the supported formats",
			"fix the codec descriptor if the implementation already supports this config",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func codecAdapterIncompatibilityField(request codecAdapterRequest, descriptors []codec.Descriptor) (string, string, string) {
	mediaCompatible := codecDescriptorsMediaCompatible(descriptors, request.Media)
	if request.Media != "" && !mediaCompatible {
		return "media", string(request.Media), joinMediaTypes(descriptorSupportedMedia(descriptors))
	}
	mediaDescriptors := descriptorsMatchingMedia(descriptors, request.Media)
	if request.SampleFormat != "" && !codecDescriptorsSampleFormatCompatible(mediaDescriptors, request.SampleFormat) {
		return "sample_format", request.SampleFormat, strings.Join(descriptorSupportedSampleFormats(mediaDescriptors), ",")
	}
	if request.PixelFormat != "" && !codecDescriptorsPixelFormatCompatible(mediaDescriptors, request.PixelFormat) {
		return "pixel_format", request.PixelFormat, strings.Join(descriptorSupportedPixelFormats(mediaDescriptors), ",")
	}
	return "codec", string(request.Codec), string(request.Codec)
}

func codecDescriptorsMediaCompatible(descriptors []codec.Descriptor, media av.MediaType) bool {
	if media == "" {
		return true
	}
	for i := range descriptors {
		if descriptors[i].Type == "" || descriptors[i].Type == media {
			return true
		}
	}
	return false
}

func descriptorsMatchingMedia(descriptors []codec.Descriptor, media av.MediaType) []codec.Descriptor {
	if media == "" {
		return descriptors
	}
	out := make([]codec.Descriptor, 0, len(descriptors))
	for i := range descriptors {
		if descriptors[i].Type == "" || descriptors[i].Type == media {
			out = append(out, descriptors[i])
		}
	}
	return out
}

func codecDescriptorsSampleFormatCompatible(descriptors []codec.Descriptor, sampleFormat string) bool {
	if sampleFormat == "" {
		return true
	}
	for i := range descriptors {
		formats := descriptors[i].Capabilities.SampleFormats
		if len(formats) == 0 || stringAllowed(formats, sampleFormat) {
			return true
		}
	}
	return false
}

func codecDescriptorsPixelFormatCompatible(descriptors []codec.Descriptor, pixelFormat string) bool {
	if pixelFormat == "" {
		return true
	}
	for i := range descriptors {
		formats := descriptors[i].Capabilities.PixelFormats
		if len(formats) == 0 || stringAllowed(formats, pixelFormat) {
			return true
		}
	}
	return false
}

func descriptorSupportedMedia(descriptors []codec.Descriptor) []av.MediaType {
	out := make([]av.MediaType, 0, len(descriptors))
	for i := range descriptors {
		media := descriptors[i].Type
		if media == "" || mediaAllowed(out, media) {
			continue
		}
		out = append(out, media)
	}
	return out
}

func descriptorSupportedSampleFormats(descriptors []codec.Descriptor) []string {
	var out []string
	for i := range descriptors {
		out = mergeStringList(out, descriptors[i].Capabilities.SampleFormats)
	}
	return out
}

func descriptorSupportedPixelFormats(descriptors []codec.Descriptor) []string {
	var out []string
	for i := range descriptors {
		out = mergeStringList(out, descriptors[i].Capabilities.PixelFormats)
	}
	return out
}

func mergeStringList(existing []string, next []string) []string {
	for i := range next {
		if next[i] == "" || stringAllowed(existing, next[i]) {
			continue
		}
		existing = append(existing, next[i])
	}
	return existing
}

func validateRecipeTransformAdapters(operation string, rt Runtime, streams []streamIntent) error {
	standard, ok := rt.(*runtime)
	if !ok || standard == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		transforms := streamIntentTransformSpecs(stream)
		for j := range transforms {
			name := transformFactoryName(transforms[j])
			if name == "" {
				continue
			}
			if _, err := standard.filters.Factory(name); err != nil {
				return recipeTransformAdapterError(operation, stream, name, err)
			}
			desc, err := standard.filters.Descriptor(name)
			if err == nil {
				if err := validateTransformAdapterDescriptor(operation, stream, transforms[j], name, desc); err != nil {
					return err
				}
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

func validateTransformAdapterDescriptor(operation string, stream streamIntent, spec TransformSpec, name string, desc filter.Descriptor) error {
	expectedInput, expectedOutput := transformAdapterExpectedMedia(name)
	if expectedInput != "" && desc.Input != "" && desc.Input != expectedInput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	if expectedOutput != "" && desc.Output != "" && desc.Output != expectedOutput {
		return transformAdapterIncompatibleError(operation, stream, name, desc, expectedInput, expectedOutput)
	}
	if spec.Resize != nil {
		if mode := resizeModeWithDefault(spec.Resize.Mode); mode != "" && len(desc.ResizeModes) != 0 && !resizeModeAllowed(desc.ResizeModes, mode) {
			return transformAdapterCapabilityError(operation, stream, name, "resize_mode", string(mode), resizeModesToStrings(desc.ResizeModes))
		}
		if format := spec.Resize.PixelFormat; format != "" && len(desc.PixelFormats) != 0 && !stringAllowed(desc.PixelFormats, format) {
			return transformAdapterCapabilityError(operation, stream, name, "pixel_format", format, desc.PixelFormats)
		}
	}
	if spec.Resample != nil {
		if format := spec.Resample.SampleFormat; format != "" && len(desc.SampleFormats) != 0 && !stringAllowed(desc.SampleFormats, format) {
			return transformAdapterCapabilityError(operation, stream, name, "sample_format", format, desc.SampleFormats)
		}
	}
	return nil
}

func resizeModeWithDefault(mode filter.ResizeMode) filter.ResizeMode {
	if mode == "" {
		return filter.ResizeExact
	}
	return mode
}

func resizeModeAllowed(allowed []filter.ResizeMode, mode filter.ResizeMode) bool {
	for i := range allowed {
		if allowed[i] == mode {
			return true
		}
	}
	return false
}

func stringAllowed(allowed []string, value string) bool {
	for i := range allowed {
		if allowed[i] == value {
			return true
		}
	}
	return false
}

func resizeModesToStrings(modes []filter.ResizeMode) []string {
	out := make([]string, 0, len(modes))
	for i := range modes {
		if modes[i] != "" {
			out = append(out, string(modes[i]))
		}
	}
	return out
}

func transformAdapterExpectedMedia(name string) (av.MediaType, av.MediaType) {
	switch name {
	case filter.FactoryResize:
		return av.MediaVideo, av.MediaVideo
	case filter.FactoryResample:
		return av.MediaAudio, av.MediaAudio
	default:
		return "", ""
	}
}

func transformAdapterIncompatibleError(operation string, stream streamIntent, name string, desc filter.Descriptor, expectedInput av.MediaType, expectedOutput av.MediaType) error {
	return &BuildError{
		Code:      "transform_adapter_incompatible",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    name + " filter adapter declares incompatible media",
		Details: []string{
			"transform=" + name,
			"expected_input=" + string(expectedInput),
			"expected_output=" + string(expectedOutput),
			"actual_input=" + string(desc.Input),
			"actual_output=" + string(desc.Output),
		},
		Suggestions: []string{
			"register a " + name + " filter adapter whose descriptor declares " + string(expectedInput) + " input and " + string(expectedOutput) + " output",
			"use .Video().Resize(...) with video resize adapters and .Audio().Resample(...) with audio resample adapters",
			"fix the adapter descriptor if the implementation already supports this transform",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func transformAdapterCapabilityError(operation string, stream streamIntent, name string, field string, requested string, supported []string) error {
	return &BuildError{
		Code:      "transform_adapter_incompatible",
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    name + " filter adapter does not support the requested " + strings.ReplaceAll(field, "_", " "),
		Details: []string{
			"transform=" + name,
			"field=" + field,
			"requested=" + requested,
			"supported=" + strings.Join(supported, ","),
		},
		Suggestions: []string{
			"choose one of the supported " + strings.ReplaceAll(field, "_", " ") + " values",
			"register a " + name + " filter adapter whose descriptor supports this transform config",
			"fix the adapter descriptor if the implementation already supports this config",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func recipeTransformAdapterError(operation string, stream streamIntent, name string, cause error) error {
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

func recipeEncodeAdapterError(operation string, stream streamIntent, registry *codec.SimpleRegistry, cause error) error {
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
			"use .To(goav.Sink(...)) to receive decoded frames without encoding",
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

func duplicateDestinationHandleError(operation string, name string) error {
	return &BuildError{
		Code:      "destination_duplicate",
		Operation: operation,
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is attached more than once", name),
		Suggestions: []string{
			"list each destination value once in .To(...)",
			"use distinct destination names when writing to separate destinations",
			"reuse one destination value from multiple branches through .Branches(...) when outputs should be grouped",
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

func chainStepAfterEncodeError(operation string, node string, step string, encode CodecSpec) error {
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
			"place .Do(...), .Resize(...), or .Resample(...) before .Encode(...)",
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
		if !spec.Settings.SampleRateSet {
			parameters.SampleRate = 0
			parameters.ClockRate = 0
		}
		if !spec.Settings.ChannelsSet {
			parameters.Channels = 0
			parameters.ChannelLayout = ""
		}
	}
	return codec.EncodeConfig{
		Parameters: parameters,
		Settings:   cloneCodecSettings(spec.Settings),
	}
}

func cloneEncodeConfig(config codec.EncodeConfig) codec.EncodeConfig {
	config.Stream.Codec.Attributes = cloneMetadata(config.Stream.Codec.Attributes)
	config.Stream.Codec.ExtraData = cloneBuffer(config.Stream.Codec.ExtraData)
	config.Stream.Metadata = cloneMetadata(config.Stream.Metadata)
	config.Parameters.Attributes = cloneMetadata(config.Parameters.Attributes)
	config.Parameters.ExtraData = cloneBuffer(config.Parameters.ExtraData)
	config.Settings = cloneCodecSettings(config.Settings)
	return config
}

func cloneBuffer(buffer av.Buffer) av.Buffer {
	buffer.Bytes = append([]byte(nil), buffer.Bytes...)
	return buffer
}

func validateRecipeEncode(spec CodecSpec, operation string, node string) error {
	if spec.Auto {
		return &BuildError{
			Code:      "encode_auto_unresolved",
			Operation: operation,
			Node:      node,
			Reason:    "automatic codec selection is not implemented for stream recipes yet",
			Suggestions: []string{
				"choose an explicit recipe encoder with .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...))",
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
			Reason:    string(spec.ID) + " recipe encoding is work in progress; recipe encode branches currently support opus, vp8, and vp9",
			Suggestions: []string{
				"decode the stream with .To(goav.Sink(...))",
				"use .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) for recipe encode branches",
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
	case spec.Settings.Bitrate < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode bitrate must be non-negative",
			Details: []string{
				fmt.Sprintf("bitrate=%d", spec.Settings.Bitrate),
			},
			Suggestions: []string{
				"pass a positive value to goav.Bitrate(...)",
				"omit goav.Bitrate(...) when the encoder should choose its default",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.Framerate.Value < 0 || spec.Settings.Framerate.Base.Num < 0 || spec.Settings.Framerate.Base.Den < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode FPS must be positive",
			Details: []string{
				fmt.Sprintf("fps_duration=%d/%d/%d", spec.Settings.Framerate.Value, spec.Settings.Framerate.Base.Num, spec.Settings.Framerate.Base.Den),
			},
			Suggestions: []string{
				"pass a positive value to goav.FPS(...)",
				"omit goav.FPS(...) when the encoder should infer frame cadence",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.KeyframeInterval < 0:
		return &BuildError{
			Code:      "encode_parameter_invalid",
			Operation: operation,
			Node:      node,
			Reason:    "encode keyframe interval must be non-negative",
			Details: []string{
				fmt.Sprintf("keyframe_interval=%d", spec.Settings.KeyframeInterval),
			},
			Suggestions: []string{
				"pass a positive value to goav.KeyframeInterval(...)",
				"omit goav.KeyframeInterval(...) when the encoder should choose its default cadence",
			},
			Cause: ErrUnsupportedBuild,
		}
	case spec.Settings.SampleRateSet && spec.Parameters.SampleRate <= 0:
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
	case spec.Settings.ChannelsSet && spec.Parameters.Channels <= 0:
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
			return mediaTransform{}, transformMediaError(base, "resize", av.MediaVideo, selector.Type)
		}
		resize := *spec.Resize
		return mediaTransform{
			name:    "resize-" + base + suffix,
			factory: filter.FactoryResize,
			video:   &resize,
		}, nil
	case spec.Resample != nil:
		if selector.Type == av.MediaVideo {
			return mediaTransform{}, transformMediaError(base, "resample", av.MediaAudio, selector.Type)
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

func transformMediaError(stream string, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: "build stream",
		Node:      stream,
		Reason:    transform + " applies to " + string(expected) + " streams",
		Details: []string{
			"expected_shape=" + shape.Frame(expected).String(),
			"actual_shape=" + shape.Frame(actual).String(),
		},
		Suggestions: []string{
			"use .Video().Resize(...) for video scaling",
			"use .Audio().Resample(...) for audio sample-rate or channel conversion",
		},
		Cause: ErrUnsupportedBuild,
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
	name             string
	selector         av.StreamSelector
	from             TapRef
	decode           bool
	decodeCodec      CodecSpec
	operations       []OperationSpec
	sharedOps        []OperationSpec
	privateOps       []OperationSpec
	destinationNames []string
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

type jobStreamBuilder struct {
	job    *Job
	stream *jobStreamBuild
	region *compositeRegion
}

// Region places this arm's frame at (x, y) on the composite canvas. It is
// composite-only — it has no effect on arms outside a Composite.
func (b *jobStreamBuilder) Region(x, y int) *jobStreamBuilder {
	b.region = &compositeRegion{x: x, y: y}
	return b
}

func (b *jobStreamBuilder) sourceStartsFrameDomain() bool {
	spec, ok := b.sourceFrameShape()
	return ok && spec.Domain == shape.DomainFrame
}

func (b *jobStreamBuilder) sourceFrameShape() (shape.Spec, bool) {
	if b == nil || b.job == nil || len(b.job.inputs) != 1 {
		return shape.Spec{}, false
	}
	spec, ok := customSourceShape(b.job.inputs[0])
	if !ok || spec.Domain != shape.DomainFrame {
		return shape.Spec{}, false
	}
	return spec, true
}

func (b *jobStreamBuilder) ensureDecodeOperation() {
	if b.sourceStartsFrameDomain() {
		return
	}
	ensureJobStreamDecodeOperation(b.current())
}

func (b *jobStreamBuilder) ensureFrameSourceShapeOperation() {
	stream := b.current()
	if stream == nil || len(stream.operations) != 0 {
		return
	}
	shape, ok := b.sourceFrameShape()
	if !ok {
		return
	}
	stream.operations = append(stream.operations, operationSpecForShape(shape))
}

func frameSourceDecodeError(operation string, node string) error {
	return &BuildError{
		Code:      "source_shape_mismatch",
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources are already decoded frames",
		Details: []string{
			"source_domain=frame",
			"operation=decode",
		},
		Suggestions: []string{
			"remove .Decode() when using goav.Source(..., shape.Frame(...), ...)",
			"use shape.Packet(...) when the custom source pushes encoded packets",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func frameSourceCopyError(operation string, node string) error {
	return &BuildError{
		Code:      "source_shape_mismatch",
		Operation: operation,
		Node:      node,
		Reason:    "frame-domain custom sources cannot use packet copy",
		Details: []string{
			"source_domain=frame",
			"operation=copy",
		},
		Suggestions: []string{
			"send frame-domain media to goav.Sink(...)",
			"encode frames before writing to file, URI, writer, or object destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func (b *jobStreamBuilder) Apply(flow Chain) *jobStreamBuilder {
	spec, err := chainSpecFrom(flow)
	if err != nil {
		b.job.setErr(err)
		return b
	}
	stream := b.current()
	if err := validateChainMedia("build stream", jobStreamName(stream), stream.selector.Type, spec); err != nil {
		b.job.setErr(err)
		return b
	}
	specSteps := chainStepsFromChainOperations(spec.operations)
	if codecIntentSet(chainEncodeSpec(stream.operations)) && (chainHasDecode(spec.operations) || len(specSteps) != 0 || codecIntentSet(chainEncodeSpec(spec.operations))) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "flow", chainEncodeSpec(stream.operations)))
		return b
	}
	if chainHasDecode(spec.operations) {
		if b.sourceStartsFrameDomain() {
			b.job.setErr(frameSourceDecodeError("build stream", jobStreamName(stream)))
			return b
		}
		if chainHasDecode(stream.operations) || operationSpecsContainChainStep(stream.operations) {
			b.job.setErr(flowDecodeDomainError("build stream", firstNonEmpty(spec.name, jobStreamName(stream), "flow")))
			return b
		}
		// The flow's OpDecode is appended below with the rest of spec.operations.
	}
	if len(specSteps) != 0 && !chainHasDecode(spec.operations) {
		b.ensureDecodeOperation()
	}
	if codecIntentSet(chainEncodeSpec(spec.operations)) && !chainEncodeSpec(spec.operations).Copy && !chainHasDecode(spec.operations) {
		b.ensureDecodeOperation()
	}
	stream.operations = append(stream.operations, cloneOperationSpecs(spec.operations)...)
	if codecIntentSet(chainEncodeSpec(spec.operations)) && chainEncodeSpec(spec.operations).Copy {
		if b.sourceStartsFrameDomain() {
			b.job.setErr(frameSourceCopyError("build stream", jobStreamName(stream)))
			return b
		}
		if chainHasDecode(stream.operations) || operationSpecsContainChainStep(stream.operations) {
			b.job.setErr(flowCopyDomainError("build stream", firstNonEmpty(spec.name, jobStreamName(stream), "flow")))
			return b
		}
	}
	return b
}

func (b *jobStreamBuilder) Decode(options ...CodecOption) *jobStreamBuilder {
	stream := b.current()
	if b.sourceStartsFrameDomain() {
		b.job.setErr(frameSourceDecodeError("build stream", jobStreamName(stream)))
		return b
	}
	decodeCodec := mergeDecodeCodecSpec(chainDecodeCodec(stream.operations), codecSpecFromOptions(options...))
	stream.operations = append(stream.operations, operationSpecForDecode(decodeCodec, string(stream.selector.Codec)))
	return b
}

func (b *jobStreamBuilder) Copy() *jobStreamBuilder {
	stream := b.current()
	if b.sourceStartsFrameDomain() {
		b.job.setErr(frameSourceCopyError("build stream", jobStreamName(stream)))
		return b
	}
	stream.operations = append(stream.operations, operationSpecForCopy(codec.Copy()))
	return b
}

func (b *jobStreamBuilder) Tap(tap TapRef) *jobStreamBuilder {
	stream := b.current()
	if tap.name == "" {
		b.job.setErr(&BuildError{
			Code:      "tap_invalid",
			Operation: "build stream",
			Node:      jobStreamName(stream),
			Reason:    "tap name is empty",
			Suggestions: []string{
				"call .Tap(goav.FrameTap(\"video.decoded\")) or another stable tap ref",
				"omit .Tap(...) when no runtime branch should attach at that point",
			},
			Cause: ErrUnsupportedBuild,
		})
		return b
	}
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		if err := validateTapDomain("build stream", jobStreamName(stream), tap, shape.DomainPacket); err != nil {
			b.job.setErr(err)
			return b
		}
		stream.operations = append(stream.operations, operationSpecForTap(tap, stream.selector.Type, operationSpecAfter(stream.operations, OpEncode)))
		return b
	}
	if err := validateTapDomain("build stream", jobStreamName(stream), tap, shape.DomainFrame); err != nil {
		b.job.setErr(err)
		return b
	}
	b.ensureDecodeOperation()
	stream.operations = append(stream.operations, operationSpecForTap(tap, stream.selector.Type, operationSpecAfter(stream.operations, initialStepAfter(chainHasDecode(stream.operations)))))
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

func lastStreamTapRef(stream *jobStreamBuild) TapRef {
	if stream == nil {
		return TapRef{}
	}
	for i := len(stream.operations) - 1; i >= 0; i-- {
		if operationSpecTapIsTerminalPacket(stream.operations[i]) {
			return PacketTap(stream.operations[i].Tap.Name)
		}
	}
	steps := jobStreamChainSteps(stream)
	if chainEncodeSpec(stream.operations).Copy && len(steps) == 0 && stream.selector.Type != "" {
		return PacketTap(defaultPacketTapName(stream.selector.Type, 0))
	}
	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].tap != "" {
			return tapWithDomain(TapRef{name: steps[i].tap, domain: steps[i].tapDomain}, shape.DomainFrame)
		}
	}
	if len(steps) == 0 && stream.selector.Type != "" && chainHasDecode(stream.operations) {
		return FrameTap(defaultDecodedTapName(stream.selector.Type))
	}
	return TapRef{}
}

func (b *jobStreamBuilder) Do(stage pipeline.Stage) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "custom stage", chainEncodeSpec(stream.operations)))
		return b
	}
	if stage == nil {
		b.job.setErr(streamStageMissingError(streamIntent{Name: jobStreamName(stream)}))
		return b
	}
	b.ensureDecodeOperation()
	stream.operations = append(stream.operations, operationSpecForStage(stage))
	return b
}

func (b *jobStreamBuilder) Shape(shape shape.Spec) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "shape", chainEncodeSpec(stream.operations)))
		return b
	}
	stream.operations = append(stream.operations, operationSpecForShape(shape))
	return b
}

func (b *jobStreamBuilder) Resize(width int, height int, options ...resizeOption) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "resize", chainEncodeSpec(stream.operations)))
		return b
	}
	b.ensureDecodeOperation()
	transform := Resize(width, height, options...)
	stream.operations = append(stream.operations, operationSpecForTransform(transform))
	return b
}

func (b *jobStreamBuilder) Resample(sampleRate int, channels int, options ...audioOption) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(chainStepAfterEncodeError("build stream", jobStreamName(stream), "resample", chainEncodeSpec(stream.operations)))
		return b
	}
	b.ensureDecodeOperation()
	transform := Resample(sampleRate, channels, options...)
	stream.operations = append(stream.operations, operationSpecForTransform(transform))
	return b
}

func (b *jobStreamBuilder) Encode(codec CodecSpec) *jobStreamBuilder {
	stream := b.current()
	if codecIntentSet(chainEncodeSpec(stream.operations)) {
		b.job.setErr(duplicateStreamEncodeError("build stream", jobStreamName(stream), chainEncodeSpec(stream.operations), codec))
		return b
	}
	b.ensureDecodeOperation()
	stream.operations = append(stream.operations, operationSpecForEncode(cloneCodecSpec(codec)))
	return b
}

func (b *jobStreamBuilder) OnCodecChange(policy CodecChangePolicy) *jobStreamBuilder {
	stream := b.current()
	stream.codecChange = policy
	return b
}

func (b *jobStreamBuilder) To(destinations ...Destination) *Job {
	stream := b.current()
	outputs := make([]destinationSpec, 0, len(destinations))
	for i := range destinations {
		destination := destinations[i]
		binding, err := destinationBindingFromDestination(destination)
		if err != nil {
			b.job.setErr(streamDestinationInvalidError(jobStreamName(stream), err.Error()))
			return b.job
		}
		output, name, err := destinationFromBinding("build stream", jobStreamName(stream), binding, i)
		if err != nil {
			b.job.setErr(err)
			return b.job
		}
		stream.outputs = append(stream.outputs, output)
		stream.outputNames = append(stream.outputNames, name)
		outputs = append(outputs, output)
	}
	if outputsContainSinkDestination(outputs) && !codecIntentSet(chainEncodeSpec(stream.operations)) {
		if b.sourceStartsFrameDomain() {
			b.ensureFrameSourceShapeOperation()
		} else {
			b.ensureDecodeOperation()
		}
	}
	return b.job
}

func destinationFromBinding(operation string, node string, destination destinationBinding, index int) (destinationSpec, string, error) {
	switch {
	case destination.hasDirect:
		return cloneDestinationSpec(destination.dest), "", nil
	default:
		return destinationSpec{}, "", destinationInvalidError(operation, node, "unsupported destination")
	}
}

func (b *jobStreamBuilder) current() *jobStreamBuild {
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
	outputs []namedDestinationSpec
	err     error

	fromBranchSplit bool
}

type namedDestinationSpec struct {
	name   string
	output destinationSpec
}

func destinationIdentity(destination namedDestinationSpec) string {
	output := destination.output
	sinkName := ""
	sinkAddr := ""
	if output.sink != nil {
		sinkName = output.sink.Name()
		sinkAddr = fmt.Sprintf("%p", output.sink)
	}
	return strings.Join([]string{
		destination.name,
		strconv.FormatUint(destination.output.id, 10),
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

func (j *branchCompositionJob) Plan() Intent {
	intent := Intent{
		Name:   firstNonEmpty(j.name, "branch-composition"),
		Inputs: []inputIntent{j.input.intent()},
	}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
	}
	for i := range j.streams {
		intent.Streams = append(intent.Streams, branchStreamIntent(j.streams[i]))
	}
	for i := range j.outputs {
		intent.Destinations = append(intent.Destinations, j.outputs[i].output.intentWithName(j.outputs[i].name))
	}
	return intent
}

func (j *branchCompositionJob) composePlan() (branchComposePlan, error) {
	if j == nil {
		return branchComposePlan{}, nil
	}
	return planBranchCompositionRecipe(j.Plan(), j.input, j.outputs, j.streams)
}

func planBranchCompositionRecipe(intent Intent, input InputSpec, namedOutputs []namedDestinationSpec, branchBuilds []streamBuild) (branchComposePlan, error) {
	streams := intent.Streams
	outputs, outputOrder := branchDestinationAttachmentSet(namedOutputs)

	branches := make([]branchComposeBranch, 0, len(streams))
	outputBranches := make(map[string][]string, len(outputs))
	if len(streams) == 0 {
		return branchComposePlan{}, branchStreamMissingError()
	}
	for i := range streams {
		stream := streams[i]
		branchName := stream.Name
		selector := streamIntentSelector(stream)
		operations := cloneOperationSpecs(stream.Operations)
		var sharedOperations []OperationSpec
		var privateOperations []OperationSpec
		if i < len(branchBuilds) {
			branchBuild := branchBuilds[i]
			operations = streamBuildOperationSpecs(branchBuild)
			sharedOperations = cloneOperationSpecs(branchBuild.sharedOps)
			privateOperations = cloneOperationSpecs(branchBuild.privateOps)
		} else {
			sharedOperations, privateOperations = splitOperationSpecsByShared(operations)
		}
		branch := branchComposeBranch{
			Name:              branchName,
			Selector:          selector,
			Copy:              stream.Encode.Copy,
			Operations:        cloneOperationSpecs(operations),
			SharedOperations:  sharedOperations,
			PrivateOperations: privateOperations,
			DecodeConfig:      cloneCodecSpec(stream.DecodeCodec),
			CodecChange:       stream.CodecChange,
			Encode:            encodeConfigFromSpec(stream.Encode),
			Labels:            append([]string(nil), stream.Destinations...),
		}
		for _, label := range stream.Destinations {
			outputBranches[label] = append(outputBranches[label], branchName)
		}
		if err := validateBranchTransforms(stream); err != nil {
			return branchComposePlan{}, err
		}
		branches = append(branches, branch)
	}

	planDestinations := make([]branchComposeTarget, 0, len(outputOrder))
	for i := range outputOrder {
		name := outputOrder[i]
		output := outputs[name]
		planTarget := branchComposeTarget{
			Name:        name,
			Destination: cloneDestinationSpec(output),
			Target:      output.output,
			Sink:        output.sink,
			Format:      output.format,
			Branches:    append([]string(nil), outputBranches[name]...),
		}
		if output.resolvedFormat != "" {
			planTarget = resolveBranchComposeTargetFormat(planTarget, output.resolvedFormat)
		}
		planDestinations = append(planDestinations, planTarget)
	}
	return branchComposePlan{
		Name:         "branch-composition",
		Input:        input.input,
		Branches:     branches,
		Destinations: planDestinations,
	}, nil
}

func branchComposePlanReady(plan branchComposePlan) bool {
	return len(plan.Branches) != 0 || len(plan.Destinations) != 0
}

func splitOperationSpecsByShared(operations []OperationSpec) ([]OperationSpec, []OperationSpec) {
	if len(operations) == 0 {
		return nil, nil
	}
	shared := make([]OperationSpec, 0)
	private := make([]OperationSpec, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		if operation.Shared {
			shared = append(shared, operation)
			continue
		}
		private = append(private, operation)
	}
	return cloneOperationSpecs(shared), cloneOperationSpecs(private)
}

func chainStepsFromChainOperations(operations []OperationSpec) []chainStep {
	if len(operations) == 0 {
		return nil
	}
	steps := make([]chainStep, 0, len(operations))
	for i := range operations {
		operation := operations[i]
		switch operation.Kind {
		case OpStage:
			if operation.Stage != nil {
				steps = append(steps, chainStep{stage: operation.Stage})
			}
		case OpShape:
			if !mediaShapeEmpty(operation.Shape) {
				steps = append(steps, chainStep{shape: operation.Shape})
			}
		case OpTransform:
			if operation.Transform.Resize != nil || operation.Transform.Resample != nil {
				steps = append(steps, chainStep{transform: cloneTransformSpec(operation.Transform)})
			}
		case OpTap:
			if operation.Tap.Name != "" && operation.Tap.Domain != shape.DomainPacket {
				steps = append(steps, chainStep{tap: operation.Tap.Name, tapDomain: operation.Tap.Domain})
			}
		}
	}
	return steps
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

func validateBranchIntentShape(stream streamIntent, index int) error {
	selector := streamIntentSelector(stream)
	if stream.Name == "" {
		return branchIntentNameMissingError(index, stream)
	}
	if err := validateRecipeStreamSelector(branchCompositionOperation, branchIntentName(stream), selector); err != nil {
		return err
	}
	if codecIntentSet(stream.Encode) {
		if stream.Encode.Copy && stream.Decode {
			return branchCopyUnsupportedError(stream)
		}
		if stream.Encode.Copy && len(streamIntentTransformSpecs(stream)) != 0 {
			return branchPacketTransformUnsupportedError(stream)
		}
		if err := validateRecipeEncode(stream.Encode, branchCompositionOperation, stream.Name); err != nil {
			return err
		}
	}
	if len(stream.Destinations) == 0 {
		return branchIntentDestinationMissingError(stream)
	}
	return validateBranchDestinations(stream)
}

func validateBranchCompositionAttachments(input InputSpec, namedOutputs []namedDestinationSpec, fromBranchSplit bool) error {
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
			return branchDestinationDuplicateError(name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateBranchDestinationKinds(intent Intent, namedOutputs []namedDestinationSpec) error {
	outputs := branchDestinationSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		hasMuxDestination := false
		for _, label := range stream.Destinations {
			output, ok := outputs[label]
			if !ok {
				continue
			}
			if output.sink == nil {
				hasMuxDestination = true
				break
			}
		}
		if hasMuxDestination && !codecIntentSet(stream.Encode) {
			return branchEncodeMissingError(stream)
		}
	}
	return nil
}

func validateBranchDestinationBindings(intent Intent, namedOutputs []namedDestinationSpec) error {
	outputs := branchDestinationLabelSet(namedOutputs)
	for i := range intent.Streams {
		stream := intent.Streams[i]
		for _, label := range stream.Destinations {
			if _, ok := outputs[label]; ok {
				continue
			}
			return branchDestinationReferenceMissingError(stream, label)
		}
	}
	return nil
}

func branchDestinationSet(namedOutputs []namedDestinationSpec) map[string]destinationSpec {
	outputs := make(map[string]destinationSpec, len(namedOutputs))
	for i := range namedOutputs {
		outputs[namedOutputs[i].name] = namedOutputs[i].output
	}
	return outputs
}

func branchDestinationAttachmentSet(namedOutputs []namedDestinationSpec) (map[string]destinationSpec, []string) {
	outputs := make(map[string]destinationSpec, len(namedOutputs))
	outputOrder := make([]string, 0, len(namedOutputs))
	for i := range namedOutputs {
		name := namedOutputs[i].name
		outputOrder = append(outputOrder, name)
		outputs[name] = namedOutputs[i].output.withName(firstNonEmpty(namedOutputs[i].output.name, name))
	}
	return outputs, outputOrder
}

func branchDestinationLabelSet(namedOutputs []namedDestinationSpec) map[string]struct{} {
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
			"add a video branch such as .Video(\"720p\").Resize(...).Encode(codec.VP9(...)).To(...)",
			"add an audio branch such as .Audio(\"main\").Resample(...).Encode(codec.Opus(...)).To(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchEncodeMissingError(stream streamIntent) error {
	return &BuildError{
		Code:      "encode_missing",
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "branch needs an encoder before writing to a muxed destination",
		Details: []string{
			"expected_shape=" + shape.New(shape.Domain(shape.DomainPacket), shape.Media(stream.Select.Type)).String(),
			"actual_shape=" + shape.Frame(stream.Select.Type).String(),
		},
		Suggestions: []string{
			"call .Encode(codec.Opus(...)), .Encode(codec.VP8(...)), or .Encode(codec.VP9(...)) before .To(...)",
			"route raw frames to goav.Sink(...) when the branch should stay decoded",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchCopyUnsupportedError(stream streamIntent) error {
	return &BuildError{
		Code:      "copy_unsupported",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    "copy branches require a packet-domain stream point",
		Suggestions: []string{
			"use goav.From(input).Copy().Branches(...) for packet-preserving planned branches",
			"attach a runtime branch from a packet tap and call .Copy() when packet-domain fanout is needed",
			"omit .Copy() when the branch should deliver decoded frames to goav.Sink(...)",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentDestinationMissingError(stream streamIntent) error {
	selector := streamIntentSelector(stream)
	return &BuildError{
		Code:      "destination_missing",
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.Name, string(selector.Type), "stream"),
		Reason:    "branch has no destination",
		Suggestions: []string{
			"finish the branch with .To(goav.File(\"web.ivf\", writer)) or .To(goav.Sink(sink))",
			"reuse the same destination value from multiple branches when they should share one mux group",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationReferenceMissingError(stream streamIntent, label string) error {
	return &BuildError{
		Code:      "destination_missing",
		Operation: branchCompositionOperation,
		Node:      stream.Name,
		Reason:    "destination " + label + " is referenced but not defined",
		Suggestions: []string{
			"pass a named goav.File(...), goav.URIOut(...), or goav.Sink(...) destination to the branch .To(...) call",
			"reuse destination values instead of repeating string destination names",
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

func branchDestinationNameEmptyError(stream streamBuild, index int) error {
	return &BuildError{
		Code:      "destination_invalid",
		Operation: branchCompositionOperation,
		Node:      firstNonEmpty(stream.name, string(stream.selector.Type), "stream"),
		Reason:    "branch destinations must be non-empty",
		Details: []string{
			fmt.Sprintf("destination index: %d", index),
		},
		Suggestions: []string{
			"call .To(goav.File(\"web.ivf\", writer)) with a non-empty destination name",
			"pass goav.Sink(goav.SinkFunc(name, fn)) for sink destinations",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchDestinationDuplicateError(name string) error {
	return &BuildError{
		Code:      "destination_duplicate",
		Operation: branchCompositionOperation,
		Node:      name,
		Reason:    fmt.Sprintf("destination %q is defined more than once with different destination handles", name),
		Suggestions: []string{
			"reuse the same destination value when multiple branches should share one mux group",
			"use distinct destination names when branches should write to different destinations",
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
			"route one branch to multiple destinations by calling .To(destination, otherDestination)",
			"route different branches to the same destination by reusing the destination value",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchIntentNameMissingError(index int, stream streamIntent) error {
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
			"use branch names as handles for graph inspection and destination planning",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateBranchDestinations(stream streamIntent) error {
	seen := make(map[string]int, len(stream.Destinations))
	for i, target := range stream.Destinations {
		if firstIndex, ok := seen[target]; ok {
			return duplicateBranchDestinationError(stream, target, firstIndex, i)
		}
		seen[target] = i
	}
	return nil
}

func duplicateBranchDestinationError(stream streamIntent, target string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Code:      "destination_duplicate",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", target),
		Details: []string{
			fmt.Sprintf("first destination index: %d", firstIndex),
			fmt.Sprintf("second destination index: %d", secondIndex),
		},
		Suggestions: []string{
			"list each destination once in .To(...)",
			"route one branch to multiple destinations with distinct values such as .To(archive, preview)",
			"reuse destination values instead of repeating destination names",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func validateBranchTransforms(stream streamIntent) error {
	transforms := streamIntentTransformSpecs(stream)
	for i := range transforms {
		transform := transforms[i]
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
				return branchTransformMediaError(stream, "resize", av.MediaVideo, stream.Select.Type)
			}
		case transform.Resample != nil:
			if stream.Select.Type == av.MediaVideo {
				return branchTransformMediaError(stream, "resample", av.MediaAudio, stream.Select.Type)
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

func streamIntentTransformSpecs(stream streamIntent) []TransformSpec {
	return transformSpecsFromOperationSpecs(stream.Operations)
}

func transformSpecsFromOperationSpecs(operations []OperationSpec) []TransformSpec {
	if len(operations) == 0 {
		return nil
	}
	transforms := make([]TransformSpec, 0)
	for i := range operations {
		if operations[i].Kind != OpTransform {
			continue
		}
		transform := cloneTransformSpec(operations[i].Transform)
		transforms = append(transforms, transform)
	}
	return transforms
}

func branchTransformMediaError(stream streamIntent, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Code:      "transform_media_mismatch",
		Operation: branchCompositionOperation,
		Node:      branchIntentName(stream),
		Reason:    transform + " applies to " + string(expected) + " branches",
		Details: []string{
			"expected_shape=" + shape.Frame(expected).String(),
			"actual_shape=" + shape.Frame(actual).String(),
		},
		Suggestions: []string{
			"use .Video(...).Resize(...) for video ladder branches",
			"use .Audio(...).Resample(...) for audio branches",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func streamIntentSelector(stream streamIntent) av.StreamSelector {
	return av.StreamSelector{
		ID:       stream.Select.ID,
		Index:    stream.Select.Index,
		UseIndex: stream.Select.UseIndex,
		Type:     stream.Select.Type,
		Codec:    stream.Select.Codec,
		Name:     stream.Select.Name,
	}
}

func branchIntentName(stream streamIntent) string {
	return firstNonEmpty(stream.Name, string(stream.Select.Type), "stream")
}

func destinationNamesWithOverrides(outputs []destinationSpec, destinationNames []string) []string {
	names := make([]string, 0, len(outputs))
	for i := range outputs {
		names = append(names, jobOutputDestinationName(outputs, destinationNames, i))
	}
	return names
}

func jobOutputDestinationName(outputs []destinationSpec, destinationNames []string, index int) string {
	if index >= 0 && index < len(destinationNames) && destinationNames[index] != "" {
		return destinationNames[index]
	}
	return outputs[index].label(fmt.Sprintf("output-%d", index))
}

func firstNonEmpty(values ...string) string {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}
