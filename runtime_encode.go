package goav

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
)

// newEncodeStageNamed opens the encoder and returns its stage with the stream
// it emits: config.Stream plus whatever the opened encoder reports.
func (b *builder) newEncodeStageNamed(ctx context.Context, name string, request compile.EncodeRequest, config codec.EncodeConfig) (*codec.EncoderStage, av.Stream, error) {
	name = firstNonEmpty(name, compile.EncodeNodeName(request))
	// File inputs reveal their rate and geometry only once opened, so the
	// backend is chosen here, against the real stream, before an adapter
	// turns an unsupported config into a bare sentinel.
	adapterRequest := compile.EncodeAdapterRequestFromPreparedStream(codec.Spec{ID: config.Parameters.ID, Type: config.Parameters.Type, Parameters: config.Parameters, Settings: config.Settings}, config.Stream)
	adapterRequest.Realtime = config.Realtime
	choice, err := refuse(compile.SelectCodecBackend("configure encode", name, b.runtime.codecs, adapterRequest, codec.ModeEncode))
	if err != nil {
		return nil, av.Stream{}, err
	}
	encoder, err := choice.Encoder.NewEncoder(ctx, config)
	if err != nil {
		return nil, av.Stream{}, encodeOpenError(name, adapterRequest, err)
	}
	stage, err := codec.NewEncoderStage(codec.EncoderStageConfig{
		Name:              name,
		Detail:            compile.JoinSpecDetail(compile.EncodeNodeDetail(request), compile.BackendDetail(choice.Descriptor)),
		Encoder:           encoder,
		Result:            encodeResultForStream(config.Stream),
		OutputStreamID:    config.Stream.ID,
		OutputCodecEpoch:  config.Stream.Epoch,
		StampOutputStream: true,
	})
	if err != nil {
		if encoder != nil {
			encoder.Close()
		}
		return nil, av.Stream{}, err
	}
	b.encoders = append(b.encoders, stage)
	return stage, withConfiguredBitrate(reportedStream(config.Stream, encoder), config.Settings.Bitrate), nil
}

// withConfiguredBitrate records the bitrate a recipe configured an encoder
// with on its stream (av.MetadataBitrate), so a media destination following
// a receiver's bandwidth estimate retargets it no higher.
func withConfiguredBitrate(stream av.Stream, bitsPerSecond int) av.Stream {
	if bitsPerSecond <= 0 {
		return stream
	}
	metadata := make(av.Metadata, len(stream.Metadata)+1)
	maps.Copy(metadata, stream.Metadata)
	metadata[av.MetadataBitrate] = strconv.Itoa(bitsPerSecond)
	stream.Metadata = metadata
	return stream
}

func prepareEncodeConfig(input av.Stream, request compile.EncodeRequest, realtime bool) (codec.EncodeConfig, av.Stream, error) {
	if !compile.StreamMatchesSelector(input, request.Selector) {
		return codec.EncodeConfig{}, av.Stream{}, encodeStreamMismatchError(request, input)
	}
	config := request.Config
	targetCodec := config.Parameters.ID
	if targetCodec == "" {
		targetCodec = config.Stream.Codec.ID
	}
	if targetCodec == "" {
		return codec.EncodeConfig{}, av.Stream{}, encodeTargetMissingError(request, input)
	}

	stream := mergeEncodeStream(input, config.Stream)
	baseParameters := input.Codec
	if targetCodec != input.Codec.ID {
		baseParameters.ID = ""
		baseParameters.Profile = ""
		baseParameters.Level = ""
		baseParameters.ExtraData = av.Buffer{}
		baseParameters.Attributes = nil
	}
	parameters := mergeCodecParameters(baseParameters, config.Stream.Codec)
	parameters = mergeCodecParameters(parameters, config.Parameters)
	parameters = applyCodecSettingsToParameters(parameters, config.Settings)
	parameters.ID = targetCodec
	if parameters.Type == "" {
		parameters.Type = stream.Type
	}
	if parameters.Type == "" {
		parameters.Type = input.Type
	}
	if stream.Type == "" {
		stream.Type = parameters.Type
	}
	if stream.ID == "" {
		stream.ID = input.ID
	}
	if stream.TimeBase == (av.TimeBase{}) && parameters.ClockRate != 0 {
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(parameters.ClockRate)}
	}
	stream.Codec = parameters

	config.Stream = stream
	config.Parameters = parameters
	if realtime {
		config.Realtime = true
		config.LowLatency = true
	}
	return config, stream, nil
}

func encodeStreamMismatchError(request compile.EncodeRequest, stream av.Stream) error {
	return streamRequestMismatchError(
		encodeStreamMismatchCode,
		"configure encode",
		compile.EncodeNodeName(request),
		request.Selector,
		stream,
		[]string{
			"use the same stream selector for Decode and Encode in the expert builder",
			"use .Audio().Decode().Encode(codec.Opus(...)) for audio streams or .Video().Decode().Encode(codec.VP8(...))/.Encode(codec.VP9(...))/.Encode(codec.AV1(...)) for video streams in recipes",
			"narrow ambiguous inputs with StreamID or StreamIndex(0)",
		},
	)
}

func encodeTargetMissingError(request compile.EncodeRequest, stream av.Stream) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeDestinationMissingCode),
		Code:      encodeDestinationMissingCode,
		Operation: "configure encode",
		Node:      compile.EncodeNodeName(request),
		Reason:    "no target codec was provided",
		fields:    errDetails(errNote("selected: " + compile.StreamDiagnostic(stream, 0))),
		fixes: buildErrorFixes([]string{
			"use " + encodeHint(stream.Type) + " in recipe encode paths",
			"set codec.EncodeConfig.Parameters.ID in the expert builder",
			"leave out .Decode() and .Encode(...) to copy packets when no encode step is intended",
		}),
		cause: errUnsupportedBuild,
	}
}

func mergeEncodeStream(base av.Stream, override av.Stream) av.Stream {
	out := base
	if override.ID != "" {
		out.ID = override.ID
	}
	if override.Index != 0 {
		out.Index = override.Index
	}
	if override.Type != "" {
		out.Type = override.Type
	}
	if override.TimeBase != (av.TimeBase{}) {
		out.TimeBase = override.TimeBase
	}
	if override.Language != "" {
		out.Language = override.Language
	}
	if override.Name != "" {
		out.Name = override.Name
	}
	if override.Epoch != 0 {
		out.Epoch = override.Epoch
	}
	if override.Metadata != nil {
		out.Metadata = override.Metadata
	}
	return out
}

// reportedStream folds the parameters an opened encoder reports
// (codec.ParameterReporter) into the stream it emits.
func reportedStream(stream av.Stream, encoder codec.Encoder) av.Stream {
	if reporter, ok := encoder.(codec.ParameterReporter); ok {
		stream.Codec = mergeCodecParameters(stream.Codec, reporter.EncodedParameters())
	}
	return stream
}

func applyCodecSettingsToParameters(parameters av.CodecParameters, settings codec.CodecSettings) av.CodecParameters {
	if settings.Profile != "" {
		parameters.Profile = settings.Profile
	}
	if settings.Level != "" {
		parameters.Level = settings.Level
	}
	return parameters
}

func mergeCodecParameters(base av.CodecParameters, override av.CodecParameters) av.CodecParameters {
	out := base
	if override.ID != "" {
		out.ID = override.ID
	}
	if override.Type != "" {
		out.Type = override.Type
	}
	if override.Profile != "" {
		out.Profile = override.Profile
	}
	if override.Level != "" {
		out.Level = override.Level
	}
	if override.ClockRate != 0 {
		out.ClockRate = override.ClockRate
	}
	if override.SampleRate != 0 {
		out.SampleRate = override.SampleRate
	}
	if override.Channels != 0 {
		out.Channels = override.Channels
	}
	if override.ChannelLayout != "" {
		out.ChannelLayout = override.ChannelLayout
	}
	if override.Width != 0 {
		out.Width = override.Width
	}
	if override.Height != 0 {
		out.Height = override.Height
	}
	if override.PixelFormat != "" {
		out.PixelFormat = override.PixelFormat
	}
	if override.SampleFormat != "" {
		out.SampleFormat = override.SampleFormat
	}
	if override.ExtraData.Bytes != nil || override.ExtraData.Owner != nil {
		out.ExtraData = override.ExtraData
	}
	if override.Attributes != nil {
		out.Attributes = override.Attributes
	}
	return out
}

// audioEncodeResultPackets is the packet slots an audio encode result
// preallocates: re-framing encoders (Opus takes fixed 2.5-120 ms frames)
// emit every whole frame one input frame completes, and 8 slots cover a
// 120 ms input at the shortest common 20 ms output with room to spare.
const audioEncodeResultPackets = 8

func encodeResultForStream(stream av.Stream) codec.EncodeResult {
	slots := 1
	if stream.Type == av.MediaAudio || stream.Codec.Type == av.MediaAudio {
		slots = audioEncodeResultPackets
	}
	packets := make([]av.Packet, slots)
	for i := range packets {
		packets[i] = av.Packet{
			StreamID:   stream.ID,
			CodecEpoch: stream.Epoch,
			Payload: av.Buffer{
				Bytes: make([]byte, 0, encodePacketBufferSize(stream)),
			},
		}
	}
	return codec.EncodeResult{
		Packets: packets[:0],
		Events:  make([]av.Event, 0, 1),
	}
}

func encodePacketBufferSize(stream av.Stream) int {
	if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
		return videoEncodePacketBufferSize(stream.Codec.Width, stream.Codec.Height)
	}
	return 4096
}

func videoEncodePacketBufferSize(width, height int) int {
	const (
		minVideoEncodePacketBuffer     = 4 * 1024 * 1024
		defaultVideoEncodePacketBuffer = 4 * 1024 * 1024
		maxVideoEncodePacketBuffer     = 32 * 1024 * 1024
	)
	if width <= 0 || height <= 0 {
		return defaultVideoEncodePacketBuffer
	}
	size := int64(width) * int64(height) * 3
	if size < minVideoEncodePacketBuffer {
		return minVideoEncodePacketBuffer
	}
	if size > maxVideoEncodePacketBuffer {
		return maxVideoEncodePacketBuffer
	}
	return int(size)
}

// encodeOpenError reports an encoder adapter rejecting its config at open as a
// BuildError naming the node and the stream it was given; errors.Is still
// reaches the adapter's cause.
func encodeOpenError(node string, request compile.CodecAdapterRequest, cause error) error {
	var buildErr *BuildError
	if errors.As(cause, &buildErr) {
		return cause
	}
	details := []string{"codec=" + string(request.Codec)}
	if request.SampleRate > 0 {
		details = append(details, "sample_rate="+strconv.Itoa(request.SampleRate))
	}
	if request.Channels > 0 {
		details = append(details, "channels="+strconv.Itoa(request.Channels))
	}
	if request.SampleFormat != "" {
		details = append(details, "sample_format="+request.SampleFormat)
	}
	if request.PixelFormat != "" {
		details = append(details, "pixel_format="+request.PixelFormat)
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeAdapterIncompatibleCode),
		Code:      encodeAdapterIncompatibleCode,
		Operation: "configure encode",
		Node:      node,
		Reason:    string(request.Codec) + " encoder rejected the stream: " + cause.Error(),
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"convert the stream to a format the encoder accepts (.Resample(...) for audio, .Resize(...) for video) before .Encode(...)",
			"check the encoder settings passed to codec." + strings.ToUpper(string(request.Codec)) + "(...)",
		}),
		cause: cause,
	}
}
