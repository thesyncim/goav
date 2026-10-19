package goav

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
)

func (b *builder) newDecodeStageNamed(ctx context.Context, name string, request compile.DecodeRequest, stream av.Stream, realtime bool, dropInputEvents bool, bounds codec.DecodeBounds) (*codec.DecoderStage, error) {
	name = firstNonEmpty(name, compile.DecodeNodeName(request.Selector))
	planned := recipeir.Stream{
		Name:     name,
		Selector: compile.StreamSelectFromAV(request.Selector),
	}
	if _, err := b.runtime.codecs.DecoderFactory(stream.Codec.ID); err != nil {
		// A file's codec is known only once it opens, so this is where a
		// descriptor-only decoder (H.264 without -tags goav_goh264) surfaces:
		// typed, naming the build tag, instead of a bare codec sentinel.
		return nil, publicError(compile.RecipeDecodeAdapterError("build decode stage", planned, stream.Codec.ID, b.runtime.codecs, err))
	}
	adapterRequest := compile.DecodeAdapterRequestFromStream(stream, planned)
	adapterRequest.Backend = request.Config.Settings.Backend
	adapterRequest.Realtime = realtime
	choice, err := refuse(compile.SelectCodecBackend("build decode stage", name, b.runtime.codecs, adapterRequest, codec.ModeDecode))
	if err != nil {
		return nil, err
	}
	factory := choice.Decoder
	stream = decodeStreamWithSpec(stream, request.Config)
	result := decodeResultForStream(stream, bounds)
	config := codec.DecodeConfig{
		Stream:     stream,
		Realtime:   realtime,
		LowLatency: realtime,
		Resilience: codec.ResiliencePolicy{
			AcceptLoss:       true,
			ConcealAudio:     stream.Type == av.MediaAudio,
			DropDamagedVideo: stream.Type == av.MediaVideo,
			RequestKeyframes: stream.Type == av.MediaVideo,
		},
		Bounds:   decodeBoundsForStream(stream, result, bounds),
		Settings: compile.CloneCodecSettings(request.Config.Settings),
	}
	stateFromFactory := false
	if stateFactory, ok := factory.(codec.DecodeStateFactory); ok {
		state, err := stateFactory.NewDecodeState(ctx, config)
		if err != nil {
			return nil, err
		}
		config.OpaqueState = state
		stateFromFactory = true
	}
	decoder, err := factory.NewDecoder(ctx, config)
	if err != nil {
		if stateFromFactory {
			closeDecodeState(config.OpaqueState)
		}
		return nil, decodeOpenError(name, stream, err)
	}
	stage, err := codec.NewDecoderStage(codec.DecoderStageConfig{
		Name:            name,
		Detail:          compile.DecodeRequestDetail(request),
		InputStream:     stream,
		Decoder:         decoder,
		Result:          result,
		DropInputEvents: dropInputEvents,
	})
	if err != nil {
		if decoder != nil {
			decoder.Close()
		} else if stateFromFactory {
			closeDecodeState(config.OpaqueState)
		}
		return nil, err
	}
	return stage, nil
}

func closeDecodeState(state any) {
	closer, ok := state.(interface{ Close() })
	if ok {
		closer.Close()
	}
}

func streamRequestMismatchError(code errcode.Code, operation string, node string, selector av.StreamSelector, stream av.Stream, suggestions []string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    "requested " + compile.ReadableSelector(selector) + " does not match selected stream",
		fields:    errDetails(errNote("selected: "+compile.StreamDiagnostic(stream, 0)), errNote("requested: "+compile.ReadableSelector(selector))),
		fixes:     buildErrorFixes(suggestions),
		cause:     errUnsupportedBuild,
	}
}

func decodeResultForStream(stream av.Stream, bounds codec.DecodeBounds) codec.DecodeResult {
	frameCount := positiveOrDefault(bounds.MaxFramesPerInput, 1)
	frames := make([]av.Frame, frameCount)
	for i := range frames {
		if stream.Type == av.MediaAudio || stream.Codec.Type == av.MediaAudio {
			frames[i].Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, audioDecodeBufferSize(stream))}}}
		}
		if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
			// Plane slots only: video decoders publish each picture in new
			// pooled planes (av.NewBuffer), not in reused result bytes.
			frames[i].Planes = make([]av.Plane, 3)
		}
	}
	return codec.DecodeResult{
		Frames:   frames[:0],
		Events:   make([]av.Event, 0, positiveOrDefault(bounds.MaxEventsPerInput, 1)),
		Requests: make([]codec.ControlRequest, 0, positiveOrDefault(bounds.MaxRequestsPerInput, 1)),
	}
}

func decodeStreamWithSpec(stream av.Stream, spec codec.Spec) av.Stream {
	if spec.ID == "" && spec.Type == "" && !compile.CodecSpecHasParameters(spec) {
		return stream
	}
	parameters := mergeCodecParameters(stream.Codec, spec.Parameters)
	if spec.ID != "" {
		parameters.ID = spec.ID
	}
	if spec.Type != "" && parameters.Type == "" {
		parameters.Type = spec.Type
	}
	if stream.Type == "" {
		stream.Type = parameters.Type
	}
	stream.Codec = parameters
	if stream.TimeBase == (av.TimeBase{}) && parameters.ClockRate != 0 {
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(parameters.ClockRate)}
	}
	return stream
}

func decodeBoundsForStream(stream av.Stream, result codec.DecodeResult, bounds codec.DecodeBounds) codec.DecodeBounds {
	merged := codec.DecodeBounds{
		MaxFramesPerInput:   cap(result.Frames),
		MaxEventsPerInput:   cap(result.Events),
		MaxRequestsPerInput: cap(result.Requests),
		MaxWidth:            stream.Codec.Width,
		MaxHeight:           stream.Codec.Height,
	}
	if bounds.MaxPayloadBytes > 0 {
		merged.MaxPayloadBytes = bounds.MaxPayloadBytes
	}
	if bounds.MaxRetainedBytes > 0 {
		merged.MaxRetainedBytes = bounds.MaxRetainedBytes
	}
	if bounds.MaxWidth > 0 {
		merged.MaxWidth = bounds.MaxWidth
	}
	if bounds.MaxHeight > 0 {
		merged.MaxHeight = bounds.MaxHeight
	}
	return merged
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func audioDecodeBufferSize(stream av.Stream) int {
	sampleRate := stream.Codec.SampleRate
	if sampleRate == 0 {
		sampleRate = int(stream.Codec.ClockRate)
	}
	if sampleRate == 0 {
		sampleRate = 48000
	}
	channels := stream.Codec.Channels
	if channels == 0 {
		channels = 2
	}
	maxSamples := max(sampleRate*120/1000, 960)
	return maxSamples * channels * 2
}

// decodeOpenError reports a decoder adapter refusing to open as a BuildError
// naming the node and stream; errors.Is still reaches the adapter's cause, so
// callers can tell a backend that is unavailable on this platform
// (codec.ErrUnavailable) from a stream it rejects.
func decodeOpenError(node string, stream av.Stream, cause error) error {
	var buildErr *BuildError
	if errors.As(cause, &buildErr) {
		return cause
	}
	id := string(stream.Codec.ID)
	code := decodeAdapterIncompatibleCode
	reason := id + " decoder rejected the stream: " + cause.Error()
	fixes := []string{
		"check the stream's codec parameters (extradata, profile, geometry) reach the decoder",
		"leave out .Decode() to keep packets without decoding",
	}
	if errors.Is(cause, codec.ErrUnavailable) {
		code = decodeAdapterUnavailableCode
		reason = id + " decoder is unavailable in this build: " + cause.Error()
		fixes = []string{
			"run on a platform the " + id + " backend supports, or register another " + id + " decoder with runconfig.WithDecoder(...)",
			"leave out .Decode() to keep packets without decoding",
		}
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: "build decode stage",
		Node:      node,
		Reason:    reason,
		fields:    errDetails(errNote("selected: " + compile.StreamDiagnostic(stream, 0))),
		fixes:     buildErrorFixes(fixes),
		cause:     cause,
	}
}
