package goav

import (
	"context"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
)

const (
	defaultVideoDecodeWidth  = 1920
	defaultVideoDecodeHeight = 1080
)

func (b *builder) newDecodeStage(ctx context.Context, request decodeRequest, stream av.Stream, realtime bool, dropInputEvents bool, bounds codec.DecodeBounds) (*codec.DecoderStage, error) {
	return b.newDecodeStageNamed(ctx, decodeNodeName(request.selector), request, stream, realtime, dropInputEvents, bounds)
}

func (b *builder) newDecodeStageNamed(ctx context.Context, name string, request decodeRequest, stream av.Stream, realtime bool, dropInputEvents bool, bounds codec.DecodeBounds) (*codec.DecoderStage, error) {
	factory, err := b.runtime.codecs.DecoderFactory(stream.Codec.ID)
	if err != nil {
		return nil, err
	}
	name = firstNonEmpty(name, decodeNodeName(request.selector))
	intent := streamIntent{
		Name:   name,
		Select: streamSelectFromAV(request.selector),
	}
	if err := validateDecodeAdapterDescriptors("build decode stage", intent, b.runtime.codecs, decodeAdapterRequestFromStream(stream, intent)); err != nil {
		return nil, err
	}
	stream = decodeStreamWithSpec(stream, request.config)
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
		Settings: cloneCodecSettings(request.config.Settings),
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
		return nil, err
	}
	stage, err := codec.NewDecoderStage(codec.DecoderStageConfig{
		Name:            name,
		Detail:          decodeRequestDetail(request),
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

func selectDecodeStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	return selectStreamWithCodecRequirement(streams, selector, true)
}

func selectStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	return selectStreamWithCodecRequirement(streams, selector, false)
}

func selectStreamWithCodecRequirement(streams []av.Stream, selector av.StreamSelector, requireCodec bool) (av.Stream, error) {
	var selected av.Stream
	matches := 0
	matched := make([]av.Stream, 0, len(streams))
	for i := range streams {
		if !streamMatchesSelector(streams[i], selector) {
			continue
		}
		selected = streams[i]
		matches++
		matched = append(matched, streams[i])
	}
	if matches == 0 {
		return av.Stream{}, streamSelectionError(errcode.StreamMissing, selector, streams)
	}
	if matches > 1 {
		return av.Stream{}, streamSelectionError(errcode.StreamAmbiguous, selector, matched)
	}
	if requireCodec && selected.Codec.ID == "" {
		return av.Stream{}, &BuildError{
			Family:    errcode.FamilyForCode(errcode.StreamCodecMissing),
			Code:      errcode.StreamCodecMissing,
			Operation: "select stream",
			Node:      selectorDetail(selector),
			Reason:    "selected stream has no codec id",
			Fields:    buildErrorFields([]string{streamDiagnostic(selected, 0)}),
			Fixes: buildErrorFixes([]string{
				"provide codec metadata on the input stream",
				"declare the receive codec on the source provider (e.g. a codec intent option)",
			}),
			Cause: ErrUnsupportedBuild,
		}
	}
	return selected, nil
}

func streamMatchesSelector(stream av.Stream, selector av.StreamSelector) bool {
	if selector.ID != "" && stream.ID != selector.ID {
		return false
	}
	if selectorHasIndex(selector) && stream.Index != selector.Index {
		return false
	}
	if selector.Type != "" && stream.Type != selector.Type {
		return false
	}
	if selector.Codec != "" && stream.Codec.ID != selector.Codec {
		return false
	}
	if selector.Name != "" && stream.Name != selector.Name {
		return false
	}
	return true
}

func selectorHasIndex(selector av.StreamSelector) bool {
	return selector.UseIndex || selector.Index != 0
}

func streamSelectionError(code errcode.Code, selector av.StreamSelector, streams []av.Stream) error {
	operation := "select stream"
	node := selectorDetail(selector)
	reason := "no stream matches " + readableSelector(selector)
	if code == errcode.StreamAmbiguous {
		reason = "multiple streams match " + readableSelector(selector)
	}
	return &BuildError{
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Fields:    buildErrorFields(streamDiagnostics(streams)),
		Fixes:     buildErrorFixes(streamSelectionSuggestions(selector, streams)),
		Cause:     ErrUnsupportedBuild,
	}
}

func streamRequestMismatchError(code errcode.Code, operation string, node string, selector av.StreamSelector, stream av.Stream, suggestions []string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    "requested " + readableSelector(selector) + " does not match selected stream",
		Fields: buildErrorFields([]string{
			"selected: " + streamDiagnostic(stream, 0),
			"requested: " + readableSelector(selector),
		}),
		Fixes: buildErrorFixes(suggestions),
		Cause: ErrUnsupportedBuild,
	}
}

func readableSelector(selector av.StreamSelector) string {
	detail := selectorDetail(selector)
	if detail == "" {
		return "the requested selector"
	}
	return detail
}

func streamDiagnostics(streams []av.Stream) []string {
	if len(streams) == 0 {
		return nil
	}
	details := make([]string, len(streams))
	for i := range streams {
		details[i] = streamDiagnostic(streams[i], i)
	}
	return details
}

func streamDiagnostic(stream av.Stream, fallbackIndex int) string {
	media := string(stream.Type)
	if media == "" {
		media = "stream"
	}
	index := stream.Index
	if index == 0 {
		index = fallbackIndex
	}
	parts := []string{media + "[" + strconv.Itoa(index) + "]"}
	if stream.ID != "" {
		parts = append(parts, "id="+string(stream.ID))
	}
	if stream.Codec.ID != "" {
		parts = append(parts, "codec="+string(stream.Codec.ID))
	}
	if stream.Name != "" {
		parts = append(parts, "name="+stream.Name)
	}
	if stream.Language != "" {
		parts = append(parts, "lang="+stream.Language)
	}
	return strings.Join(parts, " ")
}

func streamSelectionSuggestions(selector av.StreamSelector, streams []av.Stream) []string {
	call, recipeCall := streamOptionCall(selector)
	suggestions := make([]string, 0, 3)
	for i := range streams {
		if streams[i].ID != "" {
			if recipeCall {
				suggestions = append(suggestions, call+"(goav.StreamID("+strconv.Quote(string(streams[i].ID))+"))")
			} else {
				suggestions = append(suggestions, "select stream id "+strconv.Quote(string(streams[i].ID)))
			}
			break
		}
	}
	for i := range streams {
		if streams[i].Name != "" {
			if recipeCall {
				suggestions = append(suggestions, call+"(goav.StreamName("+strconv.Quote(streams[i].Name)+"))")
			} else {
				suggestions = append(suggestions, "select stream name "+strconv.Quote(streams[i].Name))
			}
			break
		}
	}
	if len(streams) != 0 {
		index := streams[0].Index
		if recipeCall {
			suggestions = append(suggestions, call+"(goav.StreamIndex("+strconv.Itoa(index)+"))")
		} else {
			suggestions = append(suggestions, "select stream index "+strconv.Itoa(index))
		}
	}
	if len(suggestions) == 0 {
		suggestions = append(suggestions, "choose a more specific stream selector")
	}
	return suggestions
}

func streamOptionCall(selector av.StreamSelector) (string, bool) {
	switch selector.Type {
	case av.MediaAudio:
		return ".Audio", true
	case av.MediaVideo:
		return ".Video", true
	default:
		return "", false
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
			frames[i] = preallocVideoDecodeFrame(stream, bounds)
		}
	}
	return codec.DecodeResult{
		Frames:   frames[:0],
		Events:   make([]av.Event, 0, positiveOrDefault(bounds.MaxEventsPerInput, 1)),
		Requests: make([]codec.ControlRequest, 0, positiveOrDefault(bounds.MaxRequestsPerInput, 1)),
	}
}

func preallocVideoDecodeFrame(stream av.Stream, bounds codec.DecodeBounds) av.Frame {
	width, height := videoDecodeGeometry(stream, bounds)
	frame := av.Frame{Planes: make([]av.Plane, 3)}
	frame.Planes[0].Buffer.Bytes = make([]byte, 0, width*height)
	frame.Planes[1].Buffer.Bytes = make([]byte, 0, ((width+1)/2)*((height+1)/2))
	frame.Planes[2].Buffer.Bytes = make([]byte, 0, ((width+1)/2)*((height+1)/2))
	return frame
}

func videoDecodeGeometry(stream av.Stream, bounds codec.DecodeBounds) (int, int) {
	width := positiveOrDefault(bounds.MaxWidth, stream.Codec.Width)
	height := positiveOrDefault(bounds.MaxHeight, stream.Codec.Height)
	if width <= 0 {
		width = defaultVideoDecodeWidth
	}
	if height <= 0 {
		height = defaultVideoDecodeHeight
	}
	return width, height
}

func decodeStreamWithSpec(stream av.Stream, spec codec.CodecSpec) av.Stream {
	if spec.ID == "" && spec.Type == "" && !codecSpecHasParameters(spec) {
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
	maxSamples := sampleRate * 120 / 1000
	if maxSamples < 960 {
		maxSamples = 960
	}
	return maxSamples * channels * 2
}

func decodeNodeName(selector av.StreamSelector) string {
	if selector.Name != "" {
		return "decode-" + selector.Name
	}
	if selector.ID != "" {
		return "decode-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "decode-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "decode-" + string(selector.Type)
	}
	if selectorHasIndex(selector) {
		return "decode-" + strconv.Itoa(selector.Index)
	}
	return "decode"
}
