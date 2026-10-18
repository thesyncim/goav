// Build-time validation that the runtime has decode and encode adapters for the planned intent.

package goav

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/recipeir"
)

func validateRecipeIRDecodeAdapters(operation string, rt *Runtime, inputs []inputIntent, streams []recipeir.Stream) error {
	if rt == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		if !recipeIRStreamNeedsDecode(stream) {
			continue
		}
		streamIntent := streamIntentFromRecipeIR(stream)
		request, ok := liveDecodeAdapterRequest(inputs, streamIntent)
		if !ok || request.Codec == "" {
			continue
		}
		if _, err := rt.codecs.DecoderFactory(request.Codec); err != nil {
			return recipeDecodeAdapterError(operation, streamIntent, request.Codec, rt.codecs, err)
		}
		if err := validateDecodeAdapterDescriptors(operation, streamIntent, rt.codecs, request); err != nil {
			return err
		}
	}
	return nil
}

func validateKnownRecipeIRDecodeAdapters(operation string, rt *Runtime, probes []format.ProbeResult, streams []recipeir.Stream) error {
	if rt == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		if !recipeIRStreamNeedsDecode(stream) {
			continue
		}
		streamIntent := streamIntentFromRecipeIR(stream)
		selected, ok := knownProbeDecodeStream(probes, streamIntent)
		if !ok || selected.Codec.ID == "" {
			continue
		}
		if _, err := rt.codecs.DecoderFactory(selected.Codec.ID); err != nil {
			return recipeDecodeAdapterError(operation, streamIntent, selected.Codec.ID, rt.codecs, err)
		}
		request := decodeAdapterRequestFromStream(selected, streamIntent)
		if err := validateDecodeAdapterDescriptors(operation, streamIntent, rt.codecs, request); err != nil {
			return err
		}
	}
	return nil
}

func validateLiveRecipeStreamSelection(inputs []inputIntent, stream recipeir.Stream) error {
	streams := liveIntentStreams(inputs)
	if len(streams) == 0 {
		return nil
	}
	_, err := selectDecodeStream(streams, recipeIRStreamSelector(stream))
	return err
}

func liveIntentStreams(inputs []inputIntent) []av.Stream {
	streams := make([]av.Stream, 0, len(inputs))
	for i := range inputs {
		stream, ok := liveIntentStream(inputs[i], i)
		if !ok {
			continue
		}
		streams = append(streams, stream)
	}
	return streams
}

func liveIntentStream(input inputIntent, index int) (av.Stream, bool) {
	if !input.Realtime || input.Codec.ID == "" {
		return av.Stream{}, false
	}
	stream := av.Stream{
		Index: index,
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
	return stream, true
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

func recipeDecodeAdapterError(operation string, stream streamIntent, codecID av.CodecID, registry *codec.SimpleRegistry, cause error) error {
	code := decodeAdapterMissingCode
	reason := "no decoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = decodeAdapterUnavailableCode
		reason = string(codecID) + " decoder adapter is descriptor-only in this build"
	}
	details := []string{"codec=" + string(codecID)}
	tagFix := "enable the adapter build tag or choose a runtime with a concrete decoder"
	if registry != nil {
		descriptors, err := registry.Find(codecID, codec.ModeDecode)
		if err == nil {
			details = append(details, codecDescriptorDetails(descriptors)...)
			for i := range descriptors {
				if tags := descriptors[i].Capabilities.BuildTags; len(tags) != 0 {
					tagFix = "build with -tags " + strings.Join(tags, ",") + " to include the " + string(codecID) + " decoder"
					break
				}
			}
		}
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    reason,
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			tagFix,
			"register a codec adapter that provides a " + string(codecID) + " decoder",
			"use input.To(output) for packet-preserving receive when decoding is not needed",
		}),
		cause: cause,
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
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(decodeAdapterIncompatibleCode),
		Code:      decodeAdapterIncompatibleCode,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    string(request.Codec) + " decoder adapter does not support the requested " + label,
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"choose a decoder adapter that supports this " + label,
			"fix the input stream metadata if it describes the wrong media or frame format",
			"fix the codec descriptor if the implementation already supports this config",
		}),
		cause: errUnsupportedBuild,
	}
}

func validateRecipeIREncodeAdapters(operation string, rt *Runtime, streams []recipeir.Stream) error {
	if rt == nil {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		codecID := recipeIRStreamEncodeSpec(stream).ID
		if codecID == "" {
			continue
		}
		streamIntent := streamIntentFromRecipeIR(stream)
		if _, err := rt.codecs.EncoderFactory(codecID); err != nil {
			return recipeEncodeAdapterError(operation, streamIntent, rt.codecs, err)
		}
		request := encodeAdapterRequestFromRecipeIRStream(stream)
		if err := validateEncodeAdapterDescriptors(operation, streamIntent, rt.codecs, request); err != nil {
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
	SampleRate   int
	Channels     int
}

func encodeAdapterRequestFromRecipeIRStream(stream recipeir.Stream) codecAdapterRequest {
	encode := recipeIRStreamEncodeSpec(stream)
	return codecAdapterRequest{
		Codec:        encode.ID,
		Media:        firstNonEmptyMedia(encode.Type, encode.Parameters.Type, stream.Selector.Type, codecMedia(encode.ID)),
		SampleFormat: firstNonEmpty(encode.Parameters.SampleFormat, recipeIRStreamSampleFormat(stream)),
		PixelFormat:  firstNonEmpty(encode.Parameters.PixelFormat, recipeIRStreamPixelFormat(stream)),
		SampleRate:   firstPositiveInt(encode.Parameters.SampleRate, recipeIRStreamResample(stream).SampleRate),
		Channels:     firstPositiveInt(encode.Parameters.Channels, recipeIRStreamResample(stream).Channels),
	}
}

func encodeAdapterRequestFromPreparedStream(spec codec.Spec, stream av.Stream) codecAdapterRequest {
	return codecAdapterRequest{
		Codec:        spec.ID,
		Media:        firstNonEmptyMedia(spec.Type, spec.Parameters.Type, stream.Type, stream.Codec.Type, codecMedia(spec.ID)),
		SampleFormat: firstNonEmpty(spec.Parameters.SampleFormat, stream.Codec.SampleFormat),
		PixelFormat:  firstNonEmpty(spec.Parameters.PixelFormat, stream.Codec.PixelFormat),
		SampleRate:   firstPositiveInt(spec.Parameters.SampleRate, stream.Codec.SampleRate),
		Channels:     firstPositiveInt(spec.Parameters.Channels, stream.Codec.Channels),
	}
}

// recipeIRStreamResample is the stream's last resample request: the plan-time
// source of the encoder's input rate when the input shape does not declare it.
func recipeIRStreamResample(stream recipeir.Stream) filter.ResampleConfig {
	transforms := recipeIRStreamTransforms(stream)
	for _, transform := range slices.Backward(transforms) {
		if transform.Kind == recipeir.TransformResample {
			return transform.Resample
		}
	}
	return filter.ResampleConfig{}
}

func recipeIRStreamSampleFormat(stream recipeir.Stream) string {
	transforms := recipeIRStreamTransforms(stream)
	for _, transform := range slices.Backward(transforms) {
		if transform.Kind == recipeir.TransformResample && transform.Resample.SampleFormat != "" {
			return transform.Resample.SampleFormat
		}
	}
	return ""
}

func recipeIRStreamPixelFormat(stream recipeir.Stream) string {
	transforms := recipeIRStreamTransforms(stream)
	for _, transform := range slices.Backward(transforms) {
		if transform.Kind == recipeir.TransformResize && transform.Resize.PixelFormat != "" {
			return transform.Resize.PixelFormat
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
	if request.SampleRate > 0 && len(desc.Capabilities.SampleRates) != 0 && !slices.Contains(desc.Capabilities.SampleRates, request.SampleRate) {
		return false
	}
	return true
}

func encodeAdapterIncompatibleError(operation string, stream streamIntent, request codecAdapterRequest, descriptors []codec.Descriptor) error {
	return encodeAdapterIncompatibleErrorAt(operation, jobStreamIntentName(stream), request, descriptors)
}

// encodeAdapterIncompatibleErrorAt is the encoder-descriptor refusal for one
// node: planning reports it from shape facts, and encoder open reports it
// from the real stream when facts arrive only with the opened input.
func encodeAdapterIncompatibleErrorAt(operation string, node string, request codecAdapterRequest, descriptors []codec.Descriptor) error {
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
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(encodeAdapterIncompatibleCode),
		Code:      encodeAdapterIncompatibleCode,
		Operation: operation,
		Node:      node,
		Reason:    string(request.Codec) + " encoder adapter does not support the requested " + label,
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			resampleFixHint(field, request, descriptors),
			"choose an encoder adapter that supports this " + label,
			"change the operation spec chain so the encoder receives one of the supported formats",
			"fix the codec descriptor if the implementation already supports this config",
		}),
		cause: errUnsupportedBuild,
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
	if rates := descriptorSupportedSampleRates(mediaDescriptors); request.SampleRate > 0 && len(rates) != 0 && !slices.Contains(rates, request.SampleRate) {
		return "sample_rate", strconv.Itoa(request.SampleRate), joinInts(rates)
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

func recipeEncodeAdapterError(operation string, stream streamIntent, registry *codec.SimpleRegistry, cause error) error {
	return recipeEncodeAdapterCodecError(operation, stream, chainEncodeSpec(stream.Operations).ID, registry, cause)
}

func recipeEncodeAdapterCodecError(operation string, stream streamIntent, codecID av.CodecID, registry *codec.SimpleRegistry, cause error) error {
	code := encodeAdapterMissingCode
	reason := "no encoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = encodeAdapterUnavailableCode
		reason = string(codecID) + " encoder adapter is descriptor-only in this build"
	}
	details := []string{"codec=" + string(codecID)}
	if registry != nil {
		descriptors, err := registry.Find(codecID, codec.ModeEncode)
		if err == nil {
			details = append(details, codecDescriptorDetails(descriptors)...)
		}
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    reason,
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"register a " + string(codecID) + " encoder with goav.New(runconfig.WithEncoder(...)) or a codec adapter that provides one",
			"use .Decode().To(goav.Sink(...)) to receive decoded frames without encoding",
			"leave out .Decode() for packet-preserving output when re-encoding is not needed: input.Video().To(output)",
		}),
		cause: cause,
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

// resampleFixHint names the exact conversion for a sample-rate refusal: the
// supported rate closest above the requested one (else the highest), at the
// stream's channel count. Other fields get no hint (an empty fix is dropped).
func resampleFixHint(field string, request codecAdapterRequest, descriptors []codec.Descriptor) string {
	rates := descriptorSupportedSampleRates(descriptors)
	if field != "sample_rate" || len(rates) == 0 {
		return ""
	}
	target := rates[len(rates)-1]
	for _, rate := range rates {
		if rate >= request.SampleRate {
			target = rate
			break
		}
	}
	channels := max(request.Channels, 1)
	return "insert .Resample(" + strconv.Itoa(target) + ", " + strconv.Itoa(channels) + ") before .Encode(...)"
}

// descriptorSupportedSampleRates is the sorted union of declared rates; empty
// when any descriptor accepts every rate.
func descriptorSupportedSampleRates(descriptors []codec.Descriptor) []int {
	var rates []int
	for i := range descriptors {
		if len(descriptors[i].Capabilities.SampleRates) == 0 {
			return nil
		}
		rates = append(rates, descriptors[i].Capabilities.SampleRates...)
	}
	slices.Sort(rates)
	return slices.Compact(rates)
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}
