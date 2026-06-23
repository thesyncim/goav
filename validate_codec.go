// Build-time validation that the runtime has decode and encode adapters for the planned intent.

package goav

import (
	"errors"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
)

func validateRecipeDecodeAdapters(operation string, rt Runtime, intent intent) error {
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

func streamNeedsDecode(stream streamIntent) bool {
	return chainHasDecode(stream.Operations) ||
		len(streamIntentTransformSpecs(stream)) != 0 ||
		chainEncodeSpec(stream.Operations).ID != ""
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
	code := errcode.DecodeAdapterMissing
	reason := "no decoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = errcode.DecodeAdapterUnavailable
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
		Code:      errcode.DecodeAdapterIncompatible,
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
		codecID := chainEncodeSpec(stream.Operations).ID
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
	encode := chainEncodeSpec(stream.Operations)
	return codecAdapterRequest{
		Codec:        encode.ID,
		Media:        firstNonEmptyMedia(encode.Type, encode.Parameters.Type, stream.Select.Type, codecMedia(encode.ID)),
		SampleFormat: firstNonEmpty(encode.Parameters.SampleFormat, streamIntentSampleFormat(stream)),
		PixelFormat:  firstNonEmpty(encode.Parameters.PixelFormat, streamIntentPixelFormat(stream)),
	}
}

func encodeAdapterRequestFromPreparedStream(spec codec.CodecSpec, stream av.Stream) codecAdapterRequest {
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
		Code:      errcode.EncodeAdapterIncompatible,
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

func recipeEncodeAdapterError(operation string, stream streamIntent, registry *codec.SimpleRegistry, cause error) error {
	codecID := chainEncodeSpec(stream.Operations).ID
	code := errcode.EncodeAdapterMissing
	reason := "no encoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = errcode.EncodeAdapterUnavailable
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
		Code:      code,
		Operation: operation,
		Node:      jobStreamIntentName(stream),
		Reason:    reason,
		Details:   details,
		Suggestions: []string{
			"register a " + string(codecID) + " encoder with goav.New(goav.WithEncoder(...)) or a codec adapter that provides one",
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
