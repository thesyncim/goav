package compile

import (
	"cmp"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
)

func validateRecipeIRDecodeAdapters(operation string, env Env, inputs []recipeir.Input, streams []recipeir.Stream) error {
	if !env.Present {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		if !recipeIRStreamNeedsDecode(stream) {
			continue
		}
		request, ok := liveDecodeAdapterRequest(inputs, stream)
		if !ok || request.Codec == "" {
			continue
		}
		if _, err := env.Codecs.DecoderFactory(request.Codec); err != nil {
			return RecipeDecodeAdapterError(operation, stream, request.Codec, env.Codecs, err)
		}
		if err := ValidateDecodeAdapterDescriptors(operation, stream, env.Codecs, request); err != nil {
			return err
		}
	}
	return nil
}

func validateKnownRecipeIRDecodeAdapters(operation string, env Env, probes []format.ProbeResult, streams []recipeir.Stream) error {
	if !env.Present {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		if !recipeIRStreamNeedsDecode(stream) {
			continue
		}
		selected, ok := knownProbeDecodeStream(probes, stream)
		if !ok || selected.Codec.ID == "" {
			continue
		}
		if _, err := env.Codecs.DecoderFactory(selected.Codec.ID); err != nil {
			return RecipeDecodeAdapterError(operation, stream, selected.Codec.ID, env.Codecs, err)
		}
		request := DecodeAdapterRequestFromStream(selected, stream)
		if err := ValidateDecodeAdapterDescriptors(operation, stream, env.Codecs, request); err != nil {
			return err
		}
	}
	return nil
}

func validateLiveRecipeStreamSelection(inputs []recipeir.Input, stream recipeir.Stream) error {
	streams := LiveInputStreams(inputs)
	if len(streams) == 0 {
		return nil
	}
	_, err := SelectDecodeStream(streams, recipeIRStreamSelector(stream))
	return err
}

func LiveInputStreams(inputs []recipeir.Input) []av.Stream {
	streams := make([]av.Stream, 0, len(inputs))
	for i := range inputs {
		stream, ok := liveInputStream(inputs[i], i)
		if !ok {
			continue
		}
		streams = append(streams, stream)
	}
	return streams
}

func liveInputStream(input recipeir.Input, index int) (av.Stream, bool) {
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

func knownProbeDecodeStream(probes []format.ProbeResult, stream recipeir.Stream) (av.Stream, bool) {
	candidates := make([]av.Stream, 0, len(probes))
	selector := recipeIRStreamSelector(stream)
	for i := range probes {
		if len(probes[i].Streams) == 0 {
			continue
		}
		selected, err := SelectDecodeStream(probes[i].Streams, selector)
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

func liveDecodeAdapterRequest(inputs []recipeir.Input, stream recipeir.Stream) (CodecAdapterRequest, bool) {
	selected, ok := liveDecodeStream(inputs, stream)
	if !ok {
		return CodecAdapterRequest{}, false
	}
	return DecodeAdapterRequestFromStream(selected, stream), true
}

func liveDecodeStream(inputs []recipeir.Input, stream recipeir.Stream) (av.Stream, bool) {
	streams := LiveInputStreams(inputs)
	if len(streams) == 0 {
		return av.Stream{}, false
	}
	selected, err := SelectDecodeStream(streams, recipeIRStreamSelector(stream))
	if err != nil || selected.Codec.ID == "" {
		return av.Stream{}, false
	}
	return selected, true
}

func RecipeDecodeAdapterError(operation string, stream recipeir.Stream, codecID av.CodecID, registry *codec.SimpleRegistry, cause error) error {
	code := builderr.DecodeAdapterMissingCode
	reason := "no decoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = builderr.DecodeAdapterUnavailableCode
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
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    reason,
		Details:   builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			tagFix,
			"register a codec adapter that provides a " + string(codecID) + " decoder",
			"use input.To(output) for packet-preserving receive when decoding is not needed",
		}),
		Cause: cause,
	}
}

func ValidateDecodeAdapterDescriptors(operation string, stream recipeir.Stream, registry *codec.SimpleRegistry, request CodecAdapterRequest) error {
	return validateCodecBackend(operation, recipeIRStreamName(stream), registry, request, codec.ModeDecode)
}

// validateCodecBackend refuses a request no registered backend serves. A
// codec with no backend at all passes: the missing-adapter checks own that
// refusal.
func validateCodecBackend(operation string, node string, registry *codec.SimpleRegistry, request CodecAdapterRequest, mode codec.Mode) error {
	if registry == nil || request.Codec == "" {
		return nil
	}
	_, err := SelectCodecBackend(operation, node, registry, request, mode)
	var refusal *builderr.Refusal
	if errors.As(err, &refusal) {
		return err
	}
	return nil
}

func DecodeAdapterRequestFromStream(stream av.Stream, planned recipeir.Stream) CodecAdapterRequest {
	return CodecAdapterRequest{
		Codec:        stream.Codec.ID,
		Media:        FirstNonEmptyMedia(stream.Codec.Type, stream.Type, planned.Selector.Type, CodecMedia(stream.Codec.ID)),
		SampleFormat: stream.Codec.SampleFormat,
		PixelFormat:  stream.Codec.PixelFormat,
		Backend:      recipeIRStreamDecodeSpec(planned).Settings.Backend,
		Width:        stream.Codec.Width,
		Height:       stream.Codec.Height,
	}
}

// recipeIRStreamDecodeSpec is the stream's decode spec: the settings its
// .Decode(...) carries.
func recipeIRStreamDecodeSpec(stream recipeir.Stream) codec.Spec {
	for i := range stream.Operations {
		if stream.Operations[i].Kind == plan.OpDecode {
			return stream.Operations[i].Decode
		}
	}
	return codec.Spec{}
}

func decodeAdapterIncompatibleErrorAt(operation string, node string, request CodecAdapterRequest, descriptors []codec.Descriptor) error {
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
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DecodeAdapterIncompatibleCode),
		Code:      builderr.DecodeAdapterIncompatibleCode,
		Operation: operation,
		Node:      node,
		Reason:    string(request.Codec) + " decoder adapter does not support the requested " + label,
		Details:   builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			"choose a decoder adapter that supports this " + label,
			"fix the input stream metadata if it describes the wrong media or frame format",
			"fix the codec descriptor if the implementation already supports this config",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func validateRecipeIREncodeAdapters(operation string, env Env, streams []recipeir.Stream) error {
	if !env.Present {
		return nil
	}
	for i := range streams {
		stream := streams[i]
		codecID := recipeIRStreamEncodeSpec(stream).ID
		if codecID == "" {
			continue
		}
		if _, err := env.Codecs.EncoderFactory(codecID); err != nil {
			return recipeEncodeAdapterError(operation, stream, env.Codecs, err)
		}
		request := encodeAdapterRequestFromRecipeIRStream(stream)
		if err := ValidateEncodeAdapterDescriptors(operation, stream, env.Codecs, request); err != nil {
			return err
		}
	}
	return nil
}

// CodecAdapterRequest is what one decode or encode asks of the backend that
// serves it: the stream facts known when it is checked, and the backend,
// profile, and bit depth its settings request.
type CodecAdapterRequest struct {
	Codec        av.CodecID
	Media        av.MediaType
	SampleFormat string
	PixelFormat  string
	SampleRate   int
	Channels     int
	Backend      string
	Profile      string
	BitDepth     int
	Width        int
	Height       int
	Realtime     bool
	// Settings are the request's codec settings, checked against the chosen
	// backend's knobs.
	Settings codec.CodecSettings
}

// Need is the request as a backend-selection need.
func (r CodecAdapterRequest) Need(mode codec.Mode) codec.Need {
	return codec.Need{
		ID:           r.Codec,
		Mode:         mode,
		Backend:      r.Backend,
		Media:        r.Media,
		Profile:      r.Profile,
		PixelFormat:  r.PixelFormat,
		SampleFormat: r.SampleFormat,
		SampleRate:   r.SampleRate,
		BitDepth:     r.BitDepth,
		Width:        r.Width,
		Height:       r.Height,
		Realtime:     r.Realtime,
		Settings:     r.Settings.KnobSettings(),
	}
}

func encodeAdapterRequestFromRecipeIRStream(stream recipeir.Stream) CodecAdapterRequest {
	return EncodeAdapterRequestFromRecipeIR(recipeIRStreamEncodeSpec(stream), stream)
}

// EncodeAdapterRequestFromRecipeIR is the plan-time request of encode on
// stream: the stream's transforms supply the facts the spec leaves open.
func EncodeAdapterRequestFromRecipeIR(encode codec.Spec, stream recipeir.Stream) CodecAdapterRequest {
	resize := recipeIRStreamResize(stream)
	return CodecAdapterRequest{
		Codec:        encode.ID,
		Media:        FirstNonEmptyMedia(encode.Type, encode.Parameters.Type, stream.Selector.Type, CodecMedia(encode.ID)),
		SampleFormat: cmp.Or(encode.Parameters.SampleFormat, recipeIRStreamSampleFormat(stream)),
		PixelFormat:  cmp.Or(encode.Parameters.PixelFormat, recipeIRStreamPixelFormat(stream)),
		SampleRate:   firstPositiveInt(encode.Parameters.SampleRate, recipeIRStreamResample(stream).SampleRate),
		Channels:     firstPositiveInt(encode.Parameters.Channels, recipeIRStreamResample(stream).Channels),
		Backend:      encode.Settings.Backend,
		Profile:      encode.Settings.Profile,
		BitDepth:     encode.Settings.BitDepth,
		Width:        firstPositiveInt(encode.Parameters.Width, resize.Width),
		Height:       firstPositiveInt(encode.Parameters.Height, resize.Height),
		Settings:     encode.Settings,
	}
}

func EncodeAdapterRequestFromPreparedStream(spec codec.Spec, stream av.Stream) CodecAdapterRequest {
	return CodecAdapterRequest{
		Codec:        spec.ID,
		Media:        FirstNonEmptyMedia(spec.Type, spec.Parameters.Type, stream.Type, stream.Codec.Type, CodecMedia(spec.ID)),
		SampleFormat: cmp.Or(spec.Parameters.SampleFormat, stream.Codec.SampleFormat),
		PixelFormat:  cmp.Or(spec.Parameters.PixelFormat, stream.Codec.PixelFormat),
		SampleRate:   firstPositiveInt(spec.Parameters.SampleRate, stream.Codec.SampleRate),
		Channels:     firstPositiveInt(spec.Parameters.Channels, stream.Codec.Channels),
		Backend:      spec.Settings.Backend,
		Profile:      spec.Settings.Profile,
		BitDepth:     spec.Settings.BitDepth,
		Width:        firstPositiveInt(spec.Parameters.Width, stream.Codec.Width),
		Height:       firstPositiveInt(spec.Parameters.Height, stream.Codec.Height),
		Settings:     spec.Settings,
	}
}

// recipeIRStreamResize is the stream's last resize: the plan-time source of
// the encoder's geometry when the input shape does not declare it.
func recipeIRStreamResize(stream recipeir.Stream) filter.ResizeConfig {
	transforms := recipeIRStreamTransforms(stream)
	for _, transform := range slices.Backward(transforms) {
		if transform.Kind == recipeir.TransformResize {
			return transform.Resize
		}
	}
	return filter.ResizeConfig{}
}

// SelectCodecBackend picks the backend serving request in mode, refusing
// with a typed build error when a named backend is missing or cannot serve
// the request (naming the backends that can), or when no backend fits. A
// codec with no backend at all returns the bare selection error: callers
// report that as the missing-adapter refusal.
func SelectCodecBackend(operation string, node string, registry *codec.SimpleRegistry, request CodecAdapterRequest, mode codec.Mode) (codec.Choice, error) {
	choice, err := registry.Select(request.Need(mode))
	if err == nil {
		if scopeErr := registry.CheckScopes(request.Codec, request.Settings); scopeErr != nil {
			return codec.Choice{}, codecKnobRefusal(operation, node, mode, choice.Descriptor, scopeErr)
		}
		if _, knobErr := choice.Descriptor.ResolveKnobs(request.Settings, request.Realtime); knobErr != nil {
			return codec.Choice{}, codecKnobRefusal(operation, node, mode, choice.Descriptor, knobErr)
		}
		return choice, nil
	}
	var failure *codec.SelectError
	if !errors.As(err, &failure) {
		return codec.Choice{}, err
	}
	if request.Backend != "" {
		return codec.Choice{}, codecBackendRefusal(operation, node, request, mode, failure)
	}
	if !errors.Is(failure.Err, codec.ErrUnsupportedFormat) {
		return codec.Choice{}, err
	}
	descriptors := make([]codec.Descriptor, len(failure.Rejected))
	for i := range failure.Rejected {
		descriptors[i] = failure.Rejected[i].Descriptor
	}
	if mode == codec.ModeDecode {
		return codec.Choice{}, decodeAdapterIncompatibleErrorAt(operation, node, request, descriptors)
	}
	return codec.Choice{}, EncodeAdapterIncompatibleErrorAt(operation, node, request, descriptors)
}

// codecBackendRefusal refuses a request naming a backend the registry does
// not hold, or one whose declared capabilities do not fit, and names the
// backends that would serve the request instead.
func codecBackendRefusal(operation string, node string, request CodecAdapterRequest, mode codec.Mode, failure *codec.SelectError) error {
	noun, code := "encoder", builderr.EncodeAdapterMissingCode
	if mode == codec.ModeDecode {
		noun, code = "decoder", builderr.DecodeAdapterMissingCode
	}
	reason := "no " + string(request.Codec) + " " + noun + " backend named " + strconv.Quote(request.Backend) + " is registered"
	if len(failure.Rejected) != 0 {
		code = builderr.EncodeAdapterIncompatibleCode
		if mode == codec.ModeDecode {
			code = builderr.DecodeAdapterIncompatibleCode
		}
		reason = string(request.Codec) + " " + noun + " backend " + request.Backend + " cannot serve the request: " + failure.Rejected[0].Reason
	}
	details := []string{"codec=" + string(request.Codec), "backend=" + request.Backend}
	fixes := []string{"register the " + request.Backend + " backend with the runtime"}
	if len(failure.Satisfying) != 0 {
		details = append(details, "satisfying="+codec.BackendList(failure.Satisfying))
		fixes = []string{"name a backend that serves this request: " + codec.BackendList(failure.Satisfying)}
	}
	fixes = append(fixes, "leave out codec.Backend(...) to select by the runtime's preference order")
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Details:   builderr.DetailLines(details),
		Fixes:     builderr.Fixes(fixes),
		Cause:     failure,
	}
}

// EncodeBackendDetail is the Describe detail naming the backend that serves
// request at plan time, "backend=videotoolbox (hardware)", or "" when none
// serves it yet.
func EncodeBackendDetail(registry *codec.SimpleRegistry, request CodecAdapterRequest) string {
	if registry == nil || request.Codec == "" {
		return ""
	}
	choice, err := registry.Select(request.Need(codec.ModeEncode))
	if err != nil {
		return ""
	}
	return BackendDetail(choice.Descriptor)
}

// BackendDetail is the node detail naming a codec backend.
func BackendDetail(desc codec.Descriptor) string {
	if desc.Backend.Name == "" {
		return ""
	}
	return "backend=" + desc.Backend.Label()
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

func ValidateEncodeAdapterDescriptors(operation string, stream recipeir.Stream, registry *codec.SimpleRegistry, request CodecAdapterRequest) error {
	return validateCodecBackend(operation, recipeIRStreamName(stream), registry, request, codec.ModeEncode)
}

// CodecDescriptorSupports reports whether desc's declared capabilities serve
// request (codec.Descriptor.Check).
func CodecDescriptorSupports(desc codec.Descriptor, request CodecAdapterRequest) bool {
	return desc.Check(request.Need("")) == nil
}

// EncodeAdapterIncompatibleErrorAt is the encoder-descriptor refusal for one
// node: planning reports it from shape facts, and encoder open reports it
// from the real stream when facts arrive only with the opened input.
func EncodeAdapterIncompatibleErrorAt(operation string, node string, request CodecAdapterRequest, descriptors []codec.Descriptor) error {
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
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.EncodeAdapterIncompatibleCode),
		Code:      builderr.EncodeAdapterIncompatibleCode,
		Operation: operation,
		Node:      node,
		Reason:    string(request.Codec) + " encoder adapter does not support the requested " + label,
		Details:   builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			resampleFixHint(field, request, descriptors),
			"choose an encoder adapter that supports this " + label,
			"change the operation spec chain so the encoder receives one of the supported formats",
			"fix the codec descriptor if the implementation already supports this config",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func codecAdapterIncompatibilityField(request CodecAdapterRequest, descriptors []codec.Descriptor) (string, string, string) {
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
	if request.Profile != "" && !anyDescriptorServes(mediaDescriptors, codec.Need{Profile: request.Profile}) {
		var profiles []string
		for i := range mediaDescriptors {
			profiles = MergeStringList(profiles, mediaDescriptors[i].Profiles)
		}
		return "profile", request.Profile, strings.Join(profiles, ",")
	}
	if request.BitDepth > 0 && !anyDescriptorServes(mediaDescriptors, codec.Need{BitDepth: request.BitDepth}) {
		var depths []int
		for i := range mediaDescriptors {
			if d := mediaDescriptors[i].Capabilities.BitDepths; len(d) != 0 {
				depths = append(depths, d...)
			} else {
				depths = append(depths, 8)
			}
		}
		slices.Sort(depths)
		return "bit_depth", strconv.Itoa(request.BitDepth), joinInts(slices.Compact(depths))
	}
	if !anyDescriptorServes(mediaDescriptors, codec.Need{Width: request.Width, Height: request.Height}) {
		return "size", strconv.Itoa(request.Width) + "x" + strconv.Itoa(request.Height), descriptorMaxSizes(mediaDescriptors)
	}
	return "codec", string(request.Codec), string(request.Codec)
}

func anyDescriptorServes(descriptors []codec.Descriptor, need codec.Need) bool {
	return slices.ContainsFunc(descriptors, func(desc codec.Descriptor) bool { return desc.Check(need) == nil })
}

func descriptorMaxSizes(descriptors []codec.Descriptor) string {
	sizes := make([]string, 0, len(descriptors))
	for i := range descriptors {
		if c := descriptors[i].Capabilities; c.MaxWidth > 0 || c.MaxHeight > 0 {
			sizes = append(sizes, "max "+strconv.Itoa(c.MaxWidth)+"x"+strconv.Itoa(c.MaxHeight))
		}
	}
	return strings.Join(sizes, ",")
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
		if media == "" || MediaAllowed(out, media) {
			continue
		}
		out = append(out, media)
	}
	return out
}

func descriptorSupportedSampleFormats(descriptors []codec.Descriptor) []string {
	var out []string
	for i := range descriptors {
		out = MergeStringList(out, descriptors[i].Capabilities.SampleFormats)
	}
	return out
}

func descriptorSupportedPixelFormats(descriptors []codec.Descriptor) []string {
	var out []string
	for i := range descriptors {
		out = MergeStringList(out, descriptors[i].Capabilities.PixelFormats)
	}
	return out
}

func MergeStringList(existing []string, next []string) []string {
	for i := range next {
		if next[i] == "" || stringAllowed(existing, next[i]) {
			continue
		}
		existing = append(existing, next[i])
	}
	return existing
}

func recipeEncodeAdapterError(operation string, stream recipeir.Stream, registry *codec.SimpleRegistry, cause error) error {
	return RecipeEncodeAdapterCodecError(operation, stream, OperationsEncodeSpec(stream.Operations).ID, registry, cause)
}

func RecipeEncodeAdapterCodecError(operation string, stream recipeir.Stream, codecID av.CodecID, registry *codec.SimpleRegistry, cause error) error {
	code := builderr.EncodeAdapterMissingCode
	reason := "no encoder adapter is registered for " + string(codecID)
	if errors.Is(cause, codec.ErrUnavailable) {
		code = builderr.EncodeAdapterUnavailableCode
		reason = string(codecID) + " encoder adapter is descriptor-only in this build"
	}
	details := []string{"codec=" + string(codecID)}
	if registry != nil {
		descriptors, err := registry.Find(codecID, codec.ModeEncode)
		if err == nil {
			details = append(details, codecDescriptorDetails(descriptors)...)
		}
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      recipeIRStreamName(stream),
		Reason:    reason,
		Details:   builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			"register a " + string(codecID) + " encoder with goav.New(runconfig.WithEncoder(...)) or a codec adapter that provides one",
			"use .Decode().To(goav.Sink(...)) to receive decoded frames without encoding",
			"leave out .Decode() for packet-preserving output when re-encoding is not needed: input.Video().To(output)",
		}),
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

// resampleFixHint names the exact conversion for a sample-rate refusal: the
// supported rate closest above the requested one (else the highest), at the
// stream's channel count. Other fields get no hint (an empty fix is dropped).
func resampleFixHint(field string, request CodecAdapterRequest, descriptors []codec.Descriptor) string {
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

// recipeIRStreamNeedsDecode reports a branch whose work needs its stream
// decoded: a decode, a transform, or an encode.
func recipeIRStreamNeedsDecode(stream recipeir.Stream) bool {
	return recipeIRStreamHasDecode(stream) ||
		len(recipeIRStreamTransforms(stream)) != 0 ||
		recipeIRStreamEncodeSpec(stream).ID != ""
}

// codecKnobRefusal refuses encode settings the chosen backend's knobs reject:
// an unknown name (with the nearest knob) or a value outside its range.
func codecKnobRefusal(operation string, node string, mode codec.Mode, desc codec.Descriptor, cause error) error {
	code := builderr.EncodeParameterInvalidCode
	if mode == codec.ModeDecode {
		code = builderr.DecodeAdapterIncompatibleCode
	}
	knobs := make([]string, len(desc.Knobs))
	for i := range desc.Knobs {
		knobs[i] = desc.Knobs[i].String()
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    cause.Error(),
		Details:   builderr.DetailLines(append([]string{"backend=" + desc.Backend.Label()}, knobs...)),
		Fixes: builderr.Fixes([]string{
			"set only the settings the " + desc.Backend.Name + " backend lists",
			"scope a setting meant for another backend as <backend>.<setting>, or name that backend with codec.Backend(...)",
		}),
		Cause: cause,
	}
}
