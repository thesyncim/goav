package rtpav

import (
	"context"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// BufferLimits bounds the reusable scratch capacity of a receive Source: ready
// packets per jitter push, events, feedback packets, and depacketized packets.
// Zero fields keep the Source defaults.
type BufferLimits struct {
	MaxReady    int
	MaxEvents   int
	MaxFeedback int
	MaxPackets  int
}

// ReceiveOption configures an Input under construction.
type ReceiveOption func(*Input)

// Input is the RTP receive provider: it owns a PacketReader plus receive
// intent (jitter buffer, feedback writer, codec, buffer limits, decode
// bounds, timestamp-gap policy) and opens it as a running pipeline source.
type Input struct {
	name          string
	receiver      PacketReader
	feedback      FeedbackWriter
	jitter        JitterBuffer
	depacketizers []Depacketizer
	codec         codec.CodecSpec
	limits        BufferLimits
	decodeBounds  codec.DecodeBounds
	maxTSGap      av.Duration
	streams       []av.Stream
}

// Receive builds an RTP receive Input around a packet reader.
func Receive(receiver PacketReader, options ...ReceiveOption) *Input {
	input := &Input{receiver: receiver}
	for i := range options {
		if options[i] != nil {
			options[i](input)
		}
	}
	return input
}

// WithName names the source node and selects the stream the codec intent
// applies to.
func WithName(name string) ReceiveOption {
	return func(input *Input) {
		input.name = name
	}
}

// WithFeedback routes RTCP feedback to an explicit writer instead of the
// receiver itself.
func WithFeedback(feedback FeedbackWriter) ReceiveOption {
	return func(input *Input) {
		input.feedback = feedback
	}
}

// WithJitter orders packets through a jitter buffer before depacketizing.
func WithJitter(jitter JitterBuffer) ReceiveOption {
	return func(input *Input) {
		input.jitter = jitter
	}
}

// WithDepacketizers registers explicit depacketizers; nil entries are skipped.
func WithDepacketizers(depacketizers ...Depacketizer) ReceiveOption {
	return func(input *Input) {
		for i := range depacketizers {
			if depacketizers[i] != nil {
				input.depacketizers = append(input.depacketizers, depacketizers[i])
			}
		}
	}
}

// WithCodec declares the codec carried by this input; a matching default
// depacketizer (Opus, VP8, VP9, H264, AV1) is derived at open.
func WithCodec(spec codec.CodecSpec) ReceiveOption {
	return func(input *Input) {
		input.codec = cloneCodecSpec(spec)
	}
}

// WithBufferLimits bounds the source scratch buffers.
func WithBufferLimits(limits BufferLimits) ReceiveOption {
	return func(input *Input) {
		input.limits = limits
	}
}

// WithDecodeBounds seeds codec.DecodeConfig.Bounds for decoders fed by this
// input.
func WithDecodeBounds(bounds codec.DecodeBounds) ReceiveOption {
	return func(input *Input) {
		input.decodeBounds = bounds
	}
}

// WithMaxTimestampGap emits a discontinuity when a positive packet timestamp
// delta exceeds this threshold. Backward timestamps always emit one.
func WithMaxTimestampGap(gap av.Duration) ReceiveOption {
	return func(input *Input) {
		input.maxTSGap = gap
	}
}

// OpenSource resolves the receiver streams and depacketizers and opens the
// running pipeline source.
func (in *Input) OpenSource(ctx context.Context) (pipeline.Source, []av.Stream, error) {
	if in.receiver == nil {
		return nil, nil, ErrNilReceiver
	}
	streams, err := in.receiver.Streams(ctx)
	if err != nil {
		return nil, nil, err
	}
	source, err := NewSource(SourceConfig{
		Name:            in.Name(),
		Detail:          in.Detail(),
		Receiver:        in.receiver,
		Feedback:        in.feedback,
		Jitter:          in.jitter,
		Depacketizers:   in.depacketizersFor(streams),
		Streams:         streams,
		MaxTimestampGap: in.maxTSGap,
		MaxReady:        in.limits.MaxReady,
		MaxEvents:       in.limits.MaxEvents,
		MaxFeedback:     in.limits.MaxFeedback,
		MaxPackets:      in.limits.MaxPackets,
	})
	if err != nil {
		return nil, nil, err
	}
	in.streams = cloneStreams(streams)
	return source, streams, nil
}

// SourceShape declares the packet-domain realtime shape of this input, plus
// any media facts asserted by the codec intent.
func (in *Input) SourceShape() shape.Spec {
	parameters := in.codec.Parameters
	media := in.codec.Type
	if media == "" {
		media = parameters.Type
	}
	id := in.codec.ID
	if id == "" {
		id = parameters.ID
	}
	declared := shape.Spec{
		Domain:       shape.DomainPacket,
		MediaKind:    media,
		Codec:        id,
		Width:        parameters.Width,
		Height:       parameters.Height,
		PixelFormat:  parameters.PixelFormat,
		SampleRate:   parameters.SampleRate,
		Channels:     parameters.Channels,
		SampleFormat: parameters.SampleFormat,
		Realtime:     true,
	}
	if depacketized, ok := in.depacketizerShape(); ok {
		return shape.Merge(depacketized, declared)
	}
	return declared
}

// DecodeBounds returns the configured decode bounds when the stream belongs
// to this input, and a zero value otherwise. Streams are the ones resolved by
// the most recent OpenSource call.
func (in *Input) DecodeBounds(id av.StreamID) codec.DecodeBounds {
	if !decodeBoundsConfigured(in.decodeBounds) {
		return codec.DecodeBounds{}
	}
	for i := range in.streams {
		if in.streams[i].ID == id {
			return in.decodeBounds
		}
	}
	return codec.DecodeBounds{}
}

// Name returns the configured node name, defaulting to "rtp".
func (in *Input) Name() string {
	if in.name != "" {
		return in.name
	}
	return "rtp"
}

// Detail returns the human-readable node detail for describe output.
func (in *Input) Detail() string {
	parts := []string{"rtp receive"}
	if in.jitter != nil {
		parts = append(parts, "jitter")
	}
	if in.codec.ID != "" {
		parts = append(parts, "codec="+string(in.codec.ID))
	} else if len(in.depacketizers) != 0 {
		parts = append(parts, "custom payloads")
	}
	if in.feedback != nil {
		parts = append(parts, "feedback")
	}
	if decodeBoundsConfigured(in.decodeBounds) {
		parts = append(parts, "decode bounds")
	}
	if in.maxTSGap.Value > 0 && in.maxTSGap.Base.Valid() {
		parts = append(parts, "timestamp gap")
	}
	return joinDetail(parts)
}

func (in *Input) depacketizersFor(streams []av.Stream) []Depacketizer {
	depacketizers := append([]Depacketizer(nil), in.depacketizers...)
	stream, ok := in.codecStream(streams)
	if !ok {
		return depacketizers
	}
	if depacketizer := defaultDepacketizerFor(in.codec.ID, stream); depacketizer != nil {
		return append(depacketizers, depacketizer)
	}
	return depacketizers
}

func (in *Input) depacketizerShape() (shape.Spec, bool) {
	var out shape.Spec
	found := false
	for i := range in.depacketizers {
		stream, ok := depacketizerStream(in.depacketizers[i])
		if !ok {
			continue
		}
		spec := shape.FromStream(stream, shape.DomainPacket)
		spec.Realtime = true
		if !found {
			out = spec
			found = true
			continue
		}
		out = commonDepacketizerShape(out, spec)
	}
	return out, found
}

func commonDepacketizerShape(a shape.Spec, b shape.Spec) shape.Spec {
	out := shape.Spec{Domain: shape.DomainPacket, Realtime: a.Realtime || b.Realtime}
	if a.MediaKind == b.MediaKind {
		out.MediaKind = a.MediaKind
	}
	if a.StreamID == b.StreamID {
		out.StreamID = a.StreamID
	}
	if a.Codec == b.Codec {
		out.Codec = a.Codec
	}
	if a.Width == b.Width {
		out.Width = a.Width
	}
	if a.Height == b.Height {
		out.Height = a.Height
	}
	if a.PixelFormat == b.PixelFormat {
		out.PixelFormat = a.PixelFormat
	}
	if a.SampleRate == b.SampleRate {
		out.SampleRate = a.SampleRate
	}
	if a.Channels == b.Channels {
		out.Channels = a.Channels
	}
	if a.SampleFormat == b.SampleFormat {
		out.SampleFormat = a.SampleFormat
	}
	return out
}

func depacketizerStream(depacketizer Depacketizer) (av.Stream, bool) {
	switch d := depacketizer.(type) {
	case *OpusDepacketizer:
		return d.stream, true
	case *VP8Depacketizer:
		return d.assembler.stream, true
	case *VP9Depacketizer:
		return d.assembler.stream, true
	case *H264Depacketizer:
		return d.assembler.stream, true
	case *AV1Depacketizer:
		return d.assembler.stream, true
	default:
		return av.Stream{}, false
	}
}

// defaultDepacketizerFor derives the built-in depacketizer for a codec, bound
// to the given stream; codecs without a built-in return nil.
func defaultDepacketizerFor(id av.CodecID, stream av.Stream) Depacketizer {
	switch id {
	case av.CodecOpus:
		return NewOpusDepacketizer(stream)
	case av.CodecVP8:
		return NewVP8Depacketizer(stream)
	case av.CodecVP9:
		return NewVP9Depacketizer(stream)
	case av.CodecH264:
		return NewH264Depacketizer(stream)
	case av.CodecAV1:
		return NewAV1Depacketizer(stream)
	default:
		return nil
	}
}

func (in *Input) codecStream(streams []av.Stream) (av.Stream, bool) {
	if in.codec.ID == "" {
		return av.Stream{}, false
	}
	stream := in.selectCodecStream(streams)
	if in.name != "" {
		stream.ID = av.StreamID(in.name)
		if stream.Name == "" {
			stream.Name = in.name
		}
	}
	if stream.Type == "" {
		stream.Type = in.codec.Type
	}
	stream.Codec = mergeCodecIntentParameters(in.codec.Parameters, stream.Codec)
	if stream.Codec.Type == "" {
		stream.Codec.Type = stream.Type
	}
	return stream, true
}

func (in *Input) selectCodecStream(streams []av.Stream) av.Stream {
	if in.name != "" {
		for i := range streams {
			if string(streams[i].ID) == in.name || streams[i].Name == in.name {
				return streams[i]
			}
		}
	}
	for i := range streams {
		if streams[i].Codec.ID == in.codec.ID && in.codec.ID != "" {
			return streams[i]
		}
	}
	for i := range streams {
		if streams[i].Type == in.codec.Type && in.codec.Type != "" {
			return streams[i]
		}
	}
	if len(streams) == 1 {
		return streams[0]
	}
	return av.Stream{}
}

func decodeBoundsConfigured(bounds codec.DecodeBounds) bool {
	return bounds.MaxFramesPerInput > 0 ||
		bounds.MaxEventsPerInput > 0 ||
		bounds.MaxRequestsPerInput > 0 ||
		bounds.MaxPayloadBytes > 0 ||
		bounds.MaxRetainedBytes > 0 ||
		bounds.MaxWidth > 0 ||
		bounds.MaxHeight > 0
}

func cloneCodecSpec(spec codec.CodecSpec) codec.CodecSpec {
	spec.Parameters.Attributes = cloneMetadata(spec.Parameters.Attributes)
	spec.Parameters.ExtraData.Bytes = append([]byte(nil), spec.Parameters.ExtraData.Bytes...)
	return spec
}

func cloneMetadata(metadata av.Metadata) av.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(av.Metadata, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func mergeCodecIntentParameters(base av.CodecParameters, override av.CodecParameters) av.CodecParameters {
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
	if override.ExtraData.Bytes != nil || override.ExtraData.Ownership != "" || override.ExtraData.Owner != nil {
		out.ExtraData = override.ExtraData
	}
	if override.Attributes != nil {
		out.Attributes = override.Attributes
	}
	return out
}

func joinDetail(parts []string) string {
	clean := parts[:0]
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, ", ")
}
