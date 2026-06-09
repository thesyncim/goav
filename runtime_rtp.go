package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
)

type rtpBuild struct {
	source       *rtpav.Source
	streams      []av.Stream
	decodeBounds codec.DecodeBounds
}

func withRTPName(name string) rtpOption {
	return func(input *rtpInput) {
		input.name = name
	}
}

func withRTPFeedback(feedback rtpav.FeedbackWriter) rtpOption {
	return func(input *rtpInput) {
		input.feedback = feedback
	}
}

func withRTPJitter(jitter rtpav.JitterBuffer) rtpOption {
	return func(input *rtpInput) {
		input.jitter = jitter
	}
}

func withRTPDepacketizers(depacketizers ...rtpav.Depacketizer) rtpOption {
	return func(input *rtpInput) {
		for i := range depacketizers {
			if depacketizers[i] != nil {
				input.depacketizers = append(input.depacketizers, depacketizers[i])
			}
		}
	}
}

func withRTPCodec(codec CodecSpec) rtpOption {
	return func(input *rtpInput) {
		input.codec = cloneCodecSpec(codec)
	}
}

func withRTPBufferLimits(limits RTPBufferLimits) rtpOption {
	return func(input *rtpInput) {
		input.limits = limits
	}
}

// withRTPDecodeBounds seeds codec.DecodeConfig.Bounds for high-level RTP decode
// builders using this packet reader.
func withRTPDecodeBounds(bounds codec.DecodeBounds) rtpOption {
	return func(input *rtpInput) {
		input.decodeBounds = bounds
	}
}

func withRTPMaxTimestampGap(gap av.Duration) rtpOption {
	return func(input *rtpInput) {
		input.maxTSGap = gap
	}
}

func (b *builder) openRTPSource(ctx context.Context, input rtpInput, index int) (rtpBuild, error) {
	if input.receiver == nil {
		return rtpBuild{}, ErrNilSource
	}
	streams, err := input.receiver.Streams(ctx)
	if err != nil {
		return rtpBuild{}, err
	}
	depacketizers := input.depacketizersFor(streams)
	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Name:            rtpNodeName(input, index),
		Detail:          rtpInputDetail(input),
		Receiver:        input.receiver,
		Feedback:        input.feedback,
		Jitter:          input.jitter,
		Depacketizers:   depacketizers,
		Streams:         streams,
		MaxTimestampGap: input.maxTSGap,
		MaxReady:        input.limits.MaxReady,
		MaxEvents:       input.limits.MaxEvents,
		MaxFeedback:     input.limits.MaxFeedback,
		MaxPackets:      input.limits.MaxPackets,
	})
	if err != nil {
		return rtpBuild{}, err
	}
	return rtpBuild{source: source, streams: streams, decodeBounds: input.decodeBounds}, nil
}

func (input rtpInput) depacketizersFor(streams []av.Stream) []rtpav.Depacketizer {
	depacketizers := append([]rtpav.Depacketizer(nil), input.depacketizers...)
	stream, ok := input.codecStream(streams)
	if !ok {
		return depacketizers
	}
	switch input.codec.ID {
	case av.CodecOpus:
		return append(depacketizers, rtpav.NewOpusDepacketizer(stream))
	case av.CodecVP8:
		return append(depacketizers, rtpav.NewVP8Depacketizer(stream))
	case av.CodecVP9:
		return append(depacketizers, rtpav.NewVP9Depacketizer(stream))
	case av.CodecH264:
		return append(depacketizers, rtpav.NewH264Depacketizer(stream))
	case av.CodecAV1:
		return append(depacketizers, rtpav.NewAV1Depacketizer(stream))
	default:
		return depacketizers
	}
}

func (input rtpInput) codecStream(streams []av.Stream) (av.Stream, bool) {
	if input.codec.ID == "" {
		return av.Stream{}, false
	}
	stream := input.selectCodecStream(streams)
	if input.name != "" {
		stream.ID = av.StreamID(input.name)
		if stream.Name == "" {
			stream.Name = input.name
		}
	}
	if stream.Type == "" {
		stream.Type = input.codec.Type
	}
	stream.Codec = mergeCodecParameters(input.codec.Parameters, stream.Codec)
	if stream.Codec.Type == "" {
		stream.Codec.Type = stream.Type
	}
	return stream, true
}

func (input rtpInput) selectCodecStream(streams []av.Stream) av.Stream {
	if input.name != "" {
		for i := range streams {
			if string(streams[i].ID) == input.name || streams[i].Name == input.name {
				return streams[i]
			}
		}
	}
	for i := range streams {
		if streams[i].Codec.ID == input.codec.ID && input.codec.ID != "" {
			return streams[i]
		}
	}
	for i := range streams {
		if streams[i].Type == input.codec.Type && input.codec.Type != "" {
			return streams[i]
		}
	}
	if len(streams) == 1 {
		return streams[0]
	}
	return av.Stream{}
}

func rtpNodeName(input rtpInput, index int) string {
	if input.name != "" {
		return input.name
	}
	if index > 0 {
		return "rtp-" + strconv.Itoa(index)
	}
	return "rtp"
}

func rtpDecodeBoundsForStream(stream av.Stream, builds []rtpBuild) codec.DecodeBounds {
	var bounds codec.DecodeBounds
	for i := range builds {
		if !rtpDecodeBoundsConfigured(builds[i].decodeBounds) {
			continue
		}
		for j := range builds[i].streams {
			if !sameDecodeBoundsStream(stream, builds[i].streams[j]) {
				continue
			}
			bounds = maxDecodeBounds(bounds, builds[i].decodeBounds)
			break
		}
	}
	return bounds
}

func rtpDecodeBoundsConfigured(bounds codec.DecodeBounds) bool {
	return bounds.MaxFramesPerInput > 0 ||
		bounds.MaxEventsPerInput > 0 ||
		bounds.MaxRequestsPerInput > 0 ||
		bounds.MaxPayloadBytes > 0 ||
		bounds.MaxRetainedBytes > 0 ||
		bounds.MaxWidth > 0 ||
		bounds.MaxHeight > 0
}

func sameDecodeBoundsStream(a av.Stream, b av.Stream) bool {
	if a.ID != "" && b.ID != "" {
		return a.ID == b.ID
	}
	if a.Index != 0 || b.Index != 0 {
		return a.Index == b.Index
	}
	return a.Type == b.Type && a.Codec.ID == b.Codec.ID
}

func maxDecodeBounds(a codec.DecodeBounds, b codec.DecodeBounds) codec.DecodeBounds {
	if b.MaxFramesPerInput > a.MaxFramesPerInput {
		a.MaxFramesPerInput = b.MaxFramesPerInput
	}
	if b.MaxEventsPerInput > a.MaxEventsPerInput {
		a.MaxEventsPerInput = b.MaxEventsPerInput
	}
	if b.MaxRequestsPerInput > a.MaxRequestsPerInput {
		a.MaxRequestsPerInput = b.MaxRequestsPerInput
	}
	if b.MaxPayloadBytes > a.MaxPayloadBytes {
		a.MaxPayloadBytes = b.MaxPayloadBytes
	}
	if b.MaxRetainedBytes > a.MaxRetainedBytes {
		a.MaxRetainedBytes = b.MaxRetainedBytes
	}
	if b.MaxWidth > a.MaxWidth {
		a.MaxWidth = b.MaxWidth
	}
	if b.MaxHeight > a.MaxHeight {
		a.MaxHeight = b.MaxHeight
	}
	return a
}
