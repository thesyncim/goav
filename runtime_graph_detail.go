package goav

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

func inputNodeDetail(input Input) string {
	parts := []string{"demux"}
	if input.URI != "" && input.URI != input.Name {
		parts = append(parts, "uri="+input.URI)
	}
	if input.Protocol != "" {
		parts = append(parts, "protocol="+string(input.Protocol))
	}
	if input.MIMEType != "" {
		parts = append(parts, "mime="+input.MIMEType)
	}
	if input.Realtime {
		parts = append(parts, "realtime")
	}
	return joinSpecDetail(parts...)
}

func outputNodeDetail(output Output) string {
	parts := []string{"mux"}
	if output.URI != "" && output.URI != output.Name {
		parts = append(parts, "uri="+output.URI)
	}
	if output.Protocol != "" {
		parts = append(parts, "protocol="+string(output.Protocol))
	}
	if output.MIMEType != "" {
		parts = append(parts, "mime="+output.MIMEType)
	}
	if output.Realtime {
		parts = append(parts, "realtime")
	}
	return joinSpecDetail(parts...)
}

func rtpInputDetail(input rtpInput) string {
	parts := []string{"rtp receive"}
	if input.jitter != nil {
		parts = append(parts, "jitter")
	}
	if len(input.depacketizers) != 0 {
		parts = append(parts, "depacketizers="+strconv.Itoa(len(input.depacketizers)))
	}
	if input.feedback != nil {
		parts = append(parts, "feedback")
	}
	return joinSpecDetail(parts...)
}

func selectNodeDetail(selector av.StreamSelector) string {
	return joinSpecDetail("select", selectorDetail(selector))
}

func decodeNodeDetail(selector av.StreamSelector) string {
	return joinSpecDetail("packets -> frames", selectorDetail(selector))
}

func encodeNodeDetail(request encodeRequest) string {
	codecID := encodeTargetCodec(request)
	if codecID == "" {
		return "frames -> packets"
	}
	return joinSpecDetail("frames -> packets", "codec="+string(codecID))
}

func encodeTargetCodec(request encodeRequest) av.CodecID {
	if request.config.Parameters.ID != "" {
		return request.config.Parameters.ID
	}
	return request.config.Stream.Codec.ID
}

func transcodeTransformDetail(transform transcodeTransform) string {
	if transform.video != nil {
		return resizeDetail(*transform.video)
	}
	if transform.audio != nil {
		return resampleDetail(*transform.audio)
	}
	if transform.factory != "" {
		return transform.factory
	}
	return "filter"
}

func resizeDetail(config filter.ResizeConfig) string {
	parts := []string{"resize"}
	if config.Width != 0 || config.Height != 0 {
		parts = append(parts, strconv.Itoa(config.Width)+"x"+strconv.Itoa(config.Height))
	}
	mode := config.Mode
	if mode == "" {
		mode = filter.ResizeExact
	}
	parts = append(parts, string(mode))
	if config.PixelFormat != "" {
		parts = append(parts, "pixfmt="+config.PixelFormat)
	}
	return joinSpecDetail(parts...)
}

func resampleDetail(config filter.ResampleConfig) string {
	parts := []string{"resample"}
	if config.SampleRate != 0 {
		parts = append(parts, strconv.Itoa(config.SampleRate)+" Hz")
	}
	if config.Channels != 0 {
		parts = append(parts, strconv.Itoa(config.Channels)+" ch")
	}
	if config.ChannelLayout != "" {
		parts = append(parts, "layout="+config.ChannelLayout)
	}
	if config.SampleFormat != "" {
		parts = append(parts, "samplefmt="+config.SampleFormat)
	}
	return joinSpecDetail(parts...)
}

func selectorDetail(selector av.StreamSelector) string {
	parts := make([]string, 0, 5)
	if selector.ID != "" {
		parts = append(parts, "stream="+string(selector.ID))
	}
	if selector.Index != 0 {
		parts = append(parts, "index="+strconv.Itoa(selector.Index))
	}
	if selector.Type != "" {
		parts = append(parts, "type="+string(selector.Type))
	}
	if selector.Codec != "" {
		parts = append(parts, "codec="+string(selector.Codec))
	}
	if selector.Name != "" {
		parts = append(parts, "name="+selector.Name)
	}
	return joinSpecDetail(parts...)
}

func joinSpecDetail(parts ...string) string {
	clean := parts[:0]
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, ", ")
}
