package compile

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
)

func OutputNodeDetailWithFormat(output format.Output, formatID av.FormatID) string {
	parts := []string{"mux"}
	if output.URI != "" && output.URI != output.Name {
		parts = append(parts, "uri="+output.URI)
	}
	if formatID != "" {
		parts = append(parts, "format="+string(formatID))
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
	return JoinSpecDetail(parts...)
}

func selectNodeDetail(selector av.StreamSelector) string {
	return JoinSpecDetail("select", selectorDetail(selector))
}

func decodeNodeDetail(selector av.StreamSelector) string {
	return JoinSpecDetail("packets -> frames", selectorDetail(selector))
}

func DecodeRequestDetail(request DecodeRequest) string {
	return decodeNodeDetail(request.Selector)
}

func EncodeNodeDetail(request EncodeRequest) string {
	codecID := EncodeTargetCodec(request)
	if codecID == "" {
		return "frames -> packets"
	}
	return JoinSpecDetail("frames -> packets", "codec="+string(codecID))
}

func EncodeTargetCodec(request EncodeRequest) av.CodecID {
	if request.Config.Parameters.ID != "" {
		return request.Config.Parameters.ID
	}
	return request.Config.Stream.Codec.ID
}

func MediaTransformDetail(transform MediaTransform) string {
	if transform.Stage != nil {
		return DescribedNodeDetail(transform.Stage)
	}
	if transform.Video != nil {
		return resizeDetail(*transform.Video)
	}
	if transform.Audio != nil {
		return resampleDetail(*transform.Audio)
	}
	if transform.Factory != "" {
		return transform.Factory
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
	return JoinSpecDetail(parts...)
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
	return JoinSpecDetail(parts...)
}

func selectorDetail(selector av.StreamSelector) string {
	parts := make([]string, 0, 5)
	if selector.ID != "" {
		parts = append(parts, "stream="+string(selector.ID))
	}
	if selectorHasIndex(selector) {
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
	return JoinSpecDetail(parts...)
}

func JoinSpecDetail(parts ...string) string {
	clean := parts[:0]
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, ", ")
}
