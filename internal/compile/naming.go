package compile

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

// runtimeBranchNodeName names an attached node under its branch.
func runtimeBranchNodeName(branchName string, localName string) string {
	return branchName + "/" + localName
}

func selectNodeName(selector av.StreamSelector) string {
	if selector.Name != "" {
		return "select-" + selector.Name
	}
	if selector.ID != "" {
		return "select-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "select-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "select-" + string(selector.Type)
	}
	if selectorHasIndex(selector) {
		return "select-" + strconv.Itoa(selector.Index)
	}
	return "select"
}

func EncodeNodeName(request EncodeRequest) string {
	if request.Name != "" {
		return "encode-" + request.Name
	}
	selector := request.Selector
	if selector.Name != "" {
		return "encode-" + selector.Name
	}
	if selector.ID != "" {
		return "encode-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "encode-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "encode-" + string(selector.Type)
	}
	if selectorHasIndex(selector) {
		return "encode-" + strconv.Itoa(selector.Index)
	}
	codecID := request.Config.Parameters.ID
	if codecID == "" {
		codecID = request.Config.Stream.Codec.ID
	}
	if codecID != "" {
		return "encode-" + string(codecID)
	}
	return "encode"
}

func MuxNodeName(output format.Output, index int) string {
	if output.Name != "" {
		return output.Name
	}
	if output.URI != "" {
		return output.URI
	}
	if index == 0 {
		return "output"
	}
	return "output-" + strconv.Itoa(index)
}

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
	codecID := encodeTargetCodec(request)
	if codecID == "" {
		return "frames -> packets"
	}
	return JoinSpecDetail("frames -> packets", "codec="+string(codecID))
}

func encodeTargetCodec(request EncodeRequest) av.CodecID {
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

type PlannedNode struct {
	Ref  pipeline.NodeRef
	Kind pipeline.NodeKind
}

func AddPlannedNode(nodes map[string]PlannedNode, spec *pipeline.Spec, name string, kind pipeline.NodeKind, ref pipeline.NodeRef, detail ...string) error {
	if name == "" {
		return pipeline.ErrUnknownNode
	}
	if _, ok := nodes[name]; ok {
		return pipeline.ErrNodeExists
	}
	nodes[name] = PlannedNode{Ref: ref, Kind: kind}
	spec.Nodes = append(spec.Nodes, pipeline.NodeSpec{Name: name, Kind: kind, Detail: firstDetail(detail)})
	return nil
}

func firstDetail(detail []string) string {
	for i := range detail {
		if detail[i] != "" {
			return detail[i]
		}
	}
	return ""
}

func DescribedNodeDetail(node any) string {
	describer, ok := node.(pipeline.NodeDescriber)
	if !ok {
		return ""
	}
	return describer.DescribeNode().Detail
}
