package compile

import (
	"strconv"
)

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
