package compile

import (
	"strconv"

	"github.com/thesyncim/goav/av"
)

func SelectNodeName(selector av.StreamSelector) string {
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
