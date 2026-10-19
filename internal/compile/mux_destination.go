package compile

import (
	"strconv"

	"github.com/thesyncim/goav/format"
)

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
