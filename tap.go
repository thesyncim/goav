package goav

import (
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

type tapIntent struct {
	Name      string
	MediaKind av.MediaType
	Domain    shape.MediaDomain
	After     plan.OperationKind
}

func defaultDecodedTapName(media av.MediaType) string {
	if media == "" {
		return "decoded"
	}
	return string(media) + ".decoded"
}

func defaultPacketTapName(media av.MediaType, index int) string {
	if media != "" {
		return string(media) + ".packets"
	}
	if index == 0 {
		return "packets"
	}
	return "packets-" + strconv.Itoa(index)
}
