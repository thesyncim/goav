package goav

import (
	"strconv"

	"github.com/thesyncim/goav/av"
)

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
