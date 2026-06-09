package goav

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

type tapIntent struct {
	Name      string
	MediaKind av.MediaType
	Domain    shape.MediaDomain
	After     OperationKind
}

func inferSpecTaps(spec pipeline.Spec) []TapInfo {
	taps := make([]TapInfo, 0)
	for i := range spec.Nodes {
		node := spec.Nodes[i]
		switch {
		case strings.HasPrefix(node.Name, "decode-"):
			media := av.MediaType(strings.TrimPrefix(node.Name, "decode-"))
			taps = append(taps, TapInfo{
				Name:      string(media) + ".decoded",
				MediaKind: media,
				Domain:    shape.DomainFrame,
				After:     OpDecode,
				Shape:     shape.Frame(media, shape.Stream(av.StreamID(media))),
				Node:      pipeline.NodeRef(node.Name),
			})
		case strings.HasPrefix(node.Name, "select-"):
			media := av.MediaType(strings.TrimPrefix(node.Name, "select-"))
			taps = append(taps, TapInfo{
				Name:      string(media) + ".packets",
				MediaKind: media,
				Domain:    shape.DomainPacket,
				After:     OpSelect,
				Shape:     shape.Packet(media, "", shape.Stream(av.StreamID(media))),
				Node:      pipeline.NodeRef(node.Name),
			})
		}
	}
	return taps
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
