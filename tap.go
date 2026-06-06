package goav

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type TapIntent struct {
	Name      string
	MediaKind av.MediaType
	Domain    MediaDomain
	After     OperationKind
}

type TapReport struct {
	Name      string
	MediaKind av.MediaType
	Domain    MediaDomain
	Caps      StreamCaps
	Node      pipeline.NodeRef
}

func tapReports(taps []TapInfo) []TapReport {
	reports := make([]TapReport, 0, len(taps))
	for i := range taps {
		reports = append(reports, TapReport{
			Name:      taps[i].Name,
			MediaKind: taps[i].MediaKind,
			Domain:    taps[i].Domain,
			Caps:      taps[i].Caps,
			Node:      taps[i].Node,
		})
	}
	return reports
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
				Domain:    DomainFrame,
				Caps:      StreamCaps{Domain: DomainFrame, MediaKind: media},
				Node:      pipeline.NodeRef(node.Name),
			})
		case strings.HasPrefix(node.Name, "select-"):
			media := av.MediaType(strings.TrimPrefix(node.Name, "select-"))
			taps = append(taps, TapInfo{
				Name:      string(media) + ".packets",
				MediaKind: media,
				Domain:    DomainPacket,
				Caps:      StreamCaps{Domain: DomainPacket, MediaKind: media},
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
