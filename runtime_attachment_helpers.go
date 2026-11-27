package goav

import (
	"errors"
	"maps"
	"slices"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

// upstreamFirst orders a branch's nodes so that none comes before a node
// routed into it; ties, and a cycle should one exist, keep the attach order.
func upstreamFirst(nodes []pipeline.NodeRef, routes []pipeline.Route) []pipeline.NodeRef {
	left := slices.Clone(nodes)
	out := make([]pipeline.NodeRef, 0, len(nodes))
	fed := func(node pipeline.NodeRef) bool {
		for _, route := range routes {
			if route.From != node.String() && slices.Contains(left, pipeline.NodeRef(route.From)) && slices.Contains(route.To, node.String()) {
				return true
			}
		}
		return false
	}
	for len(left) > 0 {
		next := slices.IndexFunc(left, func(node pipeline.NodeRef) bool { return !fed(node) })
		if next < 0 {
			next = 0
		}
		out = append(out, left[next])
		left = slices.Delete(left, next, next+1)
	}
	return out
}

func branchStatsForNodes(stats pipeline.GraphStats, nodes []pipeline.NodeRef) pipeline.GraphStats {
	if len(nodes) == 0 || len(stats.Nodes) == 0 {
		return pipeline.GraphStats{}
	}
	out := pipeline.GraphStats{
		DropReasons: make(map[pipeline.DropReason]uint64),
		Nodes:       make(map[string]pipeline.NodeStats, len(nodes)),
	}
	for i := range nodes {
		name := nodes[i].String()
		nodeStats, ok := stats.Nodes[name]
		if !ok {
			continue
		}
		out.Nodes[name] = clonePublicNodeStats(nodeStats)
		out.Messages += nodeStats.OutMessages
		out.Packets += nodeStats.OutPackets
		out.Frames += nodeStats.OutFrames
		out.Events += nodeStats.OutEvents
		out.Delivered += nodeStats.InMessages
		out.Dropped += nodeStats.Dropped
		for reason, count := range nodeStats.DropReasons {
			out.DropReasons[reason] += count
		}
		if nodeStats.LastEventPresent {
			out.LastEvent = nodeStats.LastEvent
			out.LastEventPresent = true
		}
	}
	if len(out.DropReasons) == 0 {
		out.DropReasons = nil
	}
	if len(out.Nodes) == 0 {
		out.Nodes = nil
	}
	return out
}

func clonePublicNodeStats(stats pipeline.NodeStats) pipeline.NodeStats {
	cloned := stats
	if stats.DropReasons != nil {
		cloned.DropReasons = make(map[pipeline.DropReason]uint64, len(stats.DropReasons))
		maps.Copy(cloned.DropReasons, stats.DropReasons)
	}
	return cloned
}

func isStoppedAttachmentError(err error) bool {
	return errors.Is(err, pipeline.ErrInvalidLink) ||
		errors.Is(err, pipeline.ErrUnknownNode) ||
		errors.Is(err, pipeline.ErrClosed)
}

func destinationSpecHasOutput(dest destinationSpec) bool {
	return dest.output.Name != "" ||
		dest.output.URI != "" ||
		dest.output.Writer != nil ||
		dest.format != "" ||
		dest.resolvedFormat != ""
}

func runtimeBranchStreamMedia(stream av.Stream) av.MediaType {
	if stream.Type != "" {
		return stream.Type
	}
	return stream.Codec.Type
}

func runtimeBranchAnchorShape(anchor TapSnapshot) shape.Spec {
	shape := anchor.Shape
	if shape.Domain == "" {
		shape.Domain = anchor.Domain
	}
	if shape.MediaKind == "" {
		shape.MediaKind = anchor.MediaKind
	}
	return shape
}

func streamFromRuntimeBranchShape(name string, shape shape.Spec) av.Stream {
	stream := av.Stream{
		ID:   av.StreamID(firstNonEmpty(string(shape.StreamID), name, "runtime-branch")),
		Name: firstNonEmpty(name, "runtime-branch"),
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:            shape.Codec,
			Type:          shape.MediaKind,
			SampleRate:    shape.SampleRate,
			Channels:      shape.Channels,
			ChannelLayout: "",
			Width:         shape.Width,
			Height:        shape.Height,
			PixelFormat:   shape.PixelFormat,
			SampleFormat:  shape.SampleFormat,
		},
	}
	if shape.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(shape.SampleRate)
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(shape.SampleRate)}
	}
	return stream
}

func mediaShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := previous
	spec.Domain = shape.DomainFrame
	if stream.Type != "" {
		spec.MediaKind = stream.Type
	}
	if stream.ID != "" {
		spec.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		spec.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		spec.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		spec.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		spec.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		spec.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		spec.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		spec.SampleFormat = stream.Codec.SampleFormat
	}
	return spec
}

func streamPacketShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := mediaShapeFromRuntimeBranchStream(stream, previous)
	spec.Domain = shape.DomainPacket
	return spec
}

func runtimeBranchTap(name string, node pipeline.NodeRef, spec shape.Spec, after plan.OperationKind) TapSnapshot {
	domain := spec.Domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	media := spec.MediaKind
	if spec.Domain == "" {
		spec.Domain = domain
	}
	if spec.MediaKind == "" {
		spec.MediaKind = media
	}
	return TapSnapshot{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		After:     after,
		Shape:     spec,
		Node:      node,
	}
}

func nextRuntimeAttachmentID(name string) string {
	seq := runtimeAttachmentSeq.Add(1)
	if name == "" {
		return "attachment-" + strconv.FormatUint(seq, 10)
	}
	return name + "-" + strconv.FormatUint(seq, 10)
}

func specHasNode(spec pipeline.Spec, name string) bool {
	for i := range spec.Nodes {
		if spec.Nodes[i].Name == name {
			return true
		}
	}
	return false
}
