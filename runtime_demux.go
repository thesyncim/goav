package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

type demuxBuild struct {
	source  *format.DemuxSource
	streams []av.Stream
}

// builderSources is the result of opening every builder input into the graph: the
// source node refs (one per input), the union of their streams, the per-input
// decode-bounds capabilities (only provider inputs declare them), and the realtime
// contribution folded in from the inputs. The merged compilers iterate every input
// through this one loop, kind-agnostic.
type builderSources struct {
	refs     []pipeline.NodeRef
	streams  []av.Stream
	bounds   []decodeBoundsProvider
	realtime bool
}

// addBuilderSources opens every builder input (demux or provider) through the
// unified source-opening seam, adds each as a graph source, and collects the
// refs, the stream union, the decode-bounds capabilities, and the realtime
// contribution.
func (b *builder) addBuilderSources(ctx context.Context, graph pipeline.Graph) (builderSources, error) {
	out := builderSources{
		refs:     make([]pipeline.NodeRef, 0, len(b.inputs)),
		streams:  make([]av.Stream, 0, len(b.inputs)),
		realtime: b.runtime.realtime,
	}
	names := b.inputNodeNames()
	for i := range b.inputs {
		build, err := b.inputs[i].open(ctx, b, names[i])
		if err != nil {
			return builderSources{}, err
		}
		sourceRef, err := graph.AddSource(build.source, b.runtime.buffer)
		if err != nil {
			build.source.Close()
			return builderSources{}, err
		}
		out.refs = append(out.refs, sourceRef)
		out.streams = append(out.streams, build.streams...)
		out.realtime = out.realtime || build.realtime
		if build.bounds != nil {
			out.bounds = append(out.bounds, build.bounds)
		}
	}
	return out, nil
}

func (b *builder) openDemuxSource(ctx context.Context, input format.Input) (demuxBuild, error) {
	inputProbe, err := b.runtime.formats.Probe(ctx, inputProbeRequest(input))
	if err != nil {
		return demuxBuild{}, inputFormatProbeError(input, err)
	}
	demuxFactory, err := b.runtime.formats.DemuxerFactory(inputProbe.Format)
	if err != nil {
		return demuxBuild{}, inputDemuxerMissingError(input, inputProbe.Format, err)
	}
	demuxer, err := demuxFactory.NewDemuxer(ctx, inputProbe)
	if err != nil {
		return demuxBuild{}, inputDemuxerMissingError(input, inputProbe.Format, err)
	}
	if err := demuxer.Open(ctx, input, format.OpenOptions{
		Realtime: b.runtime.realtime || input.Realtime,
		Metadata: input.Metadata,
	}); err != nil {
		demuxer.Close()
		return demuxBuild{}, err
	}

	streams := demuxer.Streams()
	if len(streams) == 0 {
		streams = inputProbe.Streams
	}
	source, err := format.NewDemuxSource(format.DemuxSourceConfig{
		Name:    demuxNodeName(input),
		Detail:  inputNodeDetail(input),
		Demuxer: demuxer,
		Result: format.ReadResult{
			Packet: &av.Packet{
				Payload: av.Buffer{Bytes: make([]byte, 0, demuxPacketBufferSize(streams))},
			},
			Events: make([]av.Event, 0, 1),
		},
	})
	if err != nil {
		demuxer.Close()
		return demuxBuild{}, err
	}
	return demuxBuild{source: source, streams: streams}, nil
}

func inputProbeRequest(input format.Input) format.ProbeRequest {
	return format.ProbeRequest{
		Name:     input.Name,
		MIMEType: input.MIMEType,
		Input:    input,
	}
}

func demuxPacketBufferSize(streams []av.Stream) int {
	size := 64 * 1024
	for i := range streams {
		if streams[i].Type == av.MediaAudio || streams[i].Codec.Type == av.MediaAudio {
			size = maxInt(size, 4096)
		}
		if streams[i].Type == av.MediaVideo || streams[i].Codec.Type == av.MediaVideo {
			size = maxInt(size, 4<<20)
		}
	}
	return size
}

func maxInt(a int, b int) int {
	if b > a {
		return b
	}
	return a
}
