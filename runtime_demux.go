package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

type demuxBuild struct {
	source  *format.DemuxSource
	streams []av.Stream
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
