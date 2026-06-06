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

func (b *builder) openDemuxSource(ctx context.Context, input Input) (demuxBuild, error) {
	inputProbe, err := b.runtime.formats.Probe(ctx, format.ProbeRequest{
		Name:     input.Name,
		MIMEType: input.MIMEType,
		Input:    input,
	})
	if err != nil {
		return demuxBuild{}, err
	}
	demuxFactory, err := b.runtime.formats.DemuxerFactory(inputProbe.Format)
	if err != nil {
		return demuxBuild{}, err
	}
	demuxer, err := demuxFactory.NewDemuxer(ctx, inputProbe)
	if err != nil {
		return demuxBuild{}, err
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
			Packet: &av.Packet{},
			Events: make([]av.Event, 0, 1),
		},
	})
	if err != nil {
		demuxer.Close()
		return demuxBuild{}, err
	}
	return demuxBuild{source: source, streams: streams}, nil
}
