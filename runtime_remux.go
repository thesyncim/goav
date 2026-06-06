package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

func (b *builder) canBuildRemux() bool {
	return len(b.inputs) == 1 &&
		len(b.outputs) > 0 &&
		len(b.decodes) == 0 &&
		len(b.encodes) == 0 &&
		len(b.filters) == 0 &&
		len(b.transcodes) == 0
}

func (b *builder) buildRemux(ctx context.Context) (Task, error) {
	graph, err := b.newGraph(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.compileRemux(ctx, graph); err != nil {
		graph.Close()
		return nil, err
	}
	return &task{graph: graph}, nil
}

func (b *builder) compileRemux(ctx context.Context, graph pipeline.Graph) error {
	demux, err := b.openDemuxSource(ctx, b.inputs[0])
	if err != nil {
		return err
	}
	sourceRef, err := graph.AddSource(demux.source, b.runtime.buffer)
	if err != nil {
		demux.source.Close()
		return err
	}

	for i := range b.outputs {
		stage, err := b.openMuxStage(ctx, b.outputs[i], i, demux.streams)
		if err != nil {
			return err
		}
		stageRef, err := graph.AddStage(stage, b.runtime.buffer)
		if err != nil {
			stage.Close()
			return err
		}
		if err := connectRefs(graph, sourceRef, stageRef); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) openMuxStage(ctx context.Context, output Output, index int, streams []av.Stream) (*format.MuxStage, error) {
	return b.openMuxStageWithFormat(ctx, output, index, streams, b.outputFormat(index))
}

func (b *builder) openMuxStageWithFormat(ctx context.Context, output Output, index int, streams []av.Stream, formatID av.FormatID) (*format.MuxStage, error) {
	explicitFormat := formatID
	if formatID == "" {
		outputProbe, err := b.runtime.formats.Probe(ctx, outputProbeRequest(output))
		if err != nil {
			return nil, err
		}
		formatID = outputProbe.Format
	}
	muxFactory, err := b.runtime.formats.MuxerFactory(formatID)
	if err != nil {
		return nil, err
	}
	muxer, err := muxFactory.NewMuxer(ctx, formatID)
	if err != nil {
		return nil, err
	}
	if err := muxer.Open(ctx, output, streams, format.OpenOptions{
		Realtime: b.runtime.realtime || output.Realtime,
		Metadata: output.Metadata,
	}); err != nil {
		muxer.Close()
		return nil, err
	}
	stage, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   muxNodeName(output, index),
		Detail: outputNodeDetailWithFormat(output, explicitFormat),
		Muxer:  muxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		muxer.Close()
		return nil, err
	}
	return stage, nil
}

func outputProbeRequest(output Output) format.ProbeRequest {
	return format.ProbeRequest{
		Name:     output.Name,
		MIMEType: output.MIMEType,
		Input: format.Input{
			Name:     output.Name,
			URI:      output.URI,
			Protocol: output.Protocol,
			MIMEType: output.MIMEType,
			Realtime: output.Realtime,
			Metadata: output.Metadata,
		},
	}
}

func demuxNodeName(input Input) string {
	if input.Name != "" {
		return input.Name
	}
	if input.URI != "" {
		return input.URI
	}
	return "input"
}

func muxNodeName(output Output, index int) string {
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
