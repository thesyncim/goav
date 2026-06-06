package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
)

func TestComponentFileRemuxFanout(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &remuxTestDemuxer{streams: streams}
	if err := demuxer.Open(ctx, format.Input{Name: "input.ogg"}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}

	sourcePacket := av.Packet{}
	source, err := format.NewDemuxSource(format.DemuxSourceConfig{
		Name:    "demux",
		Detail:  "component demux",
		Demuxer: demuxer,
		Result:  format.ReadResult{Packet: &sourcePacket, Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	archiveMuxer := &remuxTestMuxer{}
	if err := archiveMuxer.Open(ctx, format.Output{Name: "archive.ogg"}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	archive, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "archive",
		Detail: "component mux",
		Muxer:  archiveMuxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	previewMuxer := &remuxTestMuxer{}
	if err := previewMuxer.Open(ctx, format.Output{Name: "preview.ogg"}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	preview, err := format.NewMuxStage(format.MuxStageConfig{
		Name:   "preview",
		Detail: "component mux",
		Muxer:  previewMuxer,
		Result: format.WriteResult{Events: make([]av.Event, 0, 1)},
	})
	if err != nil {
		t.Fatal(err)
	}

	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-remux"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(archive, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddStage(preview, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{
		From:   "demux",
		To:     []string{"archive", "preview"},
		Policy: pipeline.RouteAll,
	}); err != nil {
		t.Fatal(err)
	}

	spec := graph.Spec()
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 ||
		spec.Nodes[0].Detail != "component demux" ||
		spec.Nodes[1].Detail != "component mux" ||
		spec.Nodes[2].Detail != "component mux" {
		t.Fatalf("spec = %+v", spec)
	}

	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if archiveMuxer.writes != 1 || previewMuxer.writes != 1 ||
		!streamIDsEqual(archiveMuxer.writtenStreams, []av.StreamID{"audio"}) ||
		!streamIDsEqual(previewMuxer.writtenStreams, []av.StreamID{"audio"}) ||
		archiveMuxer.writtenPayloads[0] != 1 ||
		previewMuxer.writtenPayloads[0] != 1 {
		t.Fatalf("archive writes=%d streams=%+v payload=%+v preview writes=%d streams=%+v payload=%+v",
			archiveMuxer.writes, archiveMuxer.writtenStreams, archiveMuxer.writtenPayloads,
			previewMuxer.writes, previewMuxer.writtenStreams, previewMuxer.writtenPayloads)
	}

	stats := graph.Stats()
	if stats.Packets != 1 || stats.Events != 2 || stats.Delivered != 6 {
		t.Fatalf("stats = %+v", stats)
	}

	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !archiveMuxer.closed || !previewMuxer.closed {
		t.Fatalf("closed demux=%v archive=%v preview=%v", demuxer.closed, archiveMuxer.closed, previewMuxer.closed)
	}
}
