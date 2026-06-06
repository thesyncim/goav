package goav

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

type remuxTestProber struct {
	streams []av.Stream
}

func (p remuxTestProber) Probe(context.Context, format.ProbeRequest) (format.ProbeResult, error) {
	return format.ProbeResult{
		Format:  av.FormatOgg,
		Score:   100,
		Streams: p.streams,
	}, nil
}

type remuxTestDemuxerFactory struct {
	demuxer *remuxTestDemuxer
}

func (f remuxTestDemuxerFactory) NewDemuxer(context.Context, format.ProbeResult) (format.Demuxer, error) {
	return f.demuxer, nil
}

type remuxTestMuxerFactory struct {
	muxers []*remuxTestMuxer
}

func (f *remuxTestMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	muxer := &remuxTestMuxer{}
	f.muxers = append(f.muxers, muxer)
	return muxer, nil
}

type remuxTestDemuxer struct {
	streams []av.Stream
	opened  bool
	closed  bool
	read    int
}

func (d *remuxTestDemuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (d *remuxTestDemuxer) Open(context.Context, format.Input, format.OpenOptions) error {
	d.opened = true
	return nil
}

func (d *remuxTestDemuxer) Streams() []av.Stream {
	return d.streams
}

func (d *remuxTestDemuxer) ReadInto(_ context.Context, out *format.ReadResult) error {
	if d.read > 0 {
		return io.EOF
	}
	d.read++
	out.PacketReady = true
	out.Packet.StreamID = "audio"
	out.Packet.Payload.Bytes = []byte{1, 2, 3}
	return nil
}

func (d *remuxTestDemuxer) Close() error {
	d.closed = true
	return nil
}

type remuxTestMuxer struct {
	opened         bool
	closed         bool
	writes         int
	streamCount    int
	lastStream     av.StreamID
	openedStreams  []av.StreamID
	writtenStreams []av.StreamID
}

func (m *remuxTestMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *remuxTestMuxer) Open(_ context.Context, _ format.Output, streams []av.Stream, _ format.OpenOptions) error {
	m.opened = true
	m.streamCount = len(streams)
	m.openedStreams = m.openedStreams[:0]
	for i := range streams {
		m.openedStreams = append(m.openedStreams, streams[i].ID)
	}
	return nil
}

func (m *remuxTestMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	m.writes++
	m.lastStream = packet.StreamID
	m.writtenStreams = append(m.writtenStreams, packet.StreamID)
	return nil
}

func (m *remuxTestMuxer) Close() error {
	m.closed = true
	return nil
}

func TestRuntimeBuilderDescribeRemux(t *testing.T) {
	spec, err := New().New().
		Input(Input{Name: "input.ogg"}).
		Output(Output{Name: "archive.ogg"}).
		Output(Output{Name: "preview.ogg"}).
		Describe()
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 {
		t.Fatalf("nodes=%d edges=%d", len(spec.Nodes), len(spec.Edges))
	}
	if !strings.Contains(spec.String(), "input.ogg -> archive.ogg") ||
		!strings.Contains(spec.Mermaid(), "preview.ogg\\nstage") {
		t.Fatalf("spec:\n%s\nmermaid:\n%s", spec.String(), spec.Mermaid())
	}
}

func TestRuntimeBuilderInputOutputRemux(t *testing.T) {
	streams := []av.Stream{{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus}}}
	demuxer := &remuxTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	registry := format.NewRegistry(
		format.WithProber(remuxTestProber{streams: streams}),
		format.WithDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
		format.WithMuxer(av.FormatOgg, muxers),
	)

	builder := New(WithFormatRegistry(registry)).New().
		Input(Input{Name: "input.ogg"}).
		Output(Output{Name: "archive.ogg"}).
		Output(Output{Name: "preview.ogg"})
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := builder.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	if planned.String() != spec.String() || planned.Mermaid() != spec.Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), spec.String())
	}
	if len(spec.Nodes) != 3 || len(spec.Edges) != 2 {
		t.Fatalf("nodes=%d edges=%d", len(spec.Nodes), len(spec.Edges))
	}
	if !strings.Contains(spec.String(), "input.ogg -> archive.ogg") ||
		!strings.Contains(spec.Mermaid(), "preview.ogg\\nstage") {
		t.Fatalf("spec:\n%s\nmermaid:\n%s", spec.String(), spec.Mermaid())
	}

	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !demuxer.opened {
		t.Fatal("demuxer not opened")
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers = %d, want 2", len(muxers.muxers))
	}
	for i := range muxers.muxers {
		muxer := muxers.muxers[i]
		if !muxer.opened || muxer.writes != 1 || muxer.streamCount != 1 || muxer.lastStream != "audio" {
			t.Fatalf("muxer[%d] opened=%v writes=%d streams=%d last=%s", i, muxer.opened, muxer.writes, muxer.streamCount, muxer.lastStream)
		}
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed {
		t.Fatal("demuxer not closed")
	}
	for i := range muxers.muxers {
		if !muxers.muxers[i].closed {
			t.Fatalf("muxer[%d] not closed", i)
		}
	}
}
