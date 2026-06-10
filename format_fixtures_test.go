package goav

import (
	"context"
	"io"

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
	opened          bool
	closed          bool
	writes          int
	streamCount     int
	lastStream      av.StreamID
	openedStreams   []av.StreamID
	writtenStreams  []av.StreamID
	writtenPayloads []byte
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
	if len(packet.Payload.Bytes) != 0 {
		m.writtenPayloads = append(m.writtenPayloads, packet.Payload.Bytes[0])
	}
	return nil
}

func (m *remuxTestMuxer) Close() error {
	m.closed = true
	return nil
}
