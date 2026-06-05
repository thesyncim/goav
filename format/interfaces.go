package format

import (
	"context"
	"io"

	"github.com/thesyncim/goav/av"
)

type Input struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Reader   io.Reader
	ReaderAt io.ReaderAt
	Size     int64
	Realtime bool
	Metadata av.Metadata
}

type Output struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Writer   io.Writer
	Realtime bool
	Metadata av.Metadata
}

type ProbeRequest struct {
	Name     string
	MIMEType string
	Header   []byte
	Input    Input
}

type ProbeResult struct {
	Format   av.FormatID
	Score    int
	Streams  []av.Stream
	Reason   string
	Metadata av.Metadata
}

type Prober interface {
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
}

type OpenOptions struct {
	Realtime bool
	Metadata av.Metadata
}

type ReadResult struct {
	Packet *av.Packet
	Events []av.Event
}

func (r *ReadResult) Reset() {
	if r.Packet != nil {
		r.Packet.Reset()
	}
	for i := range r.Events {
		r.Events[i].Reset()
	}
	r.Events = r.Events[:0]
}

type WriteResult struct {
	Events []av.Event
}

func (r *WriteResult) Reset() {
	for i := range r.Events {
		r.Events[i].Reset()
	}
	r.Events = r.Events[:0]
}

type Demuxer interface {
	Format() av.FormatID
	Open(context.Context, Input, OpenOptions) error
	Streams() []av.Stream
	ReadInto(context.Context, *ReadResult) error
	Close() error
}

type Muxer interface {
	Format() av.FormatID
	Open(context.Context, Output, []av.Stream, OpenOptions) error
	Write(context.Context, *av.Packet, *WriteResult) error
	Close() error
}

type DemuxerFactory interface {
	NewDemuxer(context.Context, ProbeResult) (Demuxer, error)
}

type MuxerFactory interface {
	NewMuxer(context.Context, av.FormatID) (Muxer, error)
}

type Registry interface {
	Probers() []Prober
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
	DemuxerFactory(av.FormatID) (DemuxerFactory, error)
	MuxerFactory(av.FormatID) (MuxerFactory, error)
}
