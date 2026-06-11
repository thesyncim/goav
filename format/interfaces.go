package format

import (
	"context"
	"io"

	"github.com/thesyncim/goav/av"
)

// Input describes where demuxable bytes come from: a reader (or random-access
// ReaderAt), the naming facts probing uses (Name, URI, MIMEType), and pacing.
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

// Output describes where muxed bytes go: the writer plus the naming facts
// output-format probing uses.
type Output struct {
	Name     string
	URI      string
	Protocol av.ProtocolID
	MIMEType string
	Writer   io.Writer
	Realtime bool
	Metadata av.Metadata
}

// ProbeRequest is what a prober sees: the name and MIME hints, the leading
// header bytes when available, and the full input.
type ProbeRequest struct {
	Name     string
	MIMEType string
	Header   []byte
	Input    Input
}

// ProbeResult is a prober's verdict: the detected format, a confidence score
// (higher wins across probers), and the streams discovered without opening a
// demuxer.
type ProbeResult struct {
	Format   av.FormatID
	Score    int
	Streams  []av.Stream
	Reason   string
	Metadata av.Metadata
}

// Descriptor declares a container format's capabilities — which media kinds,
// codecs, and stream counts it can carry — so destination validation refuses
// impossible mux plans before anything opens.
type Descriptor struct {
	Format     av.FormatID
	Media      []av.MediaType
	Codecs     []av.CodecID
	MinStreams int
	MaxStreams int
	Realtime   bool
	Metadata   av.Metadata
}

// Prober detects a format from a probe request; registries query every
// registered prober and keep the highest-scoring result.
type Prober interface {
	Probe(context.Context, ProbeRequest) (ProbeResult, error)
}

// OpenOptions carries open-time flags for demuxers and muxers, notably
// realtime pacing.
type OpenOptions struct {
	Realtime bool
	Metadata av.Metadata
}

// ReadResult is the caller-owned output buffer Demuxer.ReadInto fills: at
// most one packet plus bounded events per read, so demuxing stays
// allocation-free.
type ReadResult struct {
	// Packet is caller-owned scratch filled by Demuxer.ReadInto when
	// PacketReady is true. Borrowed packet payloads remain valid until the
	// demuxer's next ReadInto call unless the demuxer documents otherwise.
	Packet *av.Packet
	// PacketReady reports whether Packet contains a packet for this read.
	PacketReady bool
	// Events is caller-owned scratch filled without growing past capacity.
	Events []av.Event
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *ReadResult) Reset() {
	if r.Packet != nil {
		r.Packet.Reset()
	}
	r.PacketReady = false
	for i := range r.Events {
		r.Events[i].Reset()
	}
	r.Events = r.Events[:0]
}

// AddEvent appends an event without growing past capacity; ErrResultFull
// reports an exhausted buffer.
func (r *ReadResult) AddEvent(event av.Event) error {
	if len(r.Events) == cap(r.Events) {
		return ErrResultFull
	}
	index := len(r.Events)
	r.Events = r.Events[:index+1]
	r.Events[index] = event
	return nil
}

// WriteResult is the caller-owned output buffer Muxer.Write fills with any
// events the write produces.
type WriteResult struct {
	Events []av.Event
}

// Reset clears the result for reuse, keeping allocated capacity.
func (r *WriteResult) Reset() {
	for i := range r.Events {
		r.Events[i].Reset()
	}
	r.Events = r.Events[:0]
}

// AddEvent appends an event without growing past capacity; ErrResultFull
// reports an exhausted buffer.
func (r *WriteResult) AddEvent(event av.Event) error {
	if len(r.Events) == cap(r.Events) {
		return ErrResultFull
	}
	index := len(r.Events)
	r.Events = r.Events[:index+1]
	r.Events[index] = event
	return nil
}

// Demuxer reads one container: Open binds the input, Streams reports what it
// carries, and ReadInto fills caller-owned results until io.EOF. A demuxer
// that also implements Seeker honours Seek/Segment controls.
type Demuxer interface {
	Format() av.FormatID
	Open(context.Context, Input, OpenOptions) error
	Streams() []av.Stream
	ReadInto(context.Context, *ReadResult) error
	Close() error
}

// Muxer writes one container: Open binds the output and the final stream
// set, Write appends packets in delivery order, Close finalizes the file.
type Muxer interface {
	Format() av.FormatID
	Open(context.Context, Output, []av.Stream, OpenOptions) error
	Write(context.Context, *av.Packet, *WriteResult) error
	Close() error
}

// DemuxerFactory opens demuxers for a probed format; registries hold
// factories so nothing opens during planning.
type DemuxerFactory interface {
	NewDemuxer(context.Context, ProbeResult) (Demuxer, error)
}

// MuxerFactory opens muxers for a format id; registries hold factories so
// nothing opens during planning.
type MuxerFactory interface {
	NewMuxer(context.Context, av.FormatID) (Muxer, error)
}
