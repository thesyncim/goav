// Package provider declares the contracts external transports and
// destinations satisfy to plug into goav with zero goav changes: Source is
// the seam goav.Input adapts into a recipe input, Destination (with Contract,
// Info, Writer, TransactionalWriter, and OpenFunc) is the seam goav.Custom
// and goav.Writer wrap behind destination handles. Implementations live
// outside goav — rtpav.Receive and webrtcav.Track are Sources; an S3
// uploader returning a TransactionalWriter is a Destination.
package provider

import (
	"context"
	"io"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// Source is the universal transport extension point: any package that can
// open a running pipeline source plugs into goav through this one seam. The
// implementation owns the transport vocabulary (readers, jitter buffers,
// depacketizers, adaptation) and hands the runtime a running source plus the
// streams it carries; SourceShape declares the media facts the planner needs
// before the source is opened (domain, media kind, codec, realtime).
// rtpav.Receive and webrtcav.Track are Sources; an SRT, NDI, or proprietary
// ingest package plugs in the same way.
//
// Optional capabilities are discovered by type assertion:
//
//   - Name() string — the source node name used in Describe output. Repeated
//     names across the inputs of one job are disambiguated with an index
//     suffix ("rtp", "rtp-1", ...).
//   - Detail() string — the human-readable node detail for Describe output.
//   - DecodeBounds(av.StreamID) codec.DecodeBounds — per-stream decode bounds
//     seeded into decoders fed by this input, resolved after OpenSource.
type Source interface {
	// OpenSource opens the running pipeline source and resolves the streams it
	// carries. It is called once per build, after which the source's Start
	// loop delivers the media.
	OpenSource(ctx context.Context) (pipeline.Source, []av.Stream, error)
	// SourceShape declares the shape of the media this provider produces.
	SourceShape() shape.Spec
}

// Destination opens a custom destination behind a goav-owned Destination
// handle (goav.Custom).
type Destination interface {
	// Name labels the destination for routing, errors, and Describe output.
	Name() string
	// Contract declares what the destination can accept before it opens.
	Contract() Contract
	// Open opens the destination writer once goav has resolved the output
	// format and streams.
	Open(context.Context, Info) (Writer, error)
}

// Writer is the byte writer returned by custom byte destinations.
type Writer interface {
	io.Writer
	Close() error
}

// TransactionalWriter may be returned by destinations that need an explicit
// upload commit or abort boundary.
type TransactionalWriter interface {
	Writer
	Commit(context.Context) error
	Abort(context.Context) error
}

// Contract declares what a destination can accept before it opens: whether
// it consumes muxed bytes (ByteStream) or media messages, and the formats,
// MIME types, protocol, and realtime/seek capabilities the planner may rely
// on when selecting the output format.
type Contract struct {
	ByteStream bool
	Seekable   bool
	Realtime   bool
	Protocol   av.ProtocolID
	Formats    []av.FormatID
	MIMETypes  []string
}

// Info describes the resolved output context passed to custom destination
// open functions.
type Info struct {
	Name     string
	Format   av.FormatID
	MIMEType string
	Streams  []av.Stream
	Metadata av.Metadata
	Realtime bool
}

// OpenFunc opens the byte writer behind a goav.Writer destination once goav
// has resolved the output format and streams (the Info). A returned writer
// that also implements TransactionalWriter gets Commit after success, Abort
// after failure, Close exactly once either way.
type OpenFunc func(context.Context, Info) (io.WriteCloser, error)
