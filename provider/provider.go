// Package provider declares the contracts external transports and
// destinations satisfy to plug into goav with zero goav changes: Source and
// ByteSource are the inputs goav.Input adapts into a recipe input,
// Destination (with Contract, Info, Writer, TransactionalWriter, and
// OpenFunc) is the seam goav.Custom and goav.Writer wrap behind destination
// handles, and InputScheme and DestinationScheme bind URLs to them, for
// goav.URIInput and goav.URI. Implementations live outside goav —
// rtpav.Receive and webrtcav.Track are Sources, webrtcav.WHEP is an
// InputScheme; an S3 uploader returning a TransactionalWriter is a
// Destination.
package provider

import (
	"context"
	"io"
	"net/url"
	"time"

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

// Input is a recipe input a provider supplies: a Source, whose media it
// produces itself, or a ByteSource, whose container bytes the runtime probes
// and demuxes with its registered formats. Either declares its shape before
// it opens; SourceShape().Realtime marks a live input, which planning,
// Explain, and goav.Reconnect read. goav.Input refuses an Input that is
// neither.
type Input interface {
	SourceShape() shape.Spec
}

// ByteSource is an input of container bytes: a file, an HTTP resource, an
// object in a bucket, or a live transport carrying a muxed stream (MPEG-TS
// over SRT, FLV over RTMP). The runtime opens it when the run starts, probes
// the container from the input's name and MIME type, or takes
// SourceShape().Format when set, and demuxes it with its registered formats.
//
// The source may also implement Name() string and Protocol() av.ProtocolID,
// the naming facts a URL would carry. The reader may also implement
// io.ReaderAt, io.Seeker, Size() int64, and Live() bool (a stream that turns
// out live when it opens); random-access containers (MP4 with its index at
// the end) need ReaderAt or Seeker. The runtime
// closes the reader when the input closes or the run's context ends, which
// must unblock a pending Read.
type ByteSource interface {
	SourceShape() shape.Spec
	OpenBytes(ctx context.Context) (io.ReadCloser, error)
}

// Scheme is the provider of one URL scheme (file, https, whep, ...) that
// runconfig.Scheme registers for goav.URIInput and goav.URI: an InputScheme,
// a DestinationScheme, or both, so a scheme implements only the directions
// it serves; goav refuses the other with errors.ErrUnsupported. Binding is
// pure — it validates the URL and performs no I/O, because Describe and
// Explain bind too; the bound value connects when the run opens it.
//
// The *url.URL carries the caller's credentials verbatim (userinfo, token
// query parameters): keep it inside the bound value, and name things by the
// redacted form goav reports (the input's node name, Info.Name).
type Scheme any

// InputScheme binds URLs to inputs, for goav.URIInput.
type InputScheme interface {
	Input(u *url.URL) (Input, error)
}

// DestinationScheme binds URLs to destinations, for goav.URI.
type DestinationScheme interface {
	Destination(u *url.URL) (Output, error)
}

// Output is what a DestinationScheme binds a URL to: a Destination, which
// takes the muxed bytes of a container (a file, an upload, FLV over RTMP),
// or a pipeline.Sink, which takes the media itself (RTP out, WHIP) under its
// own node name. goav refuses an Output that is neither.
//
// A media sink, here or behind goav.Sink, may also implement, discovered by
// type assertion:
//
//   - InputShapes() shape.Set — what it takes, in preference order: the
//     planner encodes frames to the first codec it can encode, and refuses
//     other packets with the fix.
//   - Live() bool — true for a sink that consumes in real time (RTP out,
//     WHIP): its recipe plans realtime, so a file routed to it plays at its
//     media time instead of in one burst.
//   - UseControl(func(context.Context, control.Control) error) — the task's
//     control path, bound before OpenStreams: a receiver's keyframe request
//     becomes control.Keyframe(stream) on the encoder feeding the stream.
//   - OpenStreams(context.Context, []av.Stream) error — the streams routed
//     to it, codec parameters and extradata included, before the first
//     message. After it succeeds goav closes the sink, even if the build
//     fails.
type Output interface {
	Name() string
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
// on when selecting the output format. Fragment, for a byte stream, is the
// longest fragment the destination wants from a fragmenting muxer: an LL-HLS
// packager asks for its part duration.
type Contract struct {
	ByteStream bool
	Seekable   bool
	Realtime   bool
	Fragment   time.Duration
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
