package mp4

import (
	"bytes"
	"context"
	"io"
	"math"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// DemuxerFactory opens MP4 demuxers behind the goav format extension point.
type DemuxerFactory struct{}

// Register installs the MP4 demuxer for av.FormatMP4. The default prober
// already detects ISO BMFF (ftyp magic and the .mp4/.mov family of
// extensions), so no prober is registered here.
func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDemuxer(av.FormatMP4, DemuxerFactory{})
}

// NewDemuxer returns an MP4 demuxer for a probed MP4 result.
func (DemuxerFactory) NewDemuxer(ctx context.Context, result format.ProbeResult) (format.Demuxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Format != "" && result.Format != av.FormatMP4 {
		return nil, format.ErrNotFound
	}
	return &FormatDemuxer{}, nil
}

// FormatDemuxer adapts the random-access MP4 Demuxer to the format.Demuxer
// interface.
type FormatDemuxer struct {
	demuxer *Demuxer
	streams []av.Stream
	closed  bool
}

func (d *FormatDemuxer) Format() av.FormatID { return av.FormatMP4 }

func (d *FormatDemuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reader, size, err := resolveReaderAt(input)
	if err != nil {
		return err
	}
	demuxer, err := NewDemuxer(reader, size)
	if err != nil {
		return err
	}
	d.demuxer = demuxer
	d.streams = d.streams[:0]
	for i, tr := range demuxer.Tracks() {
		d.streams = append(d.streams, streamFromTrack(tr, i))
	}
	d.closed = false
	return nil
}

func (d *FormatDemuxer) Streams() []av.Stream {
	if len(d.streams) == 0 {
		return nil
	}
	out := make([]av.Stream, len(d.streams))
	for i := range d.streams {
		out[i] = cloneStream(d.streams[i])
	}
	return out
}

func (d *FormatDemuxer) ReadInto(ctx context.Context, out *format.ReadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil || out.Packet == nil {
		return format.ErrNilPacket
	}
	if d.closed || d.demuxer == nil {
		return io.EOF
	}
	var sample Sample
	if err := d.demuxer.ReadInto(out.Packet.Payload.Bytes[:0], &sample); err != nil {
		return err
	}
	if sample.TrackIndex < 0 || sample.TrackIndex >= len(d.streams) {
		return ErrInvalidData
	}
	stream := d.streams[sample.TrackIndex]
	out.Packet.Reset()
	out.Packet.StreamID = stream.ID
	out.Packet.Type = stream.Type
	out.Packet.Payload.Bytes = sample.Data
	out.Packet.Payload.Ownership = av.BufferOwned
	base := av.TimeBase{Num: 1, Den: int64(sample.Timescale)}
	out.Packet.PTS = av.Timestamp{Value: sample.CTS, Base: base}
	out.Packet.DTS = av.Timestamp{Value: sample.DTS, Base: base}
	out.Packet.Keyframe = sample.Keyframe
	out.PacketReady = true
	return nil
}

func (d *FormatDemuxer) Close() error {
	if d == nil {
		return nil
	}
	d.closed = true
	d.demuxer = nil
	return nil
}

// resolveReaderAt produces a random-access view of the input: the declared
// ReaderAt, an io.Reader that also implements io.ReaderAt (an *os.File from
// goav.FileInput), or a fully buffered fallback for a plain stream.
func resolveReaderAt(input format.Input) (io.ReaderAt, int64, error) {
	if input.ReaderAt != nil {
		return input.ReaderAt, sizeOf(input.Size, input.ReaderAt), nil
	}
	if input.Reader == nil {
		return nil, 0, ErrNilReader
	}
	if reader, ok := input.Reader.(io.ReaderAt); ok {
		return reader, sizeOf(input.Size, input.Reader), nil
	}
	buf, err := io.ReadAll(input.Reader)
	if err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(buf), int64(len(buf)), nil
}

// sizeOf returns a known input size, falling back to a Seek to the end, then to
// a large bound so the box walk stops at the real EOF instead of a declared one.
func sizeOf(declared int64, v any) int64 {
	if declared > 0 {
		return declared
	}
	if seeker, ok := v.(io.Seeker); ok {
		if n, err := seeker.Seek(0, io.SeekEnd); err == nil && n > 0 {
			return n
		}
	}
	return math.MaxInt64
}

func streamFromTrack(tr Track, index int) av.Stream {
	stream := av.Stream{
		ID:       av.StreamID(strconv.FormatUint(uint64(tr.ID), 10)),
		Index:    index,
		Type:     tr.Media,
		Codec:    tr.Codec,
		TimeBase: av.TimeBase{Num: 1, Den: int64(tr.Timescale)},
	}
	stream.Codec.Type = tr.Media
	return stream
}

func cloneStream(stream av.Stream) av.Stream {
	if len(stream.Codec.ExtraData.Bytes) != 0 {
		stream.Codec.ExtraData.Bytes = append([]byte(nil), stream.Codec.ExtraData.Bytes...)
	}
	return stream
}
