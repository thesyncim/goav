package mp4

import (
	"bytes"
	"context"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/h264"
)

// DemuxerFactory opens MP4 demuxers behind the goav format extension point.
type DemuxerFactory struct{}

// Register installs the MP4 demuxer and muxer for av.FormatMP4. The default
// prober already detects ISO BMFF (ftyp magic and the .mp4/.mov family of
// extensions), so no prober is registered here.
func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDemuxerDescriptor(Descriptor(), DemuxerFactory{})
	registry.RegisterMuxerDescriptor(Descriptor(), MuxerFactory{})
}

func Descriptor() format.Descriptor {
	return format.Descriptor{
		Format: av.FormatMP4,
		Media:  []av.MediaType{av.MediaAudio, av.MediaVideo},
		Codecs: []av.CodecID{
			av.CodecH264,
			av.CodecAV1,
			av.CodecVP8,
			av.CodecVP9,
			av.CodecAAC,
			av.CodecOpus,
			av.CodecFLAC,
		},
		MinStreams: 1,
		Metadata: av.Metadata{
			"summary": "MP4 reads and writes common ISO BMFF audio and video sample entries, progressive or fragmented",
		},
	}
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
	// avccLength is the NAL length-prefix width per stream index for H.264
	// tracks (0 for every other codec): goav carries H.264 as Annex B, so
	// those samples are converted on read through scratch, like the Matroska
	// demuxer does.
	avccLength []int
	scratch    []byte
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
	d.avccLength = d.avccLength[:0]
	for i, tr := range demuxer.Tracks() {
		stream := streamFromTrack(tr, i)
		d.streams = append(d.streams, stream)
		length := 0
		if stream.Codec.ID == av.CodecH264 {
			if config, err := h264.ParseConfig(stream.Codec.ExtraData.Bytes); err == nil {
				length = config.LengthSize
			}
		}
		d.avccLength = append(d.avccLength, length)
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
	length := 0
	if next, ok := d.demuxer.nextTrack(); ok && next < len(d.avccLength) {
		length = d.avccLength[next]
	}
	if length == 0 {
		if err := d.demuxer.ReadInto(out.Packet.Payload.Bytes[:0], &sample); err != nil {
			return err
		}
	} else {
		if err := d.demuxer.ReadInto(d.scratch[:0], &sample); err != nil {
			return err
		}
		d.scratch = sample.Data
		converted, err := h264.AppendAnnexB(out.Packet.Payload.Bytes[:0], sample.Data, length)
		if err != nil {
			return err
		}
		sample.Data = converted
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

// Seek implements format.Seeker. The sample tables are in memory and every
// input is random-access (a ReaderAt or a buffered reader), so MP4 inputs are
// always seekable: the next ReadInto resumes at the latest video keyframe that
// presents at or before position (the latest sample, for audio-only files),
// with every track continuing from there in time order.
func (d *FormatDemuxer) Seek(ctx context.Context, position time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed || d.demuxer == nil {
		return ErrClosed
	}
	if position < 0 {
		return ErrInvalidData
	}
	d.demuxer.seek(position)
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
