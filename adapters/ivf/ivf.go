package ivf

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

const (
	fileHeaderSize   = 32
	frameHeaderSize  = 12
	frameCountOffset = 24

	defaultTimeBaseNum = 1
	defaultTimeBaseDen = 1000
)

type Prober struct{}

type DemuxerFactory struct{}

type MuxerFactory struct{}

type Demuxer struct {
	reader      io.Reader
	stream      av.Stream
	streams     [1]av.Stream
	frameHeader [frameHeaderSize]byte
	opened      bool
	closed      bool
}

type Muxer struct {
	writer          io.Writer
	stream          av.Stream
	streamID        av.StreamID
	headerOffset    int64
	frameCount      uint32
	frameHeader     [frameHeaderSize]byte
	opened          bool
	headerWritten   bool
	patchFrameCount bool
	closed          bool
}

func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterProber(Prober{})
	registry.RegisterDemuxerDescriptor(Descriptor(), DemuxerFactory{})
	registry.RegisterMuxerDescriptor(Descriptor(), MuxerFactory{})
}

func Descriptor() format.Descriptor {
	return format.Descriptor{
		Format:     av.FormatIVF,
		Media:      []av.MediaType{av.MediaVideo},
		Codecs:     []av.CodecID{av.CodecVP8, av.CodecVP9, av.CodecAV1},
		MinStreams: 1,
		MaxStreams: 1,
		Metadata: av.Metadata{
			"summary": "IVF destinations support one VP8, VP9, or AV1 video stream",
		},
	}
}

func (Prober) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return format.ProbeResult{}, err
	}
	if len(request.Header) >= fileHeaderSize && string(request.Header[:4]) == "DKIF" {
		stream, err := parseStreamHeader(request.Header[:fileHeaderSize])
		if err != nil {
			return format.ProbeResult{}, format.ErrNotFound
		}
		return format.ProbeResult{
			Format:  av.FormatIVF,
			Score:   100,
			Streams: []av.Stream{stream},
			Reason:  "ivf header",
		}, nil
	}
	if hasIVFExtension(request.Name) || hasIVFExtension(request.Input.Name) || hasIVFExtension(request.Input.URI) {
		return format.ProbeResult{
			Format: av.FormatIVF,
			Score:  80,
			Reason: "ivf extension",
		}, nil
	}
	return format.ProbeResult{}, format.ErrNotFound
}

func (DemuxerFactory) NewDemuxer(ctx context.Context, result format.ProbeResult) (format.Demuxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Format != "" && result.Format != av.FormatIVF {
		return nil, format.ErrNotFound
	}
	return &Demuxer{}, nil
}

func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatIVF {
		return nil, format.ErrNotFound
	}
	return &Muxer{}, nil
}

func (d *Demuxer) Format() av.FormatID {
	return av.FormatIVF
}

func (d *Demuxer) Open(ctx context.Context, input format.Input, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input.Reader == nil {
		return ErrNilReader
	}
	var header [fileHeaderSize]byte
	if _, err := io.ReadFull(input.Reader, header[:]); err != nil {
		return err
	}
	stream, err := parseStreamHeader(header[:])
	if err != nil {
		return err
	}
	d.reader = input.Reader
	d.stream = stream
	d.streams[0] = stream
	d.opened = true
	d.closed = false
	return nil
}

func (d *Demuxer) Streams() []av.Stream {
	if !d.opened {
		return nil
	}
	return d.streams[:]
}

func (d *Demuxer) ReadInto(ctx context.Context, out *format.ReadResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil || out.Packet == nil {
		return format.ErrNilPacket
	}
	if d.closed {
		return io.EOF
	}
	if d.reader == nil {
		return ErrNilReader
	}
	out.PacketReady = false
	out.Packet.Reset()

	header := d.frameHeader[:]
	if _, err := io.ReadFull(d.reader, header); err != nil {
		return err
	}
	size := binary.LittleEndian.Uint32(header[:4])
	if uint64(size) > uint64(cap(out.Packet.Payload.Bytes)) {
		return ErrPayloadTooSmall
	}
	payloadSize := int(size)

	packet := out.Packet
	packet.StreamID = d.stream.ID
	packet.Type = d.stream.Type
	packet.Payload.Bytes = packet.Payload.Bytes[:payloadSize]
	packet.Payload.Ownership = av.BufferOwned
	packet.PTS = av.Timestamp{
		Value: int64(binary.LittleEndian.Uint64(header[4:])),
		Base:  d.stream.TimeBase,
	}
	if _, err := io.ReadFull(d.reader, packet.Payload.Bytes); err != nil {
		return err
	}
	out.PacketReady = true
	return nil
}

func (d *Demuxer) Close() error {
	d.closed = true
	d.reader = nil
	return nil
}

func (m *Muxer) Format() av.FormatID {
	return av.FormatIVF
}

func (m *Muxer) Open(ctx context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return ErrNilWriter
	}
	stream, err := selectVideoStream(streams)
	if err != nil {
		return err
	}
	m.writer = output.Writer
	m.stream = stream
	m.streamID = stream.ID
	m.headerOffset = 0
	m.frameCount = 0
	m.opened = true
	m.headerWritten = false
	m.patchFrameCount = false
	m.closed = false
	return nil
}

func (m *Muxer) Write(ctx context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if packet == nil {
		return format.ErrNilPacket
	}
	if m.closed {
		return io.ErrClosedPipe
	}
	if m.writer == nil {
		return ErrNilWriter
	}
	if m.streamID != "" && packet.StreamID != "" && packet.StreamID != m.streamID {
		return nil
	}
	if uint64(len(packet.Payload.Bytes)) > math.MaxUint32 {
		return ErrPayloadTooLarge
	}
	if m.frameCount == math.MaxUint32 {
		return ErrFrameCountTooLarge
	}
	if !m.headerWritten {
		m.stream.TimeBase = ivfTimeBase(m.stream, packet)
		if err := m.writeHeader(); err != nil {
			return err
		}
	}
	header := m.frameHeader[:]
	pts := packetTimestamp(packet.PTS, m.stream.TimeBase)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(packet.Payload.Bytes)))
	binary.LittleEndian.PutUint64(header[4:], uint64(pts))
	if err := writeFull(m.writer, header); err != nil {
		return err
	}
	if err := writeFull(m.writer, packet.Payload.Bytes); err != nil {
		return err
	}
	m.frameCount++
	return nil
}

func (m *Muxer) Close() error {
	if m.closed {
		return nil
	}
	if m.writer != nil && !m.headerWritten {
		m.stream.TimeBase = streamTimeBase(m.stream)
		if err := m.writeHeader(); err != nil {
			return err
		}
	}
	err := m.patchFrameCountHeader()
	m.closed = true
	m.writer = nil
	return err
}

func (m *Muxer) writeHeader() error {
	if m.headerWritten {
		return nil
	}
	if seeker, ok := m.writer.(io.Seeker); ok {
		if offset, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			m.headerOffset = offset
			m.patchFrameCount = true
		}
	}
	var header [fileHeaderSize]byte
	writeStreamHeader(header[:], m.stream)
	if err := writeFull(m.writer, header[:]); err != nil {
		return err
	}
	m.headerWritten = true
	return nil
}

func (m *Muxer) patchFrameCountHeader() error {
	if !m.patchFrameCount || !m.headerWritten || m.writer == nil {
		return nil
	}
	seeker, ok := m.writer.(io.Seeker)
	if !ok {
		return nil
	}
	current, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := seeker.Seek(m.headerOffset+frameCountOffset, io.SeekStart); err != nil {
		return err
	}
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], m.frameCount)
	if err := writeFull(m.writer, count[:]); err != nil {
		return err
	}
	if _, err := seeker.Seek(current, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func parseStreamHeader(header []byte) (av.Stream, error) {
	if len(header) < fileHeaderSize || string(header[:4]) != "DKIF" {
		return av.Stream{}, ErrInvalidHeader
	}
	if binary.LittleEndian.Uint16(header[4:6]) != 0 || binary.LittleEndian.Uint16(header[6:8]) != fileHeaderSize {
		return av.Stream{}, ErrInvalidHeader
	}
	codecID, err := codecFromFourCC(header[8:12])
	if err != nil {
		return av.Stream{}, err
	}
	timeBase := av.TimeBase{
		Num: int64(binary.LittleEndian.Uint32(header[20:24])),
		Den: int64(binary.LittleEndian.Uint32(header[16:20])),
	}
	if timeBase.Num <= 0 || timeBase.Den <= 0 {
		timeBase = av.TimeBase{Num: defaultTimeBaseNum, Den: defaultTimeBaseDen}
	}
	stream := av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: timeBase,
		Codec: av.CodecParameters{
			ID:        codecID,
			Type:      av.MediaVideo,
			ClockRate: clockRate(timeBase),
			Width:     int(binary.LittleEndian.Uint16(header[12:14])),
			Height:    int(binary.LittleEndian.Uint16(header[14:16])),
		},
	}
	return stream, nil
}

func writeStreamHeader(header []byte, stream av.Stream) {
	copy(header[:4], "DKIF")
	binary.LittleEndian.PutUint16(header[4:6], 0)
	binary.LittleEndian.PutUint16(header[6:8], fileHeaderSize)
	copy(header[8:12], fourCC(stream.Codec.ID))
	binary.LittleEndian.PutUint16(header[12:14], uint16(stream.Codec.Width))
	binary.LittleEndian.PutUint16(header[14:16], uint16(stream.Codec.Height))
	timeBase := streamTimeBase(stream)
	binary.LittleEndian.PutUint32(header[16:20], uint32(timeBase.Den))
	binary.LittleEndian.PutUint32(header[20:24], uint32(timeBase.Num))
}

func selectVideoStream(streams []av.Stream) (av.Stream, error) {
	var selected av.Stream
	for i := range streams {
		stream := streams[i]
		if stream.Type != av.MediaVideo {
			continue
		}
		if _, err := codecFourCC(stream.Codec.ID); err != nil {
			return av.Stream{}, err
		}
		if stream.Codec.Width < 0 || stream.Codec.Width > math.MaxUint16 ||
			stream.Codec.Height < 0 || stream.Codec.Height > math.MaxUint16 {
			return av.Stream{}, ErrUnsupportedStream
		}
		if selected.Type != "" {
			return av.Stream{}, ErrUnsupportedStream
		}
		selected = stream
	}
	if selected.Type == "" {
		return av.Stream{}, ErrUnsupportedStream
	}
	return selected, nil
}

func streamTimeBase(stream av.Stream) av.TimeBase {
	if stream.TimeBase.Num > 0 && stream.TimeBase.Den > 0 &&
		stream.TimeBase.Num <= math.MaxUint32 && stream.TimeBase.Den <= math.MaxUint32 {
		return stream.TimeBase
	}
	if stream.Codec.ClockRate > 0 {
		return av.TimeBase{Num: defaultTimeBaseNum, Den: int64(stream.Codec.ClockRate)}
	}
	return av.TimeBase{Num: defaultTimeBaseNum, Den: defaultTimeBaseDen}
}

func ivfTimeBase(stream av.Stream, packet *av.Packet) av.TimeBase {
	if packet != nil {
		if base, ok := frameDurationTimeBase(packet.Duration); ok {
			return base
		}
	}
	return streamTimeBase(stream)
}

func frameDurationTimeBase(duration av.Duration) (av.TimeBase, bool) {
	if duration.Value <= 0 || !duration.Base.Valid() {
		return av.TimeBase{}, false
	}
	value := uint64(duration.Value)
	num := uint64(duration.Base.Num)
	den := uint64(duration.Base.Den)
	if value > math.MaxUint64/num {
		return av.TimeBase{}, false
	}
	num *= value
	divisor := gcd64(num, den)
	num /= divisor
	den /= divisor
	if num == 0 || den == 0 || num > math.MaxUint32 || den > math.MaxUint32 {
		return av.TimeBase{}, false
	}
	return av.TimeBase{Num: int64(num), Den: int64(den)}, true
}

func packetTimestamp(timestamp av.Timestamp, base av.TimeBase) int64 {
	if timestamp.Base.Valid() && base.Valid() {
		if value, ok := av.RescaleValue(timestamp.Value, timestamp.Base, base); ok {
			return value
		}
	}
	return timestamp.Value
}

func gcd64(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a == 0 {
		return 1
	}
	return a
}

func clockRate(timeBase av.TimeBase) uint32 {
	if timeBase.Num == 1 && timeBase.Den > 0 && timeBase.Den <= math.MaxUint32 {
		return uint32(timeBase.Den)
	}
	return 0
}

func codecFromFourCC(value []byte) (av.CodecID, error) {
	switch string(value) {
	case "VP80":
		return av.CodecVP8, nil
	case "VP90":
		return av.CodecVP9, nil
	case "AV01":
		return av.CodecAV1, nil
	default:
		return av.CodecUnknown, ErrUnsupportedCodec
	}
}

func codecFourCC(id av.CodecID) (string, error) {
	switch id {
	case av.CodecVP8:
		return "VP80", nil
	case av.CodecVP9:
		return "VP90", nil
	case av.CodecAV1:
		return "AV01", nil
	default:
		return "", ErrUnsupportedCodec
	}
}

func fourCC(id av.CodecID) string {
	value, _ := codecFourCC(id)
	return value
}

func hasIVFExtension(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".ivf")
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) != 0 {
		n, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}
