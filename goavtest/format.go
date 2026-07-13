// The fake container: a deterministic muxer+demuxer pair over a trivial
// length-prefixed framing, so goav.File destinations and goav.FileInput
// sources round-trip without a real container implementation.

package goavtest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	runconfig "github.com/thesyncim/goav/runconfig"
)

// Format returns a runtime option registering a deterministic fake container
// for id: a muxer that writes a trivial length-prefixed framing (stream
// header, then one record per packet — stream id, timing, keyframe flag, and
// the payload verbatim), the matching demuxer that reads it back
// byte-faithfully, and a prober matching the "."+id name extension. A file
// muxed through it demuxes to the exact packets that were written, so
// File→FileInput round trips work without a real container.
func Format(id av.FormatID) runconfig.Option {
	return runconfig.WithFormatAdapter(func(registry *format.SimpleRegistry) {
		registry.RegisterMuxer(id, containerFactory{id: id})
		registry.RegisterDemuxer(id, containerFactory{id: id})
		registry.RegisterProber(format.NewStaticProber(format.ProbeRule{
			Format:     id,
			Extensions: []string{"." + string(id)},
			Score:      100,
			Reason:     "goavtest fake container extension",
		}))
	})
}

// containerMagic opens every fake container header.
const containerMagic = "gAVtFMT1"

type containerFactory struct {
	id av.FormatID
}

func (f containerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return &containerMuxer{id: f.id}, nil
}

func (f containerFactory) NewDemuxer(context.Context, format.ProbeResult) (format.Demuxer, error) {
	return &containerDemuxer{id: f.id}, nil
}

// containerMuxer writes the fake container: the header at Open, one record
// per packet, nothing at Close.
type containerMuxer struct {
	id     av.FormatID
	writer io.Writer
	buf    []byte
	record []byte
}

func (m *containerMuxer) Format() av.FormatID { return m.id }

// Open writes the container header: magic, format id, and every stream's
// identity and codec parameters, so the demuxer reproduces the stream set.
func (m *containerMuxer) Open(_ context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if output.Writer == nil {
		return errors.New("goavtest: fake container output has no writer")
	}
	m.writer = output.Writer
	body := appendWireString(nil, string(m.id))
	body = appendU32(body, uint32(len(streams)))
	for i := range streams {
		body = appendContainerStream(body, streams[i])
	}
	header := append([]byte(containerMagic), appendWireBytes(nil, body)...)
	_, err := m.writer.Write(header)
	return err
}

// Write appends one packet record: stream id, media kind, keyframe flag,
// PTS/DTS/duration with their timebases, and the payload bytes verbatim.
func (m *containerMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if packet == nil {
		return format.ErrNilPacket
	}
	body := appendWireString(m.buf[:0], string(packet.StreamID))
	body = appendWireString(body, string(packet.Type))
	flags := byte(0)
	if packet.Keyframe {
		flags |= 1
	}
	body = appendU8(body, flags)
	body = appendContainerTime(body, packet.PTS.Value, packet.PTS.Base)
	body = appendContainerTime(body, packet.DTS.Value, packet.DTS.Base)
	body = appendContainerTime(body, packet.Duration.Value, packet.Duration.Base)
	body = appendWireBytes(body, packet.Payload.Bytes)
	m.buf = body
	m.record = appendWireBytes(m.record[:0], body)
	_, err := m.writer.Write(m.record)
	return err
}

// Close is a no-op: the fake container needs no trailer.
func (m *containerMuxer) Close() error { return nil }

// containerDemuxer reads the fake container back: header at Open, one packet
// per ReadInto, io.EOF at the end.
type containerDemuxer struct {
	id      av.FormatID
	reader  *bufio.Reader
	streams []av.Stream
	buf     []byte
	prefix  [4]byte
}

func (d *containerDemuxer) Format() av.FormatID { return d.id }

// Open parses the container header and keeps the stream set; it reads from
// input.Reader (or input.ReaderAt from offset zero).
func (d *containerDemuxer) Open(_ context.Context, input format.Input, _ format.OpenOptions) error {
	reader := input.Reader
	if reader == nil && input.ReaderAt != nil {
		size := input.Size
		if size <= 0 {
			size = math.MaxInt64
		}
		reader = io.NewSectionReader(input.ReaderAt, 0, size)
	}
	if reader == nil {
		return errors.New("goavtest: fake container input has no reader")
	}
	d.reader = bufio.NewReader(reader)
	magic := make([]byte, len(containerMagic))
	if _, err := io.ReadFull(d.reader, magic); err != nil {
		return fmt.Errorf("goavtest: not a goavtest container: %w", err)
	}
	if string(magic) != containerMagic {
		return errors.New("goavtest: not a goavtest container (bad magic)")
	}
	body, err := d.readBlock()
	if err != nil {
		return fmt.Errorf("goavtest: truncated container header: %w", err)
	}
	r := newWireReader(body)
	r.str() // format id, informational
	count := int(r.u32())
	if !r.ok || count < 0 || count > 1024 {
		return errors.New("goavtest: corrupt container header")
	}
	d.streams = make([]av.Stream, 0, count)
	for i := 0; i < count; i++ {
		d.streams = append(d.streams, parseContainerStream(&r))
	}
	if !r.ok {
		return errors.New("goavtest: corrupt container stream table")
	}
	return nil
}

// Streams reports the stream set recorded in the container header.
func (d *containerDemuxer) Streams() []av.Stream {
	return d.streams
}

// ReadInto fills the caller-owned result with the next packet record,
// returning io.EOF at the end of the container.
func (d *containerDemuxer) ReadInto(_ context.Context, out *format.ReadResult) error {
	body, err := d.readBlock()
	if err != nil {
		return err
	}
	r := newWireReader(body)
	packet := out.Packet
	packet.Reset()
	packet.StreamID = d.streamIDFromWire(r.blob())
	packet.Type = av.MediaType(internWireString(r.blob()))
	packet.Keyframe = r.u8()&1 != 0
	packet.PTS.Value, packet.PTS.Base = parseContainerTime(&r)
	packet.DTS.Value, packet.DTS.Base = parseContainerTime(&r)
	duration, durationBase := parseContainerTime(&r)
	packet.Duration = av.Duration{Value: duration, Base: durationBase}
	packet.Payload.Bytes = append(packet.Payload.Bytes[:0], r.blob()...)
	if !r.ok {
		return errors.New("goavtest: corrupt container packet record")
	}
	out.PacketReady = true
	return nil
}

func (d *containerDemuxer) streamIDFromWire(id []byte) av.StreamID {
	for i := range d.streams {
		if bytesEqualString(id, string(d.streams[i].ID)) {
			return d.streams[i].ID
		}
	}
	return av.StreamID(string(id))
}

// Close is a no-op: the reader belongs to the input's owner.
func (d *containerDemuxer) Close() error { return nil }

// readBlock reads one u32-length-prefixed block; a clean end of input is
// io.EOF, a torn block is io.ErrUnexpectedEOF.
func (d *containerDemuxer) readBlock() ([]byte, error) {
	if _, err := io.ReadFull(d.reader, d.prefix[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	length := int(uint32(d.prefix[0]) | uint32(d.prefix[1])<<8 | uint32(d.prefix[2])<<16 | uint32(d.prefix[3])<<24)
	if cap(d.buf) < length {
		d.buf = make([]byte, length)
	}
	body := d.buf[:length]
	if _, err := io.ReadFull(d.reader, body); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	return body, nil
}

func appendContainerStream(b []byte, stream av.Stream) []byte {
	b = appendWireString(b, string(stream.ID))
	b = appendWireString(b, string(stream.Type))
	b = appendWireString(b, stream.Name)
	b = appendWireString(b, stream.Language)
	b = appendI64(b, int64(stream.Index))
	b = appendI64(b, stream.TimeBase.Num)
	b = appendI64(b, stream.TimeBase.Den)
	b = appendWireString(b, string(stream.Codec.ID))
	b = appendWireString(b, string(stream.Codec.Type))
	b = appendU32(b, stream.Codec.ClockRate)
	b = appendU32(b, uint32(stream.Codec.SampleRate))
	b = appendU32(b, uint32(stream.Codec.Channels))
	b = appendWireString(b, stream.Codec.ChannelLayout)
	b = appendWireString(b, stream.Codec.SampleFormat)
	b = appendU32(b, uint32(stream.Codec.Width))
	b = appendU32(b, uint32(stream.Codec.Height))
	b = appendWireString(b, stream.Codec.PixelFormat)
	b = appendWireBytes(b, stream.Codec.ExtraData.Bytes)
	return b
}

func parseContainerStream(r *wireReader) av.Stream {
	stream := av.Stream{
		ID:       av.StreamID(r.str()),
		Type:     av.MediaType(r.str()),
		Name:     r.str(),
		Language: r.str(),
		Index:    int(r.i64()),
		TimeBase: av.TimeBase{Num: r.i64(), Den: r.i64()},
	}
	stream.Codec = av.CodecParameters{
		ID:            av.CodecID(r.str()),
		Type:          av.MediaType(r.str()),
		ClockRate:     r.u32(),
		SampleRate:    int(r.u32()),
		Channels:      int(r.u32()),
		ChannelLayout: r.str(),
		SampleFormat:  r.str(),
		Width:         int(r.u32()),
		Height:        int(r.u32()),
		PixelFormat:   r.str(),
	}
	if extra := r.blob(); len(extra) != 0 {
		stream.Codec.ExtraData = av.Buffer{
			Bytes:     append([]byte(nil), extra...),
			Ownership: av.BufferImmutable,
		}
	}
	return stream
}

// appendContainerTime writes one timestamp or duration as value plus
// timebase.
func appendContainerTime(b []byte, value int64, base av.TimeBase) []byte {
	b = appendI64(b, value)
	b = appendI64(b, base.Num)
	b = appendI64(b, base.Den)
	return b
}

func parseContainerTime(r *wireReader) (int64, av.TimeBase) {
	value := r.i64()
	base := av.TimeBase{Num: r.i64(), Den: r.i64()}
	return value, base
}
