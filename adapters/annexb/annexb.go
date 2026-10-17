package annexb

import (
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/h264"
)

type Prober struct{}

type MuxerFactory struct{}

type Muxer struct {
	writer   io.Writer
	streamID av.StreamID
	closed   bool
	// paramSets are the stream's SPS/PPS as Annex B, written ahead of every
	// keyframe that does not carry them in band, so the output decodes from
	// its first byte and from every keyframe. lengthSize is the avcC NAL
	// length width, used to convert length-prefixed (AVCC) payloads.
	paramSets  []byte
	lengthSize int
	scratch    []byte
}

func Register(registry *format.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterProber(Prober{})
	registry.RegisterMuxerDescriptor(Descriptor(), MuxerFactory{})
}

func Descriptor() format.Descriptor {
	return format.Descriptor{
		Format:     av.FormatAnnexB,
		Media:      []av.MediaType{av.MediaVideo},
		Codecs:     []av.CodecID{av.CodecH264},
		MinStreams: 1,
		MaxStreams: 1,
		Metadata: av.Metadata{
			"summary": "Annex B destinations support one H264 video stream",
		},
	}
}

func (Prober) Probe(ctx context.Context, request format.ProbeRequest) (format.ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return format.ProbeResult{}, err
	}
	if hasStartCode(request.Header) {
		return format.ProbeResult{
			Format: av.FormatAnnexB,
			Score:  100,
			Reason: "h264 annex b start code",
		}, nil
	}
	if hasAnnexBExtension(request.Name) || hasAnnexBExtension(request.Input.Name) || hasAnnexBExtension(request.Input.URI) {
		return format.ProbeResult{
			Format: av.FormatAnnexB,
			Score:  80,
			Reason: "h264 annex b extension",
		}, nil
	}
	return format.ProbeResult{}, format.ErrNotFound
}

func (MuxerFactory) NewMuxer(ctx context.Context, id av.FormatID) (format.Muxer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id != "" && id != av.FormatAnnexB {
		return nil, format.ErrNotFound
	}
	return &Muxer{}, nil
}

func (m *Muxer) Format() av.FormatID {
	return av.FormatAnnexB
}

func (m *Muxer) Open(ctx context.Context, output format.Output, streams []av.Stream, _ format.OpenOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output.Writer == nil {
		return ErrNilWriter
	}
	stream, err := selectH264Stream(streams)
	if err != nil {
		return err
	}
	m.writer = output.Writer
	m.streamID = stream.ID
	m.closed = false
	m.paramSets, m.lengthSize = nil, 0
	if extra := stream.Codec.ExtraData.Bytes; hasStartCode(extra) {
		m.paramSets = append(m.paramSets, extra...)
	} else if config, err := h264.ParseConfig(extra); err == nil {
		m.paramSets = config.AppendParameterSets(nil)
		m.lengthSize = config.LengthSize
	}
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
	data := packet.Payload.Bytes
	if m.lengthSize > 0 && !hasStartCode(data) {
		converted, err := h264.AppendAnnexB(m.scratch[:0], data, m.lengthSize)
		if err != nil {
			return err
		}
		m.scratch = converted
		data = converted
	}
	if packet.Keyframe && len(m.paramSets) != 0 && !h264.ContainsNAL(data, h264.NALSPS) {
		if err := writeFull(m.writer, m.paramSets); err != nil {
			return err
		}
	}
	return writeFull(m.writer, data)
}

func (m *Muxer) Close() error {
	m.closed = true
	m.writer = nil
	return nil
}

func selectH264Stream(streams []av.Stream) (av.Stream, error) {
	var selected av.Stream
	for i := range streams {
		stream := streams[i]
		if stream.Codec.ID != av.CodecH264 {
			continue
		}
		if stream.Type != "" && stream.Type != av.MediaVideo {
			return av.Stream{}, ErrUnsupportedStream
		}
		if stream.Codec.Type != "" && stream.Codec.Type != av.MediaVideo {
			return av.Stream{}, ErrUnsupportedStream
		}
		if selected.Codec.ID != "" {
			return av.Stream{}, ErrUnsupportedStream
		}
		selected = stream
	}
	if selected.Codec.ID == "" {
		return av.Stream{}, ErrUnsupportedStream
	}
	return selected, nil
}

func hasStartCode(header []byte) bool {
	return len(header) >= 3 &&
		header[0] == 0x00 &&
		header[1] == 0x00 &&
		(header[2] == 0x01 || len(header) >= 4 && header[2] == 0x00 && header[3] == 0x01)
}

func hasAnnexBExtension(name string) bool {
	ext := filepath.Ext(name)
	return strings.EqualFold(ext, ".h264") ||
		strings.EqualFold(ext, ".264") ||
		strings.EqualFold(ext, ".annexb")
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
