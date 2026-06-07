package ivf

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func TestRegisterProvidesFactoriesAndProber(t *testing.T) {
	registry := format.NewRegistry()
	Register(registry)

	result, err := registry.Probe(context.Background(), format.ProbeRequest{
		Name: "video.ivf",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatIVF {
		t.Fatalf("format = %s, want %s", result.Format, av.FormatIVF)
	}
	if _, err := registry.DemuxerFactory(av.FormatIVF); err != nil {
		t.Fatalf("demuxer factory: %v", err)
	}
	if _, err := registry.MuxerFactory(av.FormatIVF); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
	desc, err := registry.MuxerDescriptor(av.FormatIVF)
	if err != nil {
		t.Fatalf("muxer descriptor: %v", err)
	}
	if desc.Format != av.FormatIVF ||
		desc.MaxStreams != 1 ||
		len(desc.Codecs) != 3 ||
		desc.Codecs[0] != av.CodecVP8 ||
		desc.Codecs[1] != av.CodecVP9 ||
		desc.Codecs[2] != av.CodecAV1 {
		t.Fatalf("descriptor = %+v, want IVF video codec capabilities", desc)
	}
}

func TestProberParsesHeaderStreams(t *testing.T) {
	stream := testStream(av.CodecAV1)
	var header [fileHeaderSize]byte
	writeStreamHeader(header[:], stream)

	result, err := (Prober{}).Probe(context.Background(), format.ProbeRequest{
		Header: header[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatIVF || result.Score != 100 {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(result.Streams))
	}
	got := result.Streams[0]
	if got.Codec.ID != av.CodecAV1 || got.Codec.Width != stream.Codec.Width || got.Codec.Height != stream.Codec.Height {
		t.Fatalf("stream = %+v, want codec/geometry from header", got)
	}
}

func TestMuxerDemuxerRoundTrip(t *testing.T) {
	ctx := context.Background()
	stream := testStream(av.CodecVP8)
	payload := []byte{1, 2, 3, 4}
	var buffer bytes.Buffer

	muxer, err := (MuxerFactory{}).NewMuxer(ctx, av.FormatIVF)
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	err = muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: payload},
		PTS:      av.Timestamp{Value: 123, Base: stream.TimeBase},
	}, &format.WriteResult{})
	if err != nil {
		t.Fatal(err)
	}
	if got := buffer.Bytes()[:4]; !bytes.Equal(got, []byte("DKIF")) {
		t.Fatalf("magic = %q, want DKIF", got)
	}

	demuxer, err := (DemuxerFactory{}).NewDemuxer(ctx, format.ProbeResult{Format: av.FormatIVF})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	streams := demuxer.Streams()
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	if streams[0].Codec.ID != av.CodecVP8 || streams[0].Codec.Width != 640 || streams[0].Codec.Height != 360 {
		t.Fatalf("stream = %+v", streams[0])
	}

	result := format.ReadResult{
		Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 16)}},
	}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady {
		t.Fatal("packet not ready")
	}
	if result.Packet.StreamID != "video" || !bytes.Equal(result.Packet.Payload.Bytes, payload) {
		t.Fatalf("packet = %+v", result.Packet)
	}
	if result.Packet.PTS.Value != 123 || result.Packet.PTS.Base != stream.TimeBase {
		t.Fatalf("pts = %+v, want value 123 base %+v", result.Packet.PTS, stream.TimeBase)
	}
	if err := demuxer.ReadInto(ctx, &result); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerSkipsUnrelatedStreams(t *testing.T) {
	ctx := context.Background()
	stream := testStream(av.CodecVP9)
	var buffer bytes.Buffer
	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != fileHeaderSize {
		t.Fatalf("bytes = %d, want only header", buffer.Len())
	}
}

func TestClosedAndUnopenedErrors(t *testing.T) {
	ctx := context.Background()
	readResult := format.ReadResult{
		Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1)}},
	}
	if err := (&Demuxer{}).ReadInto(ctx, &readResult); !errors.Is(err, ErrNilReader) {
		t.Fatalf("read err = %v, want ErrNilReader", err)
	}
	if err := (&Muxer{}).Write(ctx, &av.Packet{}, nil); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("write err = %v, want ErrNilWriter", err)
	}
}

func TestDemuxerReadIntoRequiresPayloadCapacity(t *testing.T) {
	ctx := context.Background()
	data := makeIVFData(t, testStream(av.CodecVP8), []ivfFrame{{timestamp: 1, payload: []byte{1, 2, 3}}})
	demuxer := &Demuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	result := format.ReadResult{
		Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 2)}},
	}
	if err := demuxer.ReadInto(ctx, &result); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
}

func TestMuxerRejectsUnsupportedStreamLayouts(t *testing.T) {
	ctx := context.Background()
	var buffer bytes.Buffer
	err := (&Muxer{}).Open(ctx, format.Output{Writer: &buffer}, []av.Stream{
		testStream(av.CodecVP8),
		{ID: "video-2", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo}},
	}, format.OpenOptions{})
	if !errors.Is(err, ErrUnsupportedStream) {
		t.Fatalf("err = %v, want ErrUnsupportedStream", err)
	}
}

func TestMuxerWriteAllocs(t *testing.T) {
	ctx := context.Background()
	stream := testStream(av.CodecVP8)
	writer := discardWriter{}
	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: writer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3, 4}},
		PTS:      av.Timestamp{Value: 1, Base: stream.TimeBase},
	}
	result := format.WriteResult{}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.Write(ctx, &packet, &result); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestDemuxerReadIntoAllocs(t *testing.T) {
	ctx := context.Background()
	frames := make([]ivfFrame, 1200)
	for i := range frames {
		frames[i] = ivfFrame{timestamp: uint64(i), payload: []byte{1, 2, 3, 4}}
	}
	data := makeIVFData(t, testStream(av.CodecVP8), frames)
	demuxer := &Demuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	result := format.ReadResult{
		Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 4)}},
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadInto(ctx, &result); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

type ivfFrame struct {
	timestamp uint64
	payload   []byte
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}

func testStream(codecID av.CodecID) av.Stream {
	return av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        codecID,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
}

func makeIVFData(t *testing.T, stream av.Stream, frames []ivfFrame) []byte {
	t.Helper()
	var buffer bytes.Buffer
	var header [fileHeaderSize]byte
	writeStreamHeader(header[:], stream)
	buffer.Write(header[:])
	for i := range frames {
		var frameHeader [frameHeaderSize]byte
		binary.LittleEndian.PutUint32(frameHeader[:4], uint32(len(frames[i].payload)))
		binary.LittleEndian.PutUint64(frameHeader[4:], frames[i].timestamp)
		buffer.Write(frameHeader[:])
		buffer.Write(frames[i].payload)
	}
	return buffer.Bytes()
}
