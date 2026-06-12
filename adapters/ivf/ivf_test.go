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
	Register(nil)
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

func TestProbeFactoriesAndHelpersRejectInvalidInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Prober{}).Probe(ctx, format.ProbeRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("probe canceled err = %v, want context.Canceled", err)
	}
	if _, err := (DemuxerFactory{}).NewDemuxer(ctx, format.ProbeResult{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("demuxer canceled err = %v, want context.Canceled", err)
	}
	if _, err := (MuxerFactory{}).NewMuxer(ctx, av.FormatIVF); !errors.Is(err, context.Canceled) {
		t.Fatalf("muxer canceled err = %v, want context.Canceled", err)
	}
	if _, err := (DemuxerFactory{}).NewDemuxer(context.Background(), format.ProbeResult{Format: av.FormatMatroska}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("demuxer wrong format err = %v, want ErrNotFound", err)
	}
	if _, err := (MuxerFactory{}).NewMuxer(context.Background(), av.FormatMatroska); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("muxer wrong format err = %v, want ErrNotFound", err)
	}

	header := make([]byte, fileHeaderSize)
	copy(header, "DKIF")
	binary.LittleEndian.PutUint16(header[6:8], fileHeaderSize)
	copy(header[8:12], "BAD!")
	if _, err := (Prober{}).Probe(context.Background(), format.ProbeRequest{Header: header}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("bad header probe err = %v, want ErrNotFound", err)
	}
	if _, err := (Prober{}).Probe(context.Background(), format.ProbeRequest{}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("empty probe err = %v, want ErrNotFound", err)
	}
	for _, request := range []format.ProbeRequest{
		{Input: format.Input{Name: "clip.IVF"}},
		{Input: format.Input{URI: "file:///tmp/clip.ivf"}},
	} {
		result, err := (Prober{}).Probe(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if result.Format != av.FormatIVF || result.Score != 80 {
			t.Fatalf("extension result = %+v", result)
		}
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
	if err := demuxer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := demuxer.Close(); err != nil {
		t.Fatalf("second demuxer close err = %v", err)
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
	demuxer := &Demuxer{}
	if demuxer.Format() != av.FormatIVF {
		t.Fatalf("demuxer format = %s", demuxer.Format())
	}
	if streams := demuxer.Streams(); streams != nil {
		t.Fatalf("unopened streams = %+v, want nil", streams)
	}
	if err := demuxer.Open(ctx, format.Input{}, format.OpenOptions{}); !errors.Is(err, ErrNilReader) {
		t.Fatalf("open nil reader err = %v, want ErrNilReader", err)
	}
	if err := demuxer.ReadInto(ctx, &readResult); !errors.Is(err, ErrNilReader) {
		t.Fatalf("read err = %v, want ErrNilReader", err)
	}
	if err := demuxer.ReadInto(ctx, nil); !errors.Is(err, format.ErrNilPacket) {
		t.Fatalf("nil read result err = %v, want ErrNilPacket", err)
	}
	if err := demuxer.ReadInto(ctx, &format.ReadResult{}); !errors.Is(err, format.ErrNilPacket) {
		t.Fatalf("nil packet err = %v, want ErrNilPacket", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := demuxer.ReadInto(canceled, &readResult); !errors.Is(err, context.Canceled) {
		t.Fatalf("read canceled err = %v, want context.Canceled", err)
	}

	muxer := &Muxer{}
	if muxer.Format() != av.FormatIVF {
		t.Fatalf("muxer format = %s", muxer.Format())
	}
	if err := muxer.Open(ctx, format.Output{}, []av.Stream{testStream(av.CodecVP8)}, format.OpenOptions{}); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("open nil writer err = %v, want ErrNilWriter", err)
	}
	if err := muxer.Write(ctx, &av.Packet{}, nil); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("write err = %v, want ErrNilWriter", err)
	}
	if err := muxer.Write(ctx, nil, nil); !errors.Is(err, format.ErrNilPacket) {
		t.Fatalf("nil packet write err = %v, want ErrNilPacket", err)
	}
	if err := muxer.Write(canceled, &av.Packet{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("write canceled err = %v, want context.Canceled", err)
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
	for _, tc := range []struct {
		name    string
		streams []av.Stream
		want    error
	}{
		{name: "no video", streams: []av.Stream{{ID: "audio", Type: av.MediaAudio}}, want: ErrUnsupportedStream},
		{name: "unsupported codec", streams: []av.Stream{testStream(av.CodecH264)}, want: ErrUnsupportedCodec},
		{name: "wide", streams: []av.Stream{func() av.Stream {
			stream := testStream(av.CodecVP8)
			stream.Codec.Width = 1 << 16
			return stream
		}()}, want: ErrUnsupportedStream},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (&Muxer{}).Open(ctx, format.Output{Writer: &bytes.Buffer{}}, tc.streams, format.OpenOptions{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDemuxerAndMuxerClosedBehavior(t *testing.T) {
	ctx := context.Background()
	data := makeIVFData(t, testStream(av.CodecVP8), []ivfFrame{{timestamp: 1, payload: []byte{1}}})
	demuxer := &Demuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := demuxer.Close(); err != nil {
		t.Fatal(err)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1)}}}
	if err := demuxer.ReadInto(ctx, &result); !errors.Is(err, io.EOF) {
		t.Fatalf("closed read err = %v, want EOF", err)
	}

	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &bytes.Buffer{}}, []av.Stream{testStream(av.CodecVP8)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatalf("second mux close err = %v", err)
	}
	if err := muxer.Write(ctx, &av.Packet{}, nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed write err = %v, want ErrClosedPipe", err)
	}
}

func TestHeaderHelpersCoverDefaultsAndErrors(t *testing.T) {
	if _, err := parseStreamHeader(nil); !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("short header err = %v, want ErrInvalidHeader", err)
	}
	stream := testStream(av.CodecVP9)
	var header [fileHeaderSize]byte
	writeStreamHeader(header[:], stream)
	binary.LittleEndian.PutUint16(header[4:6], 1)
	if _, err := parseStreamHeader(header[:]); !errors.Is(err, ErrInvalidHeader) {
		t.Fatalf("bad version err = %v, want ErrInvalidHeader", err)
	}

	writeStreamHeader(header[:], stream)
	binary.LittleEndian.PutUint32(header[16:20], 0)
	binary.LittleEndian.PutUint32(header[20:24], 0)
	parsed, err := parseStreamHeader(header[:])
	if err != nil {
		t.Fatal(err)
	}
	if parsed.TimeBase.Num != defaultTimeBaseNum || parsed.TimeBase.Den != defaultTimeBaseDen || parsed.Codec.ClockRate != defaultTimeBaseDen {
		t.Fatalf("parsed defaults = %+v", parsed)
	}

	if got := streamTimeBase(av.Stream{Codec: av.CodecParameters{ClockRate: 48_000}}); got.Num != 1 || got.Den != 48_000 {
		t.Fatalf("clock timebase = %+v", got)
	}
	if got := streamTimeBase(av.Stream{}); got.Num != defaultTimeBaseNum || got.Den != defaultTimeBaseDen {
		t.Fatalf("default timebase = %+v", got)
	}
	if got := clockRate(av.TimeBase{Num: 2, Den: 90_000}); got != 0 {
		t.Fatalf("non-rtp clock = %d, want 0", got)
	}
	if _, err := codecFromFourCC([]byte("NOPE")); !errors.Is(err, ErrUnsupportedCodec) {
		t.Fatalf("codecFromFourCC err = %v, want ErrUnsupportedCodec", err)
	}
	if got := fourCC(av.CodecH264); got != "" {
		t.Fatalf("unsupported fourcc = %q, want empty", got)
	}
}

func TestWriteFullHandlesPartialAndShortWrites(t *testing.T) {
	chunked := &chunkWriter{limit: 2}
	if err := writeFull(chunked, []byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunked.Bytes(), []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("chunked bytes = %v", chunked.Bytes())
	}
	if err := writeFull(zeroWriter{}, []byte{1}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero write err = %v, want ErrShortWrite", err)
	}
	want := errors.New("write failed")
	if err := writeFull(errorWriter{err: want}, []byte{1}); !errors.Is(err, want) {
		t.Fatalf("writer err = %v, want %v", err, want)
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

type chunkWriter struct {
	bytes.Buffer
	limit int
}

func (w *chunkWriter) Write(payload []byte) (int, error) {
	if len(payload) > w.limit {
		payload = payload[:w.limit]
	}
	return w.Buffer.Write(payload)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

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
